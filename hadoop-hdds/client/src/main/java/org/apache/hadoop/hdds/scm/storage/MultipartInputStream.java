/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.ratis.util.Preconditions;

/**
 * A stream for accessing multipart streams.
 */
public class MultipartInputStream extends ExtendedInputStream {

  private final String key;
  private long length;

  // List of PartInputStream, one for each part of the key
  private final List<? extends PartInputStream> partStreams;
  private final boolean isStreamBlockInputStream;

  // partOffsets[i] stores the index of the first data byte in
  // partStream w.r.t the whole key data.
  // For example, let’s say the part size is 200 bytes and part[0] stores
  // data from indices 0 - 199, part[1] from indices 200 - 399 and so on.
  // Then, partOffsets[0] = 0 (the offset of the first byte of data in
  // part[0]), partOffsets[1] = 200 and so on.
  private final long[] partOffsets;

  private boolean closed;
  // Index of the partStream corresponding to the current position of the
  // MultipartCryptoKeyInputStream.
  private int partIndex;

  // Tracks the partIndex corresponding to the last seeked position so that it
  // can be reset if a new position is seeked.
  private int prevPartIndex;

  private boolean initialized = false;

  public MultipartInputStream(String keyName,
                              List<? extends PartInputStream> inputStreams) {
    Objects.requireNonNull(inputStreams, "inputStreams == null");

    this.key = keyName;
    this.partStreams = Collections.unmodifiableList(inputStreams);
    this.isStreamBlockInputStream = !inputStreams.isEmpty() && inputStreams.get(0) instanceof StreamBlockInputStream;

    // Calculate and update the partOffsets
    this.partOffsets = new long[inputStreams.size()];
    int i = 0;
    long streamLength = 0L;
    for (PartInputStream partInputStream : inputStreams) {
      this.partOffsets[i++] = streamLength;
      if (isStreamBlockInputStream) {
        Preconditions.assertInstanceOf(partInputStream, StreamBlockInputStream.class);
      }
      streamLength += partInputStream.getLength();
    }
    this.length = streamLength;
  }

  @Override
  protected synchronized int readWithStrategy(ByteReaderStrategy strategy)
      throws IOException {
    Objects.requireNonNull(strategy, "strategy == null");
    checkOpen();

    int totalReadLen = 0;
    while (strategy.getTargetLength() > 0) {
      if (partStreams.isEmpty() ||
          partStreams.size() - 1 <= partIndex &&
              partStreams.get(partIndex).getRemaining() == 0) {
        return totalReadLen == 0 ? EOF : totalReadLen;
      }

      // Get the current partStream and read data from it
      PartInputStream current = partStreams.get(partIndex);
      int numBytesToRead = getNumBytesToRead(strategy, current);
      int numBytesRead = strategy
          .readFromBlock((InputStream) current, numBytesToRead);
      checkPartBytesRead(numBytesToRead, numBytesRead, current);
      totalReadLen += numBytesRead;

      if (current.getRemaining() <= 0 &&
          partIndex + 1 < partStreams.size()) {
        partIndex += 1;
      }
    }
    return totalReadLen;
  }

  protected int getNumBytesToRead(ByteReaderStrategy strategy,
                                  PartInputStream current) throws IOException {
    return strategy.getTargetLength();
  }

  protected void checkPartBytesRead(int numBytesToRead, int numBytesRead,
                                    PartInputStream stream) throws IOException {
  }

  /**
   * Seeks the InputStream to the specified position. This involves 2 steps:
   * 1. Updating the partIndex to the partStream corresponding to the
   * seeked position.
   * 2. Seeking the corresponding partStream to the adjusted position.
   * <p>
   * For example, let’s say the part sizes are 200 bytes and part[0] stores
   * data from indices 0 - 199, part[1] from indices 200 - 399 and so on.
   * Let’s say we seek to position 240. In the first step, the partIndex
   * would be updated to 1 as indices 200 - 399 reside in partStream[1]. In
   * the second step, the partStream[1] would be seeked to position 40 (=
   * 240 - blockOffset[1] (= 200)).
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    checkOpen();
    if (!initialized) {
      initialize();
    }
    if (pos == 0 && length == 0) {
      // It is possible for length and pos to be zero in which case
      // seek should return instead of throwing exception
      return;
    }
    if (pos < 0 || pos > length) {
      throw new EOFException(
          "EOF encountered at pos: " + pos + " for key: " + key);
    }

    // 1. Update the partIndex
    if (partIndex >= partStreams.size()) {
      partIndex = Arrays.binarySearch(partOffsets, pos);
    } else if (pos < partOffsets[partIndex]) {
      partIndex =
          Arrays.binarySearch(partOffsets, 0, partIndex, pos);
    } else if (pos >= partOffsets[partIndex] + partStreams
        .get(partIndex).getLength()) {
      partIndex = Arrays.binarySearch(partOffsets, partIndex + 1,
          partStreams.size(), pos);
    }
    if (partIndex < 0) {
      // Binary search returns -insertionPoint - 1  if element is not present
      // in the array. insertionPoint is the point at which element would be
      // inserted in the sorted array. We need to adjust the blockIndex
      // accordingly so that partIndex = insertionPoint - 1
      partIndex = -partIndex - 2;
    }

    // Reset the previous partStream's position
    partStreams.get(prevPartIndex).seek(0);

    // Reset all the partStreams above the partIndex. We do this to reset
    // any previous reads which might have updated the higher part
    // streams position.
    for (int index = partIndex + 1; index < partStreams.size(); index++) {
      partStreams.get(index).seek(0);
    }
    // 2. Seek the partStream to the adjusted position
    partStreams.get(partIndex).seek(pos - partOffsets[partIndex]);
    prevPartIndex = partIndex;
  }

  @Override
  public boolean readFully(long position, ByteBuffer buffer) throws IOException {
    if (!isStreamBlockInputStream) {
      return false;
    }

    final long oldPos = getPos();
    seek(position);
    try {
      read(new ByteBufferReader(buffer) {
        @Override
        int readImpl(InputStream inputStream) throws IOException {
          return Preconditions.assertInstanceOf(inputStream, StreamBlockInputStream.class)
              .readFully(getBuffer(), false);
        }
      });
    } finally {
      seek(oldPos);
    }
    return true;
  }

  public synchronized void initialize() throws IOException {
    // Pre-check that the stream has not been intialized already
    if (initialized) {
      return;
    }

    for (PartInputStream partInputStream : partStreams) {
      if (partInputStream instanceof BlockInputStream) {
        ((BlockInputStream) partInputStream).initialize();
      }
    }

    long streamLength = 0L;
    for (PartInputStream partInputStream : partStreams) {
      streamLength += partInputStream.getLength();
    }
    this.length = streamLength;
    initialized = true;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return length == 0 ? 0 :
        partOffsets[partIndex] + partStreams.get(partIndex).getPos();
  }

  @Override
  public synchronized int available() throws IOException {
    checkOpen();
    long remaining = length - getPos();
    return remaining <= Integer.MAX_VALUE ? (int) remaining : Integer.MAX_VALUE;
  }

  @Override
  public synchronized void unbuffer() {
    for (PartInputStream stream : partStreams) {
      stream.unbuffer();
    }
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    checkOpen();
    if (!initialized) {
      initialize();
    }

    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, length - getPos());
    seek(getPos() + toSkip);
    return toSkip;
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
    for (PartInputStream stream : partStreams) {
      stream.close();
    }
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   *
   * @throws IOException if the connection is closed.
   */
  private void checkOpen() throws IOException {
    if (closed) {
      throw new IOException(
          ": " + FSExceptionMessages.STREAM_IS_CLOSED + " Key: " + key);
    }
  }

  public long getLength() {
    return length;
  }

  @VisibleForTesting
  public synchronized int getCurrentStreamIndex() {
    return partIndex;
  }

  @VisibleForTesting
  public long getRemainingOfIndex(int index) throws IOException {
    return partStreams.get(index).getRemaining();
  }

  @VisibleForTesting
  public List<? extends PartInputStream> getPartStreams() {
    return partStreams;
  }
}
