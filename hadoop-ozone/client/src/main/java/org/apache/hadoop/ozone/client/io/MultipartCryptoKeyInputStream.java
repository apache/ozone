/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.io;

import com.google.common.base.Preconditions;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * {@link OzoneInputStream} for accessing MPU keys in encrypted buckets.
 */
public class MultipartCryptoKeyInputStream extends OzoneInputStream
    implements Seekable, CanUnbuffer {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultipartCryptoKeyInputStream.class);

  private static final int EOF = -1;

  private String key;
  private long length = 0L;
  private boolean closed = false;

  // List of OzoneCryptoInputStream, one for each part of the key
  private List<OzoneCryptoInputStream> partStreams;

  // partOffsets[i] stores the index of the first data byte in
  // partStream w.r.t the whole key data.
  // For example, let’s say the part size is 200 bytes and part[0] stores
  // data from indices 0 - 199, part[1] from indices 200 - 399 and so on.
  // Then, partOffsets[0] = 0 (the offset of the first byte of data in
  // part[0]), partOffsets[1] = 200 and so on.
  private long[] partOffsets;

  // Index of the partStream corresponding to the current position of the
  // MultipartCryptoKeyInputStream.
  private int partIndex = 0;

  // Tracks the partIndex corresponding to the last seeked position so that it
  // can be reset if a new position is seeked.
  private int prevPartIndex = 0;

  public MultipartCryptoKeyInputStream(String keyName,
      List<OzoneCryptoInputStream> inputStreams) {

    Preconditions.checkNotNull(inputStreams);

    this.key = keyName;
    this.partStreams = inputStreams;

    // Calculate and update the partOffsets
    this.partOffsets = new long[inputStreams.size()];
    int i = 0;
    for (OzoneCryptoInputStream ozoneCryptoInputStream : inputStreams) {
      this.partOffsets[i++] = length;
      length += ozoneCryptoInputStream.getLength();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read() throws IOException {
    byte[] buf = new byte[1];
    if (read(buf, 0, 1) == EOF) {
      return EOF;
    }
    return Byte.toUnsignedInt(buf[0]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkOpen();
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return 0;
    }
    int totalReadLen = 0;
    while (len > 0) {
      if (partStreams.size() == 0 ||
          (partStreams.size() - 1 <= partIndex &&
              partStreams.get(partIndex).getRemaining() == 0)) {
        return totalReadLen == 0 ? EOF : totalReadLen;
      }

      // Get the current partStream and read data from it
      OzoneCryptoInputStream current = partStreams.get(partIndex);
      int numBytesRead = current.read(b, off, len);
      totalReadLen += numBytesRead;
      off += numBytesRead;
      len -= numBytesRead;

      if (current.getRemaining() <= 0 &&
          ((partIndex + 1) < partStreams.size())) {
        partIndex += 1;
      }

    }
    return totalReadLen;
  }

  /**
   * Seeks the InputStream to the specified position. This involves 2 steps:
   *    1. Updating the partIndex to the partStream corresponding to the
   *    seeked position.
   *    2. Seeking the corresponding partStream to the adjusted position.
   *
   * For example, let’s say the part sizes are 200 bytes and part[0] stores
   * data from indices 0 - 199, part[1] from indices 200 - 399 and so on.
   * Let’s say we seek to position 240. In the first step, the partIndex
   * would be updated to 1 as indices 200 - 399 reside in partStream[1]. In
   * the second step, the partStream[1] would be seeked to position 40 (=
   * 240 - blockOffset[1] (= 200)).
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos == 0 && length == 0) {
      // It is possible for length and pos to be zero in which case
      // seek should return instead of throwing exception
      return;
    }
    if (pos < 0 || pos > length) {
      throw new EOFException("EOF encountered at pos: " + pos);
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
  public synchronized long getPos() throws IOException {
    checkOpen();
    return length == 0 ? 0 : partOffsets[partIndex] +
        partStreams.get(partIndex).getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int available() throws IOException {
    checkOpen();
    long remaining = length - getPos();
    return remaining <= Integer.MAX_VALUE ? (int) remaining : Integer.MAX_VALUE;
  }

  @Override
  public void unbuffer() {
    for (CryptoInputStream cryptoInputStream : partStreams) {
      cryptoInputStream.unbuffer();
    }
  }

  @Override
  public long skip(long n) throws IOException {
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
    for (OzoneCryptoInputStream partStream : partStreams) {
      partStream.close();
    }
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkOpen() throws IOException {
    if (closed) {
      throw new IOException(
          ": " + FSExceptionMessages.STREAM_IS_CLOSED + " Key: " + key);
    }
  }
}
