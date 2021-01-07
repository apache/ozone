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
import org.apache.hadoop.fs.Seekable;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MpuKeyInputStream extends OzoneInputStream implements Seekable,
    CanUnbuffer {

  private List<CryptoInputStream> partInputStreams;
  private List<LengthInputStream> lengthInputStreams;

  private static final int EOF = -1;


  private long[] partOffsets;

  private int partIndex = 0;

  private long length = 0L;

  private int partIndexOfPrevPos = 0;

  public MpuKeyInputStream(List<CryptoInputStream> inputStreams,
      List<LengthInputStream> lengthInputStreams) {

    Preconditions.checkNotNull(inputStreams, lengthInputStreams);
    Preconditions.checkArgument(inputStreams.size() == lengthInputStreams.size());
    this.partInputStreams = inputStreams;
    this.lengthInputStreams = lengthInputStreams;

    this.partOffsets = new long[inputStreams.size()];

    int i = 0;
    for (LengthInputStream lengthInputStream : lengthInputStreams) {
      this.partOffsets[i++] = length;
      length += lengthInputStream.getLength();
    }
  }


  @Override
  public int read() throws IOException {
    byte[] buf = new byte[1];
    if (read(buf, 0, 1) == EOF) {
      return EOF;
    }
    return Byte.toUnsignedInt(buf[0]);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
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
      if (partInputStreams.size() == 0 ||
          (partInputStreams.size() - 1 <= partIndex &&
              getRemainingInPartIndex(partIndex) == 0)) {
        return totalReadLen == 0 ? EOF : totalReadLen;
      }


      CryptoInputStream current = partInputStreams.get(partIndex);
      int numBytesToRead = Math.min(len,
          (int) getRemainingInPartIndex(partIndex));
      int numBytesRead = current.read(b, off, numBytesToRead);
      if (numBytesRead != numBytesToRead) {
        // Safety check to see underlying stream returned properly.
        throw new IOException(String.format("Bytes requested is not same as Bytes " +
            "read. Bytes Requested : %d, Bytes Read:%d", numBytesToRead,
            numBytesRead));
      }

      totalReadLen += numBytesRead;
      off += numBytesRead;
      len -= numBytesRead;
      if (getRemainingInPartIndex(partIndex) <= 0 &&
          ((partIndex + 1) < partInputStreams.size())) {
        partIndex += 1;
      }
    }
    return totalReadLen;
  }

  @Override
  public void unbuffer() {
    for (CryptoInputStream cryptoInputStream : partInputStreams) {
      cryptoInputStream.unbuffer();
    }
  }

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


    if (partIndex >= partInputStreams.size()) {
      partIndex = Arrays.binarySearch(partOffsets, pos);
    } else if (pos < partOffsets[partIndex]) {
      partIndex =
          Arrays.binarySearch(partOffsets, 0, partIndex, pos);
    } else if (pos >= partOffsets[partIndex] +
        getLengthOfPartIndex(partIndex)) {
      partIndex = Arrays
          .binarySearch(partOffsets, partIndex + 1,
              partInputStreams.size(), pos);
    }
    if (partIndex < 0) {
      // Binary search returns -insertionPoint - 1  if element is not present
      // in the array. insertionPoint is the point at which element would be
      // inserted in the sorted array. We need to adjust the blockIndex
      // accordingly so that partIndex = insertionPoint - 1
      partIndex = -partIndex - 2;
    }

    // Reset the previous partStream's position
    partInputStreams.get(partIndexOfPrevPos).seek(0);

    // Reset all the partStreams above the blockIndex. We do this to reset
    // any previous reads which might have updated the blockPosition and
    // chunkIndex.
    for (int index =  partIndex + 1; index < partInputStreams.size(); index++) {
      partInputStreams.get(index).seek(0);
    }
    // 2. Seek the partStream to the adjusted position
    partInputStreams.get(partIndex).seek(pos - partOffsets[partIndex]);
    partIndexOfPrevPos = partIndex;
  }

  @Override
  public int available() throws IOException {
    long remaining = length - getPos();
    return remaining <= Integer.MAX_VALUE ? (int) remaining :
        Integer.MAX_VALUE;
  }

  @Override
  public synchronized void close() throws IOException {
    for (CryptoInputStream partInputStream : partInputStreams) {
      partInputStream.close();
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
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return length == 0 ? 0 : partOffsets[partIndex] +
        partInputStreams.get(partIndex).getPos();
  }


  private long getRemainingInPartIndex(int partIndex) throws IOException {
    return lengthInputStreams.get(partIndex).getLength() -
        partInputStreams.get(partIndex).getPos();
  }

  private long getLengthOfPartIndex(int partIndex) {
    return lengthInputStreams.get(partIndex).getLength();
  }
}
