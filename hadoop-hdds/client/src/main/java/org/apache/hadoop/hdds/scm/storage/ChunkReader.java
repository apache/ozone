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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * Reads checksum-aligned ranges without retaining a cursor or data buffers.
 */
final class ChunkReader {
  private final ChunkInfo chunkInfo;
  private final long length;
  private final boolean verifyChecksum;
  private final CheckedFunction<ChunkInfo, ByteBuffer[], IOException> readChunk;

  ChunkReader(ChunkInfo chunkInfo, boolean verifyChecksum,
      CheckedFunction<ChunkInfo, ByteBuffer[], IOException> readChunk) {
    this.chunkInfo = chunkInfo;
    this.length = chunkInfo.getLen();
    this.verifyChecksum = verifyChecksum;
    this.readChunk = readChunk;
  }

  int read(long position, ByteBuffer destination) throws IOException {
    if (!destination.hasRemaining()) {
      return 0;
    }
    if (position < 0 || position >= length) {
      return -1;
    }
    final int toRead = (int) Math.min(destination.remaining(), length - position);
    final ChunkInfo range = getChunkInfo(position, toRead);
    final long skip = position - (range.getOffset() - chunkInfo.getOffset());
    return copyRange(readChunk.apply(range), skip, toRead, destination);
  }

  ChunkInfo getChunkInfo(long chunkRelativePosition, int toRead) {
    final long adjustedOffset;
    final long adjustedLen;
    if (verifyChecksum) {
      ChecksumBoundaries boundaries = computeChecksumBoundaries(chunkRelativePosition, toRead);
      adjustedOffset = boundaries.offset;
      adjustedLen = boundaries.length;
    } else {
      adjustedOffset = chunkRelativePosition;
      adjustedLen = toRead;
    }

    return ChunkInfo.newBuilder(chunkInfo)
        .setOffset(chunkInfo.getOffset() + adjustedOffset)
        .setLen(adjustedLen)
        .build();
  }

  private ChecksumBoundaries computeChecksumBoundaries(long startByteIndex, int dataLen) {

    int bytesPerChecksum = chunkInfo.getChecksumData().getBytesPerChecksum();
    // index of the last byte to be read from chunk, inclusively.
    final long endByteIndex = startByteIndex + dataLen - 1;

    long adjustedChunkOffset =  (startByteIndex / bytesPerChecksum)
        * bytesPerChecksum; // inclusive
    final long endIndex = ((endByteIndex / bytesPerChecksum) + 1)
        * bytesPerChecksum; // exclusive
    long adjustedChunkLen = Math.min(endIndex, length) - adjustedChunkOffset;
    return new ChecksumBoundaries(adjustedChunkOffset, adjustedChunkLen);
  }

  /**
   * Represents a byte range (offset and length) expanded to align with
   * checksum chunk boundaries required for verification.
   */
  private static final class ChecksumBoundaries {
    private final long offset;
    private final long length;

    private ChecksumBoundaries(long offset, long length) {
      this.offset = offset;
      this.length = length;
    }
  }

  private static int copyRange(ByteBuffer[] src, long skip, int toCopy, ByteBuffer dst) {
    long remainingSkip = skip;
    int copied = 0;
    for (ByteBuffer source : src) {
      ByteBuffer buffer = source.duplicate();
      if (copied >= toCopy) {
        break;
      }
      if (remainingSkip > 0) {
        if (remainingSkip >= buffer.remaining()) {
          remainingSkip -= buffer.remaining();
          continue;
        } else {
          buffer.position(Math.toIntExact(buffer.position() + remainingSkip));
          remainingSkip = 0;
        }
      }
      int n = Math.min(buffer.remaining(), toCopy - copied);
      if (n <= 0) {
        continue;
      }
      buffer.limit(buffer.position() + n);
      dst.put(buffer);
      copied += n;
    }
    return copied;
  }

}
