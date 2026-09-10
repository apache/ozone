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

package org.apache.hadoop.ozone.container.keyvalue;

import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.ratis.util.Preconditions;

/**
 * Utility class to compute checksum-aligned offsets, lengths, and buffer
 * limits for streaming readBlock operations.
 */
class ReadBlockComputation {

  private final int responseDataSize;
  private final int bitMask;
  private final List<ChunkInfo> chunks;
  private int chunkIndex;
  private long lastPosition;

  ReadBlockComputation(int responseDataSize, int bytesPerChecksum,
      List<ChunkInfo> chunks, int firstChunkIndex) {
    this.responseDataSize = responseDataSize;

    Preconditions.assertSame(1, Long.bitCount(bytesPerChecksum), "bitCount"); // check power of 2.
    // Suppose bytesPerChecksum = 00001000 (a power of 2), then bitMask = 11111000.
    // The following two computations are the same:
    // We will use (n & bitMask) to compute ((n / bytesPerChecksum) * bytesPerChecksum).
    this.bitMask = -bytesPerChecksum;

    this.chunks = chunks;
    this.chunkIndex = firstChunkIndex;
    this.lastPosition = 0;
  }

  /**
   * Binary-search for the index of the chunk whose start offset is the
   * largest value &le; {@code targetOffset}.
   */
  static int searchChunk(long targetOffset, List<ChunkInfo> chunkInfoList) {
    int low = 0;
    int high = chunkInfoList.size() - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = chunkInfoList.get(mid).getOffset();
      if (midVal <= targetOffset) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return high;
  }

  /**
   * Return the read offset aligned back to the previous checksum boundary
   * within the chunk that contains {@code blockOffset}.
   */
  static long computeAdjustedOffset(int startChunkIndex, long blockOffset,
      long bytesPerChecksum, List<ChunkInfo> chunkInfos) {
    long offsetAlignment = (blockOffset - chunkInfos.get(startChunkIndex).getOffset()) % bytesPerChecksum;
    return blockOffset - offsetAlignment;
  }

  /**
   * Return the length of data that must be read so that the range
   * {@code [blockOffset, blockOffset + blockLength)} is fully covered
   * with checksum-aligned boundaries.
   */
  static long computeAdjustedLength(long blockOffset, long blockLength, long adjustedOffset,
      long bytesPerChecksum, List<ChunkInfo> chunkInfos) {
    long blockEnd = blockOffset + blockLength - 1; // inclusive
    ChunkInfo lastChunk = chunkInfos.get(searchChunk(blockEnd, chunkInfos));
    long chunkOffset = lastChunk.getOffset();
    long chunkLength = Math.min(
        (getEndChecksumIndex(blockEnd, chunkOffset, bytesPerChecksum) + 1) * bytesPerChecksum,
        lastChunk.getLen());
    return chunkOffset + chunkLength - adjustedOffset;
  }

  private static int getEndChecksumIndex(long blockEnd, long chunkOffset, long bytesPerChecksum) {
    return (int) ((blockEnd - chunkOffset) / bytesPerChecksum);
  }

  /**
   * Compute the buffer limit for the next read iteration, aligned to
   * checksum boundaries so that partial checksums are not sent across
   * response messages.
   */
  public int computeBufferLimit(long offset, long remainingLength) {
    if (responseDataSize >= remainingLength) {
      return Math.toIntExact(remainingLength);
    }
    ChunkInfo endChunk = chunks.get(findChunk(offset + responseDataSize)); // exclusive
    final int lengthExcludingEndChunk = Math.toIntExact(endChunk.getOffset() - offset);
    // bytesPerChecksum must be a power of 2.
    return ((responseDataSize - lengthExcludingEndChunk) & bitMask) + lengthExcludingEndChunk;
  }

  /**
   * @param position must be increasing in subsequent calls to this method.
   * @return the chunk containing the given position.
   */
  int findChunk(long position) {
    Preconditions.assertTrue(position >= lastPosition);
    lastPosition = position;

    for (; chunkIndex < chunks.size(); chunkIndex++) {
      final ChunkInfo chunk = chunks.get(chunkIndex);
      if (position >= chunk.getOffset() && position < chunk.getOffset() + chunk.getLen()) {
        return chunkIndex;
      }
    }
    return chunkIndex;
  }
}
