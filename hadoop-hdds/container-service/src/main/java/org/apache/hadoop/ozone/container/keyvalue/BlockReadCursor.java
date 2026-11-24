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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;

/** Splits a streaming block read into ranges aligned to chunk-relative checksum boundaries. */
class BlockReadCursor implements Iterator<BlockReadCursor.ReadRange> {
  private static final int STREAMING_BYTES_PER_CHUNK = 1024 * 64;

  private final List<ChunkInfo> chunks;
  // [c1 offset, c2 offset, ..., cn offset, cn offset + cn len]
  private final long[] chunkOffsets;
  private final int[] intervals;
  private final int responseDataSize;
  private final long end;
  private long offset;
  private int chunkIndex;

  BlockReadCursor(long requestedOffset, long length, int responseSize, List<ChunkInfo> chunks) throws IOException {
    this.chunks = chunks;
    chunkOffsets = new long[chunks.size() + 1];
    intervals = new int[chunks.size()];
    int bufferSize = responseSize;
    for (int i = 0; i < chunks.size(); i++) {
      ChunkInfo chunk = chunks.get(i);
      chunkOffsets[i] = chunk.getOffset();
      intervals[i] = interval(chunk);
      // One checksum interval (or the short chunk containing it) must always fit in the buffer.
      bufferSize = Math.max(bufferSize, (int) Math.min(chunk.getLen(), intervals[i]));
    }
    ChunkInfo lastChunk = chunks.get(chunks.size() - 1);
    long blockEnd = lastChunk.getOffset() + lastChunk.getLen();
    chunkOffsets[chunks.size()] = blockEnd;
    this.responseDataSize = bufferSize;
    chunkIndex = findChunk(requestedOffset);
    offset = length == 0 ? requestedOffset : alignDown(requestedOffset, chunkIndex);
    if (length == 0) {
      end = offset;
    } else {
      long requestedEnd = requestedOffset + Math.min(length, blockEnd - requestedOffset);
      int last = findChunk(requestedEnd - 1);
      long floor = alignDown(requestedEnd - 1, last);
      end = floor + Math.min(intervals[last], chunkOffsets[last + 1] - floor);
    }
  }

  private static int interval(ChunkInfo chunk) throws IOException {
    // Retain the existing read alignment for chunks without checksums.
    if (chunk.getChecksumData().getType() == ChecksumType.NONE) {
      return STREAMING_BYTES_PER_CHUNK;
    }
    int size = chunk.getChecksumData().getBytesPerChecksum();
    if (size <= 0) {
      throw new IOException("Invalid bytes per checksum: " + size);
    }
    return size;
  }

  private long alignDown(long position, int index) {
    return position - (position - chunkOffsets[index]) % intervals[index];
  }

  private int findChunk(long position) {
    int index = Arrays.binarySearch(chunkOffsets, chunkIndex, chunks.size(), position);
    if (index >= 0) {
      return index;
    }
    int insertionPoint = -index - 1;
    return insertionPoint - 1;
  }

  @Override
  public boolean hasNext() {
    return offset < end;
  }

  int responseDataSize() {
    return responseDataSize;
  }

  /** Hands out the next range and advances past it, whether or not the caller reads it. */
  @Override
  public ReadRange next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No range left at offset " + offset + " of " + end);
    }
    long limit = offset + Math.min(responseDataSize, end - offset);
    if (limit < end) {
      limit = alignDown(limit, findChunk(limit));
    }
    ReadRange range = new ReadRange(offset, Math.toIntExact(limit - offset),
        chunks.subList(chunkIndex, findChunk(limit - 1) + 1));
    offset = limit;
    if (hasNext()) {
      chunkIndex = findChunk(offset);
    }
    return range;
  }

  /** A range of the block sent in one response, with the chunks it overlaps. */
  static final class ReadRange {
    private final long offset;
    private final int length;
    private final List<ChunkInfo> chunks;

    private ReadRange(long offset, int length, List<ChunkInfo> chunks) {
      this.offset = offset;
      this.length = length;
      this.chunks = chunks;
    }

    long offset() {
      return offset;
    }

    int length() {
      return length;
    }

    List<ChunkInfo> chunks() {
      return chunks;
    }
  }
}
