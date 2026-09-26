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
import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;

/** Tracks chunk-relative checksum boundaries for a streaming block read. */
class BlockReadCursor {
  private static final int STREAMING_BYTES_PER_CHUNK = 1024 * 64;

  private final List<ChunkInfo> chunks;
  // [c1 offset, c2 offset, ..., cn offset, cn offset + cn len]
  private final long[] chunkOffsets;
  private final int responseDataSize;
  private final long start;
  private final long end;
  private long offset;
  private int chunkIndex;

  BlockReadCursor(long requestedOffset, long length, int responseSize, List<ChunkInfo> chunks) throws IOException {
    this.chunks = chunks;
    chunkOffsets = new long[chunks.size() + 1];
    int bufferSize = responseSize;
    for (int i = 0; i < chunks.size(); i++) {
      ChunkInfo chunk = chunks.get(i);
      long chunkOffset = chunkOffsets[i];
      if (chunk.getOffset() != chunkOffset || chunk.getLen() <= 0 || chunk.getLen() > Long.MAX_VALUE - chunkOffset) {
        throw new IOException("Invalid chunk range: " + chunk);
      }
      chunkOffsets[i + 1] = chunkOffset + chunk.getLen();
      // One checksum interval (or the short chunk containing it) must always fit in the buffer.
      bufferSize = Math.max(bufferSize, (int) Math.min(chunk.getLen(), interval(chunk)));
    }
    long blockEnd = chunkOffsets[chunks.size()];
    if (requestedOffset < 0 || requestedOffset >= blockEnd || length < 0 || responseSize <= 0) {
      throw new IOException("Invalid streaming read range or response size");
    }
    this.responseDataSize = ChunkUtils.limitReadSize(bufferSize);
    chunkIndex = findChunk(requestedOffset);
    start = length == 0 ? requestedOffset : alignDown(requestedOffset, chunkIndex);
    offset = start;
    if (length == 0) {
      end = start;
    } else {
      long requestedEnd = requestedOffset + Math.min(length, blockEnd - requestedOffset);
      int last = findChunk(requestedEnd - 1);
      long floor = alignDown(requestedEnd - 1, last);
      end = floor + Math.min(interval(chunks.get(last)), chunkOffsets[last + 1] - floor);
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

  private long alignDown(long position, int index) throws IOException {
    return position - (position - chunkOffsets[index]) % interval(chunks.get(index));
  }

  private int findChunk(long position) {
    int index = Arrays.binarySearch(chunkOffsets, chunkIndex, chunks.size(), position);
    if (index >= 0) {
      return index;
    }
    int insertionPoint = -index - 1;
    return insertionPoint - 1;
  }

  long offset() {
    return offset;
  }

  boolean hasRemaining() {
    return offset < end;
  }

  int responseDataSize() {
    return responseDataSize;
  }

  int nextReadLength() throws IOException {
    long limit = offset + Math.min(responseDataSize, end - offset);
    if (limit < end) {
      limit = alignDown(limit, findChunk(limit));
    }
    return Math.toIntExact(limit - offset);
  }

  List<ChunkInfo> chunksForRead(int length) {
    return chunks.subList(chunkIndex, findChunk(offset + length - 1) + 1);
  }

  void advance(int bytesRead) {
    offset += bytesRead;
    if (hasRemaining()) {
      chunkIndex = findChunk(offset);
    }
  }

  long bytesRead() {
    return offset - start;
  }
}
