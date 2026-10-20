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
      /*
       * bytesPerChecksum = 20, chunkRelativePosition = 53, toRead = 15, length = 75
       *
       * 40           53     60      68     75   80
       * +-------------------+--------------+----+
       * |            [ toRead = 15  )      |////|
       * +-------------------+--------------+----+
       * |<--------adjustedLen = 35-------->|
       * ^                                  ^    ^
       * adjustedOffset                     |    endIndex
       *                                    min(endIndex, length)
       */
      int bytesPerChecksum = chunkInfo.getChecksumData().getBytesPerChecksum();
      adjustedOffset = (chunkRelativePosition / bytesPerChecksum) * bytesPerChecksum;
      long endIndex = ((chunkRelativePosition + toRead - 1) / bytesPerChecksum + 1) * bytesPerChecksum;
      adjustedLen = Math.min(endIndex, length) - adjustedOffset;
    } else {
      adjustedOffset = chunkRelativePosition;
      adjustedLen = toRead;
    }

    if (adjustedOffset == 0 && adjustedLen == length) {
      return chunkInfo;
    }
    return ChunkInfo.newBuilder(chunkInfo)
        .setOffset(chunkInfo.getOffset() + adjustedOffset)
        .setLen(adjustedLen)
        .build();
  }

  private static int copyRange(ByteBuffer[] src, long skip, int toCopy, ByteBuffer dst) {
    long remainingSkip = skip;
    int copied = 0;
    for (ByteBuffer source : src) {
      if (copied >= toCopy) {
        break;
      }
      if (remainingSkip >= source.remaining()) {
        remainingSkip -= source.remaining();
        continue;
      }
      ByteBuffer buffer = source.duplicate();
      buffer.position(buffer.position() + (int) remainingSkip);
      remainingSkip = 0;
      int n = Math.min(buffer.remaining(), toCopy - copied);
      buffer.limit(buffer.position() + n);
      dst.put(buffer);
      copied += n;
    }
    return copied;
  }

}
