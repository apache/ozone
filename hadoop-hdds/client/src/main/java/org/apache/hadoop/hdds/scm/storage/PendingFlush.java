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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.protocol.DataStreamReply;

/**
 * Represents the state of data written since the last flush boundary.
 * Single-owner: created when data is written, consumed when flush completes.
 */
class PendingFlush {
  // buffers for which putBlock is yet to be executed
  private List<StreamBuffer> buffersForPutBlock;
  private List<CompletableFuture<DataStreamReply>> chunkFutures;

  // the effective length of data flushed so far
  private long totalDataFlushedLength;

  // effective data write attempted so far for the block
  private long writtenDataLength;

  /** Add a chunk that was written to the DataStream. */
  void addChunk(StreamBuffer buffer, CompletableFuture<DataStreamReply> future) {
    if (buffersForPutBlock == null) {
      buffersForPutBlock = new ArrayList<>();
    }
    buffersForPutBlock.add(buffer);
    chunkFutures.add(future);
  }

  /** Called at flush boundary: returns buffers and resets for next flush cycle. */
  FlushBatch seal() {
    totalDataFlushedLength = writtenDataLength;
    FlushBatch batch = new FlushBatch(
        buffersForPutBlock,
        chunkFutures,
        writtenDataLength);
    buffersForPutBlock = null;
    chunkFutures = new ArrayList<>();
    return batch;
  }

  boolean hasUnflushedData() {
    return totalDataFlushedLength < writtenDataLength;
  }

  PendingFlush() {
    this.chunkFutures = new ArrayList<>();
    this.writtenDataLength = 0;
    this.totalDataFlushedLength = 0;
  }

  long getWrittenLength() {
    return writtenDataLength;
  }

  long getTotalDataFlushedLength() {
    return totalDataFlushedLength;
  }

  void addToWrittenDataLength(long len) {
    writtenDataLength += len;
  }

  boolean hasBuffersForPutBlock() {
    return buffersForPutBlock != null && !buffersForPutBlock.isEmpty();
  }

  List<CompletableFuture<DataStreamReply>> getChunkFutures() {
    return chunkFutures;
  }

  /** The sealed batch, ready for putBlock. */
  static class FlushBatch {
    private final List<StreamBuffer> buffersForPutBlock;
    private final List<CompletableFuture<DataStreamReply>> chunkFutures;
    private final long totalDataFlushedLength;

    FlushBatch(
        List<StreamBuffer> buffersForPutBlock,
        List<CompletableFuture<DataStreamReply>> chunkFutures,
        long totalDataFlushedLength
    ) {
      this.buffersForPutBlock = buffersForPutBlock;
      this.chunkFutures = chunkFutures;
      this.totalDataFlushedLength = totalDataFlushedLength;
    }

    List<StreamBuffer> getBuffersForPutBlock() {
      return buffersForPutBlock;
    }

    List<CompletableFuture<DataStreamReply>> getChunkFutures() {
      return chunkFutures;
    }

    long getTotalDataFlushedLength() {
      return totalDataFlushedLength;
    }
  }
}
