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
 * Represents chunks written since the most recent PutBlock.
 */
class PendingFlush {
  private static final CompletableFuture[] EMPTY_FUTURE_ARRAY = {};

  // buffers for which putBlock is yet to be executed
  private List<StreamBuffer> buffersForPutBlock;
  private final List<CompletableFuture<DataStreamReply>> chunkFutures = new ArrayList<>();

  /** Add a buffer for which PutBlock is yet to be executed. */
  void addBuffer(StreamBuffer buffer) {
    if (buffersForPutBlock == null) {
      buffersForPutBlock = new ArrayList<>();
    }
    buffersForPutBlock.add(buffer);
  }

  /** Add a future for a chunk write sent to the DataStream. */
  void addChunkFuture(CompletableFuture<DataStreamReply> future) {
    chunkFutures.add(future);
  }

  boolean hasBuffersForPutBlock() {
    return buffersForPutBlock != null && !buffersForPutBlock.isEmpty();
  }

  List<StreamBuffer> consumeBuffersForPutBlock() {
    List<StreamBuffer> buffers = buffersForPutBlock;
    buffersForPutBlock = null;
    return buffers;
  }

  void waitForChunkFutures() throws Exception {
    CompletableFuture.allOf(chunkFutures.toArray(EMPTY_FUTURE_ARRAY)).get();
    chunkFutures.clear();
  }
}
