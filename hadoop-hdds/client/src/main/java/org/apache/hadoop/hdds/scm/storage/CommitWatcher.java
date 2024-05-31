/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * This class maintains the map of the commitIndexes to be watched for
 * successful replication in the datanodes in a given pipeline. It also releases
 * the buffers associated with the user data back to {@Link BufferPool} once
 * minimum replication criteria is achieved during an ozone key write.
 */
package org.apache.hadoop.hdds.scm.storage;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.ozone.common.ChunkBuffer;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * This class executes watchForCommit on ratis pipeline and releases
 * buffers once data successfully gets replicated.
 */
class CommitWatcher extends AbstractCommitWatcher<ChunkBuffer> {
  // A reference to the pool of buffers holding the data
  private final BufferPool bufferPool;

  // future Map to hold up all putBlock futures
  private final ConcurrentMap<Long, CompletableFuture<ContainerCommandResponseProto>>
      futureMap = new ConcurrentHashMap<>();

  CommitWatcher(BufferPool bufferPool, XceiverClientSpi xceiverClient) {
    super(xceiverClient);
    this.bufferPool = bufferPool;
  }

  @Override
  void releaseBuffers(long index) {
    long acked = 0;
    for (ChunkBuffer buffer : remove(index)) {
      acked += buffer.position();
      bufferPool.releaseBuffer(buffer);
    }
    final long totalLength = addAckDataLength(acked);
    // When putBlock is called, a future is added.
    // When putBlock is replied, the future is removed below.
    // Therefore, the removed future should not be null.
    final CompletableFuture<ContainerCommandResponseProto> removed =
        futureMap.remove(totalLength);
    Objects.requireNonNull(removed, () -> "Future not found for "
        + totalLength + ": existing = " + futureMap.keySet());
  }

  @VisibleForTesting
  ConcurrentMap<Long, CompletableFuture<ContainerCommandResponseProto>> getFutureMap() {
    return futureMap;
  }

  public void putFlushFuture(long flushPos, CompletableFuture<ContainerCommandResponseProto> flushFuture) {
    futureMap.compute(flushPos,
        (key, previous) -> previous == null ? flushFuture :
            previous.thenCombine(flushFuture, (prev, curr) -> curr));
  }


  public void waitOnFlushFutures() throws InterruptedException, ExecutionException {
    // wait for all the transactions to complete
    CompletableFuture.allOf(futureMap.values().toArray(
        new CompletableFuture[0])).get();
  }

  @Override
  public void cleanup() {
    super.cleanup();
    futureMap.clear();
  }
}
