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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class executes watchForCommit on ratis pipeline and releases
 * buffers once data successfully gets replicated.
 */
public class CommitWatcher extends AbstractCommitWatcher<ChunkBuffer> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommitWatcher.class);

  // A reference to the pool of buffers holding the data
  private BufferPool bufferPool;

  // future Map to hold up all putBlock futures
  private ConcurrentHashMap<Long,
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto>>
      futureMap;

  // total data which has been successfully flushed and acknowledged
  // by all servers
  private long totalAckDataLength;

  public CommitWatcher(BufferPool bufferPool, XceiverClientSpi xceiverClient) {
    super(xceiverClient);
    this.bufferPool = bufferPool;
    futureMap = new ConcurrentHashMap<>();
  }

  @Override
  void releaseBuffers(long index) {
    for (ChunkBuffer buffer : remove(index)) {
      bufferPool.releaseBuffer(buffer);
      totalAckDataLength += buffer.position();
    }
    // When putBlock is called, a future is added.
    // When putBlock reply, the future is removed below.
    // Therefore, the removed future should not be null.
    final CompletableFuture<ContainerCommandResponseProto> removed =
        futureMap.remove(totalAckDataLength);
    Objects.requireNonNull(removed, () -> "Future not found for "
        + totalAckDataLength + ": existing = " + futureMap.keySet());
  }

  public ConcurrentMap<Long,
        CompletableFuture<ContainerProtos.
            ContainerCommandResponseProto>> getFutureMap() {
    return futureMap;
  }

  @Override
  public long getTotalAckDataLength() {
    return totalAckDataLength;
  }

  public void cleanup() {
    super.cleanup();
    if (futureMap != null) {
      futureMap.clear();
    }
  }
}
