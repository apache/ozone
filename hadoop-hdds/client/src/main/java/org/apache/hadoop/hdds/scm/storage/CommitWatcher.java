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

import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.ozone.common.ChunkBuffer;

/**
 * This class maintains the map of the commitIndexes to be watched for
 * successful replication in the datanodes in a given pipeline. It also releases
 * the buffers associated with the user data back to {@Link BufferPool} once
 * minimum replication criteria is achieved during an ozone key write.
 * This class executes watchForCommit on ratis pipeline and releases
 * buffers once data successfully gets replicated.
 */
class CommitWatcher extends AbstractCommitWatcher<ChunkBuffer> {
  // A reference to the pool of buffers holding the data
  private final BufferPool bufferPool;

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
    // TODO move the flush future map to BOS:
    //  When there are concurrent watchForCommits, there's no guarantee of the order of execution
    //  and the following logic to address the flushed length become irrelevant.
    //  The flush future should be handled by BlockOutputStream and use the flushIndex which is a result of
    //  executePutBlock.

    addAckDataLength(acked);
  }

  @Override
  public void cleanup() {
    super.cleanup();
  }
}
