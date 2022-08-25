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
package org.apache.hadoop.hdds.scm.storage;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * An {@link OutputStream} used by the REST service in combination with the
 * SCMClient to write the value of a key to a sequence
 * of container chunks.  Writes are buffered locally and periodically written to
 * the container as a new chunk.  In order to preserve the semantics that
 * replacement of a pre-existing key is atomic, each instance of the stream has
 * an internal unique identifier.  This unique identifier and a monotonically
 * increasing chunk index form a composite key that is used as the chunk name.
 * After all data is written, a putKey call creates or updates the corresponding
 * container key, and this call includes the full list of chunks that make up
 * the key data.  The list of chunks is updated all at once.  Therefore, a
 * concurrent reader never can see an intermediate state in which different
 * chunks of data from different versions of the key data are interleaved.
 * This class encapsulates all state management for buffering and writing
 * through to the container.
 */
public class RatisBlockOutputStream extends BlockOutputStream {
  public static final Logger LOG = LoggerFactory.getLogger(
      RatisBlockOutputStream.class);

  // This object will maintain the commitIndexes and byteBufferList in order
  // Also, corresponding to the logIndex, the corresponding list of buffers will
  // be released from the buffer pool.
  private final CommitWatcher commitWatcher;

  /**
   * Creates a new BlockOutputStream.
   *
   * @param blockID              block ID
   * @param bufferPool           pool of buffers
   */
  public RatisBlockOutputStream(
      BlockID blockID,
      XceiverClientFactory xceiverClientManager,
      Pipeline pipeline,
      BufferPool bufferPool,
      OzoneClientConfig config,
      Token<? extends TokenIdentifier> token,
      ContainerClientMetrics clientMetrics
  ) throws IOException {
    super(blockID, xceiverClientManager, pipeline,
        bufferPool, config, token, clientMetrics);
    this.commitWatcher = new CommitWatcher(bufferPool, getXceiverClient());
  }

  @Override
  public long getTotalAckDataLength() {
    return commitWatcher.getTotalAckDataLength();
  }

  @VisibleForTesting
  public Map<Long, List<ChunkBuffer>> getCommitIndex2flushedDataMap() {
    return commitWatcher.getCommitIndex2flushedDataMap();
  }

  @Override
  void releaseBuffersOnException() {
    commitWatcher.releaseBuffersOnException();
  }

  @Override
  XceiverClientReply sendWatchForCommit(boolean bufferFull) throws IOException {
    return bufferFull ? commitWatcher.watchOnFirstIndex()
        : commitWatcher.watchOnLastIndex();
  }

  @Override
  void updateCommitInfo(XceiverClientReply reply, List<ChunkBuffer> buffers) {
    commitWatcher.updateCommitInfoMap(reply.getLogIndex(), buffers);
  }

  @Override
  void putFlushFuture(long flushPos,
      CompletableFuture<ContainerCommandResponseProto> flushFuture) {
    commitWatcher.getFutureMap().put(flushPos, flushFuture);
  }

  @Override
  void waitOnFlushFutures() throws InterruptedException, ExecutionException {
    // wait for all the transactions to complete
    CompletableFuture.allOf(commitWatcher.getFutureMap().values().toArray(
        new CompletableFuture[0])).get();
  }

  @Override
  void cleanup() {
    commitWatcher.cleanup();
  }
}
