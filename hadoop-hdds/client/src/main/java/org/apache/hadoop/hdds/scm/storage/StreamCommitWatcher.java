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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * This class executes watchForCommit on ratis pipeline and releases
 * buffers once data successfully gets replicated.
 */
public class StreamCommitWatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(StreamCommitWatcher.class);

  private Map<Long, List<ByteBuffer>> commitIndexSet;

  private List<ByteBuffer> bufferPool;

  // total data which has been successfully flushed and acknowledged
  // by all servers
  private long totalAckDataLength;

  // future Map to hold up all putBlock futures
  private ConcurrentHashMap<Long,
      CompletableFuture<ContainerCommandResponseProto>>
      futureMap;

  private XceiverClientSpi xceiverClient;

  public StreamCommitWatcher(XceiverClientSpi xceiverClient,
      List<ByteBuffer> bufferPool) {
    this.xceiverClient = xceiverClient;
    commitIndexSet = new ConcurrentSkipListMap<>();
    futureMap = new ConcurrentHashMap<>();
    this.bufferPool = bufferPool;
    totalAckDataLength = 0;
  }

  public void updateCommitInfoSet(long index, List<ByteBuffer> buffers) {
    commitIndexSet.computeIfAbsent(index, k -> new LinkedList<>())
        .addAll(buffers);
  }

  int getCommitInfoSetSize() {
    return commitIndexSet.size();
  }

  /**
   * Calls watch for commit for the first index in commitIndex2flushedDataMap to
   * the Ratis client.
   * @return {@link XceiverClientReply} reply from raft client
   * @throws IOException in case watchForCommit fails
   */
  public XceiverClientReply streamWatchOnFirstIndex() throws IOException {
    if (!commitIndexSet.isEmpty()) {
      // wait for the  first commit index in the commitIndex2flushedDataMap
      // to get committed to all or majority of nodes in case timeout
      // happens.
      long index =
          commitIndexSet.keySet().stream().mapToLong(v -> v).min()
              .getAsLong();
      if (LOG.isDebugEnabled()) {
        LOG.debug("waiting for first index {} to catch up", index);
      }
      return streamWatchForCommit(index);
    } else {
      return null;
    }
  }

  /**
   * Calls watch for commit for the last index in commitIndex2flushedDataMap to
   * the Ratis client.
   * @return {@link XceiverClientReply} reply from raft client
   * @throws IOException in case watchForCommit fails
   */
  public XceiverClientReply streamWatchOnLastIndex()
      throws IOException {
    if (!commitIndexSet.isEmpty()) {
      // wait for the  commit index in the commitIndex2flushedDataMap
      // to get committed to all or majority of nodes in case timeout
      // happens.
      long index =
          commitIndexSet.keySet().stream().mapToLong(v -> v).max()
              .getAsLong();
      if (LOG.isDebugEnabled()) {
        LOG.debug("waiting for last flush Index {} to catch up", index);
      }
      return streamWatchForCommit(index);
    } else {
      return null;
    }
  }

  /**
   * calls watchForCommit API of the Ratis Client. This method is for streaming
   * and no longer requires releaseBuffers
   * @param commitIndex log index to watch for
   * @return minimum commit index replicated to all nodes
   * @throws IOException IOException in case watch gets timed out
   */
  public XceiverClientReply streamWatchForCommit(long commitIndex)
      throws IOException {
    long index;
    try {
      XceiverClientReply reply =
          xceiverClient.watchForCommit(commitIndex);
      if (reply == null) {
        index = 0;
      } else {
        index = reply.getLogIndex();
      }
      adjustBuffers(index);
      return reply;
    } catch (InterruptedException e) {
      // Re-interrupt the thread while catching InterruptedException
      Thread.currentThread().interrupt();
      throw getIOExceptionForWatchForCommit(commitIndex, e);
    } catch (TimeoutException | ExecutionException e) {
      throw getIOExceptionForWatchForCommit(commitIndex, e);
    }
  }

  void releaseBuffersOnException() {
    adjustBuffers(xceiverClient.getReplicatedMinCommitIndex());
  }

  private void adjustBuffers(long commitIndex) {
    List<Long> keyList = commitIndexSet.keySet().stream()
        .filter(p -> p <= commitIndex).collect(Collectors.toList());
    if (!keyList.isEmpty()) {
      releaseBuffers(keyList);
    }
  }

  private long releaseBuffers(List<Long> indexes) {
    Preconditions.checkArgument(!commitIndexSet.isEmpty());
    for (long index : indexes) {
      Preconditions.checkState(commitIndexSet.containsKey(index));
      final List<ByteBuffer> buffers
          = commitIndexSet.remove(index);
      long length =
          buffers.stream().mapToLong(buf -> (buf.limit() - buf.position()))
              .sum();
      totalAckDataLength += length;
      // clear the future object from the future Map
      final CompletableFuture<ContainerCommandResponseProto> remove =
          futureMap.remove(totalAckDataLength);
      if (remove == null) {
        LOG.error("Couldn't find required future for " + totalAckDataLength);
        for (Long key : futureMap.keySet()) {
          LOG.error("Existing acknowledged data: " + key);
        }
      }
      Preconditions.checkNotNull(remove);
      for (ByteBuffer byteBuffer : buffers) {
        bufferPool.remove(byteBuffer);
      }
    }
    return totalAckDataLength;
  }

  public long getTotalAckDataLength() {
    return totalAckDataLength;
  }

  private IOException getIOExceptionForWatchForCommit(long commitIndex,
                                                       Exception e) {
    LOG.warn("watchForCommit failed for index {}", commitIndex, e);
    IOException ioException = new IOException(
        "Unexpected Storage Container Exception: " + e.toString(), e);
    releaseBuffersOnException();
    return ioException;
  }

  public ConcurrentMap<Long,
        CompletableFuture<
            ContainerCommandResponseProto>> getFutureMap() {
    return futureMap;
  }

  public void cleanup() {
    if (commitIndexSet != null) {
      commitIndexSet.clear();
    }
    if (futureMap != null) {
      futureMap.clear();
    }
    commitIndexSet = null;
  }
}
