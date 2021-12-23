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
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

  private Map<Long, List<StreamBuffer>> commitIndexMap;
  private List<StreamBuffer> bufferList;

  // total data which has been successfully flushed and acknowledged
  // by all servers
  private long totalAckDataLength;

  private XceiverClientSpi xceiverClient;

  public StreamCommitWatcher(XceiverClientSpi xceiverClient,
      List<StreamBuffer> bufferList) {
    this.xceiverClient = xceiverClient;
    commitIndexMap = new ConcurrentSkipListMap<>();
    this.bufferList = bufferList;
    totalAckDataLength = 0;
  }

  public void updateCommitInfoMap(long index, List<StreamBuffer> buffers) {
    commitIndexMap.computeIfAbsent(index, k -> new LinkedList<>())
        .addAll(buffers);
  }

  int getCommitInfoMapSize() {
    return commitIndexMap.size();
  }

  /**
   * Calls watch for commit for the first index in commitIndex2flushedDataMap to
   * the Ratis client.
   * @return {@link XceiverClientReply} reply from raft client
   * @throws IOException in case watchForCommit fails
   */
  public XceiverClientReply streamWatchOnFirstIndex() throws IOException {
    if (!commitIndexMap.isEmpty()) {
      // wait for the  first commit index in the commitIndex2flushedDataMap
      // to get committed to all or majority of nodes in case timeout
      // happens.
      long index =
          commitIndexMap.keySet().stream().mapToLong(v -> v).min()
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
    if (!commitIndexMap.isEmpty()) {
      // wait for the  commit index in the commitIndex2flushedDataMap
      // to get committed to all or majority of nodes in case timeout
      // happens.
      long index =
          commitIndexMap.keySet().stream().mapToLong(v -> v).max()
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
    final long index;
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
    List<Long> keyList = commitIndexMap.keySet().stream()
        .filter(p -> p <= commitIndex).collect(Collectors.toList());
    if (!keyList.isEmpty()) {
      releaseBuffers(keyList);
    }
  }

  private long releaseBuffers(List<Long> indexes) {
    Preconditions.checkArgument(!commitIndexMap.isEmpty());
    for (long index : indexes) {
      Preconditions.checkState(commitIndexMap.containsKey(index));
      final List<StreamBuffer> buffers = commitIndexMap.remove(index);
      final long length =
          buffers.stream().mapToLong(StreamBuffer::position).sum();
      totalAckDataLength += length;
      for (StreamBuffer byteBuffer : buffers) {
        bufferList.remove(byteBuffer);
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

  public void cleanup() {
    if (commitIndexMap != null) {
      commitIndexMap.clear();
    }
    commitIndexMap = null;
  }
}
