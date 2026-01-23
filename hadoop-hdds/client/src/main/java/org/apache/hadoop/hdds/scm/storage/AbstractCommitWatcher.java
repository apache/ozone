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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes watchForCommit on ratis pipeline and releases
 * buffers once data successfully gets replicated.
 */
abstract class AbstractCommitWatcher<BUFFER> {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractCommitWatcher.class);

  /**
   * Commit index -> buffers: when a commit index is acknowledged,
   * the corresponding buffers can be released.
   */
  private final SortedMap<Long, List<BUFFER>> commitIndexMap
      = new ConcurrentSkipListMap<>();
  /**
   * Commit index -> reply future:
   * cache to reply futures to avoid sending duplicated watch requests.
   */
  private final ConcurrentMap<Long, CompletableFuture<XceiverClientReply>>
      replies = new ConcurrentHashMap<>();

  private final XceiverClientSpi client;

  private final AtomicLong totalAckDataLength = new AtomicLong();

  AbstractCommitWatcher(XceiverClientSpi client) {
    this.client = client;
  }

  @VisibleForTesting
  SortedMap<Long, List<BUFFER>> getCommitIndexMap() {
    return commitIndexMap;
  }

  synchronized void updateCommitInfoMap(long index, List<BUFFER> buffers) {
    commitIndexMap.computeIfAbsent(index, k -> new LinkedList<>())
        .addAll(buffers);
  }

  /** @return the total data which has been acknowledged. */
  long getTotalAckDataLength() {
    return totalAckDataLength.get();
  }

  long addAckDataLength(long acked) {
    return totalAckDataLength.addAndGet(acked);
  }

  /**
   * Watch for commit for the first index.
   * This is useful when the buffer is full
   * since the first chunk can be released once it has been committed.
   * Otherwise, the client write is blocked.
   *
   * @return {@link XceiverClientReply} reply from raft client
   * @throws IOException in case watchForCommit fails
   */
  XceiverClientReply watchOnFirstIndex() throws IOException {
    if (commitIndexMap.isEmpty()) {
      return null;
    }
    return watchForCommit(commitIndexMap.firstKey());
  }

  /**
   * Watch for commit for the last index.
   * This is useful when the buffer is not full
   * since it will wait for all the chunks in the buffer to get committed.
   * Since the buffer is not full, the client write is not blocked.
   *
   * @return {@link XceiverClientReply} reply from raft client
   * @throws IOException in case watchForCommit fails
   */
  XceiverClientReply watchOnLastIndex() throws IOException {
    if (commitIndexMap.isEmpty()) {
      return null;
    }
    return watchForCommit(commitIndexMap.lastKey());
  }

  /**
   * Watch for commit for a particular index.
   *
   * @param commitIndex log index to watch for
   * @return minimum commit index replicated to all nodes
   */
  CompletableFuture<XceiverClientReply> watchForCommitAsync(long commitIndex) {
    final MemoizedSupplier<CompletableFuture<XceiverClientReply>> supplier
        = JavaUtils.memoize(CompletableFuture::new);
    final CompletableFuture<XceiverClientReply> f = replies.compute(commitIndex,
        (key, value) -> value != null ? value : supplier.get());
    if (!supplier.isInitialized()) {
      // future already exists
      return f;
    }

    return client.watchForCommit(commitIndex).thenApply(reply -> {
      f.complete(reply);
      final CompletableFuture<XceiverClientReply> removed = replies.remove(commitIndex);
      Preconditions.checkState(removed == f);

      final long index = reply != null ? reply.getLogIndex() : 0;
      adjustBuffers(index);
      return reply;
    });
  }

  XceiverClientReply watchForCommit(long commitIndex) throws IOException {
    try {
      return watchForCommitAsync(commitIndex).get();
    } catch (InterruptedException e) {
      // Re-interrupt the thread while catching InterruptedException
      Thread.currentThread().interrupt();
      throw getIOExceptionForWatchForCommit(commitIndex, e);
    } catch (ExecutionException e) {
      throw getIOExceptionForWatchForCommit(commitIndex, e);
    }
  }

  List<BUFFER> remove(long i) {
    final List<BUFFER> buffers = commitIndexMap.remove(i);
    Objects.requireNonNull(buffers, () -> "commitIndexMap.remove(" + i + ")");
    return buffers;
  }

  /** Release the buffers for the given index. */
  abstract void releaseBuffers(long index);

  synchronized void adjustBuffers(long commitIndex) {
    commitIndexMap.keySet().stream()
        .filter(p -> p <= commitIndex)
        .forEach(this::releaseBuffers);
  }

  void releaseBuffersOnException() {
    adjustBuffers(client.getReplicatedMinCommitIndex());
  }

  IOException getIOExceptionForWatchForCommit(long commitIndex, Exception e) {
    LOG.warn("watchForCommit failed for index {}", commitIndex, e);
    IOException ioException = new IOException(
        "Unexpected Storage Container Exception: " + e, e);
    releaseBuffersOnException();
    return ioException;
  }

  void cleanup() {
    commitIndexMap.clear();
    replies.clear();
  }
}
