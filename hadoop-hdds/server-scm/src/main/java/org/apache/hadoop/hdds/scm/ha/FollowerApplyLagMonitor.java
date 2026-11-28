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

package org.apache.hadoop.hdds.scm.ha;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically measures how far the SCM Ratis followers are behind in
 * <em>applying</em> committed Raft log entries. The leader cannot learn a
 * follower's applied index from append-entries replies, so the leader asks
 * each follower for its {@code LogInfoProto} through the Ratis group
 * management API and caches the result.
 *
 * <p>The reported lag is the leader's commit index minus the lowest index applied by any peer, so a
 * single slow follower raises it. Peers whose applied index cannot be read are left out; if no
 * follower answers, the poll fails and the previous value is kept.
 */
class FollowerApplyLagMonitor implements AutoCloseable {
  private static final Logger LOG =
      LoggerFactory.getLogger(FollowerApplyLagMonitor.class);

  private final RaftServer.Division division;
  private final GrpcTlsConfig tlsConfig;
  private final long intervalMs;
  private final ScheduledExecutorService executor;
  private volatile long lag;
  /** Only used by the poll thread; rebuilt when the group changes. */
  private RaftClient client;
  private RaftGroup clientGroup;

  FollowerApplyLagMonitor(RaftServer.Division division,
      GrpcTlsConfig tlsConfig, long intervalMs) {
    this.division = division;
    this.tlsConfig = tlsConfig;
    this.intervalMs = intervalMs;
    this.executor = Executors.newSingleThreadScheduledExecutor(
        r -> {
          final Thread t = new Thread(r, "SCMFollowerApplyLagMonitor");
          t.setDaemon(true);
          return t;
        });
  }

  void start() {
    executor.scheduleWithFixedDelay(this::refresh, intervalMs, intervalMs,
        TimeUnit.MILLISECONDS);
  }

  /**
   * @return the last measured apply lag of the slowest follower in Raft log entries, or 0 if
   * this node is not the leader or no measurement is available.
   */
  long getLag() {
    return lag;
  }

  @VisibleForTesting
  void refresh() {
    try {
      if (division.getInfo().isLeader()) {
        lag = measure();
      } else {
        lag = 0;
        closeClient();
      }
    } catch (Exception e) {
      // Keep the previous value: a failed poll must not flip the throttle.
      LOG.warn("Failed to measure SCM follower apply lag", e);
    }
  }

  @VisibleForTesting
  long measure() throws IOException {
    final RaftGroup group = division.getGroup();
    final RaftClient raftClient = getClient(group);
    final List<Long> applied = new ArrayList<>();
    applied.add(division.getInfo().getLastAppliedIndex());
    for (RaftPeer peer : group.getPeers()) {
      if (!peer.getId().equals(division.getId())) {
        final long index = getAppliedIndex(raftClient, peer.getId(), group);
        if (index >= 0) {
          applied.add(index);
        }
      }
    }
    if (applied.size() == 1 && group.getPeers().size() > 1) {
      throw new IOException("No SCM follower reported its applied index");
    }
    return applyLag(division.getRaftLog().getLastCommittedIndex(), applied);
  }

  private RaftClient getClient(RaftGroup group) {
    if (client == null || !group.equals(clientGroup)) {
      closeClient();
      client = newClient(group);
      clientGroup = group;
    }
    return client;
  }

  private void closeClient() {
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        LOG.debug("Failed to close Raft client", e);
      }
      client = null;
      clientGroup = null;
    }
  }

  private RaftClient newClient(RaftGroup group) {
    return RaftClient.newBuilder()
        .setRaftGroup(group)
        .setProperties(RatisHelper.newRaftProperties(SupportedRpcType.GRPC))
        .setParameters(
            RatisHelper.setClientTlsConf(SupportedRpcType.GRPC, tlsConfig))
        .setRetryPolicy(RetryPolicies.noRetry())
        .build();
  }

  /** @return the follower's applied index, or -1 if it cannot be read. */
  private long getAppliedIndex(RaftClient raftClient, RaftPeerId peerId,
      RaftGroup group) {
    try {
      final GroupInfoReply reply = raftClient.getGroupManagementApi(peerId)
          .info(group.getGroupId());
      return reply.isSuccess() ? reply.getLogInfoProto().getApplied().getIndex() : -1;
    } catch (Exception e) {
      LOG.debug("Failed to get group info from SCM peer {}", peerId, e);
      return -1;
    }
  }

  /**
   * @param leaderCommitIndex the leader's commit index
   * @param appliedIndices applied index of every peer that answered, leader included
   * @return {@code leaderCommitIndex} minus the lowest applied index, never negative.
   */
  @VisibleForTesting
  static long applyLag(long leaderCommitIndex, List<Long> appliedIndices) {
    return Math.max(0, leaderCommitIndex - Collections.min(appliedIndices));
  }

  @Override
  public void close() {
    executor.shutdownNow();
    try {
      executor.awaitTermination(intervalMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    closeClient();
  }
}
