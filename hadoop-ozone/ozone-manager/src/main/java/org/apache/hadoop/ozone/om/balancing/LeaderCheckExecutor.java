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

package org.apache.hadoop.ozone.om.balancing;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUP_TRANSFER_LEADERSHIP_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUP_TRANSFER_LEADERSHIP_TIMEOUT_DEFAULT;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OmRaftGroupManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi raft group leader balancing executor.
 */
public class LeaderCheckExecutor implements Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(LeaderCheckExecutor.class);
  private final OzoneManagerRatisServer server;
  private final long transferBucketGroupLeadershipTimeoutMs;
  private final OmRaftGroupManager omRaftGroupManager;

  public LeaderCheckExecutor(OzoneManagerRatisServer server, OzoneConfiguration configuration,
                             OmRaftGroupManager omRaftGroupManager) {
    this.server = server;
    this.transferBucketGroupLeadershipTimeoutMs = configuration.getTimeDuration(
        OZONE_OM_MULTI_RAFT_BUCKET_GROUP_TRANSFER_LEADERSHIP_TIMEOUT,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUP_TRANSFER_LEADERSHIP_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS
    );
    this.omRaftGroupManager = omRaftGroupManager;
  }

  @Override
  public void run() {
    omRaftGroupManager.acquireBucketRaftGroupsReconstructionLock();
    try {
      LOG.trace("Starting leader balancing in {}", server.getRaftPeerId());
      Map<RaftGroupId, RaftPeerId> groupsWithLeaderInfo =
          StreamSupport.stream(server.getServer().getGroups().spliterator(), false)
              .filter(it -> !it.getGroupId().equals(server.getCurrentRaftGroupId()))
              .filter(it -> server.getLeaderId(it.getGroupId()) != null)
              .collect(
                  Collectors.toMap(RaftGroup::getGroupId, value -> server.getLeaderId(value.getGroupId()))
              );
      Set<RaftPeerId> peerIdSet = server.getPeerIds().stream().map(RaftPeerId::valueOf).collect(
          Collectors.toSet());
      List<LeaderChangingGroupInfo> leaderChangingGroupInfoList = BucketGroupLeaderBalancer.getLeaderChangingInfo(
              groupsWithLeaderInfo,
              peerIdSet);

      List<LeaderChangingGroupInfo> filteredForParticularPeer =
          leaderChangingGroupInfoList.stream().filter(it -> it.getOldLeader().equals(server.getRaftPeerId()))
              .collect(Collectors.toList());

      if (filteredForParticularPeer.isEmpty()) {
        LOG.trace("Skipping leader balancing");
      } else {
        sendRebalancingGroupsRequests(filteredForParticularPeer);
      }
      LOG.trace("Finish leader balancing");
    } catch (Exception e) {
      LOG.error("Error while balancing leadership", e);
    } finally {
      omRaftGroupManager.releaseBucketRaftGroupsReconstructionLock();
    }
  }

  private void sendRebalancingGroupsRequests(List<LeaderChangingGroupInfo> leaderChangingGroupInfoList) {
    for (LeaderChangingGroupInfo leaderChangingGroupInfo : leaderChangingGroupInfoList) {
      TransferLeadershipRequest request = new TransferLeadershipRequest(
          ClientId.randomId(),
          leaderChangingGroupInfo.getOldLeader(),
          leaderChangingGroupInfo.getGroupId(),
          ThreadLocalRandom.current().nextLong(),
          leaderChangingGroupInfo.getNewLeader(),
          transferBucketGroupLeadershipTimeoutMs
      );

      try {
        LOG.trace("Try to send transfer leadership request. Group: {}. {}->{}",
                leaderChangingGroupInfo.getGroupId(),
                leaderChangingGroupInfo.getOldLeader(),
                leaderChangingGroupInfo.getNewLeader()
        );
        server.getServer().transferLeadership(request);
      } catch (Exception e) {
        LOG.warn("Error while transferring leadership for group {}: {}",
            leaderChangingGroupInfo.getGroupId(),
            e.getMessage());
      }
    }
  }
}
