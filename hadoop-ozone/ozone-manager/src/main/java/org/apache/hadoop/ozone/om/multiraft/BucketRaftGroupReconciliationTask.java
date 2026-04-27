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

package org.apache.hadoop.ozone.om.multiraft;

import static org.apache.hadoop.ozone.om.OmRaftGroupManager.generateRaftGroups;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.BucketRaftGroupsStateChanged;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A background task that runs on the Ozone Manager leader to reconcile the
 * state of bucket Raft groups. It checks for any Raft groups that are closed
 * or have unhealthy peers and removes them. It also creates new Raft groups
 * if the number of existing groups is less than the expected count.
 */
public class BucketRaftGroupReconciliationTask implements BackgroundTask {

  public static final Logger LOG = LoggerFactory.getLogger(BucketRaftGroupReconciliationTask.class);

  private final OzoneManager ozoneManager;
  private final int expectedRaftGroupsCount;

  public BucketRaftGroupReconciliationTask(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
    this.expectedRaftGroupsCount = ozoneManager.getOmRaftGroupManager().getOmRaftGroupCount();
  }

  @Override
  public BackgroundTaskResult call() throws Exception {
    ozoneManager.getOmRaftGroupManager().acquireBucketRaftGroupsReconstructionLock();
    try {
      OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
      RaftGroup mainRaftGroup = omRatisServer.getCurrentRaftGroup();
      long currentMultiRaftTerm = ozoneManager.getBucketRaftGroupsReconfigurationIndex();
      if (!omRatisServer.checkLeaderStatus(mainRaftGroup.getGroupId()).equals(NOT_LEADER)) {
        LOG.trace("Start reconciling bucket group RaftGroup on leader {}", omRatisServer.getRaftPeerId());
        List<RaftGroup> groupsToBeReconfigured = new ArrayList<>();
        List<RaftGroup> existingRaftGroups = (List<RaftGroup>) omRatisServer.getServer().getGroups();
        if (existingRaftGroups.size() == 1) { // consist of only main raft group, as like as an initial setup
          LOG.trace("Create all raft groups");
          List<RaftGroupId> raftGroupIds = generateRaftGroups(currentMultiRaftTerm, expectedRaftGroupsCount);
          ozoneManager.createRaftGroups(raftGroupIds.stream().map(RaftId::getUuid).collect(Collectors.toList()), true);
        } else {
          for (RaftGroup raftGroup : existingRaftGroups) {
            if (raftGroup.getGroupId().equals(mainRaftGroup.getGroupId())) {
              continue;
            }
            RaftGroupId groupId = raftGroup.getGroupId();
            DivisionInfo divisionInfo = omRatisServer.getServer().getDivision(groupId).getInfo();
            RaftPeerId leaderId = divisionInfo.getLeaderId();
            if (leaderId == null || divisionInfo.getLifeCycleState().equals(LifeCycle.State.CLOSED)) {
              LOG.warn("Raft group {} is closed, removing it.", groupId);
              groupsToBeReconfigured.add(raftGroup);
            } else {
              RaftPeerId raftGroupLeaderId = omRatisServer.getServer().getDivision(groupId).getInfo().getLeaderId();
              UUID raftGroupUuid = groupId.getUuid();
              OzoneManagerProtocolProtos.GetRaftGroupHealthStateRequest healthRequest =
                  OzoneManagerProtocolProtos.GetRaftGroupHealthStateRequest.newBuilder()
                      .setGroupId(HddsProtos.UUID.newBuilder()
                          .setLeastSigBits(raftGroupUuid.getLeastSignificantBits())
                          .setMostSigBits(raftGroupUuid.getMostSignificantBits())
                          .build())
                      .build();
              try {
                OzoneManagerProtocolProtos.GetRaftGroupHealthStateResponse raftGroupHealthState;
                if (omRatisServer.getServer().getId().equals(raftGroupLeaderId)) {
                  raftGroupHealthState = ozoneManager.getRaftGroupHealthState(healthRequest);
                } else {
                  raftGroupHealthState = getRaftGroupHealthStateFromRemote(
                      raftGroupLeaderId, healthRequest);
                }
                boolean isNotHealthy = raftGroupHealthState.getPeerHealthInfoList()
                    .stream()
                    .anyMatch(it -> !it.getIsHealthy());
                if (isNotHealthy) {
                  groupsToBeReconfigured.add(raftGroup);
                }
              } catch (Exception e) {
                LOG.warn("Failed to get raft group health state for group {}: {}",
                    groupId, e.getMessage());
                groupsToBeReconfigured.add(raftGroup);
              }
            }
          }
          groupsToBeReconfigured.forEach(this::deleteRaftGroup);

          if (ozoneManager.isMultiRaftEnabled()) {
            LOG.trace("Raft group to be reconfigured: {}", groupsToBeReconfigured);
            if (!groupsToBeReconfigured.isEmpty()) {
              if (ozoneManager.isMultiRaftEnabled()) {
                ozoneManager.moveOmToSafeMode();
                List<RaftGroupId> raftGroupIds = generateRaftGroups(currentMultiRaftTerm + 1, expectedRaftGroupsCount);
                LOG.trace("Raft group to be created: {}", raftGroupIds);
                ozoneManager.createRaftGroups(raftGroupIds.stream().map(RaftId::getUuid).collect(Collectors.toList()),
                    false);
              }
            }
            if (existingRaftGroups.size() < expectedRaftGroupsCount + 1) {
              List<RaftGroupId> raftGroupIds = generateRaftGroups(currentMultiRaftTerm,
                  expectedRaftGroupsCount - existingRaftGroups.size() + 1);
              ozoneManager.createRaftGroups(raftGroupIds.stream().map(RaftId::getUuid).collect(Collectors.toList()),
                  false);
            }
          }
        }
        boolean raftGroupsReconfigured = existingRaftGroups.size() == 1 || !groupsToBeReconfigured.isEmpty() ||
            existingRaftGroups.size() < expectedRaftGroupsCount + 1;
        if (raftGroupsReconfigured) {
          try {
            byte[] clientId = omRatisServer.getCurrentClientId().toByteString().toByteArray();
            Server.Call fakeCall = new Server.Call(
                (int) OzoneManagerRatisServer.nextCallId(),
                0,
                null,
                null,
                RPC.RpcKind.RPC_BUILTIN,
                clientId
            );
            Server.getCurCall().set(fakeCall);
            OzoneManagerProtocolProtos.BucketRaftGroupsStateChangedRequest bucketRaftGroupsStateChangedRequest =
                OzoneManagerProtocolProtos.BucketRaftGroupsStateChangedRequest.newBuilder()
                    .setStateChangedIndex(currentMultiRaftTerm + 1)
                    .build();
            OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
                .setBucketRaftGroupsStateChangedRequest(bucketRaftGroupsStateChangedRequest)
                .setCmdType(BucketRaftGroupsStateChanged)
                .setClientId(omRatisServer.getCurrentClientId().toString())
                .build();
            omRatisServer.submitRequest(omRequest, true);
          } finally {
            Server.getCurCall().remove();
          }
        }
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    } finally {
      ozoneManager.getOmRaftGroupManager().releaseBucketRaftGroupsReconstructionLock();
    }
  }

  /**
   * Calls GetRaftGroupHealthState on the remote OM that is the leader of the
   * specified bucket raft group. Uses a direct protobuf RPC proxy to avoid
   * creating an OzoneClient, whose constructor calls getServiceInfo() which
   * requires main raft group leadership.
   */
  private OzoneManagerProtocolProtos.GetRaftGroupHealthStateResponse getRaftGroupHealthStateFromRemote(
      RaftPeerId raftGroupLeaderId,
      OzoneManagerProtocolProtos.GetRaftGroupHealthStateRequest healthRequest) throws IOException {
    String omServiceId = OmUtils.getOzoneManagerServiceId(ozoneManager.getConfiguration());
    org.apache.hadoop.ozone.om.helpers.OMNodeDetails targetNode =
        OmUtils.getAllOMHAAddresses(ozoneManager.getConfiguration(), omServiceId, true).stream()
            .filter(omNodeDetails -> omNodeDetails.getNodeId().equals(raftGroupLeaderId.toString()))
            .findFirst().get();

    OzoneConfiguration conf = OzoneConfiguration.of(ozoneManager.getConfiguration());
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class, ProtobufRpcEngine.class);
    InetSocketAddress omAddress = NetUtils.createSocketAddr(
        targetNode.getHostAddress() + ":" + targetNode.getRpcPort());
    OzoneManagerProtocolPB proxy = RPC.getProxy(OzoneManagerProtocolPB.class,
        RPC.getProtocolVersion(OzoneManagerProtocolPB.class), omAddress,
        org.apache.hadoop.security.UserGroupInformation.getCurrentUser(), conf,
        NetUtils.getDefaultSocketFactory(conf),
        org.apache.hadoop.ipc.Client.getTimeout(conf));
    try {
      OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.GetRaftGroupHealthState)
          .setGetRaftGroupHealthStateRequest(healthRequest)
          .setClientId(ozoneManager.getOmRatisServer().getCurrentClientId().toString())
          .build();
      OzoneManagerProtocolProtos.OMResponse response = proxy.submitRequest(null, omRequest);
      if (!response.getSuccess()) {
        throw new IOException("GetRaftGroupHealthState failed: " + response.getMessage());
      }
      return response.getGetRaftGroupHealthStateResponse();
    } catch (ServiceException e) {
      throw new IOException("Failed to get raft group health state from " + targetNode.getHostAddress(), e);
    } finally {
      RPC.stopProxy(proxy);
    }
  }

  private void deleteRaftGroup(RaftGroup raftGroup) {
    String rpcType = ozoneManager.getConfiguration()
        .get(ScmConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneManager.getConfiguration());

    GrpcTlsConfig tlsConfig = null;
    if (ozoneManager.isSecurityEnabled()) {
      try {
        tlsConfig = new GrpcTlsConfig(ozoneManager.getCertificateClient().getKeyManager(),
                ozoneManager.getCertificateClient().getTrustManager(), true);
      } catch (IOException ex) {
        LOG.error("Can't retrieve cert key store factory", ex);
      }
    }
    LOG.trace("{} Delete raft group {}.", ozoneManager.getOMNodeId(), raftGroup.getGroupId());
    OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
    GrpcTlsConfig finalTlsConfig = tlsConfig;
    raftGroup.getPeers().stream()
        .filter(peer -> !peer.getId().equals(omRatisServer.getRaftPeerId()))
        .forEach(peer -> {
          try (RaftClient raftClient = RatisHelper.newRaftClient(SupportedRpcType.valueOfIgnoreCase(rpcType),
              peer, retryPolicy, finalTlsConfig, ozoneManager.getConfiguration())) {
            raftClient.getGroupManagementApi(peer.getId()).remove(raftGroup.getGroupId(), true, false);
          } catch (IOException e) {
            LOG.error("An error occurred on deleting raft group {} remotely from {}",
                raftGroup.getGroupId(), peer.getId());
          }
        });
    try {
      omRatisServer.removeBucketRaftGroup(raftGroup.getGroupId());
    } catch (IOException e) {
      LOG.error("An error occurred on deleting raft group {} from {}", raftGroup.getGroupId(),
          omRatisServer.getRaftPeerId());
    }

  }
}
