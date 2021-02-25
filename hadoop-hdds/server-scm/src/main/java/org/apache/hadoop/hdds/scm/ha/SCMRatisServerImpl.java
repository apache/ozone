/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Time;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO.
 */
public class SCMRatisServerImpl implements SCMRatisServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMRatisServerImpl.class);

  private final RaftServer server;
  private final SCMStateMachine stateMachine;
  private final StorageContainerManager scm;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong callId = new AtomicLong();

  // TODO: Refactor and remove ConfigurationSource and use only
  //  SCMHAConfiguration.
  SCMRatisServerImpl(final ConfigurationSource conf,
      final StorageContainerManager scm, final DBTransactionBuffer buffer)
      throws IOException {
    this.scm = scm;
    this.stateMachine = new SCMStateMachine(scm, this, buffer);
    final RaftGroupId groupId = buildRaftGroupId(scm.getClusterId());
    LOG.info("starting Raft server for scm:{}", scm.getScmId());
    // During SCM startup, the bootstrapped node will be started just with
    // groupId information, so that it won't trigger any leader election
    // as it doesn't have any peer info.

    // The primary SCM node which is initialized using scm --init command,
    // will initialize the raft server with the peer info and it will be
    // persisted in the raft log post leader election. Now, when the primary
    // scm boots up, it has peer info embedded in the raft log and will
    // trigger leader election.
    this.server =
        newRaftServer(scm.getScmId(), conf).setStateMachine(stateMachine)
            .setGroup(RaftGroup.valueOf(groupId)).build();
  }

  public static void initialize(String clusterId, String scmId,
      SCMNodeDetails details, OzoneConfiguration conf) throws IOException {
    final RaftGroup group = buildRaftGroup(details, scmId, clusterId);
    RaftServer server = newRaftServer(scmId, conf)
        .setGroup(group).build();
    server.start();
    waitforLeaderToBeReady(server, 60000, group);
    server.close();
  }

  private static void waitforLeaderToBeReady(RaftServer server, long timeout,
      RaftGroup group)
      throws IOException {
    boolean ready;
    long st = Time.monotonicNow();
    do {
      ready = server.getDivision(group.getGroupId()).getInfo().isLeaderReady();
      if (!ready) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    } while (!ready && Time.monotonicNow() - st < timeout);

    if (!ready) {
      throw new IOException(String
          .format("Ratis group %s is not ready in %d ms", group.getGroupId(),
              timeout));
    }
  }

  private static RaftServer.Builder newRaftServer(final String scmId,
      final ConfigurationSource conf) {
    final SCMHAConfiguration haConf = conf.getObject(SCMHAConfiguration.class);
    final RaftProperties serverProperties =
        RatisUtil.newRaftProperties(haConf, conf);
    return RaftServer.newBuilder().setServerId(RaftPeerId.getRaftPeerId(scmId))
        .setProperties(serverProperties)
        .setStateMachine(new SCMStateMachine(false));
  }

  @Override
  public void start() throws IOException {
    server.start();
  }

  public RaftServer.Division getDivision() {
    try {
      return server
          .getDivision(server.getGroups().iterator().next().getGroupId());
    } catch (Exception e) {
      LOG.warn("Failed to get RaftServerDivision", e);
      return null;
    }
  }

  @VisibleForTesting
  public SCMStateMachine getStateMachine() {
    return stateMachine;
  }

  @Override
  public void registerStateMachineHandler(final RequestType handlerType,
                                          final Object handler) {
    stateMachine.registerHandler(handlerType, handler);
  }

  @Override
  public SCMRatisResponse submitRequest(SCMRatisRequest request)
      throws IOException, ExecutionException, InterruptedException {
    final RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(getDivision().getId())
        .setGroupId(getDivision().getGroup().getGroupId())
        .setCallId(nextCallId())
        .setMessage(request.encode())
        .setType(RaftClientRequest.writeRequestType())
        .build();
    final RaftClientReply raftClientReply =
        server.submitClientRequestAsync(raftClientRequest).get();
    if (LOG.isDebugEnabled()) {
      LOG.debug("request {} Reply {}", raftClientRequest, raftClientReply);
    }
    return SCMRatisResponse.decode(raftClientReply);
  }

  private long nextCallId() {
    return callId.getAndIncrement() & Long.MAX_VALUE;
  }

  @Override
  public void stop() throws IOException {
    server.close();
  }

  @Override
  public List<String> getRatisRoles() {
    return getDivision().getGroup().getPeers().stream()
        .map(peer -> peer.getAddress() == null ? "" : peer.getAddress())
        .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NotLeaderException triggerNotLeaderException() {
    return new NotLeaderException(
        getDivision().getMemberId(), null, getDivision().getGroup().getPeers());
  }

  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    List<RaftPeer> newRaftPeerList =
        new ArrayList<>(getDivision().getGroup().getPeers());
    // add the SCM node to be added to the raft peer list

    RaftPeer raftPeer = RaftPeer.newBuilder().setId(request.getScmId())
        .setAddress(request.getRatisAddr()).build();
    newRaftPeerList.add(raftPeer);

    LOG.debug("{}: Submitting SetConfiguration request to Ratis server with" +
            " new SCM peers list: {}", scm.getScmId(),
        newRaftPeerList);

    RaftServer.Division division = getDivision();
    SetConfigurationRequest configRequest =
        new SetConfigurationRequest(clientId, division.getPeer().getId(),
            division.getGroup().getGroupId(), nextCallId(), newRaftPeerList);

    try {
      RaftClientReply raftClientReply =
          division.getRaftServer().setConfiguration(configRequest);
      if (raftClientReply.isSuccess()) {
        LOG.info("Successfully added new SCM: {}.", request.getScmId());
      } else {
        LOG.error("Failed to add new SCM: {}. Ratis reply: {}" +
            request.getScmId(), raftClientReply);
        throw new IOException(raftClientReply.getException());
      }
      return raftClientReply.isSuccess();
    } catch (IOException e) {
      LOG.error("Failed to update Ratis configuration and add new peer. " +
          "Cannot add new SCM: {}.", scm.getScmId(), e);
      throw e;
    }
  }

  @VisibleForTesting
  public static void validateRatisGroupExists(OzoneConfiguration conf,
      String clusterId) throws IOException {
    final SCMHAConfiguration haConf = conf.getObject(SCMHAConfiguration.class);
    final RaftProperties properties = RatisUtil.newRaftProperties(haConf, conf);
    final RaftGroupId raftGroupId = buildRaftGroupId(clusterId);
    final AtomicBoolean found = new AtomicBoolean(false);
    RaftServerConfigKeys.storageDir(properties).parallelStream().forEach(
        (dir) -> Optional.ofNullable(dir.listFiles()).map(Arrays::stream)
            .orElse(Stream.empty()).filter(File::isDirectory).forEach(sub -> {
              try {
                LOG.info("{}: found a subdirectory {}", raftGroupId, sub);
                RaftGroupId groupId = null;
                try {
                  groupId = RaftGroupId.valueOf(UUID.fromString(sub.getName()));
                } catch (Exception e) {
                  LOG.info("{}: The directory {} is not a group directory;"
                      + " ignoring it. ", raftGroupId, sub.getAbsolutePath());
                }
                if (groupId != null) {
                  if (groupId.equals(raftGroupId)) {
                    LOG.info(
                        "{} : The directory {} found a group directory for "
                            + "cluster {}", raftGroupId, sub.getAbsolutePath(),
                        clusterId);
                    found.set(true);
                  }
                }
              } catch (Exception e) {
                LOG.warn(
                    raftGroupId + ": Failed to find the group directory "
                        + sub.getAbsolutePath() + ".", e);
              }
            }));
    if (!found.get()) {
      throw new IOException(
          "Could not find any ratis group with id " + raftGroupId);
    }
  }

  private static RaftGroup buildRaftGroup(SCMNodeDetails details,
      String scmId, String clusterId) {
    Preconditions.checkNotNull(scmId);
    final RaftGroupId groupId = buildRaftGroupId(clusterId);
    RaftPeerId selfPeerId = RaftPeerId.getRaftPeerId(scmId);

    RaftPeer localRaftPeer = RaftPeer.newBuilder().setId(selfPeerId)
        // TODO : Should we use IP instead of hostname??
        .setAddress(details.getRatisHostPortStr()).build();

    List<RaftPeer> raftPeers = new ArrayList<>();
    // Add this Ratis server to the Ratis ring
    raftPeers.add(localRaftPeer);
    final RaftGroup group =
        RaftGroup.valueOf(groupId, raftPeers);
    return group;
  }

  private static RaftGroupId buildRaftGroupId(String clusterId) {
    Preconditions.checkNotNull(clusterId);
    return RaftGroupId.valueOf(
        UUID.fromString(clusterId.replace(OzoneConsts.CLUSTER_ID_PREFIX, "")));
  }

}
