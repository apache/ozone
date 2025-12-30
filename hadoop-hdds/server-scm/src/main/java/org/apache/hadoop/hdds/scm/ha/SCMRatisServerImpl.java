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

import static org.apache.hadoop.hdds.scm.ha.HASecurityUtils.createSCMRatisTLSConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.RemoveSCMRequest;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Time;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO.
 */
public class SCMRatisServerImpl implements SCMRatisServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMRatisServerImpl.class);

  private final OzoneConfiguration ozoneConf = new OzoneConfiguration();
  private final RaftServer server;
  private final SCMStateMachine stateMachine;
  private final StorageContainerManager scm;
  private final ClientId clientId = ClientId.randomId();
  private final AtomicLong callId = new AtomicLong();
  private final RaftServer.Division division;
  private final GrpcTlsConfig grpcTlsConfig;
  private boolean isStopped;
  private final long requestTimeout;

  // TODO: Refactor and remove ConfigurationSource and use only
  //  SCMHAConfiguration.
  SCMRatisServerImpl(final ConfigurationSource conf,
      final StorageContainerManager scm, final SCMHADBTransactionBuffer buffer)
      throws IOException {
    this.scm = scm;
    
    requestTimeout = ozoneConf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_HA_RATIS_REQUEST_TIMEOUT,
        ScmConfigKeys.OZONE_SCM_HA_RATIS_REQUEST_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    Preconditions.checkArgument(requestTimeout > 1000L,
        "Ratis request timeout cannot be less than 1000ms.");
    
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

    grpcTlsConfig = createSCMRatisTLSConfig(new SecurityConfig(conf),
        scm.getScmCertificateClient());
    final Parameters parameters = RatisHelper.setServerTlsConf(grpcTlsConfig);

    this.server = newRaftServer(scm.getScmId(), conf)
        .setStateMachineRegistry((gId) -> new SCMStateMachine(scm, buffer))
        .setOption(RaftStorage.StartupOption.RECOVER)
        .setGroup(RaftGroup.valueOf(groupId))
        .setParameters(parameters).build();

    this.stateMachine =
        (SCMStateMachine) server.getDivision(groupId).getStateMachine();

    this.division = server.getDivision(groupId);
    this.isStopped = false;
  }

  public static void initialize(String clusterId, String scmId,
      SCMNodeDetails details, OzoneConfiguration conf) throws IOException {
    final RaftGroup group = buildRaftGroup(details, scmId, clusterId);
    RaftServer server = null;
    try {
      server = newRaftServer(scmId, conf).setGroup(group)
              .setStateMachineRegistry((groupId -> new SCMStateMachine()))
              .setOption(RaftStorage.StartupOption.RECOVER)
              .build();
      server.start();
      waitForLeaderToBeReady(server, conf, group);
    } finally {
      if (server != null) {
        server.close();
      }
    }
  }

  @Override
  public GrpcTlsConfig getGrpcTlsConfig() {
    return grpcTlsConfig;
  }

  @Override
  @Nullable
  public RaftPeerId getLeaderId() {
    RaftPeer raftLeaderPeer = getLeader();
    if (raftLeaderPeer != null) {
      return raftLeaderPeer.getId();
    }
    return null;
  }

  private static void waitForLeaderToBeReady(RaftServer server,
      OzoneConfiguration conf, RaftGroup group) throws IOException {
    boolean ready;
    long st = Time.monotonicNow();
    long waitTimeout = conf.getTimeDuration(
            ScmConfigKeys.OZONE_SCM_HA_RATIS_LEADER_READY_WAIT_TIMEOUT,
            ScmConfigKeys.OZONE_SCM_HA_RATIS_LEADER_READY_WAIT_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);
    long retryInterval = conf.getTimeDuration(
            ScmConfigKeys.OZONE_SCM_HA_RATIS_LEADER_READY_CHECK_INTERVAL,
            ScmConfigKeys.
                    OZONE_SCM_HA_RATIS_LEADER_READY_CHECK_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);

    do {
      ready = server.getDivision(group.getGroupId()).getInfo().isLeaderReady();
      if (!ready) {
        try {
          Thread.sleep(retryInterval);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    } while (!ready && Time.monotonicNow() - st < waitTimeout);

    if (!ready) {
      throw new IOException(String
          .format("Ratis group %s is not ready in %d ms", group.getGroupId(),
                  waitTimeout));
    }
  }

  private static RaftServer.Builder newRaftServer(final String scmId,
      final ConfigurationSource conf) {
    final RaftProperties serverProperties =
        RatisUtil.newRaftProperties(conf);
    return RaftServer.newBuilder().setServerId(RaftPeerId.getRaftPeerId(scmId))
        .setProperties(serverProperties);
  }

  @Override
  public void start() throws IOException {
    LOG.info("starting ratis server {}", server.getPeer().getAddress());
    server.start();
  }

  @Override
  public RaftServer.Division getDivision() {
    return division;
  }

  @VisibleForTesting
  public SCMStateMachine getStateMachine() {
    return stateMachine;
  }

  @Override
  public SCMStateMachine getSCMStateMachine() {
    return stateMachine;
  }

  @Override
  public void registerStateMachineHandler(final RequestType handlerType,
                                          final Object handler) {
    stateMachine.registerHandler(handlerType, handler);
  }

  @Override
  public SCMRatisResponse submitRequest(SCMRatisRequest request)
      throws IOException, ExecutionException, InterruptedException,
      TimeoutException {
    final RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(getDivision().getId())
        .setGroupId(getDivision().getGroup().getGroupId())
        .setCallId(nextCallId())
        .setMessage(request.encode())
        .setType(RaftClientRequest.writeRequestType())
        .build();
    // any request submitted to
    final RaftClientReply raftClientReply =
        server.submitClientRequestAsync(raftClientRequest)
            .get(requestTimeout, TimeUnit.MILLISECONDS);
    LOG.debug("request {} Reply {}", raftClientRequest, raftClientReply);
    return SCMRatisResponse.decode(raftClientReply);
  }

  @Override
  public boolean triggerSnapshot() throws IOException {
    final SnapshotManagementRequest req = SnapshotManagementRequest.newCreate(
        clientId, getDivision().getId(), getDivision().getGroup().getGroupId(),
        nextCallId(), requestTimeout);
    final RaftClientReply raftClientReply = server.snapshotManagement(req);
    if (!raftClientReply.isSuccess()) {
      LOG.warn("Snapshot request failed", raftClientReply.getException());
    }
    return raftClientReply.isSuccess();
  }

  private long nextCallId() {
    return callId.getAndIncrement() & Long.MAX_VALUE;
  }

  @Override
  public void stop() throws IOException {
    LOG.info("stopping ratis server {}", server.getPeer().getAddress());
    server.close();
    isStopped = true;
    getSCMStateMachine().close();
  }

  @Override
  public boolean isStopped() {
    return isStopped;
  }

  @Override
  public List<String> getRatisRoles() {
    Collection<RaftPeer> peers = division.getGroup().getPeers();
    RaftPeer leader = getLeader();
    List<String> ratisRoles = new ArrayList<>();
    for (RaftPeer peer : peers) {
      InetAddress peerInetAddress = null;
      try {
        peerInetAddress = InetAddress.getByName(
            HddsUtils.getHostName(peer.getAddress()).get());
      } catch (IOException ex) {
        LOG.error("SCM Ratis PeerInetAddress {} is unresolvable",
            peer.getAddress());
      }
      ratisRoles.add((peer.getAddress() == null ? "" :
          peer.getAddress().concat(peer.equals(leader) ?
                  ":".concat(RaftProtos.RaftPeerRole.LEADER.toString()) :
                  ":".concat(RaftProtos.RaftPeerRole.FOLLOWER.toString()))
                  .concat(":".concat(peer.getId().toString()))
                  .concat(":".concat(peerInetAddress == null ? "" :
                      peerInetAddress.getHostAddress()))));
    }
    return ratisRoles;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NotLeaderException triggerNotLeaderException() {
    ByteString leaderId =
        division.getInfo().getRoleInfoProto().getFollowerInfo().getLeaderInfo()
            .getId().getId();
    RaftPeer suggestedLeader = leaderId.isEmpty() ?
        null :
        division.getRaftConf().getPeer(RaftPeerId.valueOf(leaderId));
    return new NotLeaderException(division.getMemberId(),
        suggestedLeader,
        division.getGroup().getPeers());
  }

  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    List<RaftPeer> newRaftPeerList =
        new ArrayList<>(getDivision().getGroup().getPeers());
    // add the SCM node to be added to the raft peer list

    RaftPeer raftPeer = RaftPeer.newBuilder().setId(request.getScmId())
        .setAddress(request.getRatisAddr()).build();
    newRaftPeerList.add(raftPeer);

    LOG.info("{}: Submitting SetConfiguration request to Ratis server with" +
            " new SCM peers list: {}", scm.getScmId(),
        newRaftPeerList);
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
      LOG.warn("Failed to update Ratis configuration and add new peer. " +
          "Cannot add new SCM: {}. {}", scm.getScmId(), e.getMessage());
      LOG.debug("addSCM call failed due to: ", e);
      throw e;
    }
  }

  @Override
  public boolean removeSCM(RemoveSCMRequest request) throws IOException {
    final List<RaftPeer> newRaftPeerList =
        new ArrayList<>(division.getGroup().getPeers());
    // remove the SCM node from the raft peer list

    final RaftPeer raftPeer = RaftPeer.newBuilder().setId(request.getScmId())
        .setAddress(request.getRatisAddr()).build();

    newRaftPeerList.remove(raftPeer);

    LOG.info("{}: Submitting SetConfiguration request to Ratis server with" +
            " updated SCM peers list: {}", request.getScmId(),
        newRaftPeerList);
    final SetConfigurationRequest configRequest =
        new SetConfigurationRequest(clientId, division.getPeer().getId(),
            division.getGroup().getGroupId(), nextCallId(), newRaftPeerList);

    try {
      RaftClientReply raftClientReply = server.setConfiguration(configRequest);
      if (raftClientReply.isSuccess()) {
        LOG.info("Successfully removed SCM: {}.", request.getScmId());
      } else {
        LOG.error("Failed to remove SCM: {}. Ratis reply: {}" +
            request.getScmId(), raftClientReply);
        throw new IOException(raftClientReply.getException());
      }
      return raftClientReply.isSuccess();
    } catch (IOException e) {
      if (e instanceof NotLeaderException) {
        LOG.debug("Cannot remove peer: {}", request.getScmId(), e);
      } else {
        LOG.error("Failed to update Ratis configuration and remove peer. " +
            "Cannot remove SCM: {}.", request.getScmId(), e);
      }
      throw e;
    }
  }

  private static RaftGroup buildRaftGroup(SCMNodeDetails details,
      String scmId, String clusterId) {
    Objects.requireNonNull(scmId, "scmId == null");
    final RaftGroupId groupId = buildRaftGroupId(clusterId);
    RaftPeerId selfPeerId = getSelfPeerId(scmId);

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

  public static RaftPeerId getSelfPeerId(String scmId) {
    return RaftPeerId.getRaftPeerId(scmId);
  }

  @VisibleForTesting
  public static RaftGroupId buildRaftGroupId(String clusterId) {
    Objects.requireNonNull(clusterId, "clusterId == null");
    return RaftGroupId.valueOf(
        UUID.fromString(clusterId.replace(OzoneConsts.CLUSTER_ID_PREFIX, "")));
  }

  public RaftPeer getLeader() {
    if (division.getInfo().isLeader()) {
      return division.getPeer();
    } else {
      ByteString leaderId = division.getInfo().getRoleInfoProto()
          .getFollowerInfo().getLeaderInfo().getId().getId();
      return leaderId.isEmpty() ? null :
          division.getRaftConf().getPeer(RaftPeerId.valueOf(leaderId));
    }
  }
}
