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

package org.apache.hadoop.ozone.om.ratis;

import static org.apache.hadoop.ipc_.RpcConstants.DUMMY_CLIENT_ID;
import static org.apache.hadoop.ipc_.RpcConstants.INVALID_CALL_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HA_PREFIX;
import static org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils.createServerTlsConfig;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ServiceException;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.RatisConfUtils;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ipc_.ProtobufRpcEngine.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys.Read;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a Ratis server endpoint for OM.
 */
public final class OzoneManagerRatisServer {
  private static final Logger LOG = LoggerFactory.getLogger(OzoneManagerRatisServer.class);

  private final int port;
  private final RaftServer server;
  private final Supplier<RaftServer.Division> serverDivision;
  private final RaftGroupId raftGroupId;
  private final RaftGroup raftGroup;
  private final RaftPeerId raftPeerId;
  private final Map<String, RaftPeer> raftPeerMap;

  private final OzoneManager ozoneManager;
  private final OzoneManagerStateMachine omStateMachine;
  private final String ratisStorageDir;
  private final OMPerformanceMetrics perfMetrics;

  private final ClientId clientId = ClientId.randomId();
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();
  private final Read.Option readOption;

  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  /**
   * Returns an OM Ratis server.
   * @param conf configuration
   * @param om the OM instance starting the ratis server
   * @param raftGroupIdStr raft group id string
   * @param localRaftPeerId raft peer id of this Ratis server
   * @param addr address of the ratis server
   * @param peers peer nodes in the raft ring
   * @throws IOException
   */
  @SuppressWarnings({"parameternumber", "java:S107"})
  private OzoneManagerRatisServer(ConfigurationSource conf, OzoneManager om,
      String raftGroupIdStr, RaftPeerId localRaftPeerId,
      InetSocketAddress addr, List<RaftPeer> peers, boolean isBootstrapping,
      SecurityConfig secConfig, CertificateClient certClient)
      throws IOException {
    this.ozoneManager = om;
    this.port = addr.getPort();
    this.ratisStorageDir = OzoneManagerRatisUtils.getOMRatisDirectory(conf);
    final RaftProperties serverProperties = newRaftProperties(
        conf, port, ratisStorageDir);

    this.raftPeerId = localRaftPeerId;
    this.raftGroupId = RaftGroupId.valueOf(
        getRaftGroupIdFromOmServiceId(raftGroupIdStr));
    this.raftPeerMap = Maps.newHashMap();
    peers.forEach(e -> raftPeerMap.put(e.getId().toString(), e));
    this.raftGroup = RaftGroup.valueOf(raftGroupId, peers);

    if (isBootstrapping) {
      LOG.info("OM started in Bootstrap mode. Instantiating OM Ratis server " +
          "with groupID: {}", raftGroupIdStr);
    } else {
      StringBuilder raftPeersStr = new StringBuilder();
      for (RaftPeer peer : peers) {
        raftPeersStr.append(", ").append(peer.getAddress());
      }
      LOG.info("Instantiating OM Ratis server with groupID: {} and peers: {}",
          raftGroupIdStr, raftPeersStr.substring(2));
    }
    this.omStateMachine = getStateMachine(conf);

    this.readOption = RaftServerConfigKeys.Read.option(serverProperties);

    Parameters parameters = createServerTlsParameters(secConfig, certClient);
    this.server = RaftServer.newBuilder()
        .setServerId(this.raftPeerId)
        .setGroup(this.raftGroup)
        .setProperties(serverProperties)
        .setParameters(parameters)
        .setStateMachine(omStateMachine)
        .setOption(RaftStorage.StartupOption.RECOVER)
        .build();
    this.serverDivision = MemoizedSupplier.valueOf(() -> {
      try {
        return server.getDivision(raftGroupId);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to getDivision for " + raftGroupId, e);
      }
    });
    this.perfMetrics = om.getPerfMetrics();
  }

  /**
   * Creates an instance of OzoneManagerRatisServer.
   */
  public static OzoneManagerRatisServer newOMRatisServer(
      ConfigurationSource ozoneConf, OzoneManager omProtocol,
      OMNodeDetails omNodeDetails, Map<String, OMNodeDetails> peerNodes,
      SecurityConfig secConfig, CertificateClient certClient,
      boolean isBootstrapping) throws IOException {

    // RaftGroupId is the omServiceId
    String omServiceId = omNodeDetails.getServiceId();

    String omNodeId = omNodeDetails.getNodeId();
    RaftPeerId localRaftPeerId = RaftPeerId.getRaftPeerId(omNodeId);

    InetSocketAddress ratisAddr = new InetSocketAddress(
        omNodeDetails.getInetAddress(), omNodeDetails.getRatisPort());

    RaftPeer localRaftPeer = OzoneManagerRatisServer.createRaftPeer(omNodeDetails);

    // If OM is started in bootstrap mode, do not add peers to the RaftGroup.
    // Raft peers will be added after SetConfiguration transaction is
    // committed by leader and propagated to followers.
    List<RaftPeer> raftPeers = new ArrayList<>();
    if (!isBootstrapping) {
      // On regular startup, add all OMs to Ratis ring
      raftPeers.add(localRaftPeer);

      for (Map.Entry<String, OMNodeDetails> peerInfo : peerNodes.entrySet()) {
        String peerNodeId = peerInfo.getKey();
        OMNodeDetails peerNode = peerInfo.getValue();
        RaftPeer raftPeer = OzoneManagerRatisServer.createRaftPeer(peerNode, peerNodeId);

        // Add other OM nodes belonging to the same OM service to the Ratis ring
        raftPeers.add(raftPeer);
      }
    }

    return new OzoneManagerRatisServer(ozoneConf, omProtocol, omServiceId,
        localRaftPeerId, ratisAddr, raftPeers, isBootstrapping, secConfig,
        certClient);
  }

  /**
   * Submit request to Ratis server.
   * @param omRequest
   * @return OMResponse - response returned to the client.
   * @throws ServiceException
   */
  public OMResponse submitRequest(OMRequest omRequest, boolean isWrite) throws ServiceException {
    // In prepare mode, only prepare and cancel requests are allowed to go
    // through.
    if (ozoneManager.getPrepareState().requestAllowed(omRequest.getCmdType())) {
      RaftClientRequest raftClientRequest = createRaftRequest(omRequest, isWrite);
      RaftClientReply raftClientReply = submitRequestToRatis(raftClientRequest);
      return createOmResponse(omRequest, raftClientReply);
    } else {
      LOG.info("Rejecting write request on OM {} because it is in prepare " +
          "mode: {}", ozoneManager.getOMNodeId(),
          omRequest.getCmdType().name());

      String message = "Cannot apply write request " +
          omRequest.getCmdType().name() + " when OM is in prepare mode.";
      OMResponse.Builder omResponse = OMResponse.newBuilder()
          .setMessage(message)
          .setStatus(Status.NOT_SUPPORTED_OPERATION_WHEN_PREPARED)
          .setCmdType(omRequest.getCmdType())
          .setTraceID(omRequest.getTraceID())
          .setSuccess(false);
      return omResponse.build();
    }
  }

  private OMResponse createOmResponse(OMRequest omRequest,
      RaftClientReply raftClientReply) throws ServiceException {
    return captureLatencyNs(
        perfMetrics.getCreateOmResponseLatencyNs(),
        () -> createOmResponseImpl(omRequest, raftClientReply));
  }

  private RaftClientReply submitRequestToRatis(
      RaftClientRequest raftClientRequest) throws ServiceException {
    return captureLatencyNs(
        perfMetrics.getSubmitToRatisLatencyNs(),
        () -> submitRequestToRatisImpl(raftClientRequest));
  }

  private RaftClientRequest createRaftRequest(OMRequest omRequest, boolean isWrite) {
    return captureLatencyNs(
        perfMetrics.getCreateRatisRequestLatencyNs(),
        () -> createRaftRequestImpl(omRequest, isWrite));
  }

  /**
   * API used internally from OzoneManager Server when requests need to be submitted.
   * @param omRequest
   * @param cliId
   * @param callId
   * @return OMResponse
   * @throws ServiceException
   */
  public OMResponse submitRequest(OMRequest omRequest, ClientId cliId, long callId) throws ServiceException {
    RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
        .setClientId(cliId)
        .setServerId(getRaftPeerId())
        .setGroupId(getRaftGroupId())
        .setCallId(callId)
        .setMessage(Message.valueOf(
            OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
    RaftClientReply raftClientReply =
        submitRequestToRatis(raftClientRequest);
    return createOmResponse(omRequest, raftClientReply);
  }

  private RaftClientReply submitRequestToRatisImpl(
      RaftClientRequest raftClientRequest) throws ServiceException {
    try {
      return server.submitClientRequestAsync(raftClientRequest)
          .get();
    } catch (ExecutionException | IOException ex) {
      throw new ServiceException(ex.getMessage(), ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ServiceException(ex.getMessage(), ex);
    }
  }

  /**
   * Add new OM to the Ratis ring.
   */
  public void addOMToRatisRing(OMNodeDetails newOMNode) throws IOException {
    Objects.requireNonNull(newOMNode, "newOMNode == null");

    String newOMNodeId = newOMNode.getNodeId();
    RaftPeer newRaftPeer = OzoneManagerRatisServer.createRaftPeer(newOMNode);

    LOG.info("{}: Submitting SetConfiguration request to Ratis server to add" +
            " new OM peer {} to the Ratis group {}", ozoneManager.getOMNodeId(),
        newRaftPeer, raftGroup);

    List<RaftPeer> newPeersList = new ArrayList<>(getPeers(RaftPeerRole.FOLLOWER));
    List<RaftPeer> newListenerList = new ArrayList<>(getPeers(RaftPeerRole.LISTENER));

    if (newOMNode.isRatisListener()) {
      newListenerList.add(newRaftPeer);
    } else {
      newPeersList.add(newRaftPeer);
    }

    updateRatisConfiguration(newPeersList, newListenerList, "add", newOMNodeId);
  }

  /**
   * Remove decommissioned OM node from Ratis ring.
   */
  public void removeOMFromRatisRing(OMNodeDetails removeOMNode)
      throws IOException {
    Objects.requireNonNull(removeOMNode, "removeOMNode == null");

    String removeNodeId = removeOMNode.getNodeId();
    LOG.info("{}: Submitting SetConfiguration request to Ratis server to " +
            "remove OM peer {} from Ratis group {}", ozoneManager.getOMNodeId(),
        removeNodeId, raftGroup);

    final List<RaftPeer> newPeersList = getPeers(RaftPeerRole.FOLLOWER, removeNodeId);

    final List<RaftPeer> newListenersList = getPeers(RaftPeerRole.LISTENER, removeNodeId);

    updateRatisConfiguration(newPeersList, newListenersList, "remove", removeNodeId);
  }

  /**
   * Return a list of peer NodeIds.
   */
  public Set<String> getPeerIds() {
    return Collections.unmodifiableSet(raftPeerMap.keySet());
  }

  public Set<String> getPeerIds(RaftPeerRole role) {
    return raftPeerMap.entrySet().stream()
        .filter(entry -> entry.getValue().getStartupRole() == role)
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  public List<RaftPeer> getPeers() {
    return new ArrayList<>(raftPeerMap.values());
  }

  public List<RaftPeer> getPeers(RaftPeerRole role) {
    return raftPeerMap.values().stream()
        .filter(raftPeer -> raftPeer.getStartupRole() == role)
        .collect(Collectors.toList());
  }

  /**
   * Get peers by role, excluding a specific node ID.
   * @param role the role to filter by
   * @param excludeNodeId the node ID to exclude
   * @return list of peers with the specified role, excluding the specified node
   */
  public List<RaftPeer> getPeers(RaftPeerRole role, String excludeNodeId) {
    return raftPeerMap.entrySet().stream()
        .filter(e -> !e.getKey().equals(excludeNodeId))
        .map(Map.Entry::getValue)
        .filter(peer -> peer.getStartupRole() == role)
        .collect(Collectors.toList());
  }

  /**
   * Helper method to update Ratis configuration with new peer lists.
   * @param followers list of follower peers
   * @param listeners list of listener peers
   * @param operation description of the operation for logging
   * @param nodeId the node ID being operated on
   * @throws IOException if the configuration update fails
   */
  private void updateRatisConfiguration(List<RaftPeer> followers, List<RaftPeer> listeners,
      String operation, String nodeId) throws IOException {
    SetConfigurationRequest request = new SetConfigurationRequest(clientId,
        server.getId(), raftGroupId, nextCallId(), followers, listeners);

    RaftClientReply raftClientReply = server.setConfiguration(request);
    if (raftClientReply.isSuccess()) {
      LOG.info("{} OM {} in Ratis group {}.", operation, nodeId, raftGroupId);
    } else {
      LOG.error("Failed to {} OM {} in Ratis group {}. Ratis " +
              "SetConfiguration reply: {}", operation.toLowerCase(), nodeId, raftGroupId,
          raftClientReply);
      throw new IOException("Failed to " + operation.toLowerCase() + " OM " + nodeId + " in " +
          "Ratis ring.");
    }
  }

  private static RaftPeer createRaftPeer(OMNodeDetails omNode) {
    String nodeId = omNode.getNodeId();
    RaftPeerId raftPeerId = RaftPeerId.valueOf(nodeId);
    InetSocketAddress ratisAddr = new InetSocketAddress(
        omNode.getHostAddress(), omNode.getRatisPort());
    RaftPeerRole startRole = omNode.isRatisListener() ?
        RaftPeerRole.LISTENER : RaftPeerRole.FOLLOWER;

    return RaftPeer.newBuilder()
        .setId(raftPeerId)
        .setAddress(ratisAddr)
        .setStartupRole(startRole)
        .build();
  }

  /**
   * Helper method to create a RaftPeer from OMNodeDetails, handling unresolved hosts.
   * @param omNode the OM node details
   * @param nodeId the node ID to use
   * @return the created RaftPeer
   */
  private static RaftPeer createRaftPeer(OMNodeDetails omNode, String nodeId) {
    RaftPeerId raftPeerId = RaftPeerId.valueOf(nodeId);
    RaftPeer.Builder builder = RaftPeer.newBuilder()
        .setId(raftPeerId)
        .setStartupRole(omNode.isRatisListener() ? RaftPeerRole.LISTENER : RaftPeerRole.FOLLOWER);
    
    if (omNode.isHostUnresolved()) {
      builder.setAddress(omNode.getRatisHostPortStr());
    } else {
      InetSocketAddress ratisAddr = new InetSocketAddress(
          omNode.getInetAddress(), omNode.getRatisPort());
      builder.setAddress(ratisAddr);
    }
    
    return builder.build();
  }

  /**
   * Check if the input peerId exists in the peers list.
   * @return true if the nodeId is self or it exists in peer node list,
   *         false otherwise.
   */
  @VisibleForTesting
  public boolean doesPeerExist(String peerId) {
    return raftPeerMap.containsKey(peerId);
  }

  /**
   * Add given node to list of RaftPeers.
   */
  public void addRaftPeer(OMNodeDetails omNodeDetails) {
    String newNodeId = omNodeDetails.getNodeId();
    RaftPeer raftPeer = OzoneManagerRatisServer.createRaftPeer(omNodeDetails);
    raftPeerMap.put(newNodeId, raftPeer);

    LOG.info("Added OM {} to Ratis Peers list.", newNodeId);
  }

  /**
   * Remove given node from list of RaftPeers.
   */
  public void removeRaftPeer(OMNodeDetails omNodeDetails) {
    String removeNodeID = omNodeDetails.getNodeId();
    raftPeerMap.remove(removeNodeID);
    LOG.info("{}: Removed OM {} from Ratis Peers list.", this, removeNodeID);
  }

  /**
   * Create Write RaftClient request from OMRequest.
   * @param omRequest OM request.
   * @return RaftClientRequest - Raft Client request which is submitted to
   * ratis server.
   */
  private RaftClientRequest createRaftRequestImpl(OMRequest omRequest, boolean isWrite) {
    return RaftClientRequest.newBuilder()
        .setClientId(getClientId())
        .setServerId(server.getId())
        .setGroupId(raftGroupId)
        .setCallId(getCallId())
        .setMessage(
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(isWrite ? RaftClientRequest.writeRequestType() : getRaftReadRequestType(omRequest))
        .build();
  }

  private static RaftClientRequest.Type getRaftReadRequestType(OMRequest omRequest) {
    if (!OmUtils.specifiedReadConsistency(omRequest)) {
      // If there is no consistency hint, we simply follow the Raft server read option
      return RaftClientRequest.readRequestType();
    }
    // Allow client to decide which read consistency semantic can be used
    ReadConsistency readConsistency =
        ReadConsistency.fromProto(omRequest.getReadConsistencyHint().getReadConsistency());
    if (readConsistency.isLinearizable()) {
      // Note that the linearizable request type might not be respected
      // if the Raft server does not set the read option to LINEARIZABLE
      return RaftClientRequest.readRequestType(false);
    } else {
      // This will do a leader-only read even if the Raft server read option is LINEARIZABLE
      return RaftClientRequest.readRequestType(true);
    }
  }

  private ClientId getClientId() {
    final byte[] clientIdBytes = Server.getClientId();
    if (!ozoneManager.isTestSecureOmFlag()) {
      Preconditions.checkArgument(clientIdBytes != DUMMY_CLIENT_ID);
    }
    return ClientId.valueOf(UUID.nameUUIDFromBytes(clientIdBytes));
  }

  private long getCallId() {
    final long callId = Server.getCallId();
    if (!ozoneManager.isTestSecureOmFlag()) {
      Preconditions.checkArgument(callId != INVALID_CALL_ID);
    }
    return callId;
  }

  public OMResponse checkRetryCache() throws ServiceException {
    final ClientInvocationId invocationId = ClientInvocationId.valueOf(getClientId(), getCallId());
    final RetryCache.Entry cacheEntry = getServerDivision().getRetryCache().getIfPresent(invocationId);
    if (cacheEntry == null) {
      return null;  //cache miss
    }
    //cache hit
    try {
      RaftClientReply reply = cacheEntry.getReplyFuture().get();
      if (!reply.isSuccess()) {
        return null;
      }
      return getOMResponse(reply);
    } catch (ExecutionException ex) {
      throw new ServiceException(ex.getMessage(), ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ServiceException(ex.getMessage(), ex);
    }
  }

  /**
   * Process the raftClientReply and return OMResponse.
   * @param omRequest
   * @param reply
   * @return OMResponse - response which is returned to client.
   * @throws ServiceException
   */
  private OMResponse createOmResponseImpl(OMRequest omRequest,
      RaftClientReply reply) throws ServiceException {
    // NotLeader exception is thrown only when the raft server to which the
    // request is submitted is not the leader. This can happen first time
    // when client is submitting request to OM.

    if (!reply.isSuccess()) {
      NotLeaderException notLeaderException = reply.getNotLeaderException();
      if (notLeaderException != null) {
        throw new ServiceException(
            OMNotLeaderException.convertToOMNotLeaderException(
                  notLeaderException, getRaftPeerId()));
      }

      LeaderNotReadyException leaderNotReadyException =
          reply.getLeaderNotReadyException();
      if (leaderNotReadyException != null) {
        throw new ServiceException(new OMLeaderNotReadyException(
            leaderNotReadyException.getMessage()));
      }

      LeaderSteppingDownException leaderSteppingDownException = reply.getLeaderSteppingDownException();
      if (leaderSteppingDownException != null) {
        throw new ServiceException(new OMNotLeaderException(leaderSteppingDownException.getMessage()));
      }

      StateMachineException stateMachineException =
          reply.getStateMachineException();
      if (stateMachineException != null) {
        OMResponse.Builder omResponse = OMResponse.newBuilder()
            .setCmdType(omRequest.getCmdType())
            .setSuccess(false)
            .setTraceID(omRequest.getTraceID());
        if (stateMachineException.getCause() != null) {
          omResponse.setMessage(stateMachineException.getCause().getMessage());
          omResponse.setStatus(
              exceptionToResponseStatus(stateMachineException.getCause()));
        } else {
          // Current Ratis is setting cause, this is an safer side check.
          LOG.error("StateMachine exception cause is not set");
          omResponse.setStatus(
              OzoneManagerProtocolProtos.Status.INTERNAL_ERROR);
          omResponse.setMessage(
              StringUtils.stringifyException(stateMachineException));
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Error while executing ratis request. " +
              "stateMachineException: ", stateMachineException);
        }
        return omResponse.build();
      }
    }

    return getOMResponse(reply);
  }

  private OMResponse getOMResponse(RaftClientReply reply) throws ServiceException {
    try {
      return OMRatisHelper.getOMResponseFromRaftClientReply(reply, getLeaderId());
    } catch (IOException ex) {
      if (ex.getMessage() != null) {
        throw new ServiceException(ex.getMessage(), ex);
      } else {
        throw new ServiceException(ex);
      }
    }
  }

  /**
   * Convert exception to {@link OzoneManagerProtocolProtos.Status}.
   * @param cause - Cause from stateMachine exception
   * @return {@link OzoneManagerProtocolProtos.Status}
   */
  private OzoneManagerProtocolProtos.Status exceptionToResponseStatus(
      Throwable cause) {
    if (cause instanceof OMException) {
      return OzoneManagerProtocolProtos.Status.values()[
          ((OMException) cause).getResult().ordinal()];
    } else {
      LOG.error("Unknown error occurs", cause);
      return OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
    }
  }

  public RaftGroup getRaftGroup() {
    return this.raftGroup;
  }

  @VisibleForTesting
  public RaftServer.Division getServerDivision() {
    return serverDivision.get();
  }

  public boolean isLinearizableRead() {
    // TODO: Currently we use LINEARIZABLE read option to imply
    //  that we support follower reads although technically
    //  linearizable leader-only read is also a valid configuration.
    //  In the future, a separate configuration to check whether OM
    //  supports follower read can be added.
    return readOption == Read.Option.LINEARIZABLE;
  }

  /**
   * Initializes and returns OzoneManager StateMachine.
   */
  private OzoneManagerStateMachine getStateMachine(ConfigurationSource conf)
      throws IOException {
    return new OzoneManagerStateMachine(this,
        TracingUtil.isTracingEnabled(conf));
  }

  @VisibleForTesting
  public OzoneManagerStateMachine getOmStateMachine() {
    return omStateMachine;
  }

  public OzoneManager getOzoneManager() {
    return ozoneManager;
  }

  /**
   * Start the Ratis server.
   * @throws IOException
   */
  public void start() throws IOException {
    LOG.info("Starting {} {} at port {}", getClass().getSimpleName(),
        server.getId(), port);
    server.start();
  }

  public void stop() {
    LOG.info("Stopping {} at port {}", this, port);
    try {
      // Ratis will also close the state machine
      server.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static RaftProperties newRaftProperties(ConfigurationSource conf,
      int port, String ratisStorageDir) {
    // Set RPC type
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(conf.get(
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_DEFAULT));
    final RaftProperties properties = RatisHelper.newRaftProperties(rpc);

    // Set the ratis port number
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, port);
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, port);
    }

    // Set Ratis storage directory
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(new File(ratisStorageDir)));

    final int logAppenderBufferByteLimit = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT, StorageUnit.BYTES);
    setRaftLogProperties(properties, logAppenderBufferByteLimit, conf);

    // For grpc config
    RatisConfUtils.Grpc.setMessageSizeMax(properties, logAppenderBufferByteLimit);

    setRaftLeaderElectionProperties(properties, conf);

    setRaftRpcProperties(properties, conf);

    setRaftRetryCacheProperties(properties, conf);

    setRaftSnapshotProperties(properties, conf);

    setRaftCloseThreshold(properties, conf);

    getOMHAConfigs(conf).forEach(properties::set);
    return properties;
  }

  private static void setRaftLeaderElectionProperties(RaftProperties properties, ConfigurationSource conf) {
    // Disable/enable the pre vote feature in Ratis
    RaftServerConfigKeys.LeaderElection.setPreVote(properties, conf.getBoolean(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_ELECTION_PRE_VOTE,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_ELECTION_PRE_VOTE_DEFAULT));
  }

  private static void setRaftLogProperties(RaftProperties properties,
      int logAppenderQueueByteLimit, ConfigurationSource conf) {
    // Set RAFT segment size
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf((long) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_DEFAULT, StorageUnit.BYTES)));

    // Set to enable RAFT to purge logs up to Snapshot Index
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, conf.getBoolean(
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_UPTO_SNAPSHOT_INDEX,
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_UPTO_SNAPSHOT_INDEX_DEFAULT));

    // Set number of last RAFT logs to not be purged
    RaftServerConfigKeys.Log.setPurgePreservationLogNum(properties, conf.getLong(
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_PRESERVATION_LOG_NUM,
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_PRESERVATION_LOG_NUM_DEFAULT));

    // Set RAFT segment pre-allocated size
    RaftServerConfigKeys.Log.setPreallocatedSize(properties, SizeInBytes.valueOf((long) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT, StorageUnit.BYTES)));

    // Set RAFT buffer element limit
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT));

    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(logAppenderQueueByteLimit));
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf(logAppenderQueueByteLimit + 8));
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);

    RaftServerConfigKeys.Log.setPurgeGap(properties, conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP,
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP_DEFAULT));

    // This avoids writing commit metadata to Raft Log, which can be used to recover the
    // commit index even if a majority of servers are dead. We don't need this for OzoneManager,
    // disabling this will avoid the additional disk IO.
    RaftServerConfigKeys.Log.setLogMetadataEnabled(properties, false);

    // Set the number of maximum cached segments
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 2);

    RaftServerConfigKeys.Write.setElementLimit(properties, conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_PENDING_WRITE_ELEMENT_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_PENDING_WRITE_NUM_LIMIT_DEFAULT));
  }

  private static void setRaftRpcProperties(RaftProperties properties, ConfigurationSource conf) {
    // Set the server request timeout
    TimeUnit serverRequestTimeoutUnit = OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT.getUnit();
    final TimeDuration serverRequestTimeout = TimeDuration.valueOf(conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT.getDuration(), serverRequestTimeoutUnit),
        serverRequestTimeoutUnit);
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, serverRequestTimeout);

    // Set the server min and max timeout
    TimeUnit serverMinTimeoutUnit = OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT.getUnit();
    final TimeDuration serverMinTimeout = TimeDuration.valueOf(conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT.getDuration(), serverMinTimeoutUnit),
        serverMinTimeoutUnit);
    final TimeDuration serverMaxTimeout = serverMinTimeout.add(200, TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, serverMinTimeout);
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, serverMaxTimeout);

    // Set the server Rpc slowness timeout and Notification noLeader timeout
    TimeUnit nodeFailureTimeoutUnit = OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT.getUnit();
    final TimeDuration nodeFailureTimeout = TimeDuration.valueOf(conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT.getDuration(), nodeFailureTimeoutUnit),
        nodeFailureTimeoutUnit);
    RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties, nodeFailureTimeout);
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties, nodeFailureTimeout);
  }

  private static void setRaftRetryCacheProperties(RaftProperties properties, ConfigurationSource conf) {
    // Set timeout for server retry cache entry
    TimeUnit retryCacheTimeoutUnit = OMConfigKeys.OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DEFAULT.getUnit();
    final TimeDuration retryCacheTimeout = TimeDuration.valueOf(conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DEFAULT.getDuration(), retryCacheTimeoutUnit),
        retryCacheTimeoutUnit);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties, retryCacheTimeout);
  }

  private static void setRaftSnapshotProperties(RaftProperties properties, ConfigurationSource conf) {
    // Set auto trigger snapshot. We don't need to configure auto trigger
    // threshold in OM, as last applied index is flushed during double buffer
    // flush automatically. (But added this property internally, so that this
    // helps during testing, when want to trigger snapshots frequently, and
    // which will purge logs when purge gap condition is satisfied and which
    // will trigger installSnapshot when logs are cleaned up.)
    // The transaction info value in OM DB is used as
    // snapshot value after restart.

    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);

    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, conf.getLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_DEFAULT));
  }

  private static void setRaftCloseThreshold(RaftProperties properties, ConfigurationSource conf) {
    // Set RAFT server close threshold
    TimeUnit closeThresholdUnit = OMConfigKeys.OZONE_OM_RATIS_SERVER_CLOSE_THRESHOLD_DEFAULT.getUnit();
    final int closeThreshold = (int) TimeDuration.valueOf(conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_CLOSE_THRESHOLD_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_CLOSE_THRESHOLD_DEFAULT.getDuration(), closeThresholdUnit),
        closeThresholdUnit).toLong(TimeUnit.SECONDS);
    // TODO: update to new api setCloseThreshold(RaftProperties, TimeDuration) if available
    RaftServerConfigKeys.setCloseThreshold(properties, closeThreshold);
  }

  private static Map<String, String> getOMHAConfigs(
      ConfigurationSource configuration) {
    return configuration
        .getPropsMatchPrefixAndTrimPrefix(OZONE_OM_HA_PREFIX + ".");
  }

  public RaftPeerId getLeaderId() {
    return getServerDivision().getInfo().getLeaderId();
  }

  public OMNotLeaderException newOMNotLeaderException() {
    final RaftPeerId leaderId = getLeaderId();
    final RaftPeer leader = leaderId == null ? null : getServerDivision().getRaftConf().getPeer(leaderId);
    if (leader == null) {
      return new OMNotLeaderException(raftPeerId);
    }
    final String leaderAddress = getRaftLeaderAddress(leader);
    return new OMNotLeaderException(raftPeerId, leader.getId(), leaderAddress);
  }

  /**
   * Defines RaftServer Status.
   */
  public enum RaftServerStatus {
    NOT_LEADER,
    LEADER_AND_NOT_READY,
    LEADER_AND_READY;
  }

  /**
   * Check Leader status and return the state of the RaftServer.
   *
   * @return RaftServerStatus.
   */
  public RaftServerStatus getLeaderStatus() {
    final RaftServer.Division division = getServerDivision();
    if (division == null) {
      return RaftServerStatus.NOT_LEADER;
    } else if (!division.getInfo().isLeader()) {
      return RaftServerStatus.NOT_LEADER;
    } else if (division.getInfo().isLeaderReady()) {
      return RaftServerStatus.LEADER_AND_READY;
    } else {
      return RaftServerStatus.LEADER_AND_NOT_READY;
    }
  }

  @VisibleForTesting
  public List<String> getCurrentListenersFromRaftConf() throws IOException {
    try {
      Collection<RaftPeer> currentListeners =
          server.getDivision(raftGroupId).getRaftConf().getCurrentPeers(RaftPeerRole.LISTENER);
      List<String> currentListenerList = new ArrayList<>();
      currentListeners.forEach(e -> currentListenerList.add(e.getId().toString()));
      return currentListenerList;
    } catch (IOException e) {
      // In this case we return not a leader.
      throw new IOException("Failed to get peer information from Ratis.", e);
    }
  }

  public int getServerPort() {
    return port;
  }

  @VisibleForTesting
  public LifeCycle.State getServerState() {
    return server.getLifeCycleState();
  }

  @VisibleForTesting
  public RaftPeerId getRaftPeerId() {
    return this.raftPeerId;
  }

  @VisibleForTesting
  public String getRaftLeaderAddress(RaftPeer leaderPeer) {
    InetAddress leaderInetAddress = null;
    try {
      Optional<String> hostname =
          HddsUtils.getHostName(leaderPeer.getAddress());
      if (hostname.isPresent()) {
        leaderInetAddress = InetAddress.getByName(hostname.get());
      }
    } catch (UnknownHostException e) {
      LOG.error("OM Ratis LeaderInetAddress {} is unresolvable",
          leaderPeer.getAddress(), e);
    }
    return leaderInetAddress == null ? null :
        leaderInetAddress.toString();
  }

  public static UUID getRaftGroupIdFromOmServiceId(String omServiceId) {
    return UUID.nameUUIDFromBytes(omServiceId.getBytes(StandardCharsets.UTF_8));
  }

  public String getRatisStorageDir() {
    return ratisStorageDir;
  }

  public TermIndex getLastAppliedTermIndex() {
    return omStateMachine.getLastAppliedTermIndex();
  }

  public RaftGroupId getRaftGroupId() {
    return raftGroupId;
  }

  private static Parameters createServerTlsParameters(SecurityConfig conf,
      CertificateClient caClient) throws IOException {
    GrpcTlsConfig config = createServerTlsConfig(conf, caClient);
    return config == null ? null : RatisHelper.setServerTlsConf(config);
  }
}
