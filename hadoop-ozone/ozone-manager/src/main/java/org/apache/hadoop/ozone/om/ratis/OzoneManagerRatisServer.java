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

package org.apache.hadoop.ozone.om.ratis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ServiceException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine.Server;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ipc.RpcConstants.DUMMY_CLIENT_ID;
import static org.apache.hadoop.ipc.RpcConstants.INVALID_CALL_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HA_PREFIX;

/**
 * Creates a Ratis server endpoint for OM.
 */
public final class OzoneManagerRatisServer {
  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerRatisServer.class);

  private final int port;
  private final InetSocketAddress omRatisAddress;
  private final RaftServer server;
  private final RaftGroupId raftGroupId;
  private final RaftGroup raftGroup;
  private final RaftPeerId raftPeerId;
  private final List<RaftPeer> raftPeers;

  private final OzoneManager ozoneManager;
  private final OzoneManagerStateMachine omStateMachine;
  private final String ratisStorageDir;

  private final ClientId clientId = ClientId.randomId();
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

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
    this.omRatisAddress = addr;
    this.port = addr.getPort();
    this.ratisStorageDir = OzoneManagerRatisUtils.getOMRatisDirectory(conf);
    RaftProperties serverProperties = newRaftProperties(conf);

    this.raftPeerId = localRaftPeerId;
    this.raftGroupId = RaftGroupId.valueOf(
        getRaftGroupIdFromOmServiceId(raftGroupIdStr));
    this.raftPeers = Lists.newArrayList();
    this.raftPeers.addAll(peers);
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
          raftGroupIdStr, raftPeersStr.toString().substring(2));
    }
    this.omStateMachine = getStateMachine(conf);

    Parameters parameters = createServerTlsParameters(secConfig, certClient);
    this.server = RaftServer.newBuilder()
        .setServerId(this.raftPeerId)
        .setGroup(this.raftGroup)
        .setProperties(serverProperties)
        .setParameters(parameters)
        .setStateMachine(omStateMachine)
        .build();
  }

  /**
   * Creates an instance of OzoneManagerRatisServer.
   */
  public static OzoneManagerRatisServer newOMRatisServer(
      ConfigurationSource ozoneConf, OzoneManager omProtocol,
      OMNodeDetails omNodeDetails, List<OMNodeDetails> peerNodes,
      SecurityConfig secConfig, CertificateClient certClient,
      boolean isBootstrapping) throws IOException {

    // RaftGroupId is the omServiceId
    String omServiceId = omNodeDetails.getServiceId();

    String omNodeId = omNodeDetails.getNodeId();
    RaftPeerId localRaftPeerId = RaftPeerId.getRaftPeerId(omNodeId);

    InetSocketAddress ratisAddr = new InetSocketAddress(
        omNodeDetails.getInetAddress(), omNodeDetails.getRatisPort());

    RaftPeer localRaftPeer = RaftPeer.newBuilder()
        .setId(localRaftPeerId)
        .setAddress(ratisAddr)
        .build();

    // If OM is started in bootstrap mode, do not add peers to the RaftGroup.
    // Raft peers will be added after SetConfiguration transaction is
    // committed by leader and propagated to followers.
    List<RaftPeer> raftPeers = new ArrayList<>();
    if (!isBootstrapping) {
      // On regular startup, add all OMs to Ratis ring
      raftPeers.add(localRaftPeer);

      for (OMNodeDetails peerInfo : peerNodes) {
        String peerNodeId = peerInfo.getNodeId();
        RaftPeerId raftPeerId = RaftPeerId.valueOf(peerNodeId);
        RaftPeer raftPeer;
        if (peerInfo.isHostUnresolved()) {
          raftPeer = RaftPeer.newBuilder()
              .setId(raftPeerId)
              .setAddress(peerInfo.getRatisHostPortStr())
              .build();
        } else {
          InetSocketAddress peerRatisAddr = new InetSocketAddress(
              peerInfo.getInetAddress(), peerInfo.getRatisPort());
          raftPeer = RaftPeer.newBuilder()
              .setId(raftPeerId)
              .setAddress(peerRatisAddr)
              .build();
        }

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
  public OMResponse submitRequest(OMRequest omRequest) throws ServiceException {
    // In prepare mode, only prepare and cancel requests are allowed to go
    // through.
    if (ozoneManager.getPrepareState().requestAllowed(omRequest.getCmdType())) {
      RaftClientRequest raftClientRequest =
          createWriteRaftClientRequest(omRequest);
      RaftClientReply raftClientReply = submitRequestToRatis(raftClientRequest);

      return processReply(omRequest, raftClientReply);
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

  /**
   * API used internally from OzoneManager Server when requests needs to be
   * submitted to ratis, where the crafted RaftClientRequest is passed along.
   * @param omRequest
   * @param raftClientRequest
   * @return OMResponse
   * @throws ServiceException
   */
  public OMResponse submitRequest(OMRequest omRequest,
      RaftClientRequest raftClientRequest) throws ServiceException {
    RaftClientReply raftClientReply = submitRequestToRatis(raftClientRequest);
    return processReply(omRequest, raftClientReply);
  }

  private RaftClientReply submitRequestToRatis(
      RaftClientRequest raftClientRequest) throws ServiceException {
    try {
      return server.submitClientRequestAsync(raftClientRequest)
          .get();
    } catch (Exception ex) {
      throw new ServiceException(ex.getMessage(), ex);
    }
  }

  /**
   * Add new OM to the Ratis ring.
   */
  public void addOMToRatisRing(OMNodeDetails newOMNode) throws IOException {

    Preconditions.checkNotNull(newOMNode);

    String newOMNodeId = newOMNode.getNodeId();
    RaftPeerId newOMRaftPeerId = RaftPeerId.valueOf(newOMNodeId);
    InetSocketAddress newOMRatisAddr = new InetSocketAddress(
        newOMNode.getHostAddress(), newOMNode.getRatisPort());
    RaftPeer newRaftPeer = RaftPeer.newBuilder()
        .setId(newOMRaftPeerId)
        .setAddress(newOMRatisAddr)
        .build();

    LOG.info("{}: Submitting SetConfiguration request to Ratis server to add" +
            " new OM peer {} to the Ratis group {}", ozoneManager.getOMNodeId(),
        newRaftPeer, raftGroup);

    List<RaftPeer> newPeersList = new ArrayList<>();
    newPeersList.addAll(raftPeers);
    newPeersList.add(newRaftPeer);

    checkLeaderStatus();
    SetConfigurationRequest request = new SetConfigurationRequest(clientId,
        server.getId(), raftGroupId, nextCallId(), newPeersList);

    try {
      RaftClientReply raftClientReply = server.setConfiguration(request);
      if (raftClientReply.isSuccess()) {
        LOG.info("Added OM {} to Ratis group {}.", newOMNodeId, raftGroupId);
      } else {
        LOG.error("Failed to add OM {} to Ratis group {}. Ratis " +
                "SetConfiguration reply: {}", newOMNodeId, raftGroupId,
            raftClientReply);
        throw new IOException("Failed to add OM " + newOMNodeId + " to Ratis " +
            "ring.");
      }
    } catch (IOException e) {
      LOG.error("Failed to update Ratis configuration and add OM {} to " +
          "Ratis group {}", newOMNodeId, raftGroupId, e);
      throw e;
    }
  }

  /**
   * Return a list of peer NodeIds.
   */
  public List<String> getPeerIds() {
    List<String> peerIds = new ArrayList<>();
    for (RaftPeer raftPeer : raftPeers) {
      peerIds.add(raftPeer.getId().toString());
    }
    return peerIds;
  }

  /**
   * Check if the input peerId exists in the peers list.
   * @return true if the nodeId is self or it exists in peer node list,
   *         false otherwise.
   */
  @VisibleForTesting
  public boolean doesPeerExist(String peerId) {
    if (peerId.equals(raftPeerId.toString())) {
      return true;
    }
    for (RaftPeer raftPeer : raftPeers) {
      if (raftPeer.getId().toString().equals(peerId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Add given node to list of RaftPeers.
   */
  public void addRaftPeer(OMNodeDetails omNodeDetails) {
    InetSocketAddress newOMRatisAddr = new InetSocketAddress(
        omNodeDetails.getHostAddress(), omNodeDetails.getRatisPort());

    raftPeers.add(RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(omNodeDetails.getNodeId()))
        .setAddress(newOMRatisAddr)
        .build());

    LOG.info("Added OM {} to Ratis Peers list.", omNodeDetails.getNodeId());
  }

  /**
   * Create Write RaftClient request from OMRequest.
   * @param omRequest
   * @return RaftClientRequest - Raft Client request which is submitted to
   * ratis server.
   */
  private RaftClientRequest createWriteRaftClientRequest(OMRequest omRequest) {
    Preconditions.checkArgument(Server.getClientId() != DUMMY_CLIENT_ID);
    Preconditions.checkArgument(Server.getCallId() != INVALID_CALL_ID);
    return RaftClientRequest.newBuilder()
        .setClientId(
            ClientId.valueOf(UUID.nameUUIDFromBytes(Server.getClientId())))
        .setServerId(server.getId())
        .setGroupId(raftGroupId)
        .setCallId(Server.getCallId())
        .setMessage(
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  /**
   * Process the raftClientReply and return OMResponse.
   * @param omRequest
   * @param reply
   * @return OMResponse - response which is returned to client.
   * @throws ServiceException
   */
  private OMResponse processReply(OMRequest omRequest, RaftClientReply reply)
      throws ServiceException {
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

    try {
      return OMRatisHelper.getOMResponseFromRaftClientReply(reply);
    } catch (InvalidProtocolBufferException ex) {
      if (ex.getMessage() != null) {
        throw new ServiceException(ex.getMessage(), ex);
      } else {
        throw new ServiceException(ex);
      }
    }

    // TODO: Still need to handle RaftRetry failure exception and
    //  NotReplicated exception.
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
  public RaftServer getServer() {
    return server;
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
    try {
      server.close();
      omStateMachine.stop();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  //TODO simplify it to make it shorter
  @SuppressWarnings("methodlength")
  private RaftProperties newRaftProperties(ConfigurationSource conf) {
    final RaftProperties properties = new RaftProperties();

    // Set RPC type
    final String rpcType = conf.get(
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_DEFAULT);
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(rpcType);
    RaftConfigKeys.Rpc.setType(properties, rpc);

    // Set the ratis port number
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, port);
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, port);
    }

    // Set Ratis storage directory
    RaftServerConfigKeys.setStorageDir(properties,
        Collections.singletonList(new File(ratisStorageDir)));
    // Disable the pre vote feature in Ratis
    RaftServerConfigKeys.LeaderElection.setPreVote(properties, false);

    // Set RAFT segment size
    final int raftSegmentSize = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf(raftSegmentSize));
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);

    // Set RAFT segment pre-allocated size
    final int raftSegmentPreallocatedSize = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT,
        StorageUnit.BYTES);
    int logAppenderQueueNumElements = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT);
    final int logAppenderQueueByteLimit = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties,
        logAppenderQueueNumElements);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties,
        SizeInBytes.valueOf(logAppenderQueueByteLimit));
    RaftServerConfigKeys.Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties,
        false);
    final int logPurgeGap = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP,
        OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP_DEFAULT);
    RaftServerConfigKeys.Log.setPurgeGap(properties, logPurgeGap);

    // For grpc set the maximum message size
    // TODO: calculate the optimal max message size
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(logAppenderQueueByteLimit));

    // Set the server request timeout
    TimeUnit serverRequestTimeoutUnit =
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT.getUnit();
    long serverRequestTimeoutDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT
            .getDuration(), serverRequestTimeoutUnit);
    final TimeDuration serverRequestTimeout = TimeDuration.valueOf(
        serverRequestTimeoutDuration, serverRequestTimeoutUnit);
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties,
        serverRequestTimeout);

    // Set timeout for server retry cache entry
    TimeUnit retryCacheTimeoutUnit = OMConfigKeys
        .OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DEFAULT.getUnit();
    long retryCacheTimeoutDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DEFAULT
            .getDuration(), retryCacheTimeoutUnit);
    final TimeDuration retryCacheTimeout = TimeDuration.valueOf(
        retryCacheTimeoutDuration, retryCacheTimeoutUnit);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties,
        retryCacheTimeout);

    // Set the server min and max timeout
    TimeUnit serverMinTimeoutUnit =
        OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT.getUnit();
    long serverMinTimeoutDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT
            .getDuration(), serverMinTimeoutUnit);
    final TimeDuration serverMinTimeout = TimeDuration.valueOf(
        serverMinTimeoutDuration, serverMinTimeoutUnit);
    long serverMaxTimeoutDuration =
        serverMinTimeout.toLong(TimeUnit.MILLISECONDS) + 200;
    final TimeDuration serverMaxTimeout = TimeDuration.valueOf(
        serverMaxTimeoutDuration, TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        serverMinTimeout);
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        serverMaxTimeout);

    // Set the number of maximum cached segments
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 2);

    // TODO: set max write buffer size

    TimeUnit nodeFailureTimeoutUnit =
        OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    long nodeFailureTimeoutDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT
            .getDuration(), nodeFailureTimeoutUnit);
    final TimeDuration nodeFailureTimeout = TimeDuration.valueOf(
        nodeFailureTimeoutDuration, nodeFailureTimeoutUnit);
    RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties,
        nodeFailureTimeout);
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties,
        nodeFailureTimeout);

    // Set auto trigger snapshot. We don't need to configure auto trigger
    // threshold in OM, as last applied index is flushed during double buffer
    // flush automatically. (But added this property internally, so that this
    // helps during testing, when want to trigger snapshots frequently, and
    // which will purge logs when purge gap condition is satisfied and which
    // will trigger installSnapshot when logs are cleaned up.)
    // The transaction info value in OM DB is used as
    // snapshot value after restart.

    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(
        properties, true);

    long snapshotAutoTriggerThreshold = conf.getLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_DEFAULT);

    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties,
        snapshotAutoTriggerThreshold);

    createRaftServerProperties(conf, properties);
    return properties;
  }

  private void createRaftServerProperties(ConfigurationSource ozoneConf,
      RaftProperties raftProperties) {
    Map<String, String> ratisServerConf =
        getOMHAConfigs(ozoneConf);
    ratisServerConf.forEach((key, val) -> {
      raftProperties.set(key, val);
    });
  }

  private static Map<String, String> getOMHAConfigs(
      ConfigurationSource configuration) {
    return configuration.getPropsWithPrefix(OZONE_OM_HA_PREFIX + ".");
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
  public RaftServerStatus checkLeaderStatus() {
    try {
      RaftServer.Division division = server.getDivision(raftGroupId);
      if (division != null) {
        if (!division.getInfo().isLeader()) {
          return RaftServerStatus.NOT_LEADER;
        } else if (division.getInfo().isLeaderReady()) {
          return RaftServerStatus.LEADER_AND_READY;
        } else {
          return RaftServerStatus.LEADER_AND_NOT_READY;
        }
      }
    } catch (IOException ioe) {
      // In this case we return not a leader.
      LOG.error("Fail to get RaftServer impl and therefore it's not clear " +
          "whether it's leader. ", ioe);
    }
    return RaftServerStatus.NOT_LEADER;
  }

  /**
   * Get list of peer NodeIds from Ratis.
   * @return List of Peer NodeId's.
   */
  @VisibleForTesting
  public List<String> getCurrentPeersFromRaftConf() throws IOException {
    try {
      Collection<RaftPeer> currentPeers =
          server.getDivision(raftGroupId).getRaftConf().getCurrentPeers();
      List<String> currentPeerList = new ArrayList<>();
      currentPeers.forEach(e -> currentPeerList.add(e.getId().toString()));
      return currentPeerList;
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

  private UUID getRaftGroupIdFromOmServiceId(String omServiceId) {
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
    Parameters parameters = new Parameters();

    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      List<X509Certificate> caList = HAUtils.buildCAX509List(caClient,
          conf.getConfiguration());
      GrpcTlsConfig config = new GrpcTlsConfig(
          caClient.getPrivateKey(), caClient.getCertificate(),
          caList, true);
      GrpcConfigKeys.Server.setTlsConf(parameters, config);
      GrpcConfigKeys.Admin.setTlsConf(parameters, config);
      GrpcConfigKeys.Client.setTlsConf(parameters, config);
      GrpcConfigKeys.TLS.setConf(parameters, config);
    }

    return parameters;
  }

}
