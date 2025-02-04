/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.bucket.server.ratis;

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.RatisConfUtils;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.bucket.server.XbeiverServerSpi;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.OMHANodeDetails;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.AlreadyExistsException;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hadoop.ipc.RpcConstants.DUMMY_CLIENT_ID;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_SEGMENT_SIZE_KEY;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;
import static org.apache.ratis.util.Preconditions.assertTrue;

/**
 *
 */
public class XbeiverServerRatis implements XbeiverServerSpi {

  private static final Logger LOG = LoggerFactory
      .getLogger(XbeiverServerRatis.class);

  private final String name;
  private final RaftServer server;
  private final long requestTimeout;
  private final ConfigurationSource conf;
  private final OzoneManagerRatisServerConfig ratisServerConfig;
//  private final StateContext context;
  private final ConcurrentMap<RaftGroupId, ActivePipelineContext> activePipelines = new ConcurrentHashMap<>();
  private final OzoneManager ozoneManager;
  private int serverPort;
  private int adminPort;
  private int clientPort;
  private int dataStreamPort;
//  private OMNodeDetails omNodeDetails;
  private boolean isStarted = false;
//  private final DatanodeDetails datanodeDetails;
  private final OMNodeDetails omNodeDetails;
  // SHould rename
//  private final DatanodeRatisServerConfig ratisServerConfig;
  private final OMPerformanceMetrics perfMetrics;
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();
  private final ClientId clientId = ClientId.randomId();
  private static final List<Integer> DEFAULT_PRIORITY_LIST =
      new ArrayList<>(
          Collections.nCopies(HddsProtos.ReplicationFactor.THREE_VALUE, 0));
  private final GrpcTlsConfig tlsClientConfig;

  public XbeiverServerRatis(
      OzoneManager ozoneManager,
      ConfigurationSource conf, Parameters parameters)
      throws IOException {
    this.conf = conf;
    this.ozoneManager = ozoneManager;
    ratisServerConfig = conf.getObject(OzoneManagerRatisServerConfig.class);
    OMHANodeDetails omhaNodeDetails =
        OMHANodeDetails.loadOMHAConfig(ozoneManager.getConfiguration());
    this.omNodeDetails = omhaNodeDetails.getLocalNodeDetails();
    String omNodeId = omNodeDetails.getNodeId();
    RaftPeerId raftPeerId = RaftPeerId.valueOf(omNodeId);
    RaftProperties serverProperties = newRaftProperties();
    this.name = getClass().getSimpleName() + "(" + raftPeerId + ")";
    this.server =
        RaftServer.newBuilder().setServerId(raftPeerId)
            .setProperties(serverProperties)
            .setStateMachineRegistry(this::getStateMachine)
            .setParameters(parameters)
            .setOption(RaftStorage.StartupOption.RECOVER)
            .build();
    this.requestTimeout = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT.getDuration(),
        TimeUnit.MILLISECONDS);

    this.perfMetrics = ozoneManager.getPerfMetrics();
    CertificateClient certClient = ozoneManager.getCertificateClient();
    SecurityConfig secConf = new SecurityConfig(conf);
    if (certClient != null && secConf.isGrpcTlsEnabled()) {
      tlsClientConfig = new GrpcTlsConfig(
          certClient.getKeyManager(),
          certClient.getTrustManager(), true);
    } else {
      tlsClientConfig = null;
    }
  }

  private static void setRaftLeaderElectionProperties(RaftProperties properties, ConfigurationSource conf) {
    // Disable/enable the pre vote feature in Ratis
    RaftServerConfigKeys.LeaderElection.setPreVote(properties, conf.getBoolean(
        OMConfigKeys.OZONE_OM_RATIS_SERVER_ELECTION_PRE_VOTE,
        OMConfigKeys.OZONE_OM_RATIS_SERVER_ELECTION_PRE_VOTE_DEFAULT));
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

  public static XbeiverServerRatis newXbeiverServerRatis(OzoneManager ozoneManager,
                                                         ConfigurationSource conf
                                                         ) throws IOException {
    CertificateClient caClient = ozoneManager.getCertificateClient();
    OzoneConfiguration configuration = ozoneManager.getConfiguration();
    Parameters parameters = createTlsParameters(
        new SecurityConfig(configuration), caClient);

    return new XbeiverServerRatis(ozoneManager, conf, parameters);
  }

  @Override
  public void start() throws IOException {
    if (!isStarted) {
      LOG.info("Starting {}", name);

      server.start();

      RaftServerRpc serverRpc = server.getServerRpc();
      clientPort = serverRpc.getClientServerAddress().getPort();
      adminPort = serverRpc.getAdminServerAddress().getPort();
      serverPort = serverRpc.getInetSocketAddress().getPort();
//      if (streamEnable) {
//        DataStreamServerRpc dataStreamServerRpc =
//            server.getDataStreamServerRpc();
//        dataStreamPort = getRealPort(dataStreamServerRpc.getInetSocketAddress(),
//            DatanodeDetails.Port.Name.RATIS_DATASTREAM);
//      }
      isStarted = true;
    }
  }

  @Override
  public void stop() {
    if (isStarted) {
      try {
        LOG.info("Closing {}", name);
        // shutdown server before the executors as while shutting down,
        // some of the tasks would be executed using the executors.
        server.close();
        isStarted = false;
      } catch (IOException e) {
        LOG.error("Failed to close {}.", name, e);
      }
    }
  }

  @Override
  public OzoneManagerProtocolProtos.OMResponse submitRequest(OzoneManagerProtocolProtos.OMRequest omRequest)
      throws ServiceException {
//    , HddsProtos.PipelineID pipelineID
//    Span span = TracingUtil
//        .importAndCreateSpan(
//            "XceiverServerRatis." + omRequest.getCmdType().name(),
//            omRequest.getTraceID());
//    try (Scope ignored = GlobalTracer.get().activateSpan(span)) {
//
//      RaftClientRequest raftClientRequest =
//          createRaftClientRequest(request, pipelineID,
//              RaftClientRequest.writeRequestType());
//      RaftClientReply reply;
//      try {
//        reply = server.submitClientRequestAsync(raftClientRequest)
//            .get(requestTimeout, TimeUnit.MILLISECONDS);
//
//
//      } catch (ExecutionException | TimeoutException e) {
//        throw new ServiceException(new IOException(e.getMessage(), e));
//      } catch (InterruptedException e) {
//        Thread.currentThread().interrupt();
//        throw new ServiceException(new IOException(e.getMessage(), e));
//      } catch (IOException e) {
//        throw new ServiceException( new RuntimeException(e));
//      }
//      processReply(reply);
//    } finally {
//      span.finish();
//    }

    if (ozoneManager.getPrepareState().requestAllowed(omRequest.getCmdType())) {
      String bucket;
      String keyName;
      if (omRequest.getCreateFileRequest() != null) {
        bucket = omRequest.getCreateFileRequest().getKeyArgs().getBucketName();
      } else if (omRequest.getCommitKeyRequest() != null) {
        bucket = omRequest.getCreateFileRequest().getKeyArgs().getBucketName();
      } else {
        throw new RuntimeException("Usupported request type");
      }

      String uuidString = bucket;
      RaftGroupId raftGroupId = RaftGroupId.valueOf(
          UUID.nameUUIDFromBytes(uuidString.getBytes(StandardCharsets.UTF_8))
      );
      RaftClientRequest raftClientRequest = createRaftRequest(omRequest, raftGroupId);

      List<RaftPeer> peers = new ArrayList<>(ozoneManager.getOmRatisServer().getRaftGroup().getPeers());
      List<Integer> priorityList = ozoneManager.getOmRatisServer().getRaftGroup().getPeers().stream()
          .map(RaftPeer::getPriority)
          .collect(Collectors.toList());
      if (!isExist(raftGroupId)) {
        final RaftGroup group = RatisHelper.newRawRaftGroup(raftGroupId, peers, priorityList);
        RaftPeerId currentNodeRaftPeerId = ozoneManager.getOmRatisServer().getRaftPeerId();

        try {
          addGroup(raftGroupId, peers, priorityList);
          peers.stream().filter(
                  d -> !d.getId().equals(currentNodeRaftPeerId))
              .forEach(peer -> {
                try (RaftClient client = RatisHelper.newRaftClient(conf).apply(peer, tlsClientConfig)) {
                  client.getGroupManagementApi(peer.getId()).add(group);
                } catch (AlreadyExistsException ae) {
                  // do not log
                } catch (IOException ioe) {
                  LOG.warn("Add group failed for {}", peer, ioe);
                }
              });
        } catch (Exception e) {
          throw new ServiceException(e);
        }
      }
      RaftClientReply raftClientReply;
      try {
        raftClientReply = server.submitClientRequestAsync(raftClientRequest).get(requestTimeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
        throw new ServiceException(new RuntimeException(e));
      }
      return createOmResponse(omRequest, raftClientReply);
    } else {
      LOG.info("Rejecting write request on OM {} because it is in prepare " +
               "mode: {}", ozoneManager.getOMNodeId(),
          omRequest.getCmdType().name());

      String message = "Cannot apply write request " +
                       omRequest.getCmdType().name() + " when OM is in prepare mode.";
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse = OzoneManagerProtocolProtos.OMResponse.newBuilder()
          .setMessage(message)
          .setStatus(OzoneManagerProtocolProtos.Status.NOT_SUPPORTED_OPERATION_WHEN_PREPARED)
          .setCmdType(omRequest.getCmdType())
          .setTraceID(omRequest.getTraceID())
          .setSuccess(false);
      return omResponse.build();
    }
  }

  private RaftClientRequest createRaftRequest(OzoneManagerProtocolProtos.OMRequest omRequest, RaftGroupId raftGroupId) {
    return captureLatencyNs(
        perfMetrics.getCreateRatisRequestLatencyNs(),
        () -> createRaftRequestImpl(omRequest, raftGroupId));
  }

  private OzoneManagerProtocolProtos.OMResponse createOmResponse(OzoneManagerProtocolProtos.OMRequest omRequest,
                                                                 RaftClientReply raftClientReply) throws
      ServiceException {
    return captureLatencyNs(
        perfMetrics.getCreateOmResponseLatencyNs(),
        () -> createOmResponseImpl(omRequest, raftClientReply));
  }

  private RaftClientRequest createRaftClientRequest(
      ContainerProtos.ContainerCommandRequestProto request, HddsProtos.PipelineID pipelineID,
      RaftClientRequest.Type type) {
    return RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(server.getId())
        .setGroupId(
            RaftGroupId.valueOf(
                PipelineID.getFromProtobuf(pipelineID).getId()))
        .setCallId(nextCallId())
        .setMessage(ContainerCommandRequestMessage.toMessage(request, null))
        .setType(type)
        .build();
  }

  /**
   * Process the raftClientReply and return OMResponse.
   * @param omRequest
   * @param reply
   * @return OMResponse - response which is returned to client.
   * @throws ServiceException
   */
  private OzoneManagerProtocolProtos.OMResponse createOmResponseImpl(OzoneManagerProtocolProtos.OMRequest omRequest,
                                                                     RaftClientReply reply) throws ServiceException {
    // NotLeader exception is thrown only when the raft server to which the
    // request is submitted is not the leader. This can happen first time
    // when client is submitting request to OM.

    if (!reply.isSuccess()) {
      NotLeaderException notLeaderException = reply.getNotLeaderException();
      if (notLeaderException != null) {
        throw new ServiceException(
            OMNotLeaderException.convertToOMNotLeaderException(
                notLeaderException, RaftPeerId.getRaftPeerId(omNodeDetails.getNodeId())));
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
        OzoneManagerProtocolProtos.OMResponse.Builder omResponse = OzoneManagerProtocolProtos.OMResponse.newBuilder()
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

  private OzoneManagerProtocolProtos.OMResponse getOMResponse(RaftClientReply reply) throws ServiceException {
    try {
      return OMRatisHelper.getOMResponseFromRaftClientReply(reply);
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

  private void processReply(RaftClientReply reply) throws IOException {
    // NotLeader exception is thrown only when the raft server to which the
    // request is submitted is not the leader. The request will be rejected
    // and will eventually be executed once the request comes via the leader
    // node.
    NotLeaderException notLeaderException = reply.getNotLeaderException();
    if (notLeaderException != null) {
      throw notLeaderException;
    }
    StateMachineException stateMachineException =
        reply.getStateMachineException();
    if (stateMachineException != null) {
      throw stateMachineException;
    }
  }

  /**
   * Create Write RaftClient request from OMRequest.
   * @param omRequest
   * @return RaftClientRequest - Raft Client request which is submitted to
   * ratis server.
   */
  private RaftClientRequest createRaftRequestImpl(OzoneManagerProtocolProtos.OMRequest omRequest,
                                                  RaftGroupId raftGroupId) {
    return RaftClientRequest.newBuilder()
        .setClientId(getClientId())
        .setServerId(server.getId())
        .setGroupId(raftGroupId)
        .setCallId(ProtobufRpcEngine.Server.getCallId())
        .setMessage(
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  private ClientId getClientId() {
    final byte[] clientIdBytes = ProtobufRpcEngine.Server.getClientId();
    if (!ozoneManager.isTestSecureOmFlag()) {
      Preconditions.checkArgument(clientIdBytes != DUMMY_CLIENT_ID);
    }
    return ClientId.valueOf(UUID.nameUUIDFromBytes(clientIdBytes));
  }

  @Override
  public boolean isExist(RaftGroupId raftGroupId) {
    return activePipelines.containsKey(raftGroupId);
  }

  @Override
  public void addGroup(RaftGroupId groupId,
                       List<RaftPeer> peers) throws IOException {
    if (peers.size() == getDefaultPriorityList().size()) {
      addGroup(groupId, peers, getDefaultPriorityList());
    } else {
      addGroup(groupId, peers,
          new ArrayList<>(Collections.nCopies(peers.size(), 0)));
    }
  }

  @Override
  public void addGroup(RaftGroupId groupId,
                       List<RaftPeer> peers,
                       List<Integer> priorityList) throws IOException {
    final RaftGroup group =
        RatisHelper.newRawRaftGroup(groupId, peers, priorityList);
    GroupManagementRequest request = GroupManagementRequest.newAdd(
        clientId, server.getId(), nextCallId(), group);

    RaftClientReply reply;
    LOG.debug("Received addGroup request for group {}", groupId);

    try {
      reply = server.groupManagement(request);
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
    processReply(reply);
    LOG.info("Created group {}", groupId);
  }

  @Override
  public void removeGroup(HddsProtos.PipelineID pipelineId) throws IOException {
    XbeiverServerSpi.super.removeGroup(pipelineId);
  }

  private OzoneBucketStateMachine getStateMachine(RaftGroupId gid) {
    return new OzoneBucketStateMachine(this, ozoneManager);
  }

  public RaftProperties newRaftProperties() {
    final RaftProperties properties = new RaftProperties();

    // Set rpc type
    final RpcType rpc = setRpcType(properties);

    // set raft segment size
    final int logAppenderBufferByteLimit = setRaftSegmentAndWriteBufferSize(properties);

    // set grpc message size max
    final int max = Math.max(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE, logAppenderBufferByteLimit);
    RatisConfUtils.Grpc.setMessageSizeMax(properties, max);

    // set raft segment pre-allocated size
    setRaftSegmentPreallocatedSize(properties);

    // setup ratis stream if datastream is enabled
//    if (streamEnable) {
//      setUpRatisStream(properties);
//    }

    // Set Ratis State Machine Data configurations
    setStateMachineDataConfigurations(properties);

    // set timeout for a retry cache entry
//    setTimeoutForRetryCache(properties);

    // Set the ratis leader election timeout
//    setRatisLeaderElectionTimeout(properties);

    // Set the maximum cache segments
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 2);

    // Disable the pre vote feature in Ratis
//    RaftServerConfigKeys.LeaderElection.setPreVote(properties,
//        ratisServerConfig.isPreVoteEnabled());
    setRaftLeaderElectionProperties(properties, conf);
    // Set the ratis storage directory
    Collection<String> storageDirPaths =
        HddsServerUtil.getOzoneDatanodeRatisDirectory(conf);
    List<File> storageDirs = new ArrayList<>(storageDirPaths.size());
    storageDirPaths.forEach(d -> storageDirs.add(new File(d)));

    RaftServerConfigKeys.setStorageDir(properties, storageDirs);

    // Set the ratis port number
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Admin.setPort(properties, adminPort);
      GrpcConfigKeys.Client.setPort(properties, clientPort);
      GrpcConfigKeys.Server.setPort(properties, serverPort);
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, serverPort);
    }

    long snapshotThreshold =
        conf.getLong(OzoneConfigKeys.HDDS_RATIS_SNAPSHOT_THRESHOLD_KEY,
            OzoneConfigKeys.HDDS_RATIS_SNAPSHOT_THRESHOLD_DEFAULT);
    RaftServerConfigKeys.Snapshot.
        setAutoTriggerEnabled(properties, true);
    RaftServerConfigKeys.Snapshot.
        setAutoTriggerThreshold(properties, snapshotThreshold);

    // Set the limit on num/ bytes of pending requests a Ratis leader can hold
//    setPendingRequestsLimits(properties);

    int logQueueNumElements =
        conf.getInt(OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS,
            OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS_DEFAULT);
    final long logQueueByteLimit = (long) conf.getStorageSize(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.setQueueElementLimit(
        properties, logQueueNumElements);
    RaftServerConfigKeys.Log.setQueueByteLimit(properties,
        SizeInBytes.valueOf(logQueueByteLimit));

    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties,
        false);

    int purgeGap = conf.getInt(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_PURGE_GAP,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_PURGE_GAP_DEFAULT);
    RaftServerConfigKeys.Log.setPurgeGap(properties, purgeGap);

    //Set the number of Snapshots Retained.
//    RatisServerConfiguration ratisServerConfiguration =
//        conf.getObject(RatisServerConfiguration.class);
//    int numSnapshotsRetained =
//        ratisServerConfiguration.getNumSnapshotsRetained();
//    RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties,
//        numSnapshotsRetained);

    // Set properties starting with prefix raft.server
    RatisHelper.createRaftServerProperties(conf, properties);

    return properties;
  }

  private RpcType setRpcType(RaftProperties properties) {
    final String rpcType = conf.get(
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_DEFAULT);
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(rpcType);
    RatisHelper.setRpcType(properties, rpc);
    return rpc;
  }

  private int setRaftSegmentAndWriteBufferSize(RaftProperties properties) {
    final int logAppenderQueueNumElements = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT);
    final int logAppenderQueueByteLimit = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);

    final long raftSegmentSize = (long) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_DEFAULT,
        StorageUnit.BYTES);
    final long raftSegmentBufferSize = logAppenderQueueByteLimit + 8;

    assertTrue(raftSegmentBufferSize <= raftSegmentSize,
        () -> HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT + " = "
              + logAppenderQueueByteLimit
              + " must be <= (" + HDDS_CONTAINER_RATIS_SEGMENT_SIZE_KEY + " - 8"
              + " = " + (raftSegmentSize - 8) + ")");

    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties,
        logAppenderQueueNumElements);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties,
        SizeInBytes.valueOf(logAppenderQueueByteLimit));
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf(raftSegmentSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(properties,
        SizeInBytes.valueOf(raftSegmentBufferSize));
    return logAppenderQueueByteLimit;
  }

  private void setRaftSegmentPreallocatedSize(RaftProperties properties) {
    final long raftSegmentPreallocatedSize = (long) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
  }

  private void setStateMachineDataConfigurations(RaftProperties properties) {
    // set the configs enable and set the stateMachineData sync timeout
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, true);

    TimeUnit timeUnit = OzoneConfigKeys.
        HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT_DEFAULT.getUnit();
    long duration = conf.getTimeDuration(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT,
        OzoneConfigKeys.
            HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration dataSyncTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    RaftServerConfigKeys.Log.StateMachineData
        .setSyncTimeout(properties, dataSyncTimeout);
    // typically a pipeline close will be initiated after a node failure
    // timeout from Ratis in case a follower does not respond.
    // By this time, all the writeStateMachine calls should be stopped
    // and IOs should fail.
    // Even if the leader is not able to complete write calls within
    // the timeout seconds, it should just fail the operation and trigger
    // pipeline close. failing the writeStateMachine call with limited retries
    // will ensure even the leader initiates a pipeline close if its not
    // able to complete write in the timeout configured.

    // NOTE : the default value for the retry count in ratis is -1,
    // which means retry indefinitely.
//    final int syncTimeoutRetryDefault = (int) ratisServerConfig.getFollowerSlownessTimeout() /
//                                        dataSyncTimeout.toIntExact(TimeUnit.MILLISECONDS);
//    int numSyncRetries = conf.getInt(
//        OzoneConfigKeys.HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES,
//        syncTimeoutRetryDefault);
//    RaftServerConfigKeys.Log.StateMachineData.setSyncTimeoutRetry(properties,
//        numSyncRetries);

    // Enable the StateMachineCaching
    // By enabling caching, the state machine data (e.g. write chunk data)
    // will not be cached in Ratis log cache. The caching
    // responsibility is deferred to the StateMachine implementation itself.
    // ContainerStateMachine contains stateMachineDataCache that stores
    // write chunk data for each log entry index.
    //
    // Note that in Ratis, the state machine data is never stored as
    // part of the persisted Raft log entry. This means that the state
    // machine data (in this case, the write chunk data) is only stored in the
    // stateMachineDataCache until it's persisted in datanode storage
    // (See ContainerStateMachine#writeStateMachineData)
    //
    // This requires ContainerStateMachine to implements additional mechanisms
    // such as returning the state machine data in StateMachine#read to
    // read back the state machine data that will be sent to the Ratis
    // followers.
    RaftServerConfigKeys.Log.StateMachineData.setCachingEnabled(
        properties, true);
  }

//  void notifyGroupAdd(RaftGroupId gid) {
//    activePipelines.put(gid, new ActivePipelineContext(false, false));
//    sendPipelineReport();
//  }

//  private void sendPipelineReport() {
//    if (context !=  null) {
//      // TODO: Send IncrementalPipelineReport instead of full PipelineReport
//      context.addIncrementalReport(
//          context.getParent().getContainer().getPipelineReport());
//      context.getParent().triggerHeartbeat();
//    }
//  }

  private static class ActivePipelineContext {
    /**
     * The current datanode is the current leader of the pipeline.
     */
    private final boolean isPipelineLeader;
    /**
     * The heartbeat containing pipeline close action has been triggered.
     */
    private final boolean isPendingClose;

    ActivePipelineContext(boolean isPipelineLeader, boolean isPendingClose) {
      this.isPipelineLeader = isPipelineLeader;
      this.isPendingClose = isPendingClose;
    }

    public boolean isPipelineLeader() {
      return isPipelineLeader;
    }

    public boolean isPendingClose() {
      return isPendingClose;
    }
  }

  // For gRPC server running DN container service with gPRC TLS
  // In summary:
  // authenticate from server to client is via TLS.
  // authenticate from client to server is via block token (or container token).
  // DN Ratis server act as both SSL client and server and we must pass TLS
  // configuration for both.
  private static Parameters createTlsParameters(SecurityConfig conf,
                                                CertificateClient caClient) throws IOException {
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      GrpcTlsConfig serverConfig = new GrpcTlsConfig(
          caClient.getKeyManager(),
          caClient.getTrustManager(), true);
      GrpcTlsConfig clientConfig = new GrpcTlsConfig(
          caClient.getKeyManager(),
          caClient.getTrustManager(), false);
      return RatisHelper.setServerTlsConf(serverConfig, clientConfig);
    }

    return null;
  }

  public void notifyGroupRemove(RaftGroupId gid) {
    // Remove Group ID entry from the active pipeline map
    activePipelines.remove(gid);
  }

  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  void handleLeaderChangedNotification(RaftGroupMemberId groupMemberId,
                                       RaftPeerId raftPeerId1) {
    LOG.info("Leader change notification received for group: {} with new " +
             "leaderId: {}", groupMemberId.getGroupId(), raftPeerId1);
    // Save the reported leader to be sent with the report to SCM
    final boolean leaderForGroup = server.getId().equals(raftPeerId1);
    activePipelines.compute(groupMemberId.getGroupId(),
        (key, value) -> value == null ? new ActivePipelineContext(leaderForGroup, false) :
            new ActivePipelineContext(leaderForGroup, value.isPendingClose()));
//    if (context != null && leaderForGroup) {
      // Publish new report from leader
//      sendPipelineReport();
//    }
  }

//  private void sendPipelineReport() {
//    if (context !=  null) {
//      // TODO: Send IncrementalPipelineReport instead of full PipelineReport
//      context.addIncrementalReport(
//          context.getParent().getContainer().getPipelineReport());
//      context.getParent().triggerHeartbeat();
//    }
//  }

  private void triggerPipelineClose(RaftGroupId groupId, String detail,
                                    StorageContainerDatanodeProtocolProtos.ClosePipelineInfo.Reason reasonCode) {
    PipelineID pipelineID = PipelineID.valueOf(groupId.getUuid());
    StorageContainerDatanodeProtocolProtos.ClosePipelineInfo.Builder closePipelineInfo =
        StorageContainerDatanodeProtocolProtos.ClosePipelineInfo.newBuilder()
            .setPipelineID(pipelineID.getProtobuf())
            .setReason(reasonCode)
            .setDetailedReason(detail);

    StorageContainerDatanodeProtocolProtos.PipelineAction
        action = StorageContainerDatanodeProtocolProtos.PipelineAction.newBuilder()
        .setClosePipeline(closePipelineInfo)
        .setAction(StorageContainerDatanodeProtocolProtos.PipelineAction.Action.CLOSE)
        .build();
//    if (context != null) {
//      context.addPipelineActionIfAbsent(action);
    if (!activePipelines.get(groupId).isPendingClose()) {
      // if pipeline close action has not been triggered before, we need trigger pipeline close immediately to
      // prevent SCM to allocate blocks on the failed pipeline
//        context.getParent().triggerHeartbeat();
      activePipelines.computeIfPresent(groupId,
          (key, value) -> new ActivePipelineContext(value.isPipelineLeader(), true));
//      }
//    }
      LOG.error("pipeline Action {} on pipeline {}.Reason : {}",
          action.getAction(), pipelineID,
          action.getClosePipeline().getDetailedReason());
    }
  }

  /**
   * The fact that the snapshot contents cannot be used to actually catch up
   * the follower, it is the reason to initiate close pipeline and
   * not install the snapshot. The follower will basically never be able to
   * catch up.
   *
   * @param groupId raft group information
   * @param roleInfoProto information about the current node role and
   *                      rpc delay information.
   * @param firstTermIndexInLog After the snapshot installation is complete,
   * return the last included term index in the snapshot.
   */
  void handleInstallSnapshotFromLeader(RaftGroupId groupId,
                                       RaftProtos.RoleInfoProto roleInfoProto,
                                       TermIndex firstTermIndexInLog) {
    LOG.warn("handleInstallSnapshotFromLeader for firstTermIndexInLog={}, terminating pipeline: {}",
        firstTermIndexInLog, groupId);
    handlePipelineFailure(groupId, roleInfoProto, "install snapshot notification");
  }

  private void handlePipelineFailure(RaftGroupId groupId, RaftProtos.RoleInfoProto roleInfoProto, String reason) {
    final RaftPeerId raftPeerId = RaftPeerId.valueOf(roleInfoProto.getSelf().getId());
    org.apache.ratis.util.Preconditions.assertEquals(getServer().getId(), raftPeerId, "raftPeerId");
    final StringBuilder b = new StringBuilder()
        .append(name).append(" with datanodeId ").append(RatisHelper.toDatanodeId(raftPeerId))
        .append("handlePipelineFailure ").append(" for ").append(reason)
        .append(": ").append(roleInfoProto.getRole())
        .append(" elapsed time=").append(roleInfoProto.getRoleElapsedTimeMs()).append("ms");

    switch (roleInfoProto.getRole()) {
    case CANDIDATE:
      final long lastLeaderElapsedTime = roleInfoProto.getCandidateInfo().getLastLeaderElapsedTimeMs();
      b.append(", lastLeaderElapsedTime=").append(lastLeaderElapsedTime).append("ms");
      break;
    case FOLLOWER:
      b.append(", outstandingOp=").append(roleInfoProto.getFollowerInfo().getOutstandingOp());
      break;
    case LEADER:
//      final long followerSlownessTimeoutMs = ratisServerConfig.getFollowerSlownessTimeout();
      final long followerSlownessTimeoutMs = ratisServerConfig.getRetryCacheTimeout();
      for (RaftProtos.ServerRpcProto follower : roleInfoProto.getLeaderInfo().getFollowerInfoList()) {
        final long lastRpcElapsedTimeMs = follower.getLastRpcElapsedTimeMs();
        final boolean slow = lastRpcElapsedTimeMs > followerSlownessTimeoutMs;
        final RaftPeerId followerId = RaftPeerId.valueOf(follower.getId().getId());
        b.append("\n  Follower ").append(followerId)
            .append(" with datanodeId ").append(RatisHelper.toDatanodeId(followerId))
            .append(" is ").append(slow ? "slow" : " responding")
            .append(" with lastRpcElapsedTime=").append(lastRpcElapsedTimeMs).append("ms");
      }
      break;
    default:
      throw new IllegalStateException("Unexpected role " + roleInfoProto.getRole());
    }

    triggerPipelineClose(groupId, b.toString(),
        StorageContainerDatanodeProtocolProtos.ClosePipelineInfo.Reason.PIPELINE_FAILED);
  }

  public RaftServer getServer() {
    return server;
  }

  /**
   * @return list of default priority
   */
  public static List<Integer> getDefaultPriorityList() {
    return DEFAULT_PRIORITY_LIST;
  }
}
