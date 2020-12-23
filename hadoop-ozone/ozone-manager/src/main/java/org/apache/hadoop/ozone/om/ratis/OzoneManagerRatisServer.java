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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ipc.ProtobufRpcEngine.Server;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.OMNodeDetails;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ServiceException;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.ClientId;
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

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ipc.RpcConstants.DUMMY_CLIENT_ID;
import static org.apache.hadoop.ipc.RpcConstants.INVALID_CALL_ID;
import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HA_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_DIR;

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

  private final OzoneManager ozoneManager;
  private final OzoneManagerStateMachine omStateMachine;

  /**
   * Submit request to Ratis server.
   * @param omRequest
   * @return OMResponse - response returned to the client.
   * @throws ServiceException
   */
  public OMResponse submitRequest(OMRequest omRequest) throws ServiceException {
    RaftClientRequest raftClientRequest =
        createWriteRaftClientRequest(omRequest);
    RaftClientReply raftClientReply = submitRequestToRatis(raftClientRequest);
    return processReply(omRequest, raftClientReply);
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
   * Create Write RaftClient request from OMRequest.
   * @param omRequest
   * @return RaftClientRequest - Raft Client request which is submitted to
   * ratis server.
   */
  private RaftClientRequest createWriteRaftClientRequest(OMRequest omRequest) {
    Preconditions.checkArgument(Server.getClientId() != DUMMY_CLIENT_ID);
    Preconditions.checkArgument(Server.getCallId() != INVALID_CALL_ID);
    return new RaftClientRequest(
        ClientId.valueOf(UUID.nameUUIDFromBytes(Server.getClientId())),
        server.getId(), raftGroupId, Server.getCallId(),
        Message.valueOf(OMRatisHelper.convertRequestToByteString(omRequest)),
        RaftClientRequest.writeRequestType(), null);
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


  /**
   * Returns an OM Ratis server.
   * @param conf configuration
   * @param om the OM instance starting the ratis server
   * @param raftGroupIdStr raft group id string
   * @param localRaftPeerId raft peer id of this Ratis server
   * @param addr address of the ratis server
   * @param raftPeers peer nodes in the raft ring
   * @throws IOException
   */
  private OzoneManagerRatisServer(ConfigurationSource conf,
      OzoneManager om,
      String raftGroupIdStr, RaftPeerId localRaftPeerId,
      InetSocketAddress addr, List<RaftPeer> raftPeers)
      throws IOException {
    this.ozoneManager = om;
    this.omRatisAddress = addr;
    this.port = addr.getPort();
    RaftProperties serverProperties = newRaftProperties(conf);

    this.raftPeerId = localRaftPeerId;
    this.raftGroupId = RaftGroupId.valueOf(
        getRaftGroupIdFromOmServiceId(raftGroupIdStr));
    this.raftGroup = RaftGroup.valueOf(raftGroupId, raftPeers);

    StringBuilder raftPeersStr = new StringBuilder();
    for (RaftPeer peer : raftPeers) {
      raftPeersStr.append(", ").append(peer.getAddress());
    }
    LOG.info("Instantiating OM Ratis server with GroupID: {} and " +
        "Raft Peers: {}", raftGroupIdStr, raftPeersStr.toString().substring(2));

    this.omStateMachine = getStateMachine(conf);

    this.server = RaftServer.newBuilder()
        .setServerId(this.raftPeerId)
        .setGroup(this.raftGroup)
        .setProperties(serverProperties)
        .setStateMachine(omStateMachine)
        .build();
  }

  /**
   * Creates an instance of OzoneManagerRatisServer.
   */
  public static OzoneManagerRatisServer newOMRatisServer(
      ConfigurationSource ozoneConf, OzoneManager omProtocol,
      OMNodeDetails omNodeDetails, List<OMNodeDetails> peerNodes)
      throws IOException {

    // RaftGroupId is the omServiceId
    String omServiceId = omNodeDetails.getOMServiceId();

    String omNodeId = omNodeDetails.getOMNodeId();
    RaftPeerId localRaftPeerId = RaftPeerId.getRaftPeerId(omNodeId);

    InetSocketAddress ratisAddr = new InetSocketAddress(
        omNodeDetails.getInetAddress(), omNodeDetails.getRatisPort());

    RaftPeer localRaftPeer = RaftPeer.newBuilder()
        .setId(localRaftPeerId)
        .setAddress(ratisAddr)
        .build();

    List<RaftPeer> raftPeers = new ArrayList<>();
    // Add this Ratis server to the Ratis ring
    raftPeers.add(localRaftPeer);

    for (OMNodeDetails peerInfo : peerNodes) {
      String peerNodeId = peerInfo.getOMNodeId();
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

    return new OzoneManagerRatisServer(ozoneConf, omProtocol, omServiceId,
        localRaftPeerId, ratisAddr, raftPeers);
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
    String storageDir = OzoneManagerRatisServer.getOMRatisDirectory(conf);
    RaftServerConfigKeys.setStorageDir(properties,
        Collections.singletonList(new File(storageDir)));

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
        serverMaxTimeoutDuration, serverMinTimeoutUnit);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        serverMinTimeout);
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        serverMaxTimeout);

    // Set the number of maximum cached segments
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 2);

    // TODO: set max write buffer size

    // Set the ratis leader election timeout
    TimeUnit leaderElectionMinTimeoutUnit =
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    long leaderElectionMinTimeoutduration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        OMConfigKeys.OZONE_OM_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT
            .getDuration(), leaderElectionMinTimeoutUnit);
    final TimeDuration leaderElectionMinTimeout = TimeDuration.valueOf(
        leaderElectionMinTimeoutduration, leaderElectionMinTimeoutUnit);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        leaderElectionMinTimeout);
    long leaderElectionMaxTimeout = leaderElectionMinTimeout.toLong(
        TimeUnit.MILLISECONDS) + 200;
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        TimeDuration.valueOf(leaderElectionMaxTimeout, TimeUnit.MILLISECONDS));

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

  /**
   * Get the local directory where ratis logs will be stored.
   */
  public static String getOMRatisDirectory(ConfigurationSource conf) {
    String storageDir = conf.get(OMConfigKeys.OZONE_OM_RATIS_STORAGE_DIR);

    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = ServerUtils.getDefaultRatisDirectory(conf);
    }
    return storageDir;
  }

  public static String getOMRatisSnapshotDirectory(ConfigurationSource conf) {
    String snapshotDir = conf.get(OZONE_OM_RATIS_SNAPSHOT_DIR);

    // If ratis snapshot directory is not set, fall back to ozone.metadata.dir.
    if (Strings.isNullOrEmpty(snapshotDir)) {
      LOG.warn("{} is not configured. Falling back to {} config",
          OZONE_OM_RATIS_SNAPSHOT_DIR, OZONE_METADATA_DIRS);
      File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
      snapshotDir = Paths.get(metaDirPath.getPath(),
          OM_RATIS_SNAPSHOT_DIR).toString();
    }
    return snapshotDir;
  }

  public TermIndex getLastAppliedTermIndex() {
    return omStateMachine.getLastAppliedTermIndex();
  }

  public RaftGroupId getRaftGroupId() {
    return raftGroupId;
  }
}
