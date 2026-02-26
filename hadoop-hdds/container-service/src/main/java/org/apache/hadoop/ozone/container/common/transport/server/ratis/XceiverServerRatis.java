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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.apache.hadoop.hdds.HDDSVersion.SEPARATE_RATIS_PORTS_AVAILABLE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_SEGMENT_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_RATIS_LEADER_FIRST_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY;
import static org.apache.ratis.util.Preconditions.assertTrue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.RatisConfUtils;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a ratis server endpoint that acts as the communication layer for
 * Ozone containers.
 */
public final class XceiverServerRatis implements XceiverServerSpi {
  private static final Logger LOG = LoggerFactory.getLogger(XceiverServerRatis.class);

  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();
  private static final List<Integer> DEFAULT_PRIORITY_LIST =
      new ArrayList<>(Collections.nCopies(HddsProtos.ReplicationFactor.THREE_VALUE, 0));

  private int serverPort;
  private int adminPort;
  private int clientPort;

  private final RaftServer server;
  private final String name;
  private final List<ThreadPoolExecutor> chunkExecutors;
  private final ContainerDispatcher dispatcher;
  private final ContainerController containerController;
  private final ClientId clientId = ClientId.randomId();
  private final StateContext context;
  private boolean isStarted = false;
  private final DatanodeDetails datanodeDetails;
  private final ConfigurationSource conf;
  // TODO: Remove the gids set when Ratis supports an api to query active
  // pipelines
  private final ConcurrentMap<RaftGroupId, ActivePipelineContext> activePipelines = new ConcurrentHashMap<>();
  // Timeout used while calling submitRequest directly.
  private final long requestTimeout;
  private final boolean shouldDeleteRatisLogDirectory;
  private final boolean streamEnable;
  private final DatanodeRatisServerConfig ratisServerConfig;
  private final HddsDatanodeService datanodeService;

  private static class ActivePipelineContext {
    /** The current datanode is the current leader of the pipeline. */
    private final boolean isPipelineLeader;
    /** The heartbeat containing pipeline close action has been triggered. */
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

  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  private XceiverServerRatis(HddsDatanodeService hddsDatanodeService, DatanodeDetails dd,
      ContainerDispatcher dispatcher, ContainerController containerController,
      StateContext context, ConfigurationSource conf, Parameters parameters)
      throws IOException {
    this.conf = conf;
    Objects.requireNonNull(dd, "DatanodeDetails == null");
    datanodeService = hddsDatanodeService;
    datanodeDetails = dd;
    ratisServerConfig = conf.getObject(DatanodeRatisServerConfig.class);
    assignPorts();
    this.streamEnable = conf.getBoolean(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED_DEFAULT);
    this.context = context;
    this.dispatcher = dispatcher;
    this.containerController = containerController;
    String threadNamePrefix = datanodeDetails.threadNamePrefix();
    chunkExecutors = createChunkExecutors(conf, threadNamePrefix);
    shouldDeleteRatisLogDirectory =
        ratisServerConfig.shouldDeleteRatisLogDirectory();

    RaftProperties serverProperties = newRaftProperties();
    final RaftPeerId raftPeerId = RatisHelper.toRaftPeerId(dd);
    this.name = getClass().getSimpleName() + "(" + raftPeerId + ")";
    this.server =
        RaftServer.newBuilder().setServerId(raftPeerId)
            .setProperties(serverProperties)
            .setStateMachineRegistry(this::getStateMachine)
            .setParameters(parameters)
            .setOption(RaftStorage.StartupOption.RECOVER)
            .build();
    this.requestTimeout = conf.getTimeDuration(
        HddsConfigKeys.HDDS_DATANODE_RATIS_SERVER_REQUEST_TIMEOUT,
        HddsConfigKeys.HDDS_DATANODE_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  private void assignPorts() {
    clientPort = determinePort(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT_DEFAULT);

    if (HDDSVersion.deserialize(datanodeDetails.getInitialVersion())
        .compareTo(SEPARATE_RATIS_PORTS_AVAILABLE) >= 0) {
      adminPort = determinePort(
          OzoneConfigKeys.HDDS_CONTAINER_RATIS_ADMIN_PORT,
          OzoneConfigKeys.HDDS_CONTAINER_RATIS_ADMIN_PORT_DEFAULT);
      serverPort = determinePort(
          OzoneConfigKeys.HDDS_CONTAINER_RATIS_SERVER_PORT,
          OzoneConfigKeys.HDDS_CONTAINER_RATIS_SERVER_PORT_DEFAULT);
    } else {
      adminPort = clientPort;
      serverPort = clientPort;
    }
  }

  private int determinePort(String key, int defaultValue) {
    boolean randomPort = conf.getBoolean(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT_DEFAULT);
    return randomPort ? 0 : conf.getInt(key, defaultValue);
  }

  private ContainerStateMachine getStateMachine(RaftGroupId gid) {
    return new ContainerStateMachine(datanodeService, gid, dispatcher, containerController,
        chunkExecutors, this, conf, datanodeDetails.threadNamePrefix());
  }

  private void setUpRatisStream(RaftProperties properties) {
    // set the datastream config
    final int requestedPort;
    if (conf.getBoolean(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT,
        OzoneConfigKeys.
            HDDS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT_DEFAULT)) {
      requestedPort = 0;
    } else {
      requestedPort = conf.getInt(
          OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_PORT,
          OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_PORT_DEFAULT);
    }
    RatisHelper.enableNettyStreaming(properties);
    NettyConfigKeys.DataStream.setPort(properties, requestedPort);
    int dataStreamAsyncRequestThreadPoolSize =
        ratisServerConfig.getStreamRequestThreads();
    RaftServerConfigKeys.DataStream.setAsyncRequestThreadPoolSize(properties,
        dataStreamAsyncRequestThreadPoolSize);
    int dataStreamClientPoolSize = ratisServerConfig.getClientPoolSize();
    RaftServerConfigKeys.DataStream.setClientPoolSize(properties,
        dataStreamClientPoolSize);
  }

  @SuppressWarnings("checkstyle:methodlength")
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
    if (streamEnable) {
      setUpRatisStream(properties);
    }

    // Set Ratis State Machine Data configurations
    setStateMachineDataConfigurations(properties);

    // set timeout for a retry cache entry
    setTimeoutForRetryCache(properties);

    // Set the ratis leader election timeout
    setRatisLeaderElectionTimeout(properties);

    // Set the maximum cache segments
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 2);

    // Disable the pre vote feature in Ratis
    RaftServerConfigKeys.LeaderElection.setPreVote(properties,
        ratisServerConfig.isPreVoteEnabled());

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
    setPendingRequestsLimits(properties);

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
    RatisServerConfiguration ratisServerConfiguration =
        conf.getObject(RatisServerConfiguration.class);
    int numSnapshotsRetained =
        ratisServerConfiguration.getNumSnapshotsRetained();
    RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties,
        numSnapshotsRetained);

    // Set properties starting with prefix raft.server
    RatisHelper.createRaftServerProperties(conf, properties);

    return properties;
  }

  private void setRatisLeaderElectionTimeout(RaftProperties properties) {
    TimeUnit leaderElectionMinTimeoutUnit =
        OzoneConfigKeys.
            HDDS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    long duration = conf.getTimeDuration(
        OzoneConfigKeys.HDDS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.
            HDDS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT
            .getDuration(), leaderElectionMinTimeoutUnit);
    final TimeDuration leaderElectionMinTimeout =
        TimeDuration.valueOf(duration, leaderElectionMinTimeoutUnit);
    RaftServerConfigKeys.Rpc
        .setTimeoutMin(properties, leaderElectionMinTimeout);
    long leaderElectionMaxTimeout =
        leaderElectionMinTimeout.toLong(TimeUnit.MILLISECONDS) + 200;
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        TimeDuration.valueOf(leaderElectionMaxTimeout, TimeUnit.MILLISECONDS));
    RatisHelper.setFirstElectionTimeoutDuration(
        conf, properties, HDDS_RATIS_LEADER_FIRST_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY);
  }

  private void setTimeoutForRetryCache(RaftProperties properties) {
    TimeUnit timeUnit =
        OzoneConfigKeys.HDDS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    long duration = conf.getTimeDuration(
        OzoneConfigKeys.HDDS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.HDDS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration retryCacheTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    RaftServerConfigKeys.RetryCache
        .setExpiryTime(properties, retryCacheTimeout);
  }

  private void setRaftSegmentPreallocatedSize(RaftProperties properties) {
    final long raftSegmentPreallocatedSize = (long) conf.getStorageSize(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT,
        StorageUnit.BYTES);
    RaftServerConfigKeys.Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
  }

  private int setRaftSegmentAndWriteBufferSize(RaftProperties properties) {
    final int logAppenderQueueNumElements = conf.getInt(
        HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS,
        HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT);
    final int logAppenderQueueByteLimit = (int) conf.getStorageSize(
        HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);

    final long raftSegmentSize = (long) conf.getStorageSize(
        HDDS_CONTAINER_RATIS_SEGMENT_SIZE_KEY,
        HDDS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT,
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
    final int syncTimeoutRetryDefault = (int) ratisServerConfig.getFollowerSlownessTimeout() /
        dataSyncTimeout.toIntExact(TimeUnit.MILLISECONDS);
    int numSyncRetries = conf.getInt(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES,
        syncTimeoutRetryDefault);
    RaftServerConfigKeys.Log.StateMachineData.setSyncTimeoutRetry(properties,
        numSyncRetries);

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

  private RpcType setRpcType(RaftProperties properties) {
    final String rpcType = conf.get(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_KEY,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(rpcType);
    RatisHelper.setRpcType(properties, rpc);
    return rpc;
  }

  private void setPendingRequestsLimits(RaftProperties properties) {
    long pendingRequestsBytesLimit = (long) conf.getStorageSize(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT,
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    final int pendingRequestsMegaBytesLimit =
        HddsUtils.roundupMb(pendingRequestsBytesLimit);
    RaftServerConfigKeys.Write.setByteLimit(properties, SizeInBytes
        .valueOf(pendingRequestsMegaBytesLimit, TraditionalBinaryPrefix.MEGA));
  }

  public static XceiverServerRatis newXceiverServerRatis(HddsDatanodeService hddsDatanodeService,
      DatanodeDetails datanodeDetails, ConfigurationSource ozoneConf,
      ContainerDispatcher dispatcher, ContainerController containerController,
      CertificateClient caClient, StateContext context) throws IOException {
    Parameters parameters = createTlsParameters(
        new SecurityConfig(ozoneConf), caClient);

    return new XceiverServerRatis(hddsDatanodeService, datanodeDetails, dispatcher,
        containerController, context, ozoneConf, parameters);
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

  @Override
  public void start() throws IOException {
    if (!isStarted) {
      LOG.info("Starting {}", name);
      for (ThreadPoolExecutor executor : chunkExecutors) {
        executor.prestartAllCoreThreads();
      }
      server.start();

      RaftServerRpc serverRpc = server.getServerRpc();
      clientPort = updateDatanodePort(serverRpc.getClientServerAddress(),
          Port.Name.RATIS);
      adminPort = updateDatanodePort(serverRpc.getAdminServerAddress(),
          Port.Name.RATIS_ADMIN);
      serverPort = updateDatanodePort(serverRpc.getInetSocketAddress(),
          Port.Name.RATIS_SERVER);
      if (streamEnable) {
        DataStreamServerRpc dataStreamServerRpc =
            server.getDataStreamServerRpc();
        updateDatanodePort(dataStreamServerRpc.getInetSocketAddress(),
            Port.Name.RATIS_DATASTREAM);
      }
      isStarted = true;
    }
  }

  private int updateDatanodePort(InetSocketAddress address, Port.Name portName) {
    int realPort = address.getPort();
    final Port port = DatanodeDetails.newPort(portName, realPort);
    datanodeDetails.setPort(port);
    LOG.info("{} is started using port {}", name, port);
    return realPort;
  }

  @Override
  public void stop() {
    if (isStarted) {
      try {
        LOG.info("Closing {}", name);
        // shutdown server before the executors as while shutting down,
        // some of the tasks would be executed using the executors.
        server.close();
        for (ExecutorService executor : chunkExecutors) {
          executor.shutdown();
        }
        isStarted = false;
      } catch (IOException e) {
        LOG.error("Failed to close {}.", name, e);
      }
    }
  }

  @Override
  public int getIPCPort() {
    return clientPort;
  }

  /**
   * Returns the Replication type supported by this end-point.
   *
   * @return enum -- {Stand_Alone, Ratis, Chained}
   */
  @Override
  public HddsProtos.ReplicationType getServerType() {
    return HddsProtos.ReplicationType.RATIS;
  }

  @VisibleForTesting
  public RaftServer getServer() {
    return server;
  }

  public RaftServer.Division getServerDivision() throws IOException {
    return getServerDivision(server.getGroupIds().iterator().next());
  }

  public RaftServer.Division getServerDivision(RaftGroupId id)
      throws IOException {
    return server.getDivision(id);
  }

  public boolean getShouldDeleteRatisLogDirectory() {
    return this.shouldDeleteRatisLogDirectory;
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

  @Override
  public void submitRequest(ContainerCommandRequestProto request,
      HddsProtos.PipelineID pipelineID) throws IOException {
    Span span = TracingUtil
        .importAndCreateSpan(
            "XceiverServerRatis." + request.getCmdType().name(),
            request.getTraceID());
    try (Scope ignored = span.makeCurrent()) {
      RaftClientRequest raftClientRequest =
          createRaftClientRequest(request, pipelineID,
              RaftClientRequest.writeRequestType());
      RaftClientReply reply;
      try {
        reply = server.submitClientRequestAsync(raftClientRequest)
            .get(requestTimeout, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException e) {
        throw new IOException(e.getMessage(), e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e.getMessage(), e);
      }
      processReply(reply);
    } finally {
      span.end();
    }
  }

  private RaftClientRequest createRaftClientRequest(
      ContainerCommandRequestProto request, HddsProtos.PipelineID pipelineID,
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

  private GroupInfoRequest createGroupInfoRequest(
      HddsProtos.PipelineID pipelineID) {
    return new GroupInfoRequest(clientId, server.getId(),
        RaftGroupId.valueOf(PipelineID.getFromProtobuf(pipelineID).getId()),
        nextCallId());
  }

  private void handlePipelineFailure(RaftGroupId groupId, RoleInfoProto roleInfoProto, String reason) {
    final RaftPeerId raftPeerId = RaftPeerId.valueOf(roleInfoProto.getSelf().getId());
    Preconditions.assertEquals(getServer().getId(), raftPeerId, "raftPeerId");
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
      final long followerSlownessTimeoutMs = ratisServerConfig.getFollowerSlownessTimeout();
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

    triggerPipelineClose(groupId, b.toString(), ClosePipelineInfo.Reason.PIPELINE_FAILED);
  }

  @VisibleForTesting
  public void triggerPipelineClose(RaftGroupId groupId, String detail,
      ClosePipelineInfo.Reason reasonCode) {
    PipelineID pipelineID = PipelineID.valueOf(groupId.getUuid());

    if (context != null) {
      if (context.isPipelineCloseInProgress(pipelineID.getId())) {
        LOG.debug("Skipped triggering pipeline close for {} as it is already in progress. Reason: {}",
            pipelineID.getId(), detail);
        return;
      }
    }

    ClosePipelineInfo.Builder closePipelineInfo =
        ClosePipelineInfo.newBuilder()
            .setPipelineID(pipelineID.getProtobuf())
            .setReason(reasonCode)
            .setDetailedReason(detail);

    PipelineAction action = PipelineAction.newBuilder()
        .setClosePipeline(closePipelineInfo)
        .setAction(PipelineAction.Action.CLOSE)
        .build();
    if (context != null) {
      if (context.addPipelineActionIfAbsent(action)) {
        LOG.warn("pipeline Action {} on pipeline {}.Reason : {}",
            action.getAction(), pipelineID,
            action.getClosePipeline().getDetailedReason());
      }
      if (!activePipelines.get(groupId).isPendingClose()) {
        // if pipeline close action has not been triggered before, we need trigger pipeline close immediately to
        // prevent SCM to allocate blocks on the failed pipeline
        context.getParent().triggerHeartbeat();
        activePipelines.computeIfPresent(groupId,
            (key, value) -> new ActivePipelineContext(value.isPipelineLeader(), true));
      }
    }
  }

  @Override
  public boolean isExist(HddsProtos.PipelineID pipelineId) {
    return activePipelines.containsKey(
        RaftGroupId.valueOf(PipelineID.getFromProtobuf(pipelineId).getId()));
  }

  @Override
  public List<PipelineReport> getPipelineReport() {
    try {
      Iterable<RaftGroupId> gids = server.getGroupIds();
      List<PipelineReport> reports = new ArrayList<>();
      for (RaftGroupId groupId : gids) {
        HddsProtos.PipelineID pipelineID = PipelineID
            .valueOf(groupId.getUuid()).getProtobuf();
        boolean isLeader = activePipelines.getOrDefault(groupId,
            new ActivePipelineContext(false, false)).isPipelineLeader();
        reports.add(PipelineReport.newBuilder()
            .setPipelineID(pipelineID)
            .setIsLeader(isLeader)
            .build());
      }
      return reports;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void addGroup(HddsProtos.PipelineID pipelineId,
      List<DatanodeDetails> peers) throws IOException {
    if (peers.size() == getDefaultPriorityList().size()) {
      addGroup(pipelineId, peers, getDefaultPriorityList());
    } else {
      addGroup(pipelineId, peers,
          new ArrayList<>(Collections.nCopies(peers.size(), 0)));
    }
  }

  @Override
  public void addGroup(HddsProtos.PipelineID pipelineId,
      List<DatanodeDetails> peers,
      List<Integer> priorityList) throws IOException {
    final PipelineID pipelineID = PipelineID.getFromProtobuf(pipelineId);
    final RaftGroupId groupId = RaftGroupId.valueOf(pipelineID.getId());
    final RaftGroup group =
        RatisHelper.newRaftGroup(groupId, peers, priorityList);
    GroupManagementRequest request = GroupManagementRequest.newAdd(
        clientId, server.getId(), nextCallId(), group);

    RaftClientReply reply;
    LOG.debug("Received addGroup request for pipeline {}", pipelineID);

    try {
      reply = server.groupManagement(request);
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
    processReply(reply);
    LOG.info("Created group {}", pipelineID);
  }

  @Override
  public void removeGroup(HddsProtos.PipelineID pipelineId)
      throws IOException {
    // if shouldDeleteRatisLogDirectory is set to false, the raft log
    // directory will be renamed and kept aside for debugging.
    // In case, its set to true, the raft log directory will be removed
    GroupManagementRequest request = GroupManagementRequest.newRemove(
        clientId, server.getId(), nextCallId(),
        RaftGroupId.valueOf(PipelineID.getFromProtobuf(pipelineId).getId()),
        shouldDeleteRatisLogDirectory, !shouldDeleteRatisLogDirectory);

    RaftClientReply reply;
    try {
      reply = server.groupManagement(request);
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
    processReply(reply);
  }

  void handleFollowerSlowness(RaftGroupId groupId, RoleInfoProto roleInfoProto, RaftPeer follower) {
    handlePipelineFailure(groupId, roleInfoProto, "slow follower " + follower.getId());
  }

  void handleNoLeader(RaftGroupId groupId, RoleInfoProto roleInfoProto) {
    handlePipelineFailure(groupId, roleInfoProto, "no leader");
  }

  void handleApplyTransactionFailure(RaftGroupId groupId,
      RaftProtos.RaftPeerRole role) {
    UUID dnId = RatisHelper.toDatanodeId(getServer().getId());
    String msg =
        "Ratis Transaction failure in datanode " + dnId + " with role " + role
            + " .Triggering pipeline close action.";
    triggerPipelineClose(groupId, msg,
        ClosePipelineInfo.Reason.STATEMACHINE_TRANSACTION_FAILED);
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
                                       RoleInfoProto roleInfoProto,
                                       TermIndex firstTermIndexInLog) {
    LOG.warn("handleInstallSnapshotFromLeader for firstTermIndexInLog={}, terminating pipeline: {}",
        firstTermIndexInLog, groupId);
    handlePipelineFailure(groupId, roleInfoProto, "install snapshot notification");
  }

  /**
   * Notify the Datanode Ratis endpoint of Ratis log failure.
   * Expected to be invoked from the Container StateMachine
   * @param groupId the Ratis group/pipeline for which log has failed
   * @param t exception encountered at the time of the failure
   *
   */
  @VisibleForTesting
  public void handleNodeLogFailure(RaftGroupId groupId, Throwable t) {
    String msg = (t == null) ? "Unspecified failure reported in Ratis log"
        : t.getMessage();

    triggerPipelineClose(groupId, msg,
        ClosePipelineInfo.Reason.PIPELINE_LOG_FAILED);
  }

  public long getMinReplicatedIndex(PipelineID pipelineID) throws IOException {
    GroupInfoReply reply = getServer()
        .getGroupInfo(createGroupInfoRequest(pipelineID.getProtobuf()));
    Long minIndex = RatisHelper.getMinReplicatedIndex(reply.getCommitInfos());
    return minIndex == null ? -1 : minIndex;
  }

  public Collection<RaftPeer> getRaftPeersInPipeline(PipelineID pipelineId) throws IOException {
    final RaftGroupId groupId = RaftGroupId.valueOf(pipelineId.getId());
    return server.getDivision(groupId).getGroup().getPeers();
  }

  public void notifyGroupRemove(RaftGroupId gid) {
    // Remove Group ID entry from the active pipeline map
    activePipelines.remove(gid);
  }

  void notifyGroupAdd(RaftGroupId gid) {
    activePipelines.put(gid, new ActivePipelineContext(false, false));
    sendPipelineReport();
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
    if (context != null && leaderForGroup) {
      // Publish new report from leader
      sendPipelineReport();
    }
  }

  private void sendPipelineReport() {
    if (context !=  null) {
      // TODO: Send IncrementalPipelineReport instead of full PipelineReport
      context.addIncrementalReport(
          context.getParent().getContainer().getPipelineReport());
      context.getParent().triggerHeartbeat();
    }
  }

  private static List<ThreadPoolExecutor> createChunkExecutors(
      ConfigurationSource conf, String threadNamePrefix) {
    // TODO create single pool with N threads if using non-incremental chunks
    final int threadCountPerDisk = conf.getInt(
        OzoneConfigKeys
            .HDDS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_PER_VOLUME_KEY,
        OzoneConfigKeys
            .HDDS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_PER_VOLUME_DEFAULT);

    final int numberOfDisks =
        HddsServerUtil.getDatanodeStorageDirs(conf).size();

    ThreadPoolExecutor[] executors =
        new ThreadPoolExecutor[threadCountPerDisk * numberOfDisks];
    for (int i = 0; i < executors.length; i++) {
      ThreadFactory threadFactory = new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat(threadNamePrefix + "ChunkWriter-" + i + "-%d")
          .build();
      BlockingQueue<Runnable> workQueue = new LinkedBlockingDeque<>();
      executors[i] = new ThreadPoolExecutor(1, 1,
          0, TimeUnit.SECONDS, workQueue, threadFactory);
    }
    return ImmutableList.copyOf(executors);
  }

  /**
   * @return list of default priority
   */
  public static List<Integer> getDefaultPriorityList() {
    return DEFAULT_PRIORITY_LIST;
  }
}
