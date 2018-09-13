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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server
    .XceiverServerSpi;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.shaded.proto.RaftProtos;
import org.apache.ratis.shaded.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.shaded.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Creates a ratis server endpoint that acts as the communication layer for
 * Ozone containers.
 */
public final class XceiverServerRatis implements XceiverServerSpi {
  static final Logger LOG = LoggerFactory.getLogger(XceiverServerRatis.class);
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  private final int port;
  private final RaftServer server;
  private ThreadPoolExecutor chunkExecutor;
  private ClientId clientId = ClientId.randomId();
  private final StateContext context;
  private final ReplicationLevel replicationLevel;
  private long nodeFailureTimeoutMs;

  private XceiverServerRatis(DatanodeDetails dd, int port, String storageDir,
      ContainerDispatcher dispatcher, Configuration conf, StateContext context)
      throws IOException {
    Objects.requireNonNull(dd, "id == null");
    this.port = port;
    RaftProperties serverProperties = newRaftProperties(conf, storageDir);
    final int numWriteChunkThreads = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_DEFAULT);
    chunkExecutor =
        new ThreadPoolExecutor(numWriteChunkThreads, numWriteChunkThreads,
            100, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1024),
            new ThreadPoolExecutor.CallerRunsPolicy());
    this.context = context;
    this.replicationLevel =
        conf.getEnum(OzoneConfigKeys.DFS_CONTAINER_RATIS_REPLICATION_LEVEL_KEY,
            OzoneConfigKeys.DFS_CONTAINER_RATIS_REPLICATION_LEVEL_DEFAULT);
    ContainerStateMachine stateMachine =
        new ContainerStateMachine(dispatcher, chunkExecutor, this);
    this.server = RaftServer.newBuilder()
        .setServerId(RatisHelper.toRaftPeerId(dd))
        .setGroup(RatisHelper.emptyRaftGroup())
        .setProperties(serverProperties)
        .setStateMachine(stateMachine)
        .build();
  }


  private RaftProperties newRaftProperties(Configuration conf,
      String storageDir) {
    final RaftProperties properties = new RaftProperties();

    // Set rpc type
    final String rpcType = conf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(rpcType);
    RaftConfigKeys.Rpc.setType(properties, rpc);

    // set raft segment size
    final int raftSegmentSize = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT);
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf(raftSegmentSize));

    // set raft segment pre-allocated size
    final int raftSegmentPreallocatedSize = conf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT);
    RaftServerConfigKeys.Log.Appender.setBufferCapacity(properties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));

    // Set max write buffer size, which is the scm chunk size
    final int maxChunkSize = OzoneConfigKeys.DFS_CONTAINER_CHUNK_MAX_SIZE;
    RaftServerConfigKeys.Log.setWriteBufferSize(properties,
        SizeInBytes.valueOf(maxChunkSize));

    // Set the client requestTimeout
    TimeUnit timeUnit =
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    long duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration clientRequestTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    RaftClientConfigKeys.Rpc
        .setRequestTimeout(properties, clientRequestTimeout);

    // Set the server Request timeout
    timeUnit = OzoneConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT
        .getUnit();
    duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_SERVER_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration serverRequestTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    RaftServerConfigKeys.Rpc
        .setRequestTimeout(properties, serverRequestTimeout);

    // Enable batch append on raft server
    RaftServerConfigKeys.Log.Appender.setBatchEnabled(properties, true);

    // Set the maximum cache segments
    RaftServerConfigKeys.Log.setMaxCachedSegmentNum(properties, 2);

    // Set the ratis leader election timeout
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        TimeDuration.valueOf(800, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS));

    // set the node failure timeout
    timeUnit = OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_DEFAULT
        .getUnit();
    duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_SERVER_FAILURE_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration nodeFailureTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    RaftServerConfigKeys.setLeaderElectionTimeout(properties,
        nodeFailureTimeout);
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties,
        nodeFailureTimeout);
    nodeFailureTimeoutMs = nodeFailureTimeout.toLong(TimeUnit.MILLISECONDS);

    // Set the ratis storage directory
    RaftServerConfigKeys.setStorageDir(properties, new File(storageDir));

    // For grpc set the maximum message size
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(maxChunkSize + raftSegmentPreallocatedSize));

    // Set the ratis port number
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, port);
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, port);
    }
    return properties;
  }

  public static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails datanodeDetails, Configuration ozoneConf,
      ContainerDispatcher dispatcher, StateContext context) throws IOException {
    final String ratisDir = File.separator + "ratis";
    int localPort = ozoneConf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT_DEFAULT);
    String storageDir = ozoneConf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);

    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = ozoneConf.get(OzoneConfigKeys
          .OZONE_METADATA_DIRS);
      Preconditions.checkNotNull(storageDir, "ozone.metadata.dirs " +
          "cannot be null, Please check your configs.");
      storageDir = storageDir.concat(ratisDir);
      LOG.warn("Storage directory for Ratis is not configured. Mapping Ratis " +
              "storage under {}. It is a good idea to map this to an SSD disk.",
          storageDir);
    }

    // Get an available port on current node and
    // use that as the container port
    if (ozoneConf.getBoolean(OzoneConfigKeys
            .DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT_DEFAULT)) {
      try (ServerSocket socket = new ServerSocket()) {
        socket.setReuseAddress(true);
        SocketAddress address = new InetSocketAddress(0);
        socket.bind(address);
        localPort = socket.getLocalPort();
        LOG.info("Found a free port for the server : {}", localPort);
        // If we have random local ports configured this means that it
        // probably running under MiniOzoneCluster. Ratis locks the storage
        // directories, so we need to pass different local directory for each
        // local instance. So we map ratis directories under datanode ID.
        storageDir =
            storageDir.concat(File.separator +
                datanodeDetails.getUuidString());
      } catch (IOException e) {
        LOG.error("Unable find a random free port for the server, "
            + "fallback to use default port {}", localPort, e);
      }
    }
    datanodeDetails.setPort(
        DatanodeDetails.newPort(DatanodeDetails.Port.Name.RATIS, localPort));
    return new XceiverServerRatis(datanodeDetails, localPort, storageDir,
        dispatcher, ozoneConf, context);
  }

  @Override
  public void start() throws IOException {
    LOG.info("Starting {} {} at port {}", getClass().getSimpleName(),
        server.getId(), getIPCPort());
    chunkExecutor.prestartAllCoreThreads();
    server.start();
  }

  @Override
  public void stop() {
    try {
      chunkExecutor.shutdown();
      server.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getIPCPort() {
    return port;
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

  private void processReply(RaftClientReply reply) {

    // NotLeader exception is thrown only when the raft server to which the
    // request is submitted is not the leader. The request will be rejected
    // and will eventually be executed once the request comnes via the leader
    // node.
    NotLeaderException notLeaderException = reply.getNotLeaderException();
    if (notLeaderException != null) {
      LOG.info(reply.getNotLeaderException().getLocalizedMessage());
    }
    StateMachineException stateMachineException =
        reply.getStateMachineException();
    if (stateMachineException != null) {
      // In case the request could not be completed, StateMachine Exception
      // will be thrown. For now, Just log the message.
      // If the container could not be closed, SCM will come to know
      // via containerReports. CloseContainer should be re tried via SCM.
      LOG.error(stateMachineException.getLocalizedMessage());
    }
  }

  @Override
  public void submitRequest(
      ContainerCommandRequestProto request, HddsProtos.PipelineID pipelineID)
      throws IOException {
    // ReplicationLevel.ALL ensures the transactions corresponding to
    // the request here are applied on all the raft servers.
    RaftClientRequest raftClientRequest =
        createRaftClientRequest(request, pipelineID,
            RaftClientRequest.writeRequestType(replicationLevel));
    CompletableFuture<RaftClientReply> reply =
        server.submitClientRequestAsync(raftClientRequest);
    reply.thenAccept(this::processReply);
  }

  private RaftClientRequest createRaftClientRequest(
      ContainerCommandRequestProto request, HddsProtos.PipelineID pipelineID,
      RaftClientRequest.Type type) {
    return new RaftClientRequest(clientId, server.getId(),
        PipelineID.getFromProtobuf(pipelineID).getRaftGroupID(),
        nextCallId(), 0, Message.valueOf(request.toByteString()), type);
  }

  private void handlePipelineFailure(RaftGroupId groupId,
      RoleInfoProto roleInfoProto) {
    String msg;
    UUID datanode = RatisHelper.toDatanodeId(roleInfoProto.getSelf());
    RaftPeerId id = RaftPeerId.valueOf(roleInfoProto.getSelf().getId());
    switch (roleInfoProto.getRole()) {
    case CANDIDATE:
      msg = datanode + " is in candidate state for " +
          roleInfoProto.getCandidateInfo().getLastLeaderElapsedTimeMs() + "ms";
      break;
    case LEADER:
      StringBuilder sb = new StringBuilder();
      sb.append(datanode).append(" has not seen follower/s");
      for (RaftProtos.ServerRpcProto follower : roleInfoProto.getLeaderInfo()
          .getFollowerInfoList()) {
        if (follower.getLastRpcElapsedTimeMs() > nodeFailureTimeoutMs) {
          sb.append(" ").append(RatisHelper.toDatanodeId(follower.getId()))
              .append(" for ").append(follower.getLastRpcElapsedTimeMs())
              .append("ms");
        }
      }
      msg = sb.toString();
      break;
    default:
      LOG.error("unknown state:" + roleInfoProto.getRole());
      throw new IllegalStateException("node" + id + " is in illegal role "
          + roleInfoProto.getRole());
    }

    PipelineID pipelineID = PipelineID.valueOf(groupId);
    ClosePipelineInfo.Builder closePipelineInfo =
        ClosePipelineInfo.newBuilder()
            .setPipelineID(pipelineID.getProtobuf())
            .setReason(ClosePipelineInfo.Reason.PIPELINE_FAILED)
            .setDetailedReason(msg);

    PipelineAction action = PipelineAction.newBuilder()
        .setClosePipeline(closePipelineInfo)
        .setAction(PipelineAction.Action.CLOSE)
        .build();
    context.addPipelineActionIfAbsent(action);
    LOG.debug(
        "pipeline Action " + action.getAction() + "  on pipeline " + pipelineID
            + ".Reason : " + action.getClosePipeline().getDetailedReason());
  }

  void handleNodeSlowness(
      RaftGroup group, RoleInfoProto roleInfoProto) {
    handlePipelineFailure(group.getGroupId(), roleInfoProto);
  }

  void handleNoLeader(
      RaftGroup group, RoleInfoProto roleInfoProto) {
    handlePipelineFailure(group.getGroupId(), roleInfoProto);
  }
}