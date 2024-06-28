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

package org.apache.hadoop.hdds.scm;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.XceiverClientReply.Reason;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.tracing.TracingUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract implementation of {@link XceiverClientSpi} using Ratis.
 * The underlying RPC mechanism can be chosen via the constructor.
 */
public final class XceiverClientRatis extends XceiverClientSpi {
  public static final Logger LOG =
      LoggerFactory.getLogger(XceiverClientRatis.class);

  public static XceiverClientRatis newXceiverClientRatis(
      org.apache.hadoop.hdds.scm.pipeline.Pipeline pipeline,
      ConfigurationSource ozoneConf) {
    return newXceiverClientRatis(pipeline, ozoneConf, null);
  }

  public static XceiverClientRatis newXceiverClientRatis(
      org.apache.hadoop.hdds.scm.pipeline.Pipeline pipeline,
      ConfigurationSource ozoneConf, ClientTrustManager trustManager) {
    final String rpcType = ozoneConf
        .get(ScmConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.HDDS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneConf);
    final GrpcTlsConfig tlsConfig = RatisHelper.createTlsClientConfig(new
        SecurityConfig(ozoneConf), trustManager);
    return new XceiverClientRatis(pipeline,
        SupportedRpcType.valueOfIgnoreCase(rpcType),
        retryPolicy, tlsConfig, ozoneConf);
  }

  private final Pipeline pipeline;
  private final RpcType rpcType;
  private final AtomicReference<RaftClient> client = new AtomicReference<>();
  private final RetryPolicy retryPolicy;
  private final GrpcTlsConfig tlsConfig;
  private final ConfigurationSource ozoneConfiguration;

  // Map to track commit index at every server
  private final ConcurrentHashMap<UUID, Long> commitInfoMap;

  private final XceiverClientMetrics metrics
      = XceiverClientManager.getXceiverClientMetrics();

  /**
   * Constructs a client.
   */
  private XceiverClientRatis(Pipeline pipeline, RpcType rpcType,
      RetryPolicy retryPolicy, GrpcTlsConfig tlsConfig,
      ConfigurationSource configuration) {
    super();
    this.pipeline = pipeline;
    this.rpcType = rpcType;
    this.retryPolicy = retryPolicy;
    commitInfoMap = new ConcurrentHashMap<>();
    this.tlsConfig = tlsConfig;
    this.ozoneConfiguration = configuration;

    if (LOG.isTraceEnabled()) {
      LOG.trace("new XceiverClientRatis for pipeline " + pipeline.getId(),
          new Throwable("TRACE"));
    }
  }

  private long updateCommitInfosMap(RaftClientReply reply) {
    return Optional.ofNullable(reply)
        .filter(RaftClientReply::isSuccess)
        .map(RaftClientReply::getCommitInfos)
        .map(this::updateCommitInfosMap)
        .orElse(0L);
  }

  public long updateCommitInfosMap(
      Collection<RaftProtos.CommitInfoProto> commitInfoProtos) {
    // if the commitInfo map is empty, just update the commit indexes for each
    // of the servers
    final Stream<Long> stream;
    if (commitInfoMap.isEmpty()) {
      stream = commitInfoProtos.stream().map(this::putCommitInfo);
      // In case the commit is happening 2 way, just update the commitIndex
      // for the servers which have been successfully updating the commit
      // indexes. This is important because getReplicatedMinCommitIndex()
      // should always return the min commit index out of the nodes which have
      // been replicating data successfully.
    } else {
      stream = commitInfoProtos.stream().map(proto -> commitInfoMap
              .computeIfPresent(RatisHelper.toDatanodeId(proto.getServer()),
                  (address, index) -> proto.getCommitIndex()))
          .filter(Objects::nonNull);
    }
    return stream.mapToLong(Long::longValue).min().orElse(0);
  }

  private long putCommitInfo(RaftProtos.CommitInfoProto proto) {
    final long index = proto.getCommitIndex();
    commitInfoMap.put(RatisHelper.toDatanodeId(proto.getServer()), index);
    return index;
  }

  /**
   * Returns Ratis as pipeline Type.
   *
   * @return - Ratis
   */
  @Override
  public HddsProtos.ReplicationType getPipelineType() {
    return HddsProtos.ReplicationType.RATIS;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void connect() throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to pipeline:{} leaderDatanode:{}, " +
          "primaryDatanode:{}", getPipeline().getId(),
          RatisHelper.toRaftPeerId(pipeline.getLeaderNode()),
          RatisHelper.toRaftPeerId(pipeline.getClosestNode()));
    }

    if (!client.compareAndSet(null,
        RatisHelper.newRaftClient(rpcType, getPipeline(), retryPolicy,
            tlsConfig, ozoneConfiguration))) {
      throw new IllegalStateException("Client is already connected.");
    }
  }

  @Override
  public void close() {
    final RaftClient c = client.getAndSet(null);
    if (c != null) {
      closeRaftClient(c);
    }
  }

  private void closeRaftClient(RaftClient raftClient) {
    try {
      raftClient.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private RaftClient getClient() {
    return Objects.requireNonNull(client.get(), "client is null");
  }


  @VisibleForTesting
  public ConcurrentMap<UUID, Long> getCommitInfoMap() {
    return commitInfoMap;
  }

  private CompletableFuture<RaftClientReply> sendRequestAsyncInternal(
      ContainerCommandRequestProto request, ReplicationLevel writeReplicationLevel) {
    final ContainerCommandRequestMessage message =
        ContainerCommandRequestMessage.toMessage(request, TracingUtil.exportCurrentSpan());
    if (HddsUtils.isReadOnly(request)) {
      LOG.debug("sendCommandAsync ReadOnly message {}", message);
      return getClient().async().sendReadOnly(message);
    } else {
      LOG.debug("sendCommandAsync Write {} message {}", writeReplicationLevel, message);
      return getClient().async().send(message, writeReplicationLevel).handle((reply, e) -> {
        if (LOG.isDebugEnabled()) {
          LOG.debug("sendCommandAsync Write {} message {}: reply = {}, exception =",
              writeReplicationLevel, message, reply, e);
        }
        if (reply != null) {
          // If Raft server gives a reply, just return it.
          return reply;
        }
        // reply == null implies exception != null with the current Raft server implementation
        final Collection<CommitInfoProto> commitInfos;
        final long maxCommitIndex;
        if (e instanceof CompletionException) {
          final CompletionException ce = (CompletionException) e;
          if (e.getCause() instanceof NotReplicatedException) {
            // Unwrap exception to get commitInfos if NotReplicatedException is thrown
            final NotReplicatedException nre = (NotReplicatedException) ce.getCause();
            commitInfos = nre.getCommitInfos();
            maxCommitIndex = commitInfos.stream()
                .map(CommitInfoProto::getCommitIndex)
                .max(Long::compareTo).orElse(-1L);
          } else {
            // Otherwise just throw the exception as-is
            LOG.debug("reply is null, but an exception other than NotReplicatedException is thrown", ce);
            throw ce;
          }
        } else {
          // unreachable condition in theory
          throw new CompletionException("Unexpected exception", e);
        }
        LOG.debug("reply is null, and NotReplicatedException is thrown, crafting a new RaftClientReply");
        RaftGroupMemberId raftId = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("pEER"), RaftGroupId.emptyGroupId());
        // Craft a reply, only set useful and required fields
        RaftClientReply.Builder replyBuilder = RaftClientReply.newBuilder()
            .setCommitInfos(commitInfos)
            .setLogIndex(maxCommitIndex)
            .setSuccess(false)
            .setClientId(ClientId.emptyClientId())
            .setServerId(raftId)
            .setGroupId(RaftGroupId.emptyGroupId());
        return replyBuilder.build();
      });
    }
  }

  /**
   * Sends request async with ReplicationLevel.ALL_COMMITTED.
   * @param request ContainerCommandRequestProto
   * @param writeReplicationLevel Desired ReplicationLevel for write ops.
   * @return CompletableFuture<RaftClientReply>
   */
  private CompletableFuture<RaftClientReply> sendRequestAsync(
      ContainerCommandRequestProto request, ReplicationLevel writeReplicationLevel) {
    final String spanName = "XceiverClientRatis." + request.getCmdType().name();
    return TracingUtil.executeInNewSpan(spanName,
        () -> sendRequestAsyncInternal(request, writeReplicationLevel));
  }

  // gets the minimum log index replicated to all servers
  @Override
  public long getReplicatedMinCommitIndex() {
    return commitInfoMap.values().parallelStream()
        .mapToLong(Long::longValue).min().orElse(0);
  }

  private void addDatanodetoReply(UUID address, XceiverClientReply reply) {
    addDatanodetoReply(address, reply, XceiverClientReply.Reason.UNKNOWN);
  }

  private void addDatanodetoReply(UUID address, XceiverClientReply reply, XceiverClientReply.Reason reason) {
    LOG.debug("Adding datanode UUID {} to list for reason {}", address, reason);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(address);
    reply.addDatanode(builder.build(), reason);
  }

  private XceiverClientReply newWatchReply(
      long watchIndex, Object reason, long replyIndex) {
    LOG.debug("watchForCommit({}) returns {} {}",
        watchIndex, reason, replyIndex);
    final XceiverClientReply reply = new XceiverClientReply(null);
    reply.setLogIndex(replyIndex);
    return reply;
  }

  @Override
  public XceiverClientReply watchForCommit(long index)
      throws InterruptedException, ExecutionException, TimeoutException,
      IOException {
    final long replicatedMin = getReplicatedMinCommitIndex();
    if (replicatedMin >= index) {
      return newWatchReply(index, "replicatedMin", replicatedMin);
    }

    try {
      CompletableFuture<RaftClientReply> replyFuture = getClient().async()
          .watch(index, RaftProtos.ReplicationLevel.ALL_COMMITTED);
      final RaftClientReply reply = replyFuture.get();
      final long updated = updateCommitInfosMap(reply);
      Preconditions.checkState(updated >= index);
      return newWatchReply(index, ReplicationLevel.ALL_COMMITTED, updated);
    } catch (Exception e) {
      LOG.warn("3 way commit failed on pipeline {}", pipeline, e);
      Throwable t =
          HddsClientUtils.containsException(e, GroupMismatchException.class);
      if (t != null) {
        throw e;
      }
      final RaftClientReply reply = getClient().async()
          .watch(index, RaftProtos.ReplicationLevel.MAJORITY_COMMITTED)
          .get();
      final XceiverClientReply clientReply = newWatchReply(
          index, ReplicationLevel.MAJORITY_COMMITTED, index);
      reply.getCommitInfos().stream()
          .filter(i -> i.getCommitIndex() < index)  // only get the node that is lagging behind
          .forEach(proto -> {
            UUID address = RatisHelper.toDatanodeId(proto.getServer());
            addDatanodetoReply(address, clientReply, Reason.TIMEOUT);  // add the dead datanode to XceiverClientReply
            // since 3 way commit has failed, the updated map from now on  will
            // only store entries for those datanodes which have had successful
            // replication.
            commitInfoMap.remove(address);
            LOG.info(
                "Could not commit index {} on pipeline {} to all the nodes. " +
                "Server {} has failed. Committed by majority.",
                index, pipeline, address);
          });
      return clientReply;
    }
  }

  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   */
  @Override
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request) {
    // TODO
    return sendCommandAsyncInternal(request, ReplicationLevel.ALL_COMMITTED);
  }

  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @param writeReplicationLevel Desired ReplicationLevel for write ops.
   * @return Response to the command
   */
  @Override
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request, ReplicationLevel writeReplicationLevel) {
    return sendCommandAsyncInternal(request, writeReplicationLevel);
  }

  private XceiverClientReply sendCommandAsyncInternal(
      ContainerCommandRequestProto request, ReplicationLevel writeReplicationLevel) {
    XceiverClientReply asyncReply = new XceiverClientReply(null);
    long requestTime = System.currentTimeMillis();
    CompletableFuture<RaftClientReply> raftClientReply = sendRequestAsync(request, writeReplicationLevel);
    metrics.incrPendingContainerOpsMetrics(request.getCmdType());

    CompletableFuture<ContainerCommandResponseProto> containerCommandResponse =
        raftClientReply.whenComplete((clientReply, e) -> {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received Raft client reply {} for request: cmdType={} writeReplicationLevel={} containerID={}"
                    + " pipelineID={} traceID={} exception: {}",
                clientReply, request.getCmdType(), writeReplicationLevel, request.getContainerID(),
                request.getPipelineID(), request.getTraceID(), e);
          }
          metrics.decrPendingContainerOpsMetrics(request.getCmdType());
          metrics.addContainerOpsLatency(request.getCmdType(),
              System.currentTimeMillis() - requestTime);

          // isSuccess() == false here indicates that the request has failed to achieve ALL_COMMITTED replication level
          if (clientReply != null && !clientReply.isSuccess()) {
            // And if NotReplicatedException is thrown, it implicates the request achieved MAJORITY_COMMITTED.
            // The list of servers given by getCommitInfos() would return with their respective commitIndex.
            // Find the node with the lowest commit index and add this (timed out) node to the reply in order to
            // be populated to the failed server list later.
            long maxCommitIndex = clientReply.getLogIndex();
            UUID timedOutNode = clientReply.getCommitInfos().stream()
                .filter(i -> i.getCommitIndex() < maxCommitIndex)
                .map(CommitInfoProto::getServer)
                .map(RatisHelper::toDatanodeId)
                .findFirst().orElse(null);
            LOG.debug("timedOutNode = {}", timedOutNode);
            if (timedOutNode != null) {
              addDatanodetoReply(timedOutNode, asyncReply, Reason.TIMEOUT);
            }
          }
        }).thenApply(reply -> {
          final boolean isNotReplicatedException = reply.getCommitInfos() != null;
          final ContainerCommandResponseProto response;
          try {
            if (!reply.isSuccess()) {
              if (!isNotReplicatedException) {
                // in case of raft retry failure, the raft client is
                // not able to connect to the leader hence the pipeline
                // can not be used but this instance of RaftClient will close
                // and refreshed again. In case the client cannot connect to
                // leader, getClient call will fail.

                // No need to set the failed Server ID here. Ozone client
                // will directly exclude this pipeline in next allocate block
                // to SCM as in this case, it is the raft client which is not
                // able to connect to leader in the pipeline, though the
                // pipeline can still be functional.
                RaftException exception = reply.getException();
                Preconditions.checkNotNull(exception, "Raft reply failure but " +
                    "no exception propagated.");
                throw new CompletionException(exception);
              } else {
                // If NotReplicatedException, fake a SUCCESS reply with a custom message
                // to indicate that to the next stage
                response = ContainerCommandResponseProto.newBuilder()
                    .setResult(Result.SUCCESS)
                    .setCmdType(request.getCmdType())
                    .setMessage("TIMEOUT")  // TODO: Extract this as a constant
                    .build();
                updateCommitInfosMap(reply.getCommitInfos());
                asyncReply.setLogIndex(reply.getLogIndex());
              }
            } else {  // reply.isSuccess()
              response = ContainerCommandResponseProto.parseFrom(reply.getMessage().getContent());
              if (response.getResult() == ContainerProtos.Result.SUCCESS) {
                updateCommitInfosMap(reply.getCommitInfos());
              }
              asyncReply.setLogIndex(reply.getLogIndex());
              UUID serverId = RatisHelper.toDatanodeId(reply.getReplierId());
              addDatanodetoReply(serverId, asyncReply);  // add replier node to XceiverClientReply
            }
            return response;
          } catch (InvalidProtocolBufferException e) {
            throw new CompletionException(e);
          }
        });
    asyncReply.setResponse(containerCommandResponse);
    return asyncReply;
  }

  @Override
  public Map<DatanodeDetails, ContainerCommandResponseProto>
      sendCommandOnAllNodes(ContainerCommandRequestProto request) {
    throw new UnsupportedOperationException(
            "Operation Not supported for ratis client");
  }

  public DataStreamApi getDataStreamApi() {
    return this.getClient().getDataStreamApi();
  }
}
