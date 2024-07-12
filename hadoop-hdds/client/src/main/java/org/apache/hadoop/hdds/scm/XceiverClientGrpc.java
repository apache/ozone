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
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceStub;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.tracing.GrpcClientInterceptor;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.HddsUtils.processForDebug;

/**
 * {@link XceiverClientSpi} implementation, the standalone client.
 *
 * This class can be used to connect to a DataNode and use the
 * DatanodeClientProtocol to read and write data.
 * Writes via this client does not go through the Ratis protocol, and does not
 * replicate to any other nodes, usage of this client implementation to
 * write replicated data is ill-advised.
 *
 * User's of this class should consult the documentation that can be found in
 * the README.gRPC.md file in the package of this class for broader context on
 * how it works, and how it is integrated with the Ozone client.
 */
public class XceiverClientGrpc extends XceiverClientSpi {
  private static final Logger LOG =
      LoggerFactory.getLogger(XceiverClientGrpc.class);
  private final Pipeline pipeline;
  private final ConfigurationSource config;
  private final Map<UUID, XceiverClientProtocolServiceStub> asyncStubs;
  private final XceiverClientMetrics metrics;
  private final Map<UUID, ManagedChannel> channels;
  private final Semaphore semaphore;
  private long timeout;
  private final SecurityConfig secConfig;
  private final boolean topologyAwareRead;
  private final ClientTrustManager trustManager;
  // Cache the DN which returned the GetBlock command so that the ReadChunk
  // command can be sent to the same DN.
  private final Map<DatanodeBlockID, DatanodeDetails> getBlockDNcache;

  private boolean closed = false;

  /**
   * Constructs a client that can communicate with the Container framework on
   * data nodes via DatanodeClientProtocol.
   *
   * @param pipeline - Pipeline that defines the machines.
   * @param config   -- Ozone Config
   * @param trustManager - a {@link ClientTrustManager} with proper CA handling.
   */
  public XceiverClientGrpc(Pipeline pipeline, ConfigurationSource config,
      ClientTrustManager trustManager) {
    super();
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkNotNull(config);
    setTimeout(config.getTimeDuration(OzoneConfigKeys.
        OZONE_CLIENT_READ_TIMEOUT, OzoneConfigKeys
        .OZONE_CLIENT_READ_TIMEOUT_DEFAULT, TimeUnit.SECONDS));
    this.pipeline = pipeline;
    this.config = config;
    this.secConfig = new SecurityConfig(config);
    this.semaphore =
        new Semaphore(HddsClientUtils.getMaxOutstandingRequests(config));
    this.metrics = XceiverClientManager.getXceiverClientMetrics();
    this.channels = new HashMap<>();
    this.asyncStubs = new HashMap<>();
    this.topologyAwareRead = config.getBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_DEFAULT);
    this.trustManager = trustManager;
    this.getBlockDNcache = new ConcurrentHashMap<>();
  }

  /**
   * Constructs a client that can communicate with the Container framework on
   * data nodes via DatanodeClientProtocol.
   *
   * @param pipeline - Pipeline that defines the machines.
   * @param config   -- Ozone Config
   */
  public XceiverClientGrpc(Pipeline pipeline, ConfigurationSource config) {
    this(pipeline, config, null);
  }

  /**
   * Sets up the connection to a DataNode. Initializes the gRPC server stub, and
   * opens the gRPC channel to be used to send requests to the server.
   */
  @Override
  public void connect() throws Exception {
    // connect to the closest node, if closest node doesn't exist, delegate to
    // first node, which is usually the leader in the pipeline.
    DatanodeDetails dn = topologyAwareRead ? this.pipeline.getClosestNode() :
        this.pipeline.getFirstNode();
    // just make a connection to the picked datanode at the beginning
    connectToDatanode(dn);
  }

  private synchronized void connectToDatanode(DatanodeDetails dn)
      throws IOException {
    if (isConnected(dn)) {
      return;
    }
    // read port from the data node, on failure use default configured
    // port.
    int port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
    if (port == 0) {
      port = config.getInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
          OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT);
    }

    // Add credential context to the client call
    if (LOG.isDebugEnabled()) {
      LOG.debug("Nodes in pipeline : {}", pipeline.getNodes());
      LOG.debug("Connecting to server : {}", dn.getIpAddress());
    }
    ManagedChannel channel = createChannel(dn, port).build();
    XceiverClientProtocolServiceStub asyncStub =
        XceiverClientProtocolServiceGrpc.newStub(channel);
    asyncStubs.put(dn.getUuid(), asyncStub);
    channels.put(dn.getUuid(), channel);
  }

  protected NettyChannelBuilder createChannel(DatanodeDetails dn, int port)
      throws IOException {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(dn.getIpAddress(), port).usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
            .intercept(new GrpcClientInterceptor());
    if (secConfig.isSecurityEnabled() && secConfig.isGrpcTlsEnabled()) {
      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      if (trustManager != null) {
        sslContextBuilder.trustManager(trustManager);
      }
      if (secConfig.useTestCert()) {
        channelBuilder.overrideAuthority("localhost");
      }
      channelBuilder.useTransportSecurity().
          sslContext(sslContextBuilder.build());
    } else {
      channelBuilder.usePlaintext();
    }
    return channelBuilder;
  }

  /**
   * Checks if the client has a live connection channel to the specified
   * Datanode.
   *
   * @return True if the connection is alive, false otherwise.
   */
  @VisibleForTesting
  public boolean isConnected(DatanodeDetails details) {
    return isConnected(channels.get(details.getUuid()));
  }

  private boolean isConnected(ManagedChannel channel) {
    return channel != null && !channel.isTerminated() && !channel.isShutdown();
  }

  /**
   * Closes all the communication channels of the client one-by-one.
   * When a channel is closed, no further requests can be sent via the channel,
   * and the method waits to finish all ongoing communication.
   *
   * Note: the method wait 1 hour per channel tops and if that is not enough
   * to finish ongoing communication, then interrupts the connection anyway.
   */
  @Override
  public synchronized void close() {
    closed = true;
    for (ManagedChannel channel : channels.values()) {
      channel.shutdownNow();
      try {
        channel.awaitTermination(60, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error("InterruptedException while waiting for channel termination",
            e);
        // Re-interrupt the thread while catching InterruptedException
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request) throws IOException {
    try {
      return sendCommandWithTraceIDAndRetry(request, null).
          getResponse().get();
    } catch (ExecutionException e) {
      throw getIOExceptionForSendCommand(request, e);
    } catch (InterruptedException e) {
      LOG.error("Command execution was interrupted.");
      Thread.currentThread().interrupt();
      throw (IOException) new InterruptedIOException(
          "Command " + processForDebug(request) + " was interrupted.")
          .initCause(e);
    }
  }

  @Override
  public Map<DatanodeDetails, ContainerCommandResponseProto>
      sendCommandOnAllNodes(
      ContainerCommandRequestProto request) throws IOException {
    HashMap<DatanodeDetails, ContainerCommandResponseProto>
            responseProtoHashMap = new HashMap<>();
    List<DatanodeDetails> datanodeList = pipeline.getNodes();
    HashMap<DatanodeDetails, CompletableFuture<ContainerCommandResponseProto>>
            futureHashMap = new HashMap<>();
    if (!request.hasVersion()) {
      ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto.newBuilder(request);
      builder.setVersion(ClientVersion.CURRENT.toProtoValue());
      request = builder.build();
    }
    for (DatanodeDetails dn : datanodeList) {
      try {
        request = reconstructRequestIfNeeded(request, dn);
        futureHashMap.put(dn, sendCommandAsync(request, dn).getResponse());
      } catch (InterruptedException e) {
        LOG.error("Command execution was interrupted.");
        // Re-interrupt the thread while catching InterruptedException
        Thread.currentThread().interrupt();
      }
    }
    for (Map.Entry<DatanodeDetails,
              CompletableFuture<ContainerCommandResponseProto> >
              entry : futureHashMap.entrySet()) {
      try {
        responseProtoHashMap.put(entry.getKey(), entry.getValue().get());
      } catch (InterruptedException e) {
        LOG.error("Command execution was interrupted.");
        // Re-interrupt the thread while catching InterruptedException
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        String message =
            "Failed to execute command {} on datanode " + entry.getKey()
                .getHostName();
        if (LOG.isDebugEnabled()) {
          LOG.debug(message, processForDebug(request), e);
        } else {
          LOG.error(message + " Exception Class: {}, Exception Message: {}",
              request.getCmdType(), e.getClass().getName(), e.getMessage());
        }
      }
    }
    return responseProtoHashMap;
  }

  /**
   * @param request
   * @param dn
   * @param pipeline
   * In case of getBlock for EC keys, it is required to set replicaIndex for
   * every request with the replicaIndex for that DN for which the request is
   * sent to. This method unpacks proto and reconstructs request after setting
   * the replicaIndex field.
   * @return new updated Request
   */
  private ContainerCommandRequestProto reconstructRequestIfNeeded(
      ContainerCommandRequestProto request, DatanodeDetails dn) {
    boolean isEcRequest = pipeline.getReplicationConfig()
        .getReplicationType() == HddsProtos.ReplicationType.EC;
    if (request.hasGetBlock() && isEcRequest) {
      ContainerProtos.GetBlockRequestProto gbr = request.getGetBlock();
      request = request.toBuilder().setGetBlock(gbr.toBuilder().setBlockID(
          gbr.getBlockID().toBuilder().setReplicaIndex(
              pipeline.getReplicaIndex(dn)).build()).build()).build();
    }
    return request;
  }

  @Override
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request, List<Validator> validators)
      throws IOException {
    try {
      XceiverClientReply reply;
      reply = sendCommandWithTraceIDAndRetry(request, validators);
      return reply.getResponse().get();
    } catch (ExecutionException e) {
      throw getIOExceptionForSendCommand(request, e);
    } catch (InterruptedException e) {
      LOG.error("Command execution was interrupted.");
      Thread.currentThread().interrupt();
      throw (IOException) new InterruptedIOException(
          "Command " + processForDebug(request) + " was interrupted.")
          .initCause(e);
    }
  }

  private XceiverClientReply sendCommandWithTraceIDAndRetry(
      ContainerCommandRequestProto request, List<Validator> validators)
      throws IOException {

    String spanName = "XceiverClientGrpc." + request.getCmdType().name();

    return TracingUtil.executeInNewSpan(spanName,
        () -> {
          ContainerCommandRequestProto.Builder builder =
              ContainerCommandRequestProto.newBuilder(request)
                  .setTraceID(TracingUtil.exportCurrentSpan());
          if (!request.hasVersion()) {
            builder.setVersion(ClientVersion.CURRENT.toProtoValue());
          }
          return sendCommandWithRetry(builder.build(), validators);
        });
  }

  private XceiverClientReply sendCommandWithRetry(
      ContainerCommandRequestProto request, List<Validator> validators)
      throws IOException {
    ContainerCommandResponseProto responseProto = null;
    IOException ioException = null;

    // In case of an exception or an error, we will try to read from the
    // datanodes in the pipeline in a round-robin fashion.
    XceiverClientReply reply = new XceiverClientReply(null);
    List<DatanodeDetails> datanodeList = null;

    DatanodeBlockID blockID = null;
    if (request.getCmdType() == ContainerProtos.Type.GetBlock) {
      blockID = request.getGetBlock().getBlockID();
    } else if  (request.getCmdType() == ContainerProtos.Type.ReadChunk) {
      blockID = request.getReadChunk().getBlockID();
    } else if (request.getCmdType() == ContainerProtos.Type.GetSmallFile) {
      blockID = request.getGetSmallFile().getBlock().getBlockID();
    }

    if (blockID != null) {
      // Check if the DN to which the GetBlock command was sent has been cached.
      DatanodeDetails cachedDN = getBlockDNcache.get(blockID);
      if (cachedDN != null) {
        datanodeList = pipeline.getNodes();
        int getBlockDNCacheIndex = datanodeList.indexOf(cachedDN);
        if (getBlockDNCacheIndex > 0) {
          // Pull the Cached DN to the top of the DN list
          Collections.swap(datanodeList, 0, getBlockDNCacheIndex);
        }
      }
    }
    if (datanodeList == null) {
      if (topologyAwareRead) {
        datanodeList = pipeline.getNodesInOrder();
      } else {
        datanodeList = pipeline.getNodes();
        // Shuffle datanode list so that clients do not read in the same order
        // every time.
        Collections.shuffle(datanodeList);
      }
    }

    for (DatanodeDetails dn : datanodeList) {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Executing command {} on datanode {}",
              processForDebug(request), dn);
        }
        // In case the command gets retried on a 2nd datanode,
        // sendCommandAsyncCall will create a new channel and async stub
        // in case these don't exist for the specific datanode.
        reply.addDatanode(dn);
        responseProto = sendCommandAsync(request, dn).getResponse().get();
        if (validators != null && !validators.isEmpty()) {
          for (Validator validator : validators) {
            validator.accept(request, responseProto);
          }
        }
        if (request.getCmdType() == ContainerProtos.Type.GetBlock) {
          DatanodeBlockID getBlockID = request.getGetBlock().getBlockID();
          getBlockDNcache.put(getBlockID, dn);
        }
        break;
      } catch (IOException e) {
        ioException = e;
        responseProto = null;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to execute command {} on datanode {}",
              processForDebug(request), dn, e);
        }
      } catch (ExecutionException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to execute command {} on datanode {}",
              processForDebug(request), dn, e);
        }
        if (Status.fromThrowable(e.getCause()).getCode()
            == Status.UNAUTHENTICATED.getCode()) {
          throw new SCMSecurityException("Failed to authenticate with "
              + "GRPC XceiverServer with Ozone block token.");
        }

        ioException = new IOException(e);
      } catch (InterruptedException e) {
        LOG.error("Command execution was interrupted ", e);
        Thread.currentThread().interrupt();
      }
    }

    if (responseProto != null) {
      reply.setResponse(CompletableFuture.completedFuture(responseProto));
      return reply;
    } else {
      Objects.requireNonNull(ioException);
      String message = "Failed to execute command {}";
      if (LOG.isDebugEnabled()) {
        LOG.debug(message + " on the pipeline {}.",
                processForDebug(request), pipeline);
      } else {
        LOG.error(message + " on the pipeline {}.",
                request.getCmdType(), pipeline);
      }
      throw ioException;
    }
  }

  @Override
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException {

    Span span = GlobalTracer.get()
        .buildSpan("XceiverClientGrpc." + request.getCmdType().name()).start();

    try (Scope ignored = GlobalTracer.get().activateSpan(span)) {

      ContainerCommandRequestProto.Builder builder =
          ContainerCommandRequestProto.newBuilder(request)
              .setTraceID(TracingUtil.exportCurrentSpan());
      if (!request.hasVersion()) {
        builder.setVersion(ClientVersion.CURRENT.toProtoValue());
      }
      XceiverClientReply asyncReply =
          sendCommandAsync(builder.build(), pipeline.getFirstNode());
      if (shouldBlockAndWaitAsyncReply(request)) {
        asyncReply.getResponse().get();
      }
      return asyncReply;

    } finally {
      span.finish();
    }
  }

  /**
   * During data writes the ordering of WriteChunk and PutBlock is not ensured
   * by any outside logic, therefore in this original implementation, all reads
   * and writes are synchronized.
   * This method is providing the possibility for subclasses to override this
   * behaviour.
   *
   * @param request the request we need the decision about
   * @return true if the request is a write request.
   */
  protected boolean shouldBlockAndWaitAsyncReply(
      ContainerCommandRequestProto request) {
    return !HddsUtils.isReadOnly(request);
  }

  @VisibleForTesting
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request, DatanodeDetails dn)
      throws IOException, InterruptedException {
    checkOpen(dn);
    UUID dnId = dn.getUuid();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Send command {} to datanode {}",
          request.getCmdType(), dn.getIpAddress());
    }
    final CompletableFuture<ContainerCommandResponseProto> replyFuture =
        new CompletableFuture<>();
    semaphore.acquire();
    long requestTime = System.currentTimeMillis();
    metrics.incrPendingContainerOpsMetrics(request.getCmdType());

    // create a new grpc message stream pair for each call.
    final StreamObserver<ContainerCommandRequestProto> requestObserver =
        asyncStubs.get(dnId).withDeadlineAfter(timeout, TimeUnit.SECONDS)
            .send(new StreamObserver<ContainerCommandResponseProto>() {
              @Override
              public void onNext(ContainerCommandResponseProto value) {
                replyFuture.complete(value);
                metrics.decrPendingContainerOpsMetrics(request.getCmdType());
                long cost = System.currentTimeMillis() - requestTime;
                metrics.addContainerOpsLatency(request.getCmdType(),
                    cost);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Executed command {} on datanode {}, cost = {}, "
                          + "cmdType = {}", processForDebug(request), dn,
                      cost, request.getCmdType());
                }
                semaphore.release();
              }

              @Override
              public void onError(Throwable t) {
                replyFuture.completeExceptionally(t);
                metrics.decrPendingContainerOpsMetrics(request.getCmdType());
                long cost = System.currentTimeMillis() - requestTime;
                metrics.addContainerOpsLatency(request.getCmdType(),
                    System.currentTimeMillis() - requestTime);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Executed command {} on datanode {}, cost = {}, "
                          + "cmdType = {}", processForDebug(request), dn,
                      cost, request.getCmdType());
                }
                semaphore.release();
              }

              @Override
              public void onCompleted() {
                if (!replyFuture.isDone()) {
                  replyFuture.completeExceptionally(new IOException(
                      "Stream completed but no reply for request " +
                          processForDebug(request)));
                }
              }
            });
    requestObserver.onNext(request);
    requestObserver.onCompleted();
    return new XceiverClientReply(replyFuture);
  }

  private synchronized void checkOpen(DatanodeDetails dn)
      throws IOException {
    if (closed) {
      throw new IOException("This channel is not connected.");
    }

    ManagedChannel channel = channels.get(dn.getUuid());
    // If the channel doesn't exist for this specific datanode or the channel
    // is closed, just reconnect
    if (!isConnected(channel)) {
      reconnect(dn);
    }

  }

  private void reconnect(DatanodeDetails dn)
      throws IOException {
    ManagedChannel channel;
    try {
      connectToDatanode(dn);
      channel = channels.get(dn.getUuid());
    } catch (Exception e) {
      throw new IOException("Error while connecting", e);
    }

    if (!isConnected(channel)) {
      throw new IOException("This channel is not connected.");
    }
  }

  @Override
  public XceiverClientReply watchForCommit(long index)
      throws InterruptedException, ExecutionException, TimeoutException,
      IOException {
    // there is no notion of watch for commit index in standalone pipeline
    return null;
  }

  @Override
  public long getReplicatedMinCommitIndex() {
    return 0;
  }

  @Override
  public HddsProtos.ReplicationType getPipelineType() {
    return HddsProtos.ReplicationType.STAND_ALONE;
  }

  public ConfigurationSource getConfig() {
    return config;
  }

  @VisibleForTesting
  public static Logger getLogger() {
    return LOG;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }
}
