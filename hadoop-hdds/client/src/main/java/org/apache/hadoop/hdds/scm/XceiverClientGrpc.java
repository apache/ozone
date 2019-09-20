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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceStub;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.CheckedBiFunction;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.tracing.GrpcClientInterceptor;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A Client for the storageContainer protocol for read object data.
 */
public class XceiverClientGrpc extends XceiverClientSpi {
  static final Logger LOG = LoggerFactory.getLogger(XceiverClientGrpc.class);
  private static final String COMPONENT = "dn";
  private final Pipeline pipeline;
  private final Configuration config;
  private Map<UUID, XceiverClientProtocolServiceStub> asyncStubs;
  private XceiverClientMetrics metrics;
  private Map<UUID, ManagedChannel> channels;
  private final Semaphore semaphore;
  private boolean closed = false;
  private SecurityConfig secConfig;
  private final boolean topologyAwareRead;
  private X509Certificate caCert;

  /**
   * Constructs a client that can communicate with the Container framework on
   * data nodes.
   *
   * @param pipeline - Pipeline that defines the machines.
   * @param config   -- Ozone Config
   * @param caCert   - SCM ca certificate.
   */
  public XceiverClientGrpc(Pipeline pipeline, Configuration config,
      X509Certificate caCert) {
    super();
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkNotNull(config);
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
    this.caCert = caCert;
  }

  /**
   * Constructs a client that can communicate with the Container framework on
   * data nodes.
   *
   * @param pipeline - Pipeline that defines the machines.
   * @param config   -- Ozone Config
   */
  public XceiverClientGrpc(Pipeline pipeline, Configuration config) {
    this(pipeline, config, null);
  }

  /**
   * To be used when grpc token is not enabled.
   */
  @Override
  public void connect() throws Exception {
    // connect to the closest node, if closest node doesn't exist, delegate to
    // first node, which is usually the leader in the pipeline.
    DatanodeDetails dn = topologyAwareRead ? this.pipeline.getClosestNode() :
        this.pipeline.getFirstNode();
    // just make a connection to the picked datanode at the beginning
    connectToDatanode(dn, null);
  }

  /**
   * Passed encoded token to GRPC header when security is enabled.
   */
  @Override
  public void connect(String encodedToken) throws Exception {
    // connect to the closest node, if closest node doesn't exist, delegate to
    // first node, which is usually the leader in the pipeline.
    DatanodeDetails dn = topologyAwareRead ? this.pipeline.getClosestNode() :
        this.pipeline.getFirstNode();
    // just make a connection to the picked datanode at the beginning
    connectToDatanode(dn, encodedToken);
  }

  private void connectToDatanode(DatanodeDetails dn, String encodedToken)
      throws IOException {
    // read port from the data node, on failure use default configured
    // port.
    int port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
    if (port == 0) {
      port = config.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
    }

    // Add credential context to the client call
    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    LOG.debug("Nodes in pipeline : {}", pipeline.getNodes().toString());
    LOG.debug("Connecting to server : {}", dn.getIpAddress());
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(dn.getIpAddress(), port).usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
            .intercept(new ClientCredentialInterceptor(userName, encodedToken),
                new GrpcClientInterceptor());
    if (secConfig.isGrpcTlsEnabled()) {
      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      if (caCert != null) {
        sslContextBuilder.trustManager(caCert);
      }
      if (secConfig.useTestCert()) {
        channelBuilder.overrideAuthority("localhost");
      }
      channelBuilder.useTransportSecurity().
          sslContext(sslContextBuilder.build());
    } else {
      channelBuilder.usePlaintext();
    }
    ManagedChannel channel = channelBuilder.build();
    XceiverClientProtocolServiceStub asyncStub =
        XceiverClientProtocolServiceGrpc.newStub(channel);
    asyncStubs.put(dn.getUuid(), asyncStub);
    channels.put(dn.getUuid(), channel);
  }

  /**
   * Returns if the xceiver client connects to all servers in the pipeline.
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

  @Override
  public void close() {
    closed = true;
    for (ManagedChannel channel : channels.values()) {
      channel.shutdownNow();
      try {
        channel.awaitTermination(60, TimeUnit.MINUTES);
      } catch (Exception e) {
        LOG.error("Unexpected exception while waiting for channel termination",
            e);
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
      XceiverClientReply reply;
      reply = sendCommandWithTraceIDAndRetry(request, null);
      ContainerCommandResponseProto responseProto = reply.getResponse().get();
      return responseProto;
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException("Failed to execute command " + request, e);
    }
  }

  @Override
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request, List<CheckedBiFunction> validators)
      throws IOException {
    try {
      XceiverClientReply reply;
      reply = sendCommandWithTraceIDAndRetry(request, validators);
      ContainerCommandResponseProto responseProto = reply.getResponse().get();
      return responseProto;
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException("Failed to execute command " + request, e);
    }
  }

  private XceiverClientReply sendCommandWithTraceIDAndRetry(
      ContainerCommandRequestProto request, List<CheckedBiFunction> validators)
      throws IOException {
    try (Scope scope = GlobalTracer.get()
        .buildSpan("XceiverClientGrpc." + request.getCmdType().name())
        .startActive(true)) {
      ContainerCommandRequestProto finalPayload =
          ContainerCommandRequestProto.newBuilder(request)
              .setTraceID(TracingUtil.exportCurrentSpan()).build();
      return sendCommandWithRetry(finalPayload, validators);
    }
  }

  private XceiverClientReply sendCommandWithRetry(
      ContainerCommandRequestProto request, List<CheckedBiFunction> validators)
      throws IOException {
    ContainerCommandResponseProto responseProto = null;
    IOException ioException = null;

    // In case of an exception or an error, we will try to read from the
    // datanodes in the pipeline in a round robin fashion.

    // TODO: cache the correct leader info in here, so that any subsequent calls
    // should first go to leader
    XceiverClientReply reply = new XceiverClientReply(null);
    List<DatanodeDetails> datanodeList;
    if ((request.getCmdType() == ContainerProtos.Type.ReadChunk ||
        request.getCmdType() == ContainerProtos.Type.GetSmallFile) &&
        topologyAwareRead) {
      datanodeList = pipeline.getNodesInOrder();
    } else {
      datanodeList = pipeline.getNodes();
      // Shuffle datanode list so that clients do not read in the same order
      // every time.
      Collections.shuffle(datanodeList);
    }
    for (DatanodeDetails dn : datanodeList) {
      try {
        LOG.debug("Executing command " + request + " on datanode " + dn);
        // In case the command gets retried on a 2nd datanode,
        // sendCommandAsyncCall will create a new channel and async stub
        // in case these don't exist for the specific datanode.
        reply.addDatanode(dn);
        responseProto = sendCommandAsync(request, dn).getResponse().get();
        if (validators != null && !validators.isEmpty()) {
          for (CheckedBiFunction validator : validators) {
            validator.apply(request, responseProto);
          }
        }
        break;
      } catch (ExecutionException | InterruptedException | IOException e) {
        LOG.error("Failed to execute command " + request + " on datanode " + dn
            .getUuidString(), e);
        if (!(e instanceof IOException)) {
          if (Status.fromThrowable(e.getCause()).getCode()
              == Status.UNAUTHENTICATED.getCode()) {
            throw new SCMSecurityException("Failed to authenticate with "
                + "GRPC XceiverServer with Ozone block token.");
          }
          ioException = new IOException(e);
        } else {
          ioException = (IOException) e;
        }
        responseProto = null;
      }
    }

    if (responseProto != null) {
      reply.setResponse(CompletableFuture.completedFuture(responseProto));
      return reply;
    } else {
      Preconditions.checkNotNull(ioException);
      LOG.error("Failed to execute command {} on the pipeline {}.", request,
          pipeline);
      throw ioException;
    }
  }

  // TODO: for a true async API, once the waitable future while executing
  // the command on one channel fails, it should be retried asynchronously
  // on the future Task for all the remaining datanodes.

  // Note: this Async api is not used currently used in any active I/O path.
  // In case it gets used, the asynchronous retry logic needs to be plugged
  // in here.
  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   * @throws IOException
   */
  @Override
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException {
    try (Scope scope = GlobalTracer.get()
        .buildSpan("XceiverClientGrpc." + request.getCmdType().name())
        .startActive(true)) {

      ContainerCommandRequestProto finalPayload =
          ContainerCommandRequestProto.newBuilder(request)
              .setTraceID(TracingUtil.exportCurrentSpan())
              .build();
      XceiverClientReply asyncReply =
          sendCommandAsync(finalPayload, pipeline.getFirstNode());
      // TODO : for now make this API sync in nature as async requests are
      // served out of order over XceiverClientGrpc. This needs to be fixed
      // if this API is to be used for I/O path. Currently, this is not
      // used for Read/Write Operation but for tests.
      if (!HddsUtils.isReadOnly(request)) {
        asyncReply.getResponse().get();
      }
      return asyncReply;
    }
  }

  private XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request, DatanodeDetails dn)
      throws IOException, ExecutionException, InterruptedException {
    if (closed) {
      throw new IOException("This channel is not connected.");
    }

    UUID dnId = dn.getUuid();
    ManagedChannel channel = channels.get(dnId);
    // If the channel doesn't exist for this specific datanode or the channel
    // is closed, just reconnect
    String token = request.getEncodedToken();
    if (!isConnected(channel)) {
      reconnect(dn, token);
    }

    LOG.debug("Send command {} to datanode {}", request.getCmdType().toString(),
        dn.getNetworkFullPath());
    final CompletableFuture<ContainerCommandResponseProto> replyFuture =
        new CompletableFuture<>();
    semaphore.acquire();
    long requestTime = Time.monotonicNowNanos();
    metrics.incrPendingContainerOpsMetrics(request.getCmdType());
    // create a new grpc stream for each non-async call.

    // TODO: for async calls, we should reuse StreamObserver resources.
    final StreamObserver<ContainerCommandRequestProto> requestObserver =
        asyncStubs.get(dnId)
            .send(new StreamObserver<ContainerCommandResponseProto>() {
              @Override
              public void onNext(ContainerCommandResponseProto value) {
                replyFuture.complete(value);
                metrics.decrPendingContainerOpsMetrics(request.getCmdType());
                metrics.addContainerOpsLatency(request.getCmdType(),
                    Time.monotonicNowNanos() - requestTime);
                semaphore.release();
              }

              @Override
              public void onError(Throwable t) {
                replyFuture.completeExceptionally(t);
                metrics.decrPendingContainerOpsMetrics(request.getCmdType());
                metrics.addContainerOpsLatency(request.getCmdType(),
                    Time.monotonicNowNanos() - requestTime);
                semaphore.release();
              }

              @Override
              public void onCompleted() {
                if (!replyFuture.isDone()) {
                  replyFuture.completeExceptionally(new IOException(
                      "Stream completed but no reply for request " + request));
                }
              }
            });
    requestObserver.onNext(request);
    requestObserver.onCompleted();
    return new XceiverClientReply(replyFuture);
  }

  private void reconnect(DatanodeDetails dn, String encodedToken)
      throws IOException {
    ManagedChannel channel;
    try {
      connectToDatanode(dn, encodedToken);
      channel = channels.get(dn.getUuid());
    } catch (Exception e) {
      LOG.error("Error while connecting: ", e);
      throw new IOException(e);
    }

    if (channel == null || !isConnected(channel)) {
      throw new IOException("This channel is not connected.");
    }
  }

  @Override
  public XceiverClientReply watchForCommit(long index, long timeout)
      throws InterruptedException, ExecutionException, TimeoutException,
      IOException {
    // there is no notion of watch for commit index in standalone pipeline
    return null;
  };

  public long getReplicatedMinCommitIndex() {
    return 0;
  }
  /**
   * Returns pipeline Type.
   *
   * @return - Stand Alone as the type.
   */
  @Override
  public HddsProtos.ReplicationType getPipelineType() {
    return HddsProtos.ReplicationType.STAND_ALONE;
  }
}
