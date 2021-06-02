/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.update.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceGrpc;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.SubscribeRequest;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.SubscribeResponse;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UnsubscribeRequest;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos.UpdateRequest;
import org.apache.hadoop.hdds.scm.update.server.SCMUpdateClientInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.thirdparty.io.grpc.Deadline;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class for SCM Update Service Grpc Client.
 */
public class SCMUpdateServiceGrpcClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMUpdateServiceGrpcClient.class);
  private static final String CLIENT_NAME = "SCMUpdateServiceGrpcClient";

  private ManagedChannel channel;
  private SCMUpdateServiceGrpc.SCMUpdateServiceStub updateClient;
  private SCMUpdateServiceGrpc.SCMUpdateServiceBlockingStub subscribeClient;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  private UUID clientId = null;
  private StreamObserver<UpdateRequest> requestObserver;
  private CRLClientUpdateHandler handler;
  private long crlCheckInterval;
  private final String host;
  private final int port;
  private final ClientCRLStore clientCRLStore;
  private AtomicLong updateCount;
  private AtomicLong errorCount;
  private AtomicLong pendingCrlRemoveCount;


  public SCMUpdateServiceGrpcClient(final String host,
      final ConfigurationSource conf,
      ClientCRLStore clientCRLStore) {
    Preconditions.checkNotNull(conf);
    this.host = host;
    this.port = conf.getObject(UpdateServiceConfig.class).getPort();
    this.crlCheckInterval = conf.getObject(SCMUpdateClientConfiguration.class)
        .getClientCrlCheckInterval();

    this.clientCRLStore = clientCRLStore;
    createChannel();
    updateCount = new AtomicLong();
    errorCount = new AtomicLong();
    pendingCrlRemoveCount = new AtomicLong();
  }

  public void start() {
    if (!isRunning.compareAndSet(false, true)) {
      LOG.info("Ignore. already started.");
      return;
    }

    LOG.info("{}: starting...", CLIENT_NAME);
    if (channel == null) {
      createChannel();
    }
    clientId = subScribeClient();
    assert(clientId != null);

    // start background thread processing pending crl ids.
    handler = new CRLClientUpdateHandler(clientId, updateClient,
        this, crlCheckInterval);
    handler.start();

    LOG.info("{}: started.", CLIENT_NAME);
  }

  public void incrUpdateCount() {
    updateCount.incrementAndGet();
  }

  public void incrErrorCount() {
    errorCount.incrementAndGet();
  }

  public void incrPendingCrlRemoveCount() {
    pendingCrlRemoveCount.incrementAndGet();
  }

  @VisibleForTesting
  public long getUpdateCount() {
    return updateCount.get();
  }

  @VisibleForTesting
  public long getErrorCount() {
    return errorCount.get();
  }

  @VisibleForTesting
  public long getPendingCrlRemoveCount() {
    return pendingCrlRemoveCount.get();
  }

  public ClientCRLStore getClientCRLStore() {
    return clientCRLStore;
  }
  public AtomicBoolean getIsRunning() {
    return isRunning;
  }

  public void stop(boolean shutdown) {
    LOG.info("{}: stopping...", CLIENT_NAME);
    if (isRunning.get()) {
      // complete update request, no more client streaming
      if (requestObserver != null) {
        requestObserver.onCompleted();
        requestObserver = null;
      }

      // stop update handler
      if (handler != null) {
        handler.stop();
        handler = null;
      }

      if (shutdown) {
        shutdownChannel();
      }
      isRunning.set(false);
    }
    LOG.info("{}: stopped.", CLIENT_NAME);
  }

  public void restart() {
    resetClient();
    stop(false);
    start();
  }

  public void createChannel() {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(host, port).usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

    channel = channelBuilder.build();
    updateClient = SCMUpdateServiceGrpc.newStub(channel);
    subscribeClient = SCMUpdateServiceGrpc.newBlockingStub(channel);
  }

  public void shutdownChannel() {
    if (channel == null) {
      return;
    }

    channel.shutdown();
    try {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Failed to shutdown {} channel", CLIENT_NAME, e);
    } finally {
      channel.shutdownNow();
      channel = null;
    }
  }

  private UUID subScribeClient() {
    SubscribeRequest subReq = SubscribeRequest.newBuilder().build();
    SubscribeResponse subResp = subscribeClient.withWaitForReady()
        .subscribe(subReq);
    return SCMUpdateClientInfo.fromClientIdProto(subResp.getClientId());
  }

  private void unSubscribeClient() {
    if (clientId != null) {
      UnsubscribeRequest unsubReq = UnsubscribeRequest.newBuilder()
            .setClientId(SCMUpdateClientInfo.toClientIdProto(clientId)).build();
      subscribeClient.withWaitForReady().
          withDeadline(Deadline.after(5, TimeUnit.MILLISECONDS))
          .unsubscribe(unsubReq);
    }
  }

  // short-circuit the backoff timer and make them reconnect immediately.
  private void resetClient() {
    if (channel == null) {
      return;
    }
    channel.resetConnectBackoff();
  }
}
