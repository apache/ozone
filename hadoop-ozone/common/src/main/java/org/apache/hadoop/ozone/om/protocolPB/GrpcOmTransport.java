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

package org.apache.hadoop.ozone.om.protocolPB;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.SSL_CONNECTION_FAILURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.ha.GrpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.protocolPB.grpc.ClientAddressClientInterceptor;
import org.apache.hadoop.ozone.om.protocolPB.grpc.GrpcClientConstants;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyHint;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Grpc transport for grpc between s3g and om.
 */
public class GrpcOmTransport implements OmTransport {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOmTransport.class);

  private static final String CLIENT_NAME = "GrpcOmTransport";
  private static final int SHUTDOWN_WAIT_INTERVAL = 100;
  private static final int SHUTDOWN_MAX_WAIT_SECONDS = 5;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  // gRPC specific
  private static List<X509Certificate> caCerts = null;

  private final Map<String,
      OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub> clients;
  private final Map<String, ManagedChannel> channels;
  private final ConfigurationSource conf;

  private final AtomicReference<String> host;
  private AtomicInteger globalFailoverCount;
  private final int maxSize;
  private final SecurityConfig secConfig;

  private RetryPolicy retryPolicy;
  private final GrpcOMFailoverProxyProvider<OzoneManagerProtocolPB>
      omFailoverProxyProvider;
  private volatile boolean useFollowerRead;
  private final ReadConsistencyHint followerReadConsistency;
  private final ReadConsistencyHint leaderReadConsistency;
  private int currentFollowerReadIndex = -1;

  public static void setCaCerts(List<X509Certificate> x509Certificates) {
    caCerts = x509Certificates;
  }

  public GrpcOmTransport(ConfigurationSource conf,
                          UserGroupInformation ugi, String omServiceId)
      throws IOException {

    this.channels = new HashMap<>();
    this.clients = new HashMap<>();
    this.conf = conf;
    this.host = new AtomicReference<>();
    this.globalFailoverCount = new AtomicInteger();

    secConfig =  new SecurityConfig(conf);
    maxSize = conf.getInt(OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH,
        OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

    omFailoverProxyProvider = new GrpcOMFailoverProxyProvider<>(
        conf,
        ugi,
        omServiceId,
        OzoneManagerProtocolPB.class);

    this.useFollowerRead = conf.getBoolean(
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_DEFAULT);
    String defaultFollowerReadConsistencyStr = conf.get(
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_DEFAULT_CONSISTENCY_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_DEFAULT_CONSISTENCY_DEFAULT
    );
    ReadConsistency defaultFollowerReadConsistency =
        ReadConsistency.valueOf(defaultFollowerReadConsistencyStr);
    String defaultLeaderReadConsistencyStr = conf.get(
        OzoneConfigKeys.OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_KEY,
        OzoneConfigKeys.OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_DEFAULT);
    ReadConsistency defaultLeaderReadConsistency =
        ReadConsistency.valueOf(defaultLeaderReadConsistencyStr);
    Preconditions.assertTrue(defaultFollowerReadConsistency.allowFollowerRead(),
        "Invalid follower read consistency " + defaultFollowerReadConsistency);
    Preconditions.assertTrue(!defaultLeaderReadConsistency.allowFollowerRead(),
        "Invalid leader read consistency " + defaultLeaderReadConsistency);
    this.followerReadConsistency = defaultFollowerReadConsistency.getHint();
    this.leaderReadConsistency = defaultLeaderReadConsistency.getHint();

    start();
  }

  public void start() throws IOException {
    host.set(omFailoverProxyProvider
        .getGrpcProxyAddress(
            omFailoverProxyProvider.getCurrentProxyOMNodeId()));

    if (!isRunning.compareAndSet(false, true)) {
      LOG.info("Ignore. already started.");
      return;
    }

    for (String nodeId : omFailoverProxyProvider.getOMProxyMap().getNodeIds()) {
      String hostaddr = omFailoverProxyProvider.getGrpcProxyAddress(nodeId);
      HostAndPort hp = HostAndPort.fromString(hostaddr);

      NettyChannelBuilder channelBuilder =
          NettyChannelBuilder.forAddress(hp.getHost(), hp.getPort())
              .usePlaintext()
              .proxyDetector(uri -> null)
              .maxInboundMessageSize(maxSize);

      if (secConfig.isSecurityEnabled() && secConfig.isGrpcTlsEnabled()) {
        try {
          SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
          if (caCerts != null) {
            sslContextBuilder.trustManager(caCerts);
          } else {
            LOG.error("x509Certificates empty");
          }
          channelBuilder.useTransportSecurity().
              sslContext(sslContextBuilder.build());
        } catch (Exception ex) {
          LOG.error("cannot establish TLS for grpc om transport client");
        }
      } else {
        channelBuilder.usePlaintext();
      }

      channels.put(hostaddr,
          channelBuilder.intercept(new ClientAddressClientInterceptor())
              .build());
      clients.put(hostaddr,
          OzoneManagerServiceGrpc
              .newBlockingStub(channels.get(hostaddr)));
    }
    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    retryPolicy = omFailoverProxyProvider.getRetryPolicy(maxFailovers);
    LOG.info("{}: started", CLIENT_NAME);
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    if (useFollowerRead && OmUtils.shouldSendToFollower(payload)) {
      return submitRequestWithFollowerRead(payload);
    }
    return submitRequestToLeader(addReadConsistencyHint(payload,
        leaderReadConsistency));
  }

  private OMResponse submitRequestWithFollowerRead(OMRequest payload)
      throws IOException {
    OMRequest followerPayload = addReadConsistencyHint(payload,
        followerReadConsistency);
    int failedCount = 0;
    for (int i = 0; useFollowerRead &&
        i < omFailoverProxyProvider.getOMProxyMap().getNodeIds().size(); i++) {
      String nodeId = getCurrentFollowerReadNodeId();
      String followerHost = omFailoverProxyProvider.getGrpcProxyAddress(nodeId);
      try {
        OMResponse response = submitRequestToHost(followerPayload, followerHost);
        LOG.debug("Invocation with cmdType {} using follower read host {} was successful",
            followerPayload.getCmdType(), followerHost);
        return response;
      } catch (StatusRuntimeException e) {
        LOG.debug("Invocation with cmdType {} using follower read host {} failed",
            followerPayload.getCmdType(), followerHost, e);
        Exception unwrapped = unwrapException(new Exception(e));
        if (OMFailoverProxyProviderBase.getNotLeaderException(unwrapped) != null) {
          LOG.debug("Encountered OMNotLeaderException from {}. Disable OM follower read and retry OM leader directly.",
              followerHost);
          useFollowerRead = false;
          break;
        }
        if (OMFailoverProxyProviderBase.getLeaderNotReadyException(unwrapped) != null) {
          break;
        }
        ReadIndexException readIndexException =
            OMFailoverProxyProviderBase.getReadIndexException(unwrapped);
        ReadException readException =
            OMFailoverProxyProviderBase.getReadException(unwrapped);
        if (readIndexException != null || readException != null ||
            omFailoverProxyProvider.shouldFailoverForFollowerRead(unwrapped)) {
          failedCount++;
          changeFollowerReadProxy(nodeId);
        } else {
          throw e;
        }
      }
    }
    if (failedCount > 0) {
      LOG.warn("{} nodes have failed for read request with cmdType {}. Falling back to leader.",
          failedCount, payload.getCmdType());
    }
    return submitRequestToLeader(addReadConsistencyHint(payload,
        leaderReadConsistency));
  }

  private OMResponse submitRequestToLeader(OMRequest payload)
      throws IOException {
    int requestFailoverCount = 0;
    boolean tryOtherHost = true;
    int expectedFailoverCount = 0;
    ResultCodes resultCode = ResultCodes.INTERNAL_ERROR;
    while (tryOtherHost) {
      tryOtherHost = false;
      expectedFailoverCount = globalFailoverCount.get();
      try {
        return submitRequestToHost(payload, host.get());
      } catch (StatusRuntimeException e) {
        LOG.error("Failed to submit request", e);
        if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
          if (e.getCause() != null &&
              e.getCause() instanceof javax.net.ssl.SSLHandshakeException) {
            throw new OMException(SSL_CONNECTION_FAILURE);
          }
          resultCode = ResultCodes.TIMEOUT;
        }
        Exception exp = new Exception(e);
        tryOtherHost = shouldRetry(unwrapException(exp),
            expectedFailoverCount, ++requestFailoverCount);
        if (!tryOtherHost) {
          throw new OMException(resultCode);
        }
      }
    }
    throw new OMException(resultCode);
  }

  private OMResponse submitRequestToHost(OMRequest payload, String targetHost)
      throws IOException {
    AtomicReference<OMResponse> resp = new AtomicReference<>();
    InetAddress inetAddress = InetAddress.getLocalHost();
    Context.current()
        .withValue(GrpcClientConstants.CLIENT_IP_ADDRESS_CTX_KEY,
            inetAddress.getHostAddress())
        .withValue(GrpcClientConstants.CLIENT_HOSTNAME_CTX_KEY,
            inetAddress.getHostName())
        .run(() -> resp.set(clients.get(targetHost)
            .submitRequest(payload)));
    return resp.get();
  }

  private OMRequest addReadConsistencyHint(OMRequest payload,
      ReadConsistencyHint readConsistencyHint) {
    if (!payload.hasReadConsistencyHint() && readConsistencyHint != null) {
      return payload.toBuilder()
          .setReadConsistencyHint(readConsistencyHint)
          .build();
    }
    return payload;
  }

  private synchronized String getCurrentFollowerReadNodeId() {
    if (currentFollowerReadIndex < 0) {
      currentFollowerReadIndex = 0;
    }
    return new ArrayList<>(omFailoverProxyProvider.getOMProxyMap().getNodeIds())
        .get(currentFollowerReadIndex);
  }

  private synchronized void changeFollowerReadProxy(String currentNodeId) {
    String currentFollowerReadNodeId = getCurrentFollowerReadNodeId();
    if (currentFollowerReadNodeId.equals(currentNodeId)) {
      currentFollowerReadIndex = (currentFollowerReadIndex + 1) %
          omFailoverProxyProvider.getOMProxyMap().getNodeIds().size();
    }
  }

  private Exception unwrapException(Exception ex) {
    Exception grpcException = null;
    try {
      StatusRuntimeException srexp =
          (StatusRuntimeException)ex.getCause();
      Status status = srexp.getStatus();
      LOG.debug("GRPC exception wrapped: {}", status.getDescription());
      if (status.getCode() == Status.Code.INTERNAL) {
        // exception potentially generated by OzoneManagerServiceGrpc
        Class<?> realClass = Class.forName(status.getDescription()
            .substring(0, status.getDescription()
                .indexOf(':')));
        Class<? extends Exception> cls = realClass
            .asSubclass(Exception.class);
        Constructor<? extends Exception> cn = cls.getConstructor(String.class);
        cn.setAccessible(true);
        grpcException = cn.newInstance(status.getDescription());
        IOException remote = null;
        try {
          String description = status.getDescription();
          int colonIndex = description.indexOf(':');
          remote = new RemoteException(description.substring(0, colonIndex),
              description.substring(colonIndex + 2));
          grpcException.initCause(remote);
        } catch (Exception e) {
          LOG.error("cannot get cause for remote exception");
        }
      } else if ((status.getCode() == Status.Code.RESOURCE_EXHAUSTED) ||
              (status.getCode() == Status.Code.DATA_LOSS)) {
        grpcException = srexp;
      } else {
        // exception generated by connection failure, gRPC
        grpcException = ex;
      }
    } catch (Exception e) {
      grpcException = new IOException(e);
      LOG.error("error unwrapping exception from OMResponse {}");
    }
    return grpcException;
  }

  private boolean shouldRetry(Exception ex, int expectedFailoverCount, int requestFailoverCount) {
    boolean retry = false;
    RetryPolicy.RetryAction action = null;
    try {
      action = retryPolicy.shouldRetry(ex, 0, requestFailoverCount, true);
      LOG.debug("grpc failover retry action {}", action.action);
      if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
        LOG.error("Retry request failed. Action : {}, {}",
            action.action, ex.toString());
      } else {
        if (action.action == RetryPolicy.RetryAction.RetryDecision.RETRY ||
            (action.action == RetryPolicy.RetryAction.RetryDecision
                .FAILOVER_AND_RETRY)) {
          if (action.delayMillis > 0) {
            try {
              Thread.sleep(action.delayMillis);
            } catch (Exception e) {
              LOG.error("Error trying sleep thread for {}", action.delayMillis);
            }
          }
          // switch om host to current proxy OMNodeId
          if (globalFailoverCount.get() == expectedFailoverCount) {
            omFailoverProxyProvider.performFailover(null);
            globalFailoverCount.getAndIncrement();
          } else {
            LOG.warn("A failover has occurred since the start of current" +
                " thread retry, NOT failover using current proxy");
          }
          host.set(omFailoverProxyProvider
              .getGrpcProxyAddress(
                  omFailoverProxyProvider.getCurrentProxyOMNodeId()));
          retry = true;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed failover exception {}", e);
    }
    return retry;
  }

  // stub implementation for interface
  @Override
  public Text getDelegationTokenService() {
    return new Text();
  }

  public void shutdown() {
    for (ManagedChannel channel : channels.values()) {
      channel.shutdown();
    }

    final long maxWaitNanos = TimeUnit.SECONDS.toNanos(SHUTDOWN_MAX_WAIT_SECONDS);
    long deadline = System.nanoTime() + maxWaitNanos;
    List<ManagedChannel> nonTerminated = new ArrayList<>(channels.values());

    while (!nonTerminated.isEmpty() && System.nanoTime() < deadline) {
      nonTerminated.removeIf(ManagedChannel::isTerminated);
      if (nonTerminated.isEmpty()) {
        break;
      }
      try {
        Thread.sleep(SHUTDOWN_WAIT_INTERVAL);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for channels to terminate", e);
        Thread.currentThread().interrupt();
        break;
      }
    }

    if (!nonTerminated.isEmpty()) {
      List<String> failedChannels = channels.entrySet().stream()
          .filter(e -> !e.getValue().isTerminated())
          .map(Map.Entry::getKey)
          .collect(Collectors.toList());
      LOG.warn("Channels {} did not terminate within timeout.", failedChannels);
    }

    LOG.info("{}: stopped", CLIENT_NAME);
  }

  @Override
  public void close() throws IOException {
    shutdown();
  }

  /**
   * GrpcOmTransport configuration in Java style configuration class.
   */
  @ConfigGroup(prefix = "ozone.om.grpc")
  public static final class GrpcOmTransportConfig {
    @Config(key = "ozone.om.grpc.port", defaultValue = "8981",
        description = "Port used for"
            + " the GrpcOmTransport OzoneManagerServiceGrpc server",
        tags = {ConfigTag.MANAGEMENT})
    private int port;

    public int getPort() {
      return port;
    }

    public GrpcOmTransportConfig setPort(int portParam) {
      this.port = portParam;
      return this;
    }
  }

  @VisibleForTesting
  public void startClient(ManagedChannel testChannel) throws IOException {
    for (String nodeId : omFailoverProxyProvider.getOMProxyMap().getNodeIds()) {
      String hostaddr = omFailoverProxyProvider.getGrpcProxyAddress(nodeId);

      clients.put(hostaddr,
          OzoneManagerServiceGrpc
              .newBlockingStub(testChannel));
    }
    LOG.info("{}: started", CLIENT_NAME);
  }

  @VisibleForTesting
  public void startClient(String nodeId, ManagedChannel testChannel) throws IOException {
    String hostaddr = omFailoverProxyProvider.getGrpcProxyAddress(nodeId);
    clients.put(hostaddr,
        OzoneManagerServiceGrpc
            .newBlockingStub(testChannel));
    LOG.info("{}: started test client for {}", CLIENT_NAME, nodeId);
  }

  @VisibleForTesting
  public synchronized void changeFollowerReadInitialProxy(String nodeId) {
    List<String> nodeIds = new ArrayList<>(
        omFailoverProxyProvider.getOMProxyMap().getNodeIds());
    for (int i = 0; i < nodeIds.size(); i++) {
      if (nodeIds.get(i).equals(nodeId)) {
        currentFollowerReadIndex = i;
        return;
      }
    }
  }

  @VisibleForTesting
  public void changeLeaderProxyForTest(String nodeId) throws IOException {
    omFailoverProxyProvider.setNextOmProxy(nodeId);
    omFailoverProxyProvider.performFailover(null);
    host.set(omFailoverProxyProvider.getGrpcProxyAddress(nodeId));
  }

}
