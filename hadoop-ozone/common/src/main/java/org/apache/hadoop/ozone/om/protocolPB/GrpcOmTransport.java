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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
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
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.ha.GrpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.ClientCall;
import org.apache.ratis.thirdparty.io.grpc.ClientInterceptor;
import org.apache.ratis.thirdparty.io.grpc.ClientInterceptors;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCall;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCalls;
import org.apache.ratis.thirdparty.io.netty.channel.Channel;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.Epoll;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Grpc transport for grpc between s3g and om.
 */
public class GrpcOmTransport implements OmTransport {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOmTransport.class);

  private static final String CLIENT_NAME = "GrpcOmTransport";
  private static final String SERVICE_NAME = "hadoop.ozone.OzoneManagerService";
  private static final int SHUTDOWN_WAIT_INTERVAL = 100;
  private static final int SHUTDOWN_MAX_WAIT_SECONDS = 5;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  // gRPC specific
  private static List<X509Certificate> caCerts = null;

  private static final Metadata.Key<String> CLIENT_HOSTNAME_METADATA_KEY =
      Metadata.Key.of("CLIENT_HOSTNAME", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CLIENT_IP_ADDRESS_METADATA_KEY =
      Metadata.Key.of("CLIENT_IP_ADDRESS", Metadata.ASCII_STRING_MARSHALLER);

  private static final MethodDescriptor<OMRequest, OMResponse>
      SUBMIT_REQUEST_METHOD = MethodDescriptor.<OMRequest, OMResponse>newBuilder()
      .setType(MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(MethodDescriptor.generateFullMethodName(
          SERVICE_NAME, "submitRequest"))
      .setRequestMarshaller(new Proto2Marshaller<>(OMRequest::parseFrom))
      .setResponseMarshaller(new Proto2Marshaller<>(OMResponse::parseFrom))
      .build();

  private final Map<String, ManagedChannel> channels;
  private final ConfigurationSource conf;

  private final AtomicReference<String> host;
  private AtomicInteger globalFailoverCount;
  private final int maxSize;
  private final SecurityConfig secConfig;
  private EventLoopGroup eventLoopGroup;
  private Class<? extends Channel> channelType;

  private RetryPolicy retryPolicy;
  private final GrpcOMFailoverProxyProvider<OzoneManagerProtocolPB>
      omFailoverProxyProvider;

  public static void setCaCerts(List<X509Certificate> x509Certificates) {
    caCerts = x509Certificates;
  }

  public GrpcOmTransport(ConfigurationSource conf,
                          UserGroupInformation ugi, String omServiceId)
      throws IOException {

    this.channels = new HashMap<>();
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

    ThreadFactory factory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(CLIENT_NAME + "-ELG-%d")
        .build();

    if (Epoll.isAvailable()) {
      eventLoopGroup = new EpollEventLoopGroup(0, factory);
      channelType = EpollSocketChannel.class;
    } else {
      eventLoopGroup = new NioEventLoopGroup(0, factory);
      channelType = NioSocketChannel.class;
    }
    LOG.info("{} channel type {}", CLIENT_NAME, channelType.getSimpleName());

    for (String nodeId : omFailoverProxyProvider.getOMProxyMap().getNodeIds()) {
      String hostaddr = omFailoverProxyProvider.getGrpcProxyAddress(nodeId);
      HostAndPort hp = HostAndPort.fromString(hostaddr);

      NettyChannelBuilder channelBuilder =
          NettyChannelBuilder.forAddress(hp.getHost(), hp.getPort())
              .usePlaintext()
              .eventLoopGroup(eventLoopGroup)
              .channelType(channelType)
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
          channelBuilder.build());
    }
    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    retryPolicy = omFailoverProxyProvider.getRetryPolicy(maxFailovers);
    LOG.info("{}: started", CLIENT_NAME);
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    AtomicReference<OMResponse> resp = new AtomicReference<>();
    int requestFailoverCount = 0;
    boolean tryOtherHost = true;
    int expectedFailoverCount = 0;
    ResultCodes resultCode = ResultCodes.INTERNAL_ERROR;
    while (tryOtherHost) {
      tryOtherHost = false;
      expectedFailoverCount = globalFailoverCount.get();
      try {
        InetAddress inetAddress = InetAddress.getLocalHost();
        final ManagedChannel channel = channels.get(host.get());
        if (channel == null) {
          throw new OMException(ResultCodes.INTERNAL_ERROR);
        }
        resp.set(submitRequest(channel, payload,
            inetAddress.getHostName(), inetAddress.getHostAddress()));
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
    return resp.get();
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
          String cause = status.getDescription();
          int colonIndex = cause.indexOf(':');
          cause = cause.substring(colonIndex + 2);
          remote = new RemoteException(cause.substring(0, colonIndex),
              cause.substring(colonIndex + 1));
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

    if (eventLoopGroup != null) {
      try {
        eventLoopGroup.shutdownGracefully().sync();
      } catch (InterruptedException e) {
        LOG.error("Interrupted while shutting down event loop group", e);
        Thread.currentThread().interrupt();
      }
    }

    LOG.info("{}: stopped", CLIENT_NAME);
  }

  @Override
  public void close() throws IOException {
    shutdown();
  }

  private OMResponse submitRequest(ManagedChannel channel, OMRequest request,
      String clientHostname, String clientIpAddress) {
    Metadata headers = new Metadata();
    if (clientHostname != null) {
      headers.put(CLIENT_HOSTNAME_METADATA_KEY, clientHostname);
    }
    if (clientIpAddress != null) {
      headers.put(CLIENT_IP_ADDRESS_METADATA_KEY, clientIpAddress);
    }

    org.apache.ratis.thirdparty.io.grpc.Channel intercepted =
        ClientInterceptors.intercept(channel, new FixedHeadersInterceptor(headers));
    return ClientCalls.blockingUnaryCall(intercepted, SUBMIT_REQUEST_METHOD,
        CallOptions.DEFAULT, request);
  }

  private static final class FixedHeadersInterceptor implements ClientInterceptor {
    private final Metadata headers;

    private FixedHeadersInterceptor(Metadata headers) {
      this.headers = headers;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor,
        CallOptions callOptions,
        org.apache.ratis.thirdparty.io.grpc.Channel channel) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          channel.newCall(methodDescriptor, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata metadata) {
          metadata.merge(headers);
          super.start(responseListener, metadata);
        }
      };
    }
  }

  private static final class Proto2Marshaller<T> implements MethodDescriptor.Marshaller<T> {
    private final Proto2Parser<T> parser;

    private Proto2Marshaller(Proto2Parser<T> parser) {
      this.parser = parser;
    }

    @Override
    public InputStream stream(T value) {
      if (!(value instanceof com.google.protobuf.MessageLite)) {
        throw new IllegalArgumentException("Expected protobuf request/response");
      }
      return new ByteArrayInputStream(((com.google.protobuf.MessageLite) value).toByteArray());
    }

    @Override
    public T parse(InputStream stream) {
      try {
        return parser.parse(stream);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @FunctionalInterface
  private interface Proto2Parser<T> {
    T parse(InputStream stream) throws IOException;
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

      channels.put(hostaddr, testChannel);
    }
    LOG.info("{}: started", CLIENT_NAME);
  }

}
