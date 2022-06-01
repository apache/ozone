/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.HashMap;
import java.util.Map;

import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.hadoop.ipc.RemoteException;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.ozone.om.ha.GrpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContextBuilder;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH;
import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT;

/**
 * Grpc transport for grpc between s3g and om.
 */
public class GrpcOmTransport implements OmTransport {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOmTransport.class);

  private static final String CLIENT_NAME = "GrpcOmTransport";
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  // gRPC specific
  private static List<X509Certificate> caCerts = null;

  private OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub client;
  private Map<String,
      OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub> clients;
  private Map<String, ManagedChannel> channels;
  private int lastVisited = -1;
  private ConfigurationSource conf;

  private AtomicReference<String> host;
  private AtomicInteger syncFailoverCount;
  private final int maxSize;
  private SecurityConfig secConfig;

  public static void setCaCerts(List<X509Certificate> x509Certificates) {
    caCerts = x509Certificates;
  }

  private List<String> oms;
  private RetryPolicy retryPolicy;
  private int failoverCount = 0;
  private GrpcOMFailoverProxyProvider<OzoneManagerProtocolPB>
      omFailoverProxyProvider;

  public GrpcOmTransport(ConfigurationSource conf,
                          UserGroupInformation ugi, String omServiceId)
      throws IOException {

    this.channels = new HashMap<>();
    this.clients = new HashMap<>();
    this.conf = conf;
    this.host = new AtomicReference();
    this.failoverCount = 0;
    this.syncFailoverCount = new AtomicInteger();


    secConfig =  new SecurityConfig(conf);
    maxSize = conf.getInt(OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH,
        OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

    omFailoverProxyProvider = new GrpcOMFailoverProxyProvider(
        conf,
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

    List<String> nodes = omFailoverProxyProvider.getGrpcOmNodeIDList();
    for (String nodeId : nodes) {
      String hostaddr = omFailoverProxyProvider.getGrpcProxyAddress(nodeId);
      HostAndPort hp = HostAndPort.fromString(hostaddr);

      NettyChannelBuilder channelBuilder =
          NettyChannelBuilder.forAddress(hp.getHost(), hp.getPort())
              .usePlaintext()
              .maxInboundMessageSize(maxSize);

      if (secConfig.isGrpcTlsEnabled()) {
        try {
          SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
          if (secConfig.isSecurityEnabled()) {
            if (caCerts != null) {
              sslContextBuilder.trustManager(caCerts);
            } else {
              LOG.error("x509Certicates empty");
            }
            channelBuilder.useTransportSecurity().
                sslContext(sslContextBuilder.build());
          } else {
            LOG.error("ozone.security not enabled when TLS specified," +
                " using plaintext");
          }
        } catch (Exception ex) {
          LOG.error("cannot establish TLS for grpc om transport client");
        }
      } else {
        channelBuilder.usePlaintext();
      }

      channels.put(hostaddr, channelBuilder.build());
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
    OMResponse resp = null;
    boolean tryOtherHost = true;
    int expectedFailoverCount = 0;
    ResultCodes resultCode = ResultCodes.INTERNAL_ERROR;
    while (tryOtherHost) {
      tryOtherHost = false;
      expectedFailoverCount = syncFailoverCount.get();
      try {
        resp = clients.get(host.get()).submitRequest(payload);
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
          resultCode = ResultCodes.TIMEOUT;
        }
        Exception exp = new Exception(e);
        tryOtherHost = shouldRetry(unwrapException(exp),
            expectedFailoverCount);
        if (!tryOtherHost) {
          throw new OMException(resultCode);
        }
      }
    }
    return resp;
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
                .indexOf(":")));
        Class<? extends Exception> cls = realClass
            .asSubclass(Exception.class);
        Constructor<? extends Exception> cn = cls.getConstructor(String.class);
        cn.setAccessible(true);
        grpcException = cn.newInstance(status.getDescription());
        IOException remote = null;
        try {
          String cause = status.getDescription();
          cause = cause.substring(cause.indexOf(":") + 2);
          remote = new RemoteException(cause.substring(0, cause.indexOf(":")),
              cause.substring(cause.indexOf(":") + 1));
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

  private boolean shouldRetry(Exception ex, int expectedFailoverCount) {
    boolean retry = false;
    RetryPolicy.RetryAction action = null;
    try {
      action = retryPolicy.shouldRetry((Exception)ex, 0, failoverCount++, true);
      LOG.debug("grpc failover retry action {}", action.action);
      if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
        retry = false;
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
          if (syncFailoverCount.get() == expectedFailoverCount) {
            omFailoverProxyProvider.performFailover(null);
            syncFailoverCount.getAndIncrement();
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
    for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
      ManagedChannel channel = entry.getValue();
      channel.shutdown();
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.error("failed to shutdown OzoneManagerServiceGrpc channel {} : {}",
            entry.getKey(), e);
      }
    }
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
    @Config(key = "port", defaultValue = "8981",
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
    List<String> nodes = omFailoverProxyProvider.getGrpcOmNodeIDList();
    for (String nodeId : nodes) {
      String hostaddr = omFailoverProxyProvider.getGrpcProxyAddress(nodeId);

      clients.put(hostaddr,
          OzoneManagerServiceGrpc
              .newBlockingStub(testChannel));
    }
    LOG.info("{}: started", CLIENT_NAME);
  }

}
