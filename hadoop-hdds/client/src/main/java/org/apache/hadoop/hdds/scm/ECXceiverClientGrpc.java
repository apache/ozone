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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.tracing.GrpcClientInterceptor;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_ENABLE_RETRIES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_ENABLE_RETRIES_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_MAX_RETRIES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_MAX_RETRIES_DEFAULT;

/**
 * {@link XceiverClientSpi} implementation to work specifically with EC
 * related requests. The only difference at the moment from the basic
 * {@link XceiverClientGrpc} is that this implementation does async calls when
 * a write request is posted via the sendCommandAsync method.
 *
 * @see <a href="https://issues.apache.org/jira/browse/HDDS-5954">HDDS-5954</a>
 */
public class ECXceiverClientGrpc extends XceiverClientGrpc {

  private final boolean enableRetries;

  public ECXceiverClientGrpc(
      Pipeline pipeline,
      ConfigurationSource config,
      List<X509Certificate> caCerts) {
    super(pipeline, config, caCerts);
    this.enableRetries = config.getBoolean(OZONE_CLIENT_EC_ENABLE_RETRIES,
        OZONE_CLIENT_EC_ENABLE_RETRIES_DEFAULT);
  }

  /**
   * For EC writes, due to outside syncronization points during writes, it is
   * not necessary to block any async requests that are
   * arriving via the
   * {@link #sendCommandAsync(ContainerProtos.ContainerCommandRequestProto)}
   * method.
   *
   * @param request the request we need the decision about
   * @return false always to do not block async requests.
   */
  @Override
  protected boolean shouldBlockAndWaitAsyncReply(
      ContainerProtos.ContainerCommandRequestProto request) {
    return false;
  }

  @Override
  protected ManagedChannel createChannel(DatanodeDetails dn, int port)
      throws IOException {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(dn.getIpAddress(), port).usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
            .intercept(new GrpcClientInterceptor());
    if (getSecConfig().isGrpcTlsEnabled()) {
      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      if (getCaCerts() != null) {
        sslContextBuilder.trustManager(getCaCerts());
      }
      if (getSecConfig().useTestCert()) {
        channelBuilder.overrideAuthority("localhost");
      }
      channelBuilder.useTransportSecurity().
          sslContext(sslContextBuilder.build());
    } else {
      channelBuilder.usePlaintext();
    }
    if (enableRetries) {
      double maxAttempts = getConfig().getInt(OZONE_CLIENT_EC_GRPC_MAX_RETRIES,
          OZONE_CLIENT_EC_GRPC_MAX_RETRIES_DEFAULT);

      channelBuilder.defaultServiceConfig(createRetryServiceConfig(maxAttempts))
          .maxRetryAttempts((int) maxAttempts).enableRetry();
    }
    return channelBuilder.build();
  }

  private Map<String, Object> createRetryServiceConfig(double maxAttempts) {
    Map<String, Object> retryPolicy = new HashMap<>();
    // Maximum number of RPC attempts which including the original RPC.
    retryPolicy.put("maxAttempts", maxAttempts);
    // The initial retry attempt will occur at random(0, initialBackoff)
    retryPolicy.put("initialBackoff", "0.5s");
    retryPolicy.put("maxBackoff", "3s");
    retryPolicy.put("backoffMultiplier", 1.5D);
    //Status codes for with RPC retry are attempted.
    retryPolicy.put("retryableStatusCodes", Arrays.asList(
        Status.Code.UNAVAILABLE.name(),
        Status.Code.DEADLINE_EXCEEDED.name()));
    Map<String, Object> methodConfig = new HashMap<>();
    methodConfig.put("retryPolicy", retryPolicy);

    Map<String, Object> name = new HashMap<>();
    name.put("service", "hadoop.hdds.datanode.XceiverClientProtocolService");
    methodConfig.put("name", Collections.singletonList(name));

    Map<String, Object> serviceConfig = new HashMap<>();
    serviceConfig.put("methodConfig",
        Collections.singletonList(methodConfig));
    return serviceConfig;
  }
}
