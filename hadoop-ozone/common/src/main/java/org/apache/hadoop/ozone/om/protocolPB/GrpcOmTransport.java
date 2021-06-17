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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;
import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
/**
 * Grpc transport for grpc between s3g and om.
 */
public class GrpcOmTransport implements OmTransport {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOmTransport.class);

  private final OMFailoverProxyProvider omFailoverProxyProvider;

  // gRPC specific
  private final ManagedChannel channel;

  private final OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub client;

  //private final String host = "0.0.0.0";
  private String host = "om";
  private final int port = 8981;

  public GrpcOmTransport(ConfigurationSource conf,
                          UserGroupInformation ugi, String omServiceId)
      throws IOException {
    Optional<String> omHost = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);
    this.host = omHost.orElse("0.0.0.0");
    this.omFailoverProxyProvider = new OMFailoverProxyProvider(conf, ugi,
        omServiceId);

    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(this.host, port)
            .usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

    channel = channelBuilder.build();
    client = OzoneManagerServiceGrpc.newBlockingStub(channel);
  }

  @Override
  @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    for (TokenIdentifier tid: ugi.
            getTokenIdentifiers()) {
      if (tid instanceof OzoneTokenIdentifier) {
        OzoneTokenIdentifier oti = (OzoneTokenIdentifier)tid;
        LOG.info(oti.toString());
        if (oti.getTokenType().equals(S3AUTHINFO)) {
          payload = OMRequest.newBuilder(payload)
              .setSignature(oti.getSignature())
              .setStringToSign(oti.getStrToSign())
              .setAwsAccessId(oti.getAwsAccessId())
              .setUserInfo(OzoneManagerProtocolProtos
                  .UserInfo.newBuilder()
                  .setUserName(ugi.getUserName()).build())
              .build();
        }
      }
    }
    LOG.debug("OMRequest {}", payload);
    return client.submitRequest(payload);
  }

  @Override
  public Text getDelegationTokenService() {
    return omFailoverProxyProvider.getCurrentProxyDelegationToken();
  }
  /**
   * Creates a {@link RetryProxy} encapsulating the
   * {@link OMFailoverProxyProvider}. The retry proxy fails over on network
   * exception or if the current proxy is not the leader OM.
   */
  private OzoneManagerProtocolPB createRetryProxy(
      OMFailoverProxyProvider failoverProxyProvider, int maxFailovers) {
    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy.create(
        OzoneManagerProtocolPB.class, failoverProxyProvider,
        failoverProxyProvider.getRetryPolicy(maxFailovers));
    return proxy;
  }

  @VisibleForTesting
  public OMFailoverProxyProvider getOmFailoverProxyProvider() {
    return omFailoverProxyProvider;
  }

  public void shutdown() {
    if (channel == null) {
      return;
    }
    channel.shutdown();
    try {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("failed to shutdown OzoneManagerServiceGrpc channel", e);
    } finally {
      channel.shutdownNow();
    }
  }

  @Override
  public void close() throws IOException {
    omFailoverProxyProvider.close();
    shutdown();
  }
}
