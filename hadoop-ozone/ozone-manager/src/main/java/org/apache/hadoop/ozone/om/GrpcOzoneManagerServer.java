/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.grpc.Server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_GRPC_TLS_PROVIDER;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_GRPC_TLS_PROVIDER_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT;

/**
 * Separated network server for gRPC transport OzoneManagerService s3g->OM.
 */
public class GrpcOzoneManagerServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOzoneManagerServer.class);

  private Server server;
  private int port = 8981;
  private final int maxSize;

  public GrpcOzoneManagerServer(OzoneConfiguration config,
                                OzoneManagerProtocolServerSideTranslatorPB
                                    omTranslator,
                                OzoneDelegationTokenSecretManager
                                    delegationTokenMgr,
                                CertificateClient caClient) {
    maxSize = config.getInt(OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH,
        OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);
    OptionalInt haPort = HddsUtils.getNumberFromConfigKeys(config,
        ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_GRPC_PORT_KEY,
            config.get(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY),
            config.get(OMConfigKeys.OZONE_OM_NODE_ID_KEY)),
        OMConfigKeys.OZONE_OM_GRPC_PORT_KEY);
    if (haPort.isPresent()) {
      this.port = haPort.getAsInt();
    } else {
      this.port = config.getObject(
          GrpcOmTransport.GrpcOmTransportConfig.class).
          getPort();
    }
    
    init(omTranslator,
        delegationTokenMgr,
        config,
        caClient);
  }

  public void init(OzoneManagerProtocolServerSideTranslatorPB omTranslator,
                   OzoneDelegationTokenSecretManager delegationTokenMgr,
                   OzoneConfiguration omServerConfig,
                   CertificateClient caClient) {
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .maxInboundMessageSize(maxSize)
        .addService(new OzoneManagerServiceGrpc(omTranslator,
            delegationTokenMgr,
            omServerConfig));

    SecurityConfig secConf = new SecurityConfig(omServerConfig);
    if (secConf.isGrpcTlsEnabled()) {
      try {
        if (secConf.isSecurityEnabled()) {
          SslContextBuilder sslClientContextBuilder =
              SslContextBuilder.forServer(caClient.getPrivateKey(),
                  caClient.getCertificate());
          SslContextBuilder sslContextBuilder = GrpcSslContexts.configure(
              sslClientContextBuilder,
              SslProvider.valueOf(omServerConfig.get(HDDS_GRPC_TLS_PROVIDER,
                  HDDS_GRPC_TLS_PROVIDER_DEFAULT)));
          nettyServerBuilder.sslContext(sslContextBuilder.build());
        } else {
          LOG.error("ozone.security not enabled when TLS specified," +
                            " creating Om S3g GRPC channel using plaintext");
        }
      } catch (Exception ex) {
        LOG.error("Unable to setup TLS for secure Om S3g GRPC channel.", ex);
      }
    }

    server = nettyServerBuilder.build();
  }

  public void start() throws IOException {
    server.start();
    LOG.info("{} is started using port {}", getClass().getSimpleName(),
        server.getPort());
    port = server.getPort();
  }

  public void stop() {
    try {
      server.shutdown().awaitTermination(10L, TimeUnit.SECONDS);
      LOG.info("Server {} is shutdown", getClass().getSimpleName());
    } catch (InterruptedException ex) {
      LOG.warn("{} couldn't be stopped gracefully", getClass().getSimpleName());
    }
  }
  public int getPort() {
    return port;
  }
}
