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
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.RpcController;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc
    .OzoneManagerServiceImplBase;
import org.apache.hadoop.ozone.protocolPB
    .OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.ClientId;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;

class OzoneManagerServiceGrpc extends OzoneManagerServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerServiceGrpc.class);
  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private OzoneManagerProtocolServerSideTranslatorPB omTranslator;
  private OzoneDelegationTokenSecretManager delegationTokenMgr;
  private final SecurityConfig secConfig;

  OzoneManagerServiceGrpc(
      OzoneManagerProtocolServerSideTranslatorPB omTranslator,
      OzoneDelegationTokenSecretManager delegationTokenMgr,
      OzoneConfiguration configuration) {
    this.omTranslator = omTranslator;
    this.delegationTokenMgr = delegationTokenMgr;
    this.secConfig = new SecurityConfig(configuration);
  }

  @Override
  public void submitRequest(org.apache.hadoop.ozone.protocol.proto.
                                  OzoneManagerProtocolProtos.
                                  OMRequest request,
                            io.grpc.stub.StreamObserver<org.apache.
                                hadoop.ozone.protocol.proto.
                                OzoneManagerProtocolProtos.OMResponse>
                                responseObserver) {
    LOG.info("GrpcOzoneManagerServer: OzoneManagerServiceImplBase " +
        "processing s3g client submit request");
    AtomicInteger callCount = new AtomicInteger(0);

    if (secConfig.isSecurityEnabled()) {
      if (request.hasStringToSign()) {
        LOG.info(request.getStringToSign());
        OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
        identifier.setTokenType(S3AUTHINFO);
        identifier.setStrToSign(request.getStringToSign());
        identifier.setSignature(request.getSignature());
        identifier.setAwsAccessId(request.getAwsAccessId());
        identifier.setOwner(new Text(request.getAwsAccessId()));
        LOG.info("validating S3 identifier:{}",
            identifier);
        try {
          delegationTokenMgr.retrievePassword(identifier);
          LOG.info("valid signature");
        } catch (OzoneDelegationTokenSecretManager.InvalidToken e) {
          LOG.info("signatures do NOT match for S3 identifier:{}",
              identifier, e);
        }
      }
    }

    try {
        // need to look into handling the error path, trapping exception
      org.apache.hadoop.ipc.Server.getCurCall().set(new Call(1,
          callCount.incrementAndGet(),
          null,
          null,
          RPC.RpcKind.RPC_PROTOCOL_BUFFER,
          ClientId.getClientId()));

      OMResponse omResponse =
          UserGroupInformation.getCurrentUser().doAs(
            (PrivilegedExceptionAction<OMResponse>) () -> {
              return this.omTranslator.
                  submitRequest(NULL_RPC_CONTROLLER, request);
            });
      responseObserver.onNext(omResponse);
    } catch (Exception e) {}
    responseObserver.onCompleted();
  }
}

/**
 * Separated network server for gRPC transport OzoneManagerService s3g->OM.
 */
public class GrpcOzoneManagerServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOzoneManagerServer.class);

  private Server server;
  private final String host = "0.0.0.0";
  private int port = 8981;

  public GrpcOzoneManagerServer(OzoneConfiguration config,
                                OzoneManagerProtocolServerSideTranslatorPB
                                    omTranslator,
                                OzoneDelegationTokenSecretManager
                                    delegationTokenMgr) {
    this.port = config.getObject(
        GrpcOzoneManagerServerConfig.class).
        getPort();
    init(omTranslator,
        delegationTokenMgr,
        config);
  }

  public void init(OzoneManagerProtocolServerSideTranslatorPB omTranslator,
                   OzoneDelegationTokenSecretManager delegationTokenMgr,
                   OzoneConfiguration omServerConfig) {
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
        .addService(new OzoneManagerServiceGrpc(omTranslator,
            delegationTokenMgr,
            omServerConfig));

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
    } catch (InterruptedException ex) {
      LOG.warn("{} couldn't be stopped gracefully", getClass().getSimpleName());
    }
  }

  public int getPort() {
    return port; }

  @ConfigGroup(prefix = "ozone.om.protocolPB")
  public static final class GrpcOzoneManagerServerConfig {
    @Config(key = "port", defaultValue = "8981",
        description = "Port used for"
            + " the GrpcOmTransport OzoneManagerServiceGrpc server",
        tags = {ConfigTag.MANAGEMENT})
    private int port;

    public int getPort() {
      return port;
    }

    public GrpcOzoneManagerServerConfig setPort(int portParam) {
      this.port = portParam;
      return this;
    }
  }
}
