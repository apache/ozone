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
package org.apache.hadoop.ozone.om.protocolPB;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc.OzoneManagerServiceImplBase;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OzoneManagerServiceGrpc extends OzoneManagerServiceImplBase {}

/**
 * Separated network server for gRPC transport OzoneManagerService s3g->OM.
 */
public class GrpcOzoneManagerServer {

    private static final Logger LOG =
            LoggerFactory.getLogger(GrpcOzoneManagerServer.class);

    private Server server;

    private final String host = "127.0.0.1";
    private int port = 8981;

    public GrpcOzoneManagerServer(GrpcOzoneManagerServerConfig omServerConfig) {
        this.port = omServerConfig.getPort();
        init();
    }

    public void init() {
        NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
                .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
                .addService(new OzoneManagerServiceGrpc());

        server = nettyServerBuilder.build();
    }

    public void start() throws IOException {
        server.start();

        if (port == 0) {
            LOG.info("{} is started using port {}", getClass().getSimpleName(),
                    server.getPort());
        }

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
        return port;
    }

    @ConfigGroup(prefix = "ozone.om.protocolPB")
    public static final class GrpcOzoneManagerServerConfig {

        @Config(key = "port", defaultValue = "8981", description = "Port used for"
                + " the GrpcOmTransport OzoneManagerServiceGrpc server", tags = {
                ConfigTag.MANAGEMENT})
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
