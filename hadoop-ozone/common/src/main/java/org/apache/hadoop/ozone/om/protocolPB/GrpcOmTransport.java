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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.IntraDatanodeProtocolServiceGrpc;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub;
import io.grpc.ManagedChannel;
//import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.hadoop.ozone.OzoneConsts;

import io.grpc.netty.NettyChannelBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
//import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Grpc transport for grpc between s3g and om.
 */
public class GrpcOmTransport implements OmTransport {

    /**
     * RpcController is not used and hence is set to null.
     */
    private static final RpcController NULL_RPC_CONTROLLER = null;

    private static final Logger LOG =
            LoggerFactory.getLogger(GrpcOmTransport.class);

    private final OMFailoverProxyProvider omFailoverProxyProvider;

    private final OzoneManagerProtocolPB rpcProxy;

    // gRPC specific
    private final ManagedChannel channel;

    private final OzoneManagerServiceGrpc.OzoneManagerServiceBlockingStub client;

    private final String host = "0.0.0.0";
    private final int port = 8981;

    public GrpcOmTransport(ConfigurationSource conf,
                              UserGroupInformation ugi, String omServiceId) throws IOException {

        RPC.setProtocolEngine(OzoneConfiguration.of(conf),
                OzoneManagerProtocolPB.class,
                ProtobufRpcEngine.class);

        this.omFailoverProxyProvider = new OMFailoverProxyProvider(conf, ugi,
                omServiceId);

        int maxFailovers = conf.getInt(
                OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
                OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

        this.rpcProxy = createRetryProxy(omFailoverProxyProvider, maxFailovers);


        NettyChannelBuilder channelBuilder =
                NettyChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE);

        channel = channelBuilder.build();
        client = OzoneManagerServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public OMResponse submitRequest(OMRequest payload) throws IOException {
        //return client.submitRequest(payload);
        try {
            OMResponse omResponse =
                    rpcProxy.submitRequest(NULL_RPC_CONTROLLER, payload);

            if (omResponse.hasLeaderOMNodeId() && omFailoverProxyProvider != null) {
                String leaderOmId = omResponse.getLeaderOMNodeId();

                // Failover to the OM node returned by OMResponse leaderOMNodeId if
                // current proxy is not pointing to that node.
                omFailoverProxyProvider.performFailoverIfRequired(leaderOmId);
            }

            return omResponse;
        } catch (ServiceException e) {
            OMNotLeaderException notLeaderException =
                    OMFailoverProxyProvider.getNotLeaderException(e);
            if (notLeaderException == null) {
                throw ProtobufHelper.getRemoteException(e);
            }
            throw new IOException("Could not determine or connect to OM Leader.");
        }

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
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("failed to shutdown OzoneManagerServiceGrpc channel", e);
        }
    }

    @Override
    public void close() throws IOException {
        omFailoverProxyProvider.close();
        shutdown();
    }
}
