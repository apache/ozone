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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocol.OMMetadata;
import org.apache.hadoop.ozone.om.protocol.OMMetadataProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerMetadataProtocolProtos.OMMetadataInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerMetadataProtocolProtos.OMMetadataInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerMetadataProtocolProtos.OMNodeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protocol implementation for getting OM metadata information.
 */
public class OMMetadataProtocolClientSideImpl implements
    OMMetadataProtocol {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(OMMetadataProtocolClientSideImpl.class);

  private final String omNodeID;
  private final OMMetadataProtocolPB rpcProxy;

  public OMMetadataProtocolClientSideImpl(ConfigurationSource conf,
      UserGroupInformation ugi, String omNodeId, InetSocketAddress omAddress)
      throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OMInterServiceProtocolPB.class, ProtobufRpcEngine.class);

    this.omNodeID = omNodeId;

    // OM metadata is requested from a specific OM and hence there is no need
    // of any failover provider.
    RetryPolicy connectionRetryPolicy = RetryPolicies
        .failoverOnNetworkException(0);
    Configuration hadoopConf = LegacyHadoopConfigurationSource
        .asHadoopConfiguration(conf);

    OMMetadataProtocolPB proxy = RPC.getProtocolProxy(OMMetadataProtocolPB.class,
        RPC.getProtocolVersion(OMMetadataProtocolPB.class), omAddress, ugi,
        hadoopConf, NetUtils.getDefaultSocketFactory(hadoopConf),
        (int) OmUtils.getOMClientRpcTimeOut(conf),
        connectionRetryPolicy).getProxy();

    RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        10, 1000, TimeUnit.MILLISECONDS);

    this.rpcProxy = (OMMetadataProtocolPB) RetryProxy.create(
        OMMetadataProtocolPB.class, proxy, retryPolicy);
  }

  @Override
  public OMMetadata getOMMetadata() throws IOException {
    try {
      OMMetadataInfoResponse metadataInfoResponse = rpcProxy.getOMMetadataInfo(
          NULL_RPC_CONTROLLER, OMMetadataInfoRequest.newBuilder().build());

      OMMetadata.Builder omMedatataBuilder = new OMMetadata.Builder();
      if (metadataInfoResponse.getSuccess()) {
        if (metadataInfoResponse.getNodesInMemoryCount() > 0) {
          for (OMNodeInfo omNodeInfo :
              metadataInfoResponse.getNodesInMemoryList()) {
            omMedatataBuilder.addToNodesInMemory(
                OMNodeDetails.getFromProtobuf(omNodeInfo));
          }
        }
        if (metadataInfoResponse.getNodesInNewConfCount() > 0) {
          for (OMNodeInfo omNodeInfo :
              metadataInfoResponse.getNodesInNewConfList()) {
            omMedatataBuilder.addToNodesInNewConf(
                OMNodeDetails.getFromProtobuf(omNodeInfo));
          }
        }
      }
      return omMedatataBuilder.build();
    } catch (ServiceException e) {
      LOG.error("Failed to retrieve metadata information of OM {}", omNodeID,
          e);
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }
}
