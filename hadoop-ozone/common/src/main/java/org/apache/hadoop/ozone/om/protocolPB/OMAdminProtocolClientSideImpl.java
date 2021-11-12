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
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocol.OMConfiguration;
import org.apache.hadoop.ozone.om.protocol.OMAdminProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMNodeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protocol implementation for getting OM metadata information.
 */
public class OMAdminProtocolClientSideImpl implements OMAdminProtocol {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(OMAdminProtocolClientSideImpl.class);

  private final OMNodeDetails remoteOmNodeDetails;
  private final OMAdminProtocolPB rpcProxy;

  public OMAdminProtocolClientSideImpl(ConfigurationSource conf,
      UserGroupInformation ugi, OMNodeDetails omNodeDetails)
      throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OMAdminProtocolPB.class, ProtobufRpcEngine.class);

    this.remoteOmNodeDetails = omNodeDetails;

    int maxRetries = conf.getInt(
        OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_MAX_RETRIES_KEY,
        OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_MAX_RETRIES_DEFAULT);
    long waitBetweenRetries = conf.getLong(
        OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_WAIT_BETWEEN_RETRIES_KEY,
        OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_WAIT_BETWEEN_RETRIES_DEFAULT);

    // OM metadata is requested from a specific OM and hence there is no need
    // of any failover provider.
    RetryPolicy connectionRetryPolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(maxRetries, waitBetweenRetries,
            TimeUnit.MILLISECONDS);
    Configuration hadoopConf = LegacyHadoopConfigurationSource
        .asHadoopConfiguration(conf);

    OMAdminProtocolPB proxy = RPC.getProtocolProxy(
        OMAdminProtocolPB.class,
        RPC.getProtocolVersion(OMAdminProtocolPB.class),
        remoteOmNodeDetails.getRpcAddress(), ugi, hadoopConf,
        NetUtils.getDefaultSocketFactory(hadoopConf),
        (int) OmUtils.getOMClientRpcTimeOut(conf), connectionRetryPolicy)
        .getProxy();

    RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        10, 1000, TimeUnit.MILLISECONDS);

    this.rpcProxy = (OMAdminProtocolPB) RetryProxy.create(
        OMAdminProtocolPB.class, proxy, retryPolicy);
  }

  @Override
  public OMConfiguration getOMConfiguration() throws IOException {
    try {
      OMConfigurationResponse getConfigResponse = rpcProxy.getOMConfiguration(
          NULL_RPC_CONTROLLER, OMConfigurationRequest.newBuilder().build());

      OMConfiguration.Builder omMedatataBuilder = new OMConfiguration.Builder();
      if (getConfigResponse.getSuccess()) {
        if (getConfigResponse.getNodesInMemoryCount() > 0) {
          for (OMNodeInfo omNodeInfo :
              getConfigResponse.getNodesInMemoryList()) {
            omMedatataBuilder.addToNodesInMemory(
                OMNodeDetails.getFromProtobuf(omNodeInfo));
          }
        }
        if (getConfigResponse.getNodesInNewConfCount() > 0) {
          for (OMNodeInfo omNodeInfo :
              getConfigResponse.getNodesInNewConfList()) {
            omMedatataBuilder.addToNodesInNewConf(
                OMNodeDetails.getFromProtobuf(omNodeInfo));
          }
        }
      }
      return omMedatataBuilder.build();
    } catch (ServiceException e) {
      LOG.error("Failed to retrieve configuration of OM {}",
          remoteOmNodeDetails.getOMPrintInfo(), e);
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }
}
