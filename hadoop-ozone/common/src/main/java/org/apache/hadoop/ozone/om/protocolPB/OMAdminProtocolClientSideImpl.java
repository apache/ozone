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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocol.OMConfiguration;
import org.apache.hadoop.ozone.om.protocol.OMAdminProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.DecommissionOMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.DecommissionOMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMNodeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protocol implementation for OM admin operations.
 */
public final class OMAdminProtocolClientSideImpl implements OMAdminProtocol {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(OMAdminProtocolClientSideImpl.class);

  private final OMAdminProtocolPB rpcProxy;
  private final String omPrintInfo; // For targeted OM proxy

  private OMAdminProtocolClientSideImpl(OMAdminProtocolPB proxy,
      String printInfo) {
    this.rpcProxy = proxy;
    this.omPrintInfo = printInfo;
  }

  /**
   * Create OM Admin Protocol Client for contacting the given OM (does not
   * failover to different OM). Use for admin commands such as
   * getOMConfiguration which are targeted to a specific OM.
   */
  public static OMAdminProtocolClientSideImpl createProxyForSingleOM(
      OzoneConfiguration conf, UserGroupInformation ugi,
      OMNodeDetails omNodeDetails) throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OMAdminProtocolPB.class, ProtobufRpcEngine.class);

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
        omNodeDetails.getRpcAddress(), ugi, hadoopConf,
        NetUtils.getDefaultSocketFactory(hadoopConf),
        (int) OmUtils.getOMClientRpcTimeOut(conf), connectionRetryPolicy)
        .getProxy();

    RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        10, 1000, TimeUnit.MILLISECONDS);

    OMAdminProtocolPB rpcProxy = (OMAdminProtocolPB) RetryProxy.create(
        OMAdminProtocolPB.class, proxy, retryPolicy);

    return new OMAdminProtocolClientSideImpl(rpcProxy,
        omNodeDetails.getOMPrintInfo());
  }

  /**
   * Create OM Admin Protocol Client for contacting the OM ring (failover
   * till the current OM leader is reached). Use for admin commands such as
   * decommissionOM which should reach the OM leader.
   */
  public static OMAdminProtocolClientSideImpl createProxyForOMHA(
      OzoneConfiguration conf, UserGroupInformation ugi, String omServiceId)
      throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OMAdminProtocolPB.class, ProtobufRpcEngine.class);

    HadoopRpcOMFailoverProxyProvider omFailoverProxyProvider =
        new HadoopRpcOMFailoverProxyProvider(conf, ugi, omServiceId,
            OMAdminProtocolPB.class);

    // Multiple the max number of retries with number of OMs to calculate the
    // max number of failovers.
    int maxFailovers = conf.getInt(
        OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_MAX_RETRIES_KEY,
        OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_MAX_RETRIES_DEFAULT) *
        omFailoverProxyProvider.getOMProxies().size();

    OMAdminProtocolPB retryProxy = (OMAdminProtocolPB) RetryProxy.create(
        OMAdminProtocolPB.class, omFailoverProxyProvider,
        omFailoverProxyProvider.getRetryPolicy(maxFailovers));

    List<OMNodeDetails> allOMNodeDetails = OmUtils.getAllOMHAAddresses(conf,
        omServiceId, false);

    return new OMAdminProtocolClientSideImpl(retryProxy,
        OmUtils.getOMAddressListPrintString(allOMNodeDetails));
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
      LOG.error("Failed to retrieve configuration of OM {}", omPrintInfo, e);
    }
    return null;
  }

  @Override
  public void decommission(OMNodeDetails removeOMNode) throws IOException {
    DecommissionOMRequest decommOMRequest = DecommissionOMRequest.newBuilder()
        .setNodeId(removeOMNode.getNodeId())
        .setNodeAddress(removeOMNode.getHostAddress())
        .build();

    DecommissionOMResponse response;
    try {
      response = rpcProxy.decommission(NULL_RPC_CONTROLLER, decommOMRequest);
    } catch (ServiceException e) {
      OMNotLeaderException notLeaderException =
          HadoopRpcOMFailoverProxyProvider.getNotLeaderException(e);
      if (notLeaderException != null) {
        throwException(notLeaderException.getMessage());
      }

      OMLeaderNotReadyException leaderNotReadyException =
          HadoopRpcOMFailoverProxyProvider.getLeaderNotReadyException(e);
      if (leaderNotReadyException != null) {
        throwException(leaderNotReadyException.getMessage());
      }
      throw ProtobufHelper.getRemoteException(e);
    }

    if (!response.getSuccess()) {
      throwException("Request to decommission" + removeOMNode.getOMPrintInfo() +
          ", sent to " + omPrintInfo + " failed with error: " +
          response.getErrorMsg());
    }
  }

  private void throwException(String errorMsg)
      throws IOException {
    throw new IOException("Failed to Decommission OM. Error: " + errorMsg);
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }
}
