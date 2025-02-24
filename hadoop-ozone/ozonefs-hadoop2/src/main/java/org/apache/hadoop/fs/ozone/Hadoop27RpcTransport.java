/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.HadoopRpcSingleOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Hadoop RPC based transport with failover support.
 */
public class Hadoop27RpcTransport implements OmTransport {

  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final String omServiceId;

  // This RPC proxy is used for requests made for OM leader.
  private final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider;
  private final OzoneManagerProtocolPB rpcProxy;

  // This RPC proxy is used for requests made for a specific OM node.
  private final Map<String, HadoopRpcSingleOMFailoverProxyProvider<OzoneManagerProtocolPB>> omFailoverProxyProviders;
  private final Map<String, OzoneManagerProtocolPB> rpcProxies;

  public Hadoop27RpcTransport(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {
    this.omServiceId = omServiceId;

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    this.omFailoverProxyProvider = new HadoopRpcOMFailoverProxyProvider<>(
            conf, ugi, omServiceId, OzoneManagerProtocolPB.class);

    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    this.rpcProxy = createRetryProxy(omFailoverProxyProvider, maxFailovers);

    Map<String, OzoneManagerProtocolPB> rpcProxiesMap = new HashMap<>();
    Map<String, HadoopRpcSingleOMFailoverProxyProvider<OzoneManagerProtocolPB>>
        omFailoverProxyProvidersMap = new HashMap<>();
    Collection<String> omNodeIds = OmUtils.getActiveOMNodeIds(conf, omServiceId);
    for (String omNodeId : omNodeIds) {
      HadoopRpcSingleOMFailoverProxyProvider<OzoneManagerProtocolPB> singleOMFailoverProxyProvider =
          new HadoopRpcSingleOMFailoverProxyProvider<>(conf, ugi, omServiceId, omNodeId, OzoneManagerProtocolPB.class);
      omFailoverProxyProvidersMap.putIfAbsent(omNodeId, singleOMFailoverProxyProvider);
      rpcProxiesMap.putIfAbsent(omNodeId, createRetryProxy(singleOMFailoverProxyProvider, maxFailovers));
    }
    this.omFailoverProxyProviders = Collections.unmodifiableMap(omFailoverProxyProvidersMap);
    this.rpcProxies = Collections.unmodifiableMap(rpcProxiesMap);
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    try {
      OMResponse omResponse =
          rpcProxy.submitRequest(NULL_RPC_CONTROLLER, payload);

      if (omResponse.hasLeaderOMNodeId() && omFailoverProxyProvider != null) {
        String leaderOmId = omResponse.getLeaderOMNodeId();

        // Failover to the OM node returned by OMResponse leaderOMNodeId if
        // current proxy is not pointing to that node.
        omFailoverProxyProvider.setNextOmProxy(leaderOmId);
        omFailoverProxyProvider.performFailover(null);
      }
      return omResponse;
    } catch (ServiceException e) {
      OMNotLeaderException notLeaderException =
          HadoopRpcOMFailoverProxyProvider.getNotLeaderException(e);
      if (notLeaderException == null) {
        throw ProtobufHelper.getRemoteException(e);
      }
      throw new IOException("Could not determine or connect to OM Leader.");
    }
  }

  @Override
  public OMResponse submitRequest(String omNodeId, OMRequest payload) throws IOException {
    try {
      OzoneManagerProtocolPB singleRpcProxy = rpcProxies.get(omNodeId);
      if (singleRpcProxy == null) {
        throw new IOException(String.format("Could not find any configured client for OM node %s in service %s. " +
            "Please configure the system with %s", omNodeId, omServiceId, OZONE_OM_ADDRESS_KEY));
      }
      if (!payload.hasOmNodeId()) {
        payload = payload.toBuilder().setOmNodeId(omNodeId).build();
      }
      return singleRpcProxy.submitRequest(NULL_RPC_CONTROLLER, payload);
    } catch (ServiceException e) {
      throw new IOException("Could not connect to OM node " + omNodeId);
    }
  }

  @Override
  public Text getDelegationTokenService() {
    return null;
  }

  /**
   * Creates a {@link RetryProxy} encapsulating the
   * {@link HadoopRpcOMFailoverProxyProvider}. The retry proxy fails over on
   * network exception or if the current proxy is not the leader OM.
   */
  private OzoneManagerProtocolPB createRetryProxy(
      HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> failoverProxyProvider,
      int maxFailovers) {

    return (OzoneManagerProtocolPB) RetryProxy.create(
        OzoneManagerProtocolPB.class, failoverProxyProvider,
        failoverProxyProvider.getRetryPolicy(maxFailovers));
  }

  /**
   * Creates a {@link RetryProxy} encapsulating the
   * {@link HadoopRpcSingleOMFailoverProxyProvider}. The retry proxy fails over on
   * network exception.
   */
  private OzoneManagerProtocolPB createRetryProxy(
      HadoopRpcSingleOMFailoverProxyProvider<OzoneManagerProtocolPB> singleOMFailoverProxyProvider,
      int maxRetry) {

    return (OzoneManagerProtocolPB) RetryProxy.create(
        OzoneManagerProtocolPB.class, singleOMFailoverProxyProvider,
        singleOMFailoverProxyProvider.getRetryPolicy(maxRetry));
  }

  @Override
  public void close() throws IOException {
    omFailoverProxyProvider.close();
    for (HadoopRpcSingleOMFailoverProxyProvider<OzoneManagerProtocolPB> failoverProxyProvider
        : omFailoverProxyProviders.values()) {
      failoverProxyProvider.close();
    }
  }

}
