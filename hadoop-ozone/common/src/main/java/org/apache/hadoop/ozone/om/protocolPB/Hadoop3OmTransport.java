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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Full-featured Hadoop RPC implementation with failover support.
 */
public class Hadoop3OmTransport implements OmTransport {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(Hadoop3OmTransport.class);

  private final OMFailoverProxyProvider omFailoverProxyProvider;

  private final OzoneManagerProtocolPB rpcProxy;

  public Hadoop3OmTransport(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    this.omFailoverProxyProvider = new OMFailoverProxyProvider(conf, ugi,
        omServiceId, OzoneManagerProtocolPB.class);

    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    this.rpcProxy = createRetryProxy(omFailoverProxyProvider, maxFailovers);
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

  @Override
  public void close() throws IOException {
    omFailoverProxyProvider.close();
  }
}
