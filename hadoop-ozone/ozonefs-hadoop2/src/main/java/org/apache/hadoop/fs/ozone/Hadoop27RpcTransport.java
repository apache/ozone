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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_KEY;


import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFollowerReadFailoverProxyProvider;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.request.invocation.OzoneRetryInvocationHandler;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop RPC based transport with failover support for Hadoop 2.7.
 * Supports multi-raft routing based on bucket paths.
 */
public class Hadoop27RpcTransport implements OmTransport {

  private static final Logger LOG =
      LoggerFactory.getLogger(Hadoop27RpcTransport.class);

  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final OzoneManagerProtocolPB rpcProxy;

  private final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider;
  private final HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider;

  private final OzoneRetryInvocationHandler<OzoneManagerProtocolPB> retryInvocationHandler;

  public Hadoop27RpcTransport(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    this.omFailoverProxyProvider = new HadoopRpcOMFailoverProxyProvider<>(
        conf, ugi, omServiceId, OzoneManagerProtocolPB.class);

    boolean followerReadEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_DEFAULT);

    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    String defaultLeaderReadConsistencyStr = conf.get(OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_KEY,
        OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_DEFAULT);
    ReadConsistency defaultLeaderReadConsistency = ReadConsistency.valueOf(defaultLeaderReadConsistencyStr);

    String defaultFollowerReadConsistencyStr = conf.get(
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_DEFAULT_CONSISTENCY_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_DEFAULT_CONSISTENCY_DEFAULT
    );
    ReadConsistency defaultFollowerReadConsistency =
        ReadConsistency.valueOf(defaultFollowerReadConsistencyStr);
    this.followerReadFailoverProxyProvider =
        new HadoopRpcOMFollowerReadFailoverProxyProvider(omFailoverProxyProvider,
            defaultFollowerReadConsistency,
            defaultLeaderReadConsistency,
            followerReadEnabled);
    this.retryInvocationHandler = new OzoneRetryInvocationHandler<>(
        followerReadFailoverProxyProvider, followerReadFailoverProxyProvider.getRetryPolicy(maxFailovers));
    this.rpcProxy = createRetryProxy(retryInvocationHandler);
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    try {
      // Set the OMRequest in the proxy provider for multi-raft routing
      omFailoverProxyProvider.setOmRequest(payload);
      OMResponse omResponse =
          rpcProxy.submitRequest(NULL_RPC_CONTROLLER, payload);

      // Handle leader failover based on response
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
  public OMResponse submitRequest(OMRequest payload, String omNodeId) throws IOException {
    // TODO try to send request to the certain om node with omNodeId
    return submitRequest(payload);
  }

  @Override
  public Text getDelegationTokenService() {
    return null;
  }

  /**
   * Creates a {@link org.apache.hadoop.io.retry.RetryProxy} encapsulating the
   * {@link HadoopRpcOMFailoverProxyProvider}. The retry proxy
   * fails over on network exception or if the current proxy
   * is not the leader OM. Uses OzoneRetryInvocationHandler for multi-raft
   * bucket-to-raft-group routing.
   */
  private OzoneManagerProtocolPB createRetryProxy(InvocationHandler invocationHandler) {
    return (OzoneManagerProtocolPB) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {OzoneManagerProtocolPB.class},
        invocationHandler);
  }

  @Override
  public void close() throws IOException {
    if (followerReadFailoverProxyProvider != null) {
      followerReadFailoverProxyProvider.close();
    } else {
      omFailoverProxyProvider.close();
    }
  }
}
