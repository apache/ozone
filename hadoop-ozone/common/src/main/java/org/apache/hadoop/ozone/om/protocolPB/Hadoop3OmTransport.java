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

package org.apache.hadoop.ozone.om.protocolPB;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Full-featured Hadoop RPC implementation with failover support.
 */
public class Hadoop3OmTransport implements OmTransport {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider;
  private HadoopRpcOMFollowerReadFailoverProxyProvider<OzoneManagerProtocolPB> followerReadFailoverProxyProvider;

  private final OzoneManagerProtocolPB rpcProxy;

  public Hadoop3OmTransport(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    this.omFailoverProxyProvider = new HadoopRpcOMFailoverProxyProvider<>(
            conf, ugi, omServiceId, OzoneManagerProtocolPB.class);

    boolean followerReadEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_DEFAULT
    );

    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    // TODO: In the future, we might support more FollowerReadProxyProvider strategies depending on factors
    //  like latency, applied index, etc.
    //  So instead of enabling using follower read configuration, we can simply let user to configure the
    //  failover proxy provider instead (similar to dfs.client.failover.proxy.provider.<nameservice>)
    if (followerReadEnabled) {
      this.followerReadFailoverProxyProvider = new HadoopRpcOMFollowerReadFailoverProxyProvider<>(
          omServiceId, OzoneManagerProtocolPB.class, omFailoverProxyProvider
      );
      this.rpcProxy = OzoneManagerProtocolPB.newProxy(followerReadFailoverProxyProvider, maxFailovers);
    } else {
      // TODO: It should be possible to simply instantiate HadoopRpcOMFollowerReadFailoverProxyProvider
      //  even if the follower read is not enabled. We can try this to ensure that the tests still pass which
      //  suggests that the HadoopRpcOMFollowerReadFailoverProxyProvider is a indeed a superset of
      //  HadoopRpcOMFollowerReadFailoverProxyProvider
      this.rpcProxy = OzoneManagerProtocolPB.newProxy(omFailoverProxyProvider, maxFailovers);
    }
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
  public Text getDelegationTokenService() {
    return omFailoverProxyProvider.getCurrentProxyDelegationToken();
  }

  @VisibleForTesting
  public HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> getOmFailoverProxyProvider() {
    return omFailoverProxyProvider;
  }

  @VisibleForTesting
  public HadoopRpcOMFollowerReadFailoverProxyProvider getOmFollowerReadFailoverProxyProvider() {
    return followerReadFailoverProxyProvider;
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
