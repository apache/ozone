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
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyHint;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Hadoop RPC based transport with failover support.
 */
public class Hadoop27RpcTransport implements OmTransport {

  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final OzoneManagerProtocolPB rpcProxy;

  private final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider;
  private final boolean followerReadEnabled;
  private final ReadConsistencyHint defaultLeaderReadConsistencyHint;
  private HadoopRpcOMFollowerReadFailoverProxyProvider followerReadFailoverProxyProvider;

  public Hadoop27RpcTransport(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    this.omFailoverProxyProvider = new HadoopRpcOMFailoverProxyProvider<>(
            conf, ugi, omServiceId, OzoneManagerProtocolPB.class);

    followerReadEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_DEFAULT
    );

    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    String defaultLeaderReadConsistencyStr = conf.get(OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_KEY,
        OZONE_CLIENT_LEADER_READ_DEFAULT_CONSISTENCY_DEFAULT);
    ReadConsistency defaultLeaderReadConsistency = ReadConsistency.valueOf(defaultLeaderReadConsistencyStr);
    defaultLeaderReadConsistencyHint = defaultLeaderReadConsistency.getHint();

    if (followerReadEnabled) {
      String defaultFollowerReadConsistencyStr = conf.get(
          OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_DEFAULT_CONSISTENCY_KEY,
          OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_DEFAULT_CONSISTENCY_DEFAULT
      );
      ReadConsistency defaultFollowerReadConsistency =
          ReadConsistency.valueOf(defaultFollowerReadConsistencyStr);
      this.followerReadFailoverProxyProvider =
          new HadoopRpcOMFollowerReadFailoverProxyProvider(omFailoverProxyProvider,
              defaultFollowerReadConsistency,
              defaultLeaderReadConsistency);
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
      return rpcProxy.submitRequest(NULL_RPC_CONTROLLER, getOMRequest(payload));
    } catch (ServiceException e) {
      OMNotLeaderException notLeaderException =
          HadoopRpcOMFailoverProxyProvider.getNotLeaderException(e);
      if (notLeaderException == null) {
        throw ProtobufHelper.getRemoteException(e);
      }
      throw new IOException("Could not determine or connect to OM Leader.");
    }
  }

  private OMRequest getOMRequest(OMRequest basePayload) {
    if (followerReadEnabled) {
      // Follower read uses FollowerReadInvocationHandler to set the invocation handler
      // Return the request payload as is
      return basePayload;
    }
    if (basePayload.hasReadConsistencyHint()) {
      // If there is already user-defined read consistency hint, we should respect it
      return basePayload;
    }

    return basePayload.toBuilder()
        .setReadConsistencyHint(defaultLeaderReadConsistencyHint)
        .build();
  }

  @Override
  public Text getDelegationTokenService() {
    return null;
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
