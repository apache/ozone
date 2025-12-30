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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocol.OMInterServiceProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.BootstrapOMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.BootstrapOMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.ErrorCode;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Protocol implementation for Inter OM communication.
 */
public class OMInterServiceProtocolClientSideImpl implements
    OMInterServiceProtocol {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final HadoopRpcOMFailoverProxyProvider omFailoverProxyProvider;

  private final OMInterServiceProtocolPB rpcProxy;

  public OMInterServiceProtocolClientSideImpl(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OMInterServiceProtocolPB.class, ProtobufRpcEngine.class);

    this.omFailoverProxyProvider = new HadoopRpcOMFailoverProxyProvider(
            conf, ugi, omServiceId, OMInterServiceProtocolPB.class);

    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    this.rpcProxy = (OMInterServiceProtocolPB) RetryProxy.create(
        OMInterServiceProtocolPB.class, omFailoverProxyProvider,
        omFailoverProxyProvider.getRetryPolicy(maxFailovers));
  }

  @Override
  public void bootstrap(OMNodeDetails newOMNode) throws IOException {
    BootstrapOMRequest bootstrapOMRequest = BootstrapOMRequest.newBuilder()
        .setNodeId(newOMNode.getNodeId())
        .setHostAddress(newOMNode.getHostAddress())
        .setRatisPort(newOMNode.getRatisPort())
        .setIsListener(newOMNode.isRatisListener())
        .build();

    BootstrapOMResponse response;
    try {
      response = rpcProxy.bootstrap(NULL_RPC_CONTROLLER, bootstrapOMRequest);
    } catch (ServiceException e) {
      OMNotLeaderException notLeaderException =
          HadoopRpcOMFailoverProxyProvider.getNotLeaderException(e);
      if (notLeaderException != null) {
        throwException(ErrorCode.LEADER_UNDETERMINED,
            notLeaderException.getMessage());
      }

      OMLeaderNotReadyException leaderNotReadyException =
          HadoopRpcOMFailoverProxyProvider.getLeaderNotReadyException(e);
      if (leaderNotReadyException != null) {
        throwException(ErrorCode.LEADER_NOT_READY,
            leaderNotReadyException.getMessage());
      }
      throw ProtobufHelper.getRemoteException(e);
    }

    if (!response.getSuccess()) {
      throwException(response.getErrorCode(), response.getErrorMsg());
    }
  }

  private void throwException(ErrorCode errorCode, String errorMsg)
      throws IOException {
    throw new IOException("Failed to Bootstrap OM. Error Code: " + errorCode +
        ", Error Message: " + errorMsg);
  }

  @Override
  public void close() throws IOException {
    omFailoverProxyProvider.close();
  }
}
