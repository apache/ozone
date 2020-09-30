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
package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Hadoop RPC based transport (wihout HA support).
 */
public class Hadoop27RpcTransport implements OmTransport {

  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final OzoneManagerProtocolPB proxy;

  public Hadoop27RpcTransport(
      ConfigurationSource conf) throws IOException {
    InetSocketAddress socket = OmUtils.getOmAddressForClients(conf);
    long version = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    OzoneConfiguration ozoneConfiguration = OzoneConfiguration.of(conf);

    RPC.setProtocolEngine(ozoneConfiguration,
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    proxy = RPC.getProtocolProxy(OzoneManagerProtocolPB.class, version,
        socket, UserGroupInformation.getCurrentUser(),
        ozoneConfiguration,
        NetUtils.getDefaultSocketFactory(ozoneConfiguration),
        getRpcTimeout(ozoneConfiguration), null).getProxy();
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    try {
      return proxy.submitRequest(NULL_RPC_CONTROLLER, payload);
    } catch (ServiceException e) {
      throw new IOException("Service exception during the OM call", e);
    }
  }

  @Override
  public Text getDelegationTokenService() {
    return null;
  }

  @Override
  public void close() throws IOException {
  }

  private int getRpcTimeout(OzoneConfiguration conf) {
    return conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);
  }
}
