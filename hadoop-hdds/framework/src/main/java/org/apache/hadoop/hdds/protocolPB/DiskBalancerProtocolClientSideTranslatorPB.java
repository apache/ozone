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

package org.apache.hadoop.hdds.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.GetDiskBalancerInfoRequestProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.GetDiskBalancerInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.StartDiskBalancerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.StopDiskBalancerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.UpdateDiskBalancerConfigurationRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerConfigurationProto;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.ProtocolMetaInterface;
import org.apache.hadoop.ipc_.ProtocolTranslator;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RpcClientUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Client-side translator for {@link DiskBalancerProtocol}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DiskBalancerProtocolClientSideTranslatorPB
    implements DiskBalancerProtocol, ProtocolMetaInterface, ProtocolTranslator {

  private static final RpcController NULL_CONTROLLER = null;

  private final DiskBalancerProtocolPB rpcProxy;

  /**
   * Creates a new client-side translator.
   * 
   * @param addr the datanode address to connect to
   * @param ugi the user group information for authentication
   * @param conf the Ozone configuration
   * @throws IOException if the RPC proxy cannot be created
   */
  public DiskBalancerProtocolClientSideTranslatorPB(InetSocketAddress addr,
      UserGroupInformation ugi, OzoneConfiguration conf) throws IOException {
    this.rpcProxy = createDiskBalancerProtocolProxy(addr, ugi, conf);
  }

  /**
   * Creates the underlying RPC proxy for communication with the datanode.
   * 
   * @param addr the datanode address
   * @param ugi the user group information
   * @param conf the configuration
   * @return the RPC proxy
   * @throws IOException if proxy creation fails
   */
  static DiskBalancerProtocolPB createDiskBalancerProtocolProxy(InetSocketAddress addr,
      UserGroupInformation ugi, OzoneConfiguration conf) throws IOException {
    Configuration hadoopConf = LegacyHadoopConfigurationSource
        .asHadoopConfiguration(conf);
    RPC.setProtocolEngine(conf, DiskBalancerProtocolPB.class,
        ProtobufRpcEngine.class);
    return RPC.getProtocolProxy(DiskBalancerProtocolPB.class,
        RPC.getProtocolVersion(DiskBalancerProtocolPB.class), addr, ugi,
        hadoopConf, NetUtils.getDefaultSocketFactory(hadoopConf)).getProxy();
  }

  @Override
  public DatanodeDiskBalancerInfoProto getDiskBalancerInfo(GetDiskBalancerInfoRequestProto request) throws IOException {
    try {
      GetDiskBalancerInfoResponseProto response =
          rpcProxy.getDiskBalancerInfo(NULL_CONTROLLER, request);
      return response.getInfo();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Starts the DiskBalancer service on the datanode.
   * 
   * @param config optional configuration (threshold, bandwidth, threads, etc.)
   * @throws IOException if the RPC call fails, access is denied, or service is disabled
   */
  @Override
  public void startDiskBalancer(@Nullable DiskBalancerConfigurationProto config)
      throws IOException {
    StartDiskBalancerRequestProto.Builder builder =
        StartDiskBalancerRequestProto.newBuilder();
    if (config != null) {
      builder.setConfig(config);
    }
    try {
      rpcProxy.startDiskBalancer(NULL_CONTROLLER, builder.build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Stops the DiskBalancer service on the datanode.
   * 
   * @throws IOException if the RPC call fails, access is denied, or service is disabled
   */
  @Override
  public void stopDiskBalancer() throws IOException {
    StopDiskBalancerRequestProto request = StopDiskBalancerRequestProto
        .newBuilder().build();
    try {
      rpcProxy.stopDiskBalancer(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Updates the DiskBalancer configuration on the datanode.
   * 
   * @param config the new configuration (must not be null)
   * @throws IOException if the RPC call fails, access is denied, or service is disabled
   * @throws NullPointerException if config is null
   */
  @Override
  public void updateDiskBalancerConfiguration(DiskBalancerConfigurationProto config)
      throws IOException {
    Objects.requireNonNull(config, "DiskBalancer configuration is required");
    UpdateDiskBalancerConfigurationRequestProto request =
        UpdateDiskBalancerConfigurationRequestProto.newBuilder()
            .setConfig(config)
            .build();
    try {
      rpcProxy.updateDiskBalancerConfiguration(NULL_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Returns the underlying RPC proxy object. Provides access to
   * the raw Protobuf proxy for advanced use cases or debugging.
   */
  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy, DiskBalancerProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(DiskBalancerProtocolPB.class), methodName);
  }
}


