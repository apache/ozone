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

import com.google.common.collect.Maps;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetConfigurationChangeProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetReconfigureStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetReconfigureStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetServerNameRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.GetServerNameResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.ListReconfigurePropertiesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.ListReconfigurePropertiesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.StartReconfigureRequestProto;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.ProtocolMetaInterface;
import org.apache.hadoop.ipc_.ProtocolTranslator;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RpcClientUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the client side translator to translate the requests made on
 * {@link ReconfigureProtocol} interfaces to the RPC server implementing
 * {@link ReconfigureProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ReconfigureProtocolClientSideTranslatorPB implements
    ProtocolMetaInterface, ReconfigureProtocol, ProtocolTranslator {
  private static final Logger LOG = LoggerFactory
      .getLogger(ReconfigureProtocolClientSideTranslatorPB.class);

  private static final RpcController NULL_CONTROLLER = null;

  private static final GetServerNameRequestProto VOID_GET_SERVER_NAME =
      GetServerNameRequestProto.newBuilder().build();

  private static final StartReconfigureRequestProto VOID_START_RECONFIG =
      StartReconfigureRequestProto.newBuilder().build();

  private static final ListReconfigurePropertiesRequestProto
      VOID_LIST_RECONFIGURABLE_PROPERTIES =
      ListReconfigurePropertiesRequestProto.newBuilder().build();

  private static final GetReconfigureStatusRequestProto
      VOID_GET_RECONFIG_STATUS =
      GetReconfigureStatusRequestProto.newBuilder().build();

  private final ReconfigureProtocolPB rpcProxy;

  public ReconfigureProtocolClientSideTranslatorPB(HddsProtos.NodeType nodeType, InetSocketAddress addr,
      UserGroupInformation ugi, OzoneConfiguration conf)
      throws IOException {
    rpcProxy = createReconfigureProtocolProxy(nodeType, addr, ugi, conf);
  }

  static ReconfigureProtocolPB createReconfigureProtocolProxy(HddsProtos.NodeType nodeType,
      InetSocketAddress addr, UserGroupInformation ugi,
      OzoneConfiguration conf) throws IOException {
    Configuration hadoopConf = LegacyHadoopConfigurationSource
        .asHadoopConfiguration(conf);
    if (nodeType == HddsProtos.NodeType.OM) {
      RPC.setProtocolEngine(OzoneConfiguration.of(conf),
          ReconfigureProtocolOmPB.class, ProtobufRpcEngine.class);
      return RPC.getProtocolProxy(
              ReconfigureProtocolOmPB.class,
              RPC.getProtocolVersion(ReconfigureProtocolOmPB.class),
              addr, ugi, hadoopConf,
              NetUtils.getDefaultSocketFactory(hadoopConf))
          .getProxy();
    } else if (nodeType == HddsProtos.NodeType.DATANODE) {
      RPC.setProtocolEngine(OzoneConfiguration.of(conf),
          ReconfigureProtocolDatanodePB.class, ProtobufRpcEngine.class);
      return RPC.getProtocolProxy(
              ReconfigureProtocolDatanodePB.class,
              RPC.getProtocolVersion(ReconfigureProtocolDatanodePB.class),
              addr, ugi, hadoopConf,
              NetUtils.getDefaultSocketFactory(hadoopConf))
          .getProxy();
    } else {
      RPC.setProtocolEngine(OzoneConfiguration.of(conf),
          ReconfigureProtocolPB.class, ProtobufRpcEngine.class);
      return RPC.getProtocolProxy(
              ReconfigureProtocolPB.class,
              RPC.getProtocolVersion(ReconfigureProtocolPB.class),
              addr, ugi, hadoopConf,
              NetUtils.getDefaultSocketFactory(hadoopConf))
          .getProxy();
    }
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public String getServerName() throws IOException {
    try {
      GetServerNameResponseProto response =
          rpcProxy.getServerName(NULL_CONTROLLER, VOID_GET_SERVER_NAME);
      return response.getName();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void startReconfigure() throws IOException {
    try {
      rpcProxy.startReconfigure(NULL_CONTROLLER, VOID_START_RECONFIG);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public ReconfigurationTaskStatus getReconfigureStatus()
      throws IOException {
    try {
      GetReconfigureStatusResponseProto response = rpcProxy
          .getReconfigureStatus(NULL_CONTROLLER, VOID_GET_RECONFIG_STATUS);
      return getReconfigureStatus(response);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private ReconfigurationTaskStatus getReconfigureStatus(
      GetReconfigureStatusResponseProto response) {
    Map<PropertyChange, Optional<String>> statusMap = null;
    long startTime;
    long endTime = 0;

    startTime = response.getStartTime();
    if (response.hasEndTime()) {
      endTime = response.getEndTime();
    }
    if (response.getChangesCount() > 0) {
      statusMap = Maps.newHashMap();
      for (GetConfigurationChangeProto change : response.getChangesList()) {
        PropertyChange pc = new PropertyChange(change.getName(),
            change.getNewValue(), change.getOldValue());
        String errorMessage = null;
        if (change.hasErrorMessage()) {
          errorMessage = change.getErrorMessage();
        }
        statusMap.put(pc, Optional.ofNullable(errorMessage));
      }
    }
    return new ReconfigurationTaskStatus(startTime, endTime, statusMap);
  }
  
  @Override
  public List<String> listReconfigureProperties() throws IOException {
    ListReconfigurePropertiesResponseProto response;
    try {
      response = rpcProxy.listReconfigureProperties(NULL_CONTROLLER,
          VOID_LIST_RECONFIGURABLE_PROPERTIES);
      return response.getNameList();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        ReconfigureProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(ReconfigureProtocolPB.class),
        methodName);
  }
}
