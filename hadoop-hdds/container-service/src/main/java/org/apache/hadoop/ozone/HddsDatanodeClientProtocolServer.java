/*
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

package org.apache.hadoop.ozone;

import com.google.common.collect.Lists;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolPB;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.utils.VersionInfo;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.HddsUtils.preserveThreadName;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.CLIENT_RPC;

/**
 * The RPC server that listens to requests from clients.
 */
public class HddsDatanodeClientProtocolServer extends ServiceRuntimeInfoImpl
    implements ReconfigureProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(HddsDatanodeClientProtocolServer.class);
  private final RPC.Server rpcServer;
  private InetSocketAddress clientRpcAddress;
  private final DatanodeDetails datanodeDetails;
  private final HddsDatanodeService service;
  private final OzoneConfiguration conf;

  protected HddsDatanodeClientProtocolServer(HddsDatanodeService service,
      DatanodeDetails datanodeDetails, OzoneConfiguration conf,
      VersionInfo versionInfo) throws IOException {
    super(versionInfo);
    this.datanodeDetails = datanodeDetails;
    this.service = service;
    this.conf = conf;

    rpcServer = getRpcServer(conf);
    clientRpcAddress = ServerUtils.updateRPCListenAddress(this.conf,
        HDDS_DATANODE_CLIENT_ADDRESS_KEY,
        HddsUtils.getDatanodeRpcAddress(conf), rpcServer);
    this.datanodeDetails.setPort(CLIENT_RPC, clientRpcAddress.getPort());
  }

  public void start() {
    LOG.info("RPC server for Client " + getClientRpcAddress());
    rpcServer.start();
  }

  public void stop() {
    try {
      LOG.info("Stopping the RPC server for Client Protocol");
      getClientRpcServer().stop();
    } catch (Exception ex) {
      LOG.error("Client Protocol RPC stop failed.", ex);
    }
  }

  public void join() throws InterruptedException {
    LOG.trace("Join RPC server for Client Protocol");
    getClientRpcServer().join();
  }

  @Override
  public String getServerName() {
    return "Datanode";
  }

  @Override
  public void startReconfigure() throws IOException {
    service.checkAdminUserPrivilege(getRemoteUser());
    startReconfigurationTask();
  }

  @Override
  public ReconfigurationTaskStatus getReconfigureStatus() throws IOException {
    service.checkAdminUserPrivilege(getRemoteUser());
    return getReconfigurationTaskStatus();
  }

  @Override
  public List<String> listReconfigureProperties() throws IOException {
    service.checkAdminUserPrivilege(getRemoteUser());
    return Lists.newArrayList(service.getReconfigurableProperties());
  }

  // optimize ugi lookup for RPC operations to avoid a trip through
  // UGI.getCurrentUser which is synch'ed
  private static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Collection<String> getReconfigurableProperties() {
    return service.getReconfigurableProperties();
  }

  @Override
  public String reconfigurePropertyImpl(String property, String newVal)
      throws ReconfigurationException {
    return service.reconfigurePropertyImpl(property, newVal);
  }

  /**
   * Creates a new instance of rpc server. If an earlier instance is already
   * running then returns the same.
   */
  private RPC.Server getRpcServer(OzoneConfiguration configuration)
      throws IOException {
    InetSocketAddress rpcAddress = HddsUtils.getDatanodeRpcAddress(conf);
    // Add reconfigureProtocolService.
    RPC.setProtocolEngine(
        configuration, ReconfigureProtocolPB.class, ProtobufRpcEngine.class);

    final int handlerCount = conf.getInt(HDDS_DATANODE_HANDLER_COUNT_KEY,
        HDDS_DATANODE_HANDLER_COUNT_DEFAULT);
    ReconfigureProtocolServerSideTranslatorPB reconfigureServerProtocol
        = new ReconfigureProtocolServerSideTranslatorPB(this);
    BlockingService reconfigureService = ReconfigureProtocolProtos
        .ReconfigureProtocolService.newReflectiveBlockingService(
            reconfigureServerProtocol);

    return preserveThreadName(() -> startRpcServer(configuration, rpcAddress,
        ReconfigureProtocolPB.class, reconfigureService, handlerCount));
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param configuration configuration
   * @param addr          configured address of RPC server
   * @param protocol      RPC protocol provided by RPC server
   * @param instance      RPC protocol implementation instance
   * @param handlerCount  RPC server handler count
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private RPC.Server startRpcServer(
      Configuration configuration, InetSocketAddress addr,
      Class<?> protocol, BlockingService instance,
      int handlerCount)
      throws IOException {
    return new RPC.Builder(configuration)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(null)
        .build();
  }

  private RPC.Server getClientRpcServer() {
    return rpcServer;
  }

  public InetSocketAddress getClientRpcAddress() {
    return clientRpcAddress;
  }

  @Override
  public void close() throws IOException {
    stop();
  }
}
