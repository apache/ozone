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

package org.apache.hadoop.ozone.recon.scm;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.protocol.ReconDatanodeProtocol;
import org.apache.hadoop.ozone.protocolPB.ReconDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.authorize.PolicyProvider;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;

import com.google.protobuf.BlockingService;

/**
 * Recon's Datanode protocol server extended from SCM.
 */
public class ReconDatanodeProtocolServer extends SCMDatanodeProtocolServer
    implements ReconDatanodeProtocol {

  public ReconDatanodeProtocolServer(OzoneConfiguration conf,
                                     OzoneStorageContainerManager scm,
                                     EventPublisher eventPublisher)
      throws IOException {
    super(conf, scm, eventPublisher);
  }

  @Override
  protected RPC.Server getRpcServer(OzoneConfiguration conf,
      InetSocketAddress datanodeRpcAddr,
      ProtocolMessageMetrics metrics) throws IOException {
    final int handlerCount = conf.getInt(OZONE_SCM_HANDLER_COUNT_KEY,
        OZONE_SCM_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(conf, ReconDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);

    BlockingService dnProtoPbService =
        StorageContainerDatanodeProtocolProtos
            .StorageContainerDatanodeProtocolService
            .newReflectiveBlockingService(
                new StorageContainerDatanodeProtocolServerSideTranslatorPB(
                    this, metrics));

    return startRpcServer(
            conf,
            datanodeRpcAddr,
            ReconDatanodeProtocolPB.class,
            dnProtoPbService,
            handlerCount);
  }

  @Override
  public ProtocolMessageMetrics getProtocolMessageMetrics() {
    return ProtocolMessageMetrics
        .create("ReconDatanodeProtocol", "Recon Datanode protocol",
            StorageContainerDatanodeProtocolProtos.Type.values());
  }

  @Override
  protected String getDatanodeAddressKey() {
    return OZONE_RECON_DATANODE_ADDRESS_KEY;
  }

  @Override
  public InetSocketAddress getDataNodeBindAddress(OzoneConfiguration conf) {
    return HddsServerUtil.getReconDataNodeBindAddress(conf);
  }

  @Override
  protected PolicyProvider getPolicyProvider() {
    return ReconPolicyProvider.getInstance();
  }
}
