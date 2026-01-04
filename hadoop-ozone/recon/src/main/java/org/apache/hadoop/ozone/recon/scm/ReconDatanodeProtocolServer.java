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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.protocol.ReconDatanodeProtocol;
import org.apache.hadoop.ozone.protocolPB.ReconDatanodeProtocolPB;
import org.apache.hadoop.security.authorize.PolicyProvider;

/**
 * Recon's Datanode protocol server extended from SCM.
 */
public class ReconDatanodeProtocolServer extends SCMDatanodeProtocolServer
    implements ReconDatanodeProtocol {

  public ReconDatanodeProtocolServer(OzoneConfiguration conf,
                                     OzoneStorageContainerManager scm,
                                     EventPublisher eventPublisher)
      throws IOException {
    super(conf, scm, eventPublisher, null);
  }

  @Override
  public ProtocolMessageMetrics<StorageContainerDatanodeProtocolProtos.Type>
      getProtocolMessageMetrics() {
    return ProtocolMessageMetrics
        .create("ReconDatanodeProtocol", "Recon Datanode protocol",
            StorageContainerDatanodeProtocolProtos.Type.values());
  }

  @Override
  protected String getDatanodeAddressKey() {
    return OZONE_RECON_DATANODE_ADDRESS_KEY;
  }

  @Override
  public InetSocketAddress getDataNodeBindAddress(
      OzoneConfiguration conf, SCMNodeDetails scmNodeDetails) {
    return HddsServerUtil.getReconDataNodeBindAddress(conf);
  }

  @Override
  protected PolicyProvider getPolicyProvider() {
    return ReconPolicyProvider.getInstance();
  }

  @Override
  protected Class<ReconDatanodeProtocolPB> getProtocolClass() {
    return ReconDatanodeProtocolPB.class;
  }

}
