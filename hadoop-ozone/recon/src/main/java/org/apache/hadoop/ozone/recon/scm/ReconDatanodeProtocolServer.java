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
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;

/**
 * Recon's Datanode protocol server extended from SCM.
 */
public class ReconDatanodeProtocolServer extends SCMDatanodeProtocolServer {

  public ReconDatanodeProtocolServer(OzoneConfiguration conf,
                                     OzoneStorageContainerManager scm,
                                     EventPublisher eventPublisher)
      throws IOException {
    super(conf, scm, eventPublisher);
  }

  @Override
  protected String getScmDatanodeAddressKey() {
    return OZONE_RECON_DATANODE_ADDRESS_KEY;
  }

  @Override
  public InetSocketAddress getDataNodeBindAddress(OzoneConfiguration conf) {
    return HddsServerUtil.getReconDataNodeBindAddress(conf);
  }
}
