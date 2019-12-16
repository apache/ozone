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

import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.buildRpcServerStartMessage;

import java.io.IOException;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's 'lite' version of SCM.
 */
public class ReconStorageContainerManager
    implements OzoneStorageContainerManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(ReconStorageContainerManager.class);

  private final OzoneConfiguration ozoneConfiguration;
  private final ReconDatanodeProtocolServer datanodeProtocolServer;
  private final EventQueue eventQueue;
  private final SCMStorageConfig scmStorageConfig;
  private static final String RECON_SCM_CONFIG_PREFIX = "recon.";

  @Inject
  public ReconStorageContainerManager(OzoneConfiguration conf)
      throws IOException {
    this.eventQueue = new EventQueue();
    this.ozoneConfiguration = getReconScmConfiguration(conf);
    this.scmStorageConfig = new SCMStorageConfig(conf);
    this.datanodeProtocolServer = new ReconDatanodeProtocolServer(
        conf, this, eventQueue);
  }

  /**
   *  For every config key which is prefixed by 'recon.scm', create a new
   *  config key without the prefix keeping the same value.
   *  For example, if recon.scm.a.b. = xyz, we add a new config like
   *  a.b.c = xyz. This is done to override Recon's passive SCM configs if
   *  needed.
   * @param configuration configuration object.
   * @return same configuration object with possible added elements.
   */
  private OzoneConfiguration getReconScmConfiguration(
      OzoneConfiguration configuration) {
    OzoneConfiguration reconScmConfiguration =
        new OzoneConfiguration(configuration);
    Map<String, String> reconScmConfigs =
        configuration.getPropsWithPrefix(RECON_SCM_CONFIG_PREFIX);
    for (Map.Entry<String, String> entry : reconScmConfigs.entrySet()) {
      reconScmConfiguration.set(entry.getKey(), entry.getValue());
    }
    return reconScmConfiguration;
  }

  /**
   * Start the Recon SCM subsystems.
   */
  public void start() {
    if (LOG.isInfoEnabled()) {
      LOG.info(buildRpcServerStartMessage(
          "Recon ScmDatanodeProtocol RPC server",
          getDatanodeProtocolServer().getDatanodeRpcAddress()));
    }
    getDatanodeProtocolServer().start();
  }

  /**
   * Stop the Recon SCM subsystems.
   */
  public void stop() {
    getDatanodeProtocolServer().stop();
  }

  public ReconDatanodeProtocolServer getDatanodeProtocolServer() {
    return datanodeProtocolServer;
  }

  @Override
  public NodeManager getScmNodeManager() {
    return new ReconNodeManager(ozoneConfiguration, scmStorageConfig,
        eventQueue, null);
  }

  @Override
  public BlockManager getScmBlockManager() {
    return null;
  }

  @Override
  public PipelineManager getPipelineManager() {
    return null;
  }

  @Override
  public ContainerManager getContainerManager() {
    return null;
  }

  @Override
  public ReplicationManager getReplicationManager() {
    return null;
  }
}
