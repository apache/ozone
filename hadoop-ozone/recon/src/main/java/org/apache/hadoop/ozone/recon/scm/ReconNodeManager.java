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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_SCM_NODE_DB;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.MetadataStore;
import org.apache.hadoop.hdds.utils.MetadataStoreBuilder;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon SCM's Node manager that includes persistence.
 */
public class ReconNodeManager extends SCMNodeManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(ReconNodeManager.class);

  private final MetadataStore nodeStore;

  public ReconNodeManager(OzoneConfiguration conf,
                          SCMStorageConfig scmStorageConfig,
                          EventPublisher eventPublisher,
                          NetworkTopology networkTopology) throws IOException {
    super(conf, scmStorageConfig, eventPublisher, networkTopology);
    final File nodeDBPath = getNodeDBPath(conf);
    final int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
    this.nodeStore = MetadataStoreBuilder.newBuilder()
        .setConf(conf)
        .setDbFile(nodeDBPath)
        .setCacheSize(cacheSize * OzoneConsts.MB)
        .build();
    loadExistingNodes();
  }

  private void loadExistingNodes() {
    try {
      List<Map.Entry<byte[], byte[]>> range = nodeStore
          .getSequentialRangeKVs(null, Integer.MAX_VALUE, null);
      int nodeCount = 0;
      for (Map.Entry<byte[], byte[]> entry : range) {
        DatanodeDetails datanodeDetails = DatanodeDetails.getFromProtoBuf(
            HddsProtos.DatanodeDetailsProto.PARSER.parseFrom(entry.getValue()));
        register(datanodeDetails, null, null);
        nodeCount++;
      }
      LOG.info("Loaded {} nodes from node DB.", nodeCount);
    } catch (IOException ioEx) {
      LOG.error("Exception while loading existing nodes.", ioEx);
    }
  }

  /**
   * Add a new new node to the NodeDB. Must be called after register.
   * @param datanodeDetails Datanode details.
   */
  public void addNodeToDB(DatanodeDetails datanodeDetails) throws IOException {
    byte[] nodeIdBytes =
        StringUtils.string2Bytes(datanodeDetails.getUuidString());
    byte[] nodeDetailsBytes =
        datanodeDetails.getProtoBufMessage().toByteArray();
    nodeStore.put(nodeIdBytes, nodeDetailsBytes);
    LOG.info("Adding new node {} to Node DB.", datanodeDetails.getUuid());
  }

  protected File getNodeDBPath(Configuration conf) {
    File metaDir = ServerUtils.getScmDbDir(conf);
    return new File(metaDir, RECON_SCM_NODE_DB);
  }

  @Override
  public void close() throws IOException {
    if (nodeStore != null) {
      nodeStore.close();
    }
    super.close();
  }
}
