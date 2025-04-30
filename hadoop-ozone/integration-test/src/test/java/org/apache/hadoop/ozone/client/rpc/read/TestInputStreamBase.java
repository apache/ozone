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

package org.apache.hadoop.ozone.client.rpc.read;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class TestInputStreamBase {

  static final int CHUNK_SIZE = 1024 * 1024;          // 1MB
  static final int FLUSH_SIZE = 2 * CHUNK_SIZE;       // 2MB
  static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;   // 4MB
  static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;   // 8MB
  static final int BYTES_PER_CHECKSUM = 256 * 1024;   // 256KB

  private MiniOzoneCluster cluster;

  protected static MiniOzoneCluster newCluster() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    OzoneClientConfig config = conf.getObject(OzoneClientConfig.class);
    config.setBytesPerChecksum(BYTES_PER_CHECKSUM);
    conf.setFromObject(config);

    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 5);
    conf.setQuietMode(false);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 64,
        StorageUnit.MB);

    ReplicationManagerConfiguration repConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .applyTo(conf);

    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
  }

  static String getNewKeyName() {
    return UUID.randomUUID().toString();
  }

  protected void updateConfig(ContainerLayoutVersion layout) {
    cluster.getHddsDatanodes().forEach(dn -> dn.getConf().setEnum(OZONE_SCM_CONTAINER_LAYOUT_KEY, layout));
    closeContainers();
  }

  protected MiniOzoneCluster getCluster() {
    return cluster;
  }

  @BeforeAll
  void setup() throws Exception {
    cluster = newCluster();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  private void closeContainers() {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    scm.getContainerManager().getContainers().forEach(container -> {
      if (container.isOpen()) {
        try {
          TestHelper.waitForContainerClose(getCluster(), container.getContainerID());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
