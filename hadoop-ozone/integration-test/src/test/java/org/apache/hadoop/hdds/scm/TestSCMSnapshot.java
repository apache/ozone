/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.ha.SCMHAConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;

/**
 * Tests snapshots in SCM HA.
 */
public class TestSCMSnapshot {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    SCMHAConfiguration scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    scmhaConfiguration.setRatisSnapshotThreshold(1L);
    conf.setFromObject(scmhaConfiguration);
    cluster = MiniOzoneCluster
        .newBuilder(conf)
        .setNumDatanodes(3)
        .setScmId(UUID.randomUUID().toString())
        .build();
    cluster.waitForClusterToBeReady();
  }

  @Test
  public void testSnapshot() throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    long snapshotInfo1 = scm.getScmHAManager().asSCMHADBTransactionBuffer()
        .getLatestTrxInfo().getTransactionIndex();
    ContainerManagerV2 containerManager = scm.getContainerManager();
    PipelineManager pipelineManager = scm.getPipelineManager();
    Pipeline ratisPipeline1 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            new RatisReplicationConfig(THREE), "Owner1").getPipelineID());
    pipelineManager.openPipeline(ratisPipeline1.getId());
    Pipeline ratisPipeline2 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            new RatisReplicationConfig(ONE), "Owner2").getPipelineID());
    pipelineManager.openPipeline(ratisPipeline2.getId());
    long snapshotInfo2 = scm.getScmHAManager().asSCMHADBTransactionBuffer()
        .getLatestTrxInfo().getTransactionIndex();

    Assert.assertTrue(
        String.format("Snapshot index 2 {} should greater than Snapshot " +
            "index 1 {}", snapshotInfo2, snapshotInfo1),
        snapshotInfo2 > snapshotInfo1);

    cluster.restartStorageContainerManager(false);
    TransactionInfo trxInfoAfterRestart =
        scm.getScmHAManager().asSCMHADBTransactionBuffer().getLatestTrxInfo();
    Assert.assertTrue(
        trxInfoAfterRestart.getTransactionIndex() >= snapshotInfo2);
    try {
      pipelineManager.getPipeline(ratisPipeline1.getId());
      pipelineManager.getPipeline(ratisPipeline2.getId());
    } catch (PipelineNotFoundException e) {
      Assert.fail("Should not see a PipelineNotFoundException");
    }
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
