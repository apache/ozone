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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests snapshots in SCM HA.
 */
public class TestSCMSnapshot {
  private static MiniOzoneCluster cluster;

  @BeforeAll
  public static void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD, 1L);
    cluster = MiniOzoneCluster
        .newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @Test
  public void testSnapshot() throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    long snapshotInfo1 = scm.getScmHAManager().asSCMHADBTransactionBuffer()
        .getLatestTrxInfo().getTransactionIndex();
    ContainerManager containerManager = scm.getContainerManager();
    PipelineManager pipelineManager = scm.getPipelineManager();
    Pipeline ratisPipeline1 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            RatisReplicationConfig.getInstance(THREE), "Owner1")
            .getPipelineID());
    pipelineManager.openPipeline(ratisPipeline1.getId());
    Pipeline ratisPipeline2 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            RatisReplicationConfig.getInstance(ONE), "Owner2").getPipelineID());
    pipelineManager.openPipeline(ratisPipeline2.getId());
    long snapshotInfo2 = scm.getScmHAManager().asSCMHADBTransactionBuffer()
        .getLatestTrxInfo().getTransactionIndex();

    assertThat(snapshotInfo2).isGreaterThan(snapshotInfo1);

    cluster.restartStorageContainerManager(false);
    TransactionInfo trxInfoAfterRestart =
        scm.getScmHAManager().asSCMHADBTransactionBuffer().getLatestTrxInfo();
    assertThat(trxInfoAfterRestart.getTransactionIndex()).isGreaterThanOrEqualTo(snapshotInfo2);
    assertDoesNotThrow(() ->
        pipelineManager.getPipeline(ratisPipeline1.getId()));
    assertDoesNotThrow(() ->
        pipelineManager.getPipeline(ratisPipeline2.getId()));
  }

  @AfterAll
  public static void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
