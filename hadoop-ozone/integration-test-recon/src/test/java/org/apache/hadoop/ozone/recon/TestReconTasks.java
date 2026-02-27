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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer.runTestOzoneContainerViaDataNode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2.UnhealthyContainerRecordV2;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Integration Tests for Recon SCM tasks using ContainerHealthTaskV2.
 */
public class TestReconTasks {
  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private ReconService recon;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "5s");
    conf.set(HDDS_PIPELINE_REPORT_INTERVAL, "5s");

    ReconTaskConfig taskConfig = conf.getObject(ReconTaskConfig.class);
    taskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(10));
    conf.setFromObject(taskConfig);

    conf.set("ozone.scm.stale.node.interval", "6s");
    conf.set("ozone.scm.dead.node.interval", "8s");
    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ONE, 30000);
    GenericTestUtils.setLogLevel(SCMDatanodeHeartbeatDispatcher.class,
        Level.DEBUG);
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSyncSCMContainerInfo() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManager scmContainerManager = scm.getContainerManager();
    ContainerManager reconContainerManager = reconScm.getContainerManager();
    final ContainerInfo container1 = scmContainerManager.allocateContainer(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE), "admin");
    final ContainerInfo container2 = scmContainerManager.allocateContainer(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE), "admin");
    scmContainerManager.updateContainerState(container1.containerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    scmContainerManager.updateContainerState(container2.containerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    scmContainerManager.updateContainerState(container1.containerID(),
        HddsProtos.LifeCycleEvent.CLOSE);
    scmContainerManager.updateContainerState(container2.containerID(),
        HddsProtos.LifeCycleEvent.CLOSE);
    int scmContainersCount = scmContainerManager.getContainers().size();
    int reconContainersCount = reconContainerManager.getContainers().size();
    assertNotEquals(scmContainersCount, reconContainersCount);
    reconScm.syncWithSCMContainerInfo();
    reconContainersCount = reconContainerManager.getContainers().size();
    assertEquals(scmContainersCount, reconContainersCount);
  }

  @Test
  public void testContainerHealthTaskV2WithSCMSync() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();

    LambdaTestUtils.await(60000, 5000,
        () -> (!reconPipelineManager.getPipelines().isEmpty()));

    ContainerManager scmContainerManager = scm.getContainerManager();
    ReconContainerManager reconContainerManager =
        (ReconContainerManager) reconScm.getContainerManager();

    ContainerInfo containerInfo = scmContainerManager.allocateContainer(
        RatisReplicationConfig.getInstance(ONE), "test");
    long containerID = containerInfo.getContainerID();

    Pipeline pipeline = scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);

    assertEquals(scmContainerManager.getContainers(),
        reconContainerManager.getContainers());

    cluster.shutdownHddsDatanode(pipeline.getFirstNode());

    LambdaTestUtils.await(120000, 6000, () -> {
      List<UnhealthyContainerRecordV2> allMissingContainers =
          reconContainerManager.getContainerSchemaManagerV2()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
                  0L, 0L, 1000);
      return allMissingContainers.size() == 1;
    });

    cluster.restartHddsDatanode(pipeline.getFirstNode(), true);

    LambdaTestUtils.await(120000, 10000, () -> {
      List<UnhealthyContainerRecordV2> allMissingContainers =
          reconContainerManager.getContainerSchemaManagerV2()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
                  0L, 0L, 1000);
      return allMissingContainers.isEmpty();
    });
    IOUtils.closeQuietly(client);
  }

  @Test
  public void testContainerHealthTaskV2EmptyMissingContainerDownNode()
      throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    ReconContainerMetadataManager reconContainerMetadataManager =
        recon.getReconServer().getReconContainerMetadataManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    ReconContainerManager reconContainerManager =
        (ReconContainerManager) reconScm.getContainerManager();

    ContainerInfo containerInfo = scm.getContainerManager()
        .allocateContainer(RatisReplicationConfig.getInstance(ONE), "test");
    long containerID = containerInfo.getContainerID();

    // Explicitly set key count to 0 so missing classification becomes EMPTY_MISSING.
    try (RDBBatchOperation batch = RDBBatchOperation.newAtomicOperation()) {
      reconContainerMetadataManager.batchStoreContainerKeyCounts(batch, containerID, 0L);
      reconContainerMetadataManager.commitBatchOperation(batch);
    }

    Pipeline pipeline = scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);
    cluster.shutdownHddsDatanode(pipeline.getFirstNode());

    LambdaTestUtils.await(120000, 6000, () -> {
      List<UnhealthyContainerRecordV2> emptyMissing =
          reconContainerManager.getContainerSchemaManagerV2()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.EMPTY_MISSING,
                  0L, 0L, 1000);
      return emptyMissing.size() == 1;
    });

    cluster.restartHddsDatanode(pipeline.getFirstNode(), true);
    LambdaTestUtils.await(120000, 10000, () -> {
      List<UnhealthyContainerRecordV2> emptyMissing =
          reconContainerManager.getContainerSchemaManagerV2()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.EMPTY_MISSING,
                  0L, 0L, 1000);
      return emptyMissing.isEmpty();
    });
    IOUtils.closeQuietly(client);
  }
}
