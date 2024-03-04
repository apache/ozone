/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.recon;

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
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.event.Level;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer.runTestOzoneContainerViaDataNode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Integration Tests for Recon's tasks.
 */
@Timeout(300)
public class TestReconTasks {
  private MiniOzoneCluster cluster = null;
  private OzoneConfiguration conf;

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
    cluster =  MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1)
        .includeRecon(true).build();
    cluster.waitForClusterToBeReady();
    GenericTestUtils.setLogLevel(SCMDatanodeHeartbeatDispatcher.LOG,
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
            cluster.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManager scmContainerManager = scm.getContainerManager();
    ContainerManager reconContainerManager = reconScm.getContainerManager();
    final ContainerInfo container1 = scmContainerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), "admin");
    final ContainerInfo container2 = scmContainerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), "admin");
    scmContainerManager.updateContainerState(container1.containerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    scmContainerManager.updateContainerState(container2.containerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    scmContainerManager.updateContainerState(container1.containerID(),
        HddsProtos.LifeCycleEvent.CLOSE);
    scmContainerManager.updateContainerState(container2.containerID(),
        HddsProtos.LifeCycleEvent.CLOSE);
    int scmContainersCount = scmContainerManager.getContainers().size();
    int reconContainersCount = reconContainerManager
        .getContainers().size();
    assertNotEquals(scmContainersCount, reconContainersCount);
    reconScm.syncWithSCMContainerInfo();
    reconContainersCount = reconContainerManager
        .getContainers().size();
    assertEquals(scmContainersCount, reconContainersCount);
  }

  @Test
  public void testMissingContainerDownNode() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            cluster.getReconServer().getReconStorageContainerManager();
    ReconContainerMetadataManager reconContainerMetadataManager =
        cluster.getReconServer().getReconContainerMetadataManager();

    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();

    // Make sure Recon's pipeline state is initialized.
    LambdaTestUtils.await(60000, 5000,
        () -> (reconPipelineManager.getPipelines().size() >= 1));

    ContainerManager scmContainerManager = scm.getContainerManager();
    ReconContainerManager reconContainerManager =
        (ReconContainerManager) reconScm.getContainerManager();
    ContainerInfo containerInfo =
        scmContainerManager
            .allocateContainer(RatisReplicationConfig.getInstance(ONE), "test");
    long containerID = containerInfo.getContainerID();

    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      reconContainerMetadataManager
          .batchStoreContainerKeyCounts(rdbBatchOperation, containerID, 2L);
      reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
    }

    Pipeline pipeline =
        scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);

    // Make sure Recon got the container report with new container.
    assertEquals(scmContainerManager.getContainers(),
        reconContainerManager.getContainers());

    // Bring down the Datanode that had the container replica.
    cluster.shutdownHddsDatanode(pipeline.getFirstNode());

    LambdaTestUtils.await(120000, 6000, () -> {
      List<UnhealthyContainers> allMissingContainers =
          reconContainerManager.getContainerSchemaManager()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
                  0, 1000);
      return (allMissingContainers.size() == 1);
    });

    // Restart the Datanode to make sure we remove the missing container.
    cluster.restartHddsDatanode(pipeline.getFirstNode(), true);
    LambdaTestUtils.await(120000, 10000, () -> {
      List<UnhealthyContainers> allMissingContainers =
          reconContainerManager.getContainerSchemaManager()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
                  0, 1000);
      return (allMissingContainers.isEmpty());
    });
    IOUtils.closeQuietly(client);
  }

  /**
   * This test verifies the count of MISSING and EMPTY_MISSING containers.
   * Following steps being followed in a single DN cluster.
   *    --- Allocate a container in SCM.
   *    --- Client writes the chunk and put block to only DN successfully.
   *    --- Shuts down the only DN.
   *    --- Since container to key mapping doesn't have any key mapped to
   *        container, missing container will be marked EMPTY_MISSING.
   *    --- Add a key mapping entry to key container mapping table for the
   *        container added.
   *    --- Now container will no longer be marked as EMPTY_MISSING and just
   *        as MISSING.
   *    --- Restart the only DN in cluster.
   *    --- Now container no longer will be marked as MISSING.
   *
   * @throws Exception
   */
  @Test
  public void testEmptyMissingContainerDownNode() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            cluster.getReconServer().getReconStorageContainerManager();
    ReconContainerMetadataManager reconContainerMetadataManager =
        cluster.getReconServer().getReconContainerMetadataManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();

    // Make sure Recon's pipeline state is initialized.
    LambdaTestUtils.await(60000, 1000,
        () -> (reconPipelineManager.getPipelines().size() >= 1));

    ContainerManager scmContainerManager = scm.getContainerManager();
    ReconContainerManager reconContainerManager =
        (ReconContainerManager) reconScm.getContainerManager();
    ContainerInfo containerInfo =
        scmContainerManager
            .allocateContainer(RatisReplicationConfig.getInstance(ONE), "test");
    long containerID = containerInfo.getContainerID();

    Pipeline pipeline =
        scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);

    // Make sure Recon got the container report with new container.
    assertEquals(scmContainerManager.getContainers(),
        reconContainerManager.getContainers());

    // Bring down the Datanode that had the container replica.
    cluster.shutdownHddsDatanode(pipeline.getFirstNode());

    LambdaTestUtils.await(25000, 1000, () -> {
      List<UnhealthyContainers> allEmptyMissingContainers =
          reconContainerManager.getContainerSchemaManager()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.
                      EMPTY_MISSING,
                  0, 1000);
      return (allEmptyMissingContainers.size() == 1);
    });


    // Now add a container to key mapping count as 3. This data is used to
    // identify if container is empty in terms of keys mapped to container.
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      reconContainerMetadataManager
          .batchStoreContainerKeyCounts(rdbBatchOperation, containerID, 3L);
      reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
    }

    // Verify again and now container is not empty missing but just missing.
    LambdaTestUtils.await(25000, 1000, () -> {
      List<UnhealthyContainers> allMissingContainers =
          reconContainerManager.getContainerSchemaManager()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
                  0, 1000);
      return (allMissingContainers.size() == 1);
    });

    LambdaTestUtils.await(25000, 1000, () -> {
      List<UnhealthyContainers> allEmptyMissingContainers =
          reconContainerManager.getContainerSchemaManager()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.
                      EMPTY_MISSING,
                  0, 1000);
      return (allEmptyMissingContainers.isEmpty());
    });

    // Now remove keys from container. This data is used to
    // identify if container is empty in terms of keys mapped to container.
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      reconContainerMetadataManager
          .batchStoreContainerKeyCounts(rdbBatchOperation, containerID, 0L);
      reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
    }

    // Check existing container state in UNHEALTHY_CONTAINER table
    // will be updated as EMPTY_MISSING
    LambdaTestUtils.await(25000, 1000, () -> {
      List<UnhealthyContainers> allEmptyMissingContainers =
          reconContainerManager.getContainerSchemaManager()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.
                      EMPTY_MISSING,
                  0, 1000);
      return (allEmptyMissingContainers.size() == 1);
    });

    // Now restart the cluster and verify the container is no longer missing.
    cluster.restartHddsDatanode(pipeline.getFirstNode(), true);
    LambdaTestUtils.await(25000, 1000, () -> {
      List<UnhealthyContainers> allMissingContainers =
          reconContainerManager.getContainerSchemaManager()
              .getUnhealthyContainers(
                  ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
                  0, 1000);
      return (allMissingContainers.isEmpty());
    });

    IOUtils.closeQuietly(client);
  }
}
