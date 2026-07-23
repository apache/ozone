/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container;

import static java.util.Collections.emptyList;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests that a container is allocated on a pipeline whose
 * datanodes advertise the requested {@link StorageTier}, and that
 * {@link PipelineManager#getPipelines(ReplicationConfig, PipelineState,
 * java.util.Collection, java.util.Collection, StorageTier)} filters by tier.
 */
public class TestAllocateContainerWithStorageTier {

  @TempDir
  private File dir;
  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private StorageContainerManager scm;
  private ContainerManager containerManager;
  private final ReplicationConfig ratisThree =
      RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
  private final ReplicationConfig standaloneOne =
      StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE);

  private void createCluster(List<List<StorageType>> storageTypeList) throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 1);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(storageTypeList.size())
        .setNumDataVolumes(storageTypeList.get(0).size())
        .setDatanodeStorageType(storageTypeList)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();

    scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
  }

  private void cleanUp() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAllocateContainerWithStorageTierAllTiersAvailable() throws Exception {
    createCluster(Arrays.asList(
        Arrays.asList(StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE),
        Arrays.asList(StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE),
        Arrays.asList(StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE)));
    try {
      PipelineManager pipelineManager = scm.getPipelineManager();
      assertTrue(containerManager.getContainers().isEmpty());
      ContainerInfo container;
      List<Pipeline> pipelines;

      // All datanodes have DISK, SSD, ARCHIVE volumes: allocate on any tier.
      container = containerManager.allocateContainer(ratisThree, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager, ratisThree, StorageTier.DISK, 1);

      container = containerManager.allocateContainer(ratisThree, "admin", StorageTier.SSD);
      assertContainer(container.containerID(), containerManager, ratisThree, StorageTier.SSD, 2);

      container = containerManager.allocateContainer(ratisThree, "admin", StorageTier.ARCHIVE);
      assertContainer(container.containerID(), containerManager, ratisThree, StorageTier.ARCHIVE, 3);

      container = containerManager.allocateContainer(standaloneOne, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager, standaloneOne, StorageTier.DISK, 4);

      container = containerManager.allocateContainer(standaloneOne, "admin", StorageTier.SSD);
      assertContainer(container.containerID(), containerManager, standaloneOne, StorageTier.SSD, 5);

      container = containerManager.allocateContainer(standaloneOne, "admin", StorageTier.ARCHIVE);
      assertContainer(container.containerID(), containerManager, standaloneOne, StorageTier.ARCHIVE, 6);

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, emptyList(), emptyList(), StorageTier.DISK);
      assertPipeline(pipelines, 1, StorageTier.DISK);

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, emptyList(), emptyList(), StorageTier.SSD);
      assertPipeline(pipelines, 1, StorageTier.SSD);

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, emptyList(), emptyList(), StorageTier.ARCHIVE);
      assertPipeline(pipelines, 1, StorageTier.ARCHIVE);

    } finally {
      cleanUp();
    }
  }

  @Test
  public void testAllocateContainerWithStorageTierPartialTierAvailability() throws Exception {
    createCluster(Arrays.asList(
        Arrays.asList(StorageType.DISK, StorageType.DISK, StorageType.ARCHIVE),
        Arrays.asList(StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE),
        Arrays.asList(StorageType.DISK, StorageType.SSD, StorageType.SSD)));
    try {
      PipelineManager pipelineManager = scm.getPipelineManager();
      assertTrue(containerManager.getContainers().isEmpty());
      ContainerInfo container;
      List<Pipeline> pipelines;

      // Every datanode has DISK; SSD and ARCHIVE are not uniformly available
      // across all three, so RATIS_THREE only works on DISK.
      assertThrows(IOException.class, () -> containerManager.allocateContainer(
          ratisThree, "admin", StorageTier.SSD));

      container = containerManager.allocateContainer(ratisThree, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager, ratisThree, StorageTier.DISK, 1);

      assertThrows(IOException.class, () -> containerManager.allocateContainer(
          RatisReplicationConfig.getInstance(ReplicationFactor.THREE), "admin", StorageTier.ARCHIVE));

      container = containerManager.allocateContainer(standaloneOne, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager, standaloneOne, StorageTier.DISK, 2);

      container = containerManager.allocateContainer(standaloneOne, "admin", StorageTier.ARCHIVE);
      assertContainer(container.containerID(), containerManager, standaloneOne, StorageTier.ARCHIVE, 3);

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, emptyList(), emptyList(), StorageTier.DISK);
      assertPipeline(pipelines, 1, StorageTier.DISK);

      assertTrue(pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, emptyList(), emptyList(), StorageTier.SSD).isEmpty());

      assertTrue(pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, emptyList(), emptyList(), StorageTier.ARCHIVE).isEmpty());
    } finally {
      cleanUp();
    }
  }

  @Test
  public void testAllocateContainerWithStorageTierOnlyDisk() throws Exception {
    createCluster(Arrays.asList(
        Arrays.asList(StorageType.DISK, StorageType.DISK, StorageType.DISK),
        Arrays.asList(StorageType.DISK, StorageType.DISK, StorageType.DISK),
        Arrays.asList(StorageType.DISK, StorageType.DISK, StorageType.DISK)));
    try {
      PipelineManager pipelineManager = scm.getPipelineManager();
      assertTrue(containerManager.getContainers().isEmpty());
      ContainerInfo container;
      List<Pipeline> pipelines;

      // Only DISK is available.
      assertThrows(IOException.class, () -> containerManager.allocateContainer(
          ratisThree, "admin", StorageTier.SSD));

      container = containerManager.allocateContainer(ratisThree, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager, ratisThree, StorageTier.DISK, 1);

      assertThrows(IOException.class, () -> containerManager.allocateContainer(
          ratisThree, "admin", StorageTier.ARCHIVE));

      assertThrows(IOException.class, () -> containerManager.allocateContainer(
          standaloneOne, "admin", StorageTier.SSD));

      container = containerManager.allocateContainer(standaloneOne, "admin", StorageTier.DISK);
      assertContainer(container.containerID(), containerManager, standaloneOne, StorageTier.DISK, 2);

      assertThrows(IOException.class, () -> containerManager.allocateContainer(
          standaloneOne, "admin", StorageTier.ARCHIVE));

      pipelines = pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, emptyList(), emptyList(), StorageTier.DISK);
      assertPipeline(pipelines, 1, StorageTier.DISK);

      assertTrue(pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, emptyList(), emptyList(), StorageTier.SSD).isEmpty());

      assertTrue(pipelineManager.getPipelines(
          ratisThree, PipelineState.OPEN, emptyList(), emptyList(), StorageTier.ARCHIVE).isEmpty());

    } finally {
      cleanUp();
    }
  }

  private void assertContainer(ContainerID containerID, ContainerManager manager,
      ReplicationConfig replicationConfig, StorageTier expectedStorageTier, int expectedTotalCount)
      throws IOException {
    ContainerInfo containerInfo = manager.getContainer(containerID);
    Pipeline pipeline = scm.getPipelineManager().getPipeline(containerInfo.getPipelineID());

    assertNotNull(containerInfo);
    assertEquals(expectedTotalCount, manager.getContainers().size());
    assertEquals(replicationConfig.getRequiredNodes(), pipeline.getNodes().size());
    assertEquals(expectedStorageTier, pipeline.getSupportedStorageTier());
    assertEquals(expectedStorageTier, containerInfo.getStorageTier());
  }

  private void assertPipeline(List<Pipeline> pipelines, int expectedContainerCount,
      StorageTier expectedStorageTier) {
    assertFalse(pipelines.isEmpty());
    List<ContainerInfo> containerInfos = new ArrayList<>();
    for (Pipeline pipeline : pipelines) {
      ContainerInfo containerInfo = containerManager.getMatchingContainer(0, "admin",
          pipeline, new HashSet<>(), expectedStorageTier);
      if (containerInfo != null) {
        containerInfos.add(containerInfo);
      }
    }
    assertEquals(expectedContainerCount, containerInfos.size());
    for (ContainerInfo containerInfo : containerInfos) {
      assertEquals(expectedStorageTier, containerInfo.getStorageTier());
    }
  }
}
