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

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.junit.jupiter.api.Test;

/**
 * Test Recon Container Manager.
 */
public class TestReconContainerManager
    extends AbstractReconContainerManagerTest {
  @Test
  public void testAddNewOpenContainer() throws IOException, TimeoutException {
    ContainerWithPipeline containerWithPipeline =
        getTestContainer(LifeCycleState.OPEN);
    ContainerID containerID =
        containerWithPipeline.getContainerInfo().containerID();
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();

    ReconContainerManager containerManager = getContainerManager();
    assertFalse(containerManager.containerExist(containerID));
    assertFalse(getContainerTable().isExist(containerID));

    containerManager.addNewContainer(containerWithPipeline);

    assertTrue(containerManager.containerExist(containerID));

    List<ContainerInfo> containers =
        containerManager.getContainers(LifeCycleState.OPEN);
    assertEquals(1, containers.size());
    assertEquals(containerInfo, containers.get(0));
    NavigableSet<ContainerID> containersInPipeline =
        getPipelineManager().getContainersInPipeline(
            containerWithPipeline.getPipeline().getId());
    assertEquals(1, containersInPipeline.size());
    assertEquals(containerID, containersInPipeline.first());

    // Verify container DB.
    SCMHAManager scmhaManager = containerManager.getSCMHAManager();
    scmhaManager.getDBTransactionBuffer().close();
    assertTrue(getContainerTable().isExist(containerID));
  }

  @Test
  public void testAddNewClosedContainer()
      throws IOException, TimeoutException {
    ContainerWithPipeline containerWithPipeline = getTestContainer(CLOSED);
    ContainerID containerID =
        containerWithPipeline.getContainerInfo().containerID();
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();

    ReconContainerManager containerManager = getContainerManager();
    assertFalse(containerManager.containerExist(containerID));
    assertFalse(getContainerTable().isExist(containerID));

    containerManager.addNewContainer(containerWithPipeline);

    assertTrue(containerManager.containerExist(containerID));

    List<ContainerInfo> containers = containerManager.getContainers(CLOSED);
    assertEquals(1, containers.size());
    assertEquals(containerInfo, containers.get(0));
    // Verify container DB.
    SCMHAManager scmhaManager = containerManager.getSCMHAManager();
    scmhaManager.getDBTransactionBuffer().close();
    assertTrue(getContainerTable().isExist(containerID));
  }

  @Test
  public void testCheckAndAddNewContainer() throws Exception {
    ContainerID containerID = ContainerID.valueOf(100L);
    ReconContainerManager containerManager = getContainerManager();
    assertFalse(containerManager.containerExist(containerID));
    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    containerManager.checkAndAddNewContainer(containerID,
        OPEN, datanodeDetails);
    assertTrue(containerManager.containerExist(containerID));

    // Doing it one more time should not change any state.
    containerManager.checkAndAddNewContainer(containerID, OPEN,
        datanodeDetails);
    assertTrue(containerManager.containerExist(containerID));
    assertEquals(LifeCycleState.OPEN,
        getContainerManager().getContainer(containerID).getState());
  }

  @Test
  public void testCheckAndAddNewContainerBatch() throws IOException {
    List<ContainerReplicaProto> containerReplicaProtoList = new LinkedList<>();
    ReconContainerManager containerManager = getContainerManager();
    State[] stateTypes = State.values();
    LifeCycleState[] lifeCycleStateTypes = LifeCycleState.values();
    int lifeCycleStateCount = lifeCycleStateTypes.length;
    for (int i = 200; i < 300; i++) {
      assertFalse(containerManager.containerExist(ContainerID.valueOf(i)));
      ContainerReplicaProto.Builder ciBuilder =
          ContainerReplicaProto.newBuilder();
      ContainerReplicaProto crp = ciBuilder.
          setContainerID(i).
          setState(stateTypes[i % lifeCycleStateCount]).build();
      containerReplicaProtoList.add(crp);
    }

    containerManager.checkAndAddNewContainerBatch(containerReplicaProtoList);
    for (long i = 200L; i < 300L; i++) {
      assertTrue(containerManager.containerExist(ContainerID.valueOf(i)));
    }

    // Doing it one more time should not change any state.
    containerManager.checkAndAddNewContainerBatch(containerReplicaProtoList);
    for (int i = 200; i < 300; i++) {
      assertTrue(containerManager.containerExist(ContainerID.valueOf(i)));
      assertEquals(lifeCycleStateTypes[i % lifeCycleStateCount],
          getContainerManager().
            getContainer(ContainerID.valueOf(i)).getState());
    }
  }

  @Test
  public void testUpdateContainerStateFromOpen() throws Exception {
    ContainerWithPipeline containerWithPipeline =
        getTestContainer(LifeCycleState.OPEN);
    ContainerID containerID =
        containerWithPipeline.getContainerInfo().containerID();

    // Adding container #100.
    getContainerManager().addNewContainer(containerWithPipeline);
    assertEquals(LifeCycleState.OPEN,
        getContainerManager().getContainer(containerID).getState());

    DatanodeDetails datanodeDetails = randomDatanodeDetails();

    // First report with "CLOSED" replica state moves container state to
    // "CLOSING".
    getContainerManager().checkAndAddNewContainer(containerID, State.CLOSED,
        datanodeDetails);
    assertEquals(CLOSING,
        getContainerManager().getContainer(containerID).getState());
  }

  ContainerInfo newContainerInfo(long containerId, Pipeline pipeline) {
    return new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setReplicationConfig(RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE))
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setOwner("owner2")
        .setNumberOfKeys(99L)
        .setPipelineID(pipeline.getId())
        .build();
  }

  @Test
  public void testUpdateAndRemoveContainerReplica()
      throws IOException, TimeoutException {
    // Sanity checking updateContainerReplica and ContainerReplicaHistory

    // Init Container 1
    final long cIDlong1 = 1L;
    final ContainerID containerID1 = ContainerID.valueOf(cIDlong1);

    // Init DN01
    final UUID uuid1 = UUID.randomUUID();
    final DatanodeDetails datanodeDetails1 = DatanodeDetails.newBuilder()
        .setUuid(uuid1).setHostName("host1").setIpAddress("127.0.0.1").build();
    ContainerReplica containerReplica1 = ContainerReplica.newBuilder()
        .setContainerID(containerID1).setContainerState(State.OPEN)
        .setDatanodeDetails(datanodeDetails1).setSequenceId(1001L)
        .setChecksums(ContainerChecksums.of(1234L, 0L)).build();

    final ReconContainerManager containerManager = getContainerManager();
    final Map<Long, Map<UUID, ContainerReplicaHistory>> repHistMap =
        containerManager.getReplicaHistoryMap();
    // Should be empty at the beginning
    assertEquals(0, repHistMap.size());

    // Put a replica info and call updateContainerReplica
    Pipeline pipeline = getRandomPipeline();
    getPipelineManager().addPipeline(pipeline);
    for (int i = 1; i <= 10; i++) {
      final ContainerInfo info = newContainerInfo(i, pipeline);
      containerManager.addNewContainer(
          new ContainerWithPipeline(info, pipeline));
    }

    containerManager.updateContainerReplica(containerID1, containerReplica1);
    // Should have 1 container entry in the replica history map
    assertEquals(1, repHistMap.size());
    // Should only have 1 entry for this replica (on DN01)
    assertEquals(1, repHistMap.get(cIDlong1).size());
    ContainerReplicaHistory repHist1 = repHistMap.get(cIDlong1).get(uuid1);
    assertEquals(uuid1, repHist1.getUuid());
    // Because this is a new entry, first seen time equals last seen time
    assertEquals(repHist1.getLastSeenTime(), repHist1.getFirstSeenTime());
    assertEquals(containerReplica1.getSequenceId().longValue(),
        repHist1.getBcsId());
    assertEquals(containerReplica1.getDataChecksum(), repHist1.getDataChecksum());

    // Let's update the entry again
    containerReplica1 = ContainerReplica.newBuilder()
        .setContainerID(containerID1).setContainerState(State.OPEN)
        .setDatanodeDetails(datanodeDetails1).setSequenceId(1051L).build();
    containerManager.updateContainerReplica(containerID1, containerReplica1);
    // Should still have 1 entry in the replica history map
    assertEquals(1, repHistMap.size());
    // Now last seen time should be larger than first seen time
    assertThat(repHist1.getLastSeenTime()).isGreaterThan(repHist1.getFirstSeenTime());
    assertEquals(1051L, repHist1.getBcsId());

    // Init DN02
    final UUID uuid2 = UUID.randomUUID();
    final DatanodeDetails datanodeDetails2 = DatanodeDetails.newBuilder()
        .setUuid(uuid2).setHostName("host2").setIpAddress("127.0.0.2").build();
    final ContainerReplica containerReplica2 = ContainerReplica.newBuilder()
        .setContainerID(containerID1).setContainerState(State.OPEN)
        .setDatanodeDetails(datanodeDetails2).setSequenceId(1051L)
        .setChecksums(ContainerChecksums.of(1234L, 0L)).build();

    // Add replica to DN02
    containerManager.updateContainerReplica(containerID1, containerReplica2);

    // Should still have 1 container entry in the replica history map
    assertEquals(1, repHistMap.size());
    // Should have 2 entries for this replica (on DN01 and DN02)
    assertEquals(2, repHistMap.get(cIDlong1).size());
    ContainerReplicaHistory repHist2 = repHistMap.get(cIDlong1).get(uuid2);
    assertEquals(uuid2, repHist2.getUuid());
    // Because this is a new entry, first seen time equals last seen time
    assertEquals(repHist2.getLastSeenTime(), repHist2.getFirstSeenTime());
    assertEquals(1051L, repHist2.getBcsId());
    assertEquals(containerReplica2.getDataChecksum(), repHist2.getDataChecksum());

    // Remove replica from DN01
    containerManager.removeContainerReplica(containerID1, containerReplica1);
    // Should still have 1 container entry in the replica history map
    assertEquals(1, repHistMap.size());
    // Should have 1 entry for this replica
    assertEquals(1, repHistMap.get(cIDlong1).size());
    // And the only entry should match DN02
    assertEquals(uuid2,
        repHistMap.get(cIDlong1).keySet().iterator().next());
  }

  @Test
  public void testAddNewContainerWithMissingPipeline()
      throws IOException, TimeoutException {
    // Create a container with a pipeline that Recon does not know about yet
    Pipeline newPipeline = getRandomPipeline();
    ContainerInfo containerInfo = newContainerInfo(101L, newPipeline);
    ContainerWithPipeline containerWithPipeline =
        new ContainerWithPipeline(containerInfo, newPipeline);

    // Ensure pipeline is not present
    ReconContainerManager containerManager = getContainerManager();
    assertFalse(getPipelineManager().containsPipeline(
        newPipeline.getId()));

    // Add a new container, and it should add the missing pipeline first
    containerManager.addNewContainer(containerWithPipeline);

    // Pipeline should be added now
    assertTrue(getPipelineManager().containsPipeline(
        newPipeline.getId()));
    assertTrue(containerManager.containerExist(containerInfo.containerID()));
  }

}
