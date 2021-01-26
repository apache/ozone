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

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Recon Container Manager.
 */
public class TestReconContainerManager
    extends AbstractReconContainerManagerTest {

  @Test
  public void testAddNewOpenContainer() throws IOException {
    ContainerWithPipeline containerWithPipeline =
        getTestContainer(LifeCycleState.OPEN);
    ContainerID containerID =
        containerWithPipeline.getContainerInfo().containerID();
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();

    ReconContainerManager containerManager = getContainerManager();
    assertFalse(containerManager.exists(containerID));
    assertFalse(getContainerTable().isExist(containerID));

    containerManager.addNewContainer(
        containerID.getId(), containerWithPipeline);

    assertTrue(containerManager.exists(containerID));

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
    assertTrue(getContainerTable().isExist(containerID));
  }

  @Test
  public void testAddNewClosedContainer() throws IOException {
    ContainerWithPipeline containerWithPipeline = getTestContainer(CLOSED);
    ContainerID containerID =
        containerWithPipeline.getContainerInfo().containerID();
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();

    ReconContainerManager containerManager = getContainerManager();
    assertFalse(containerManager.exists(containerID));
    assertFalse(getContainerTable().isExist(containerID));

    containerManager.addNewContainer(
        containerID.getId(), containerWithPipeline);

    assertTrue(containerManager.exists(containerID));

    List<ContainerInfo> containers = containerManager.getContainers(CLOSED);
    assertEquals(1, containers.size());
    assertEquals(containerInfo, containers.get(0));
    // Verify container DB.
    assertTrue(getContainerTable().isExist(containerID));
  }

  @Test
  public void testCheckAndAddNewContainer() throws IOException {
    ContainerID containerID = new ContainerID(100L);
    ReconContainerManager containerManager = getContainerManager();
    assertFalse(containerManager.exists(containerID));
    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    containerManager.checkAndAddNewContainer(containerID,
        OPEN, datanodeDetails);
    assertTrue(containerManager.exists(containerID));

    // Doing it one more time should not change any state.
    containerManager.checkAndAddNewContainer(containerID, OPEN,
        datanodeDetails);
    assertTrue(containerManager.exists(containerID));
    assertEquals(LifeCycleState.OPEN,
        getContainerManager().getContainer(containerID).getState());
  }

  @Test
  public void testUpdateContainerStateFromOpen() throws IOException {
    ContainerWithPipeline containerWithPipeline =
        getTestContainer(LifeCycleState.OPEN);

    long id = containerWithPipeline.getContainerInfo().getContainerID();
    ContainerID containerID =
        containerWithPipeline.getContainerInfo().containerID();

    // Adding container #100.
    getContainerManager().addNewContainer(id, containerWithPipeline);
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

  ContainerInfo newContainerInfo(long containerId) {
    return new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setOwner("owner2")
        .setNumberOfKeys(99L)
        .setReplicationFactor(HddsProtos.ReplicationFactor.THREE)
        .setPipelineID(PipelineID.randomId())
        .build();
  }

  void putContainerInfos(ReconContainerManager containerManager, int num)
      throws IOException {
    for (int i = 1; i <= num; i++) {
      final ContainerInfo info = newContainerInfo(i);
      containerManager.getContainerStore().put(new ContainerID(i), info);
      containerManager.getContainerStateManager()
          .addContainerInfo(i, info, null, null);
    }
  }

  @Test
  public void testUpdateAndRemoveContainerReplica() throws IOException {
    // Sanity checking updateContainerReplica and ContainerReplicaHistory

    // Init Container 1
    final long cIDlong1 = 1L;
    final ContainerID containerID1 = new ContainerID(cIDlong1);

    // Init DN01
    final UUID uuid1 = UUID.randomUUID();
    final DatanodeDetails datanodeDetails1 = DatanodeDetails.newBuilder()
        .setUuid(uuid1).setHostName("host1").setIpAddress("127.0.0.1").build();
    final ContainerReplica containerReplica1 = ContainerReplica.newBuilder()
        .setContainerID(containerID1).setContainerState(State.OPEN)
        .setDatanodeDetails(datanodeDetails1).build();

    final ReconContainerManager containerManager = getContainerManager();
    final Map<Long, Map<UUID, ContainerReplicaHistory>> repHistMap =
        containerManager.getReplicaHistoryMap();
    // Should be empty at the beginning
    Assert.assertEquals(0, repHistMap.size());

    // Put a replica info and call updateContainerReplica
    putContainerInfos(containerManager, 10);
    containerManager.updateContainerReplica(containerID1, containerReplica1);
    // Should have 1 container entry in the replica history map
    Assert.assertEquals(1, repHistMap.size());
    // Should only have 1 entry for this replica (on DN01)
    Assert.assertEquals(1, repHistMap.get(cIDlong1).size());
    ContainerReplicaHistory repHist1 = repHistMap.get(cIDlong1).get(uuid1);
    Assert.assertEquals(uuid1, repHist1.getUuid());
    // Because this is a new entry, first seen time equals last seen time
    assertEquals(repHist1.getLastSeenTime(), repHist1.getFirstSeenTime());

    // Let's update the entry again
    containerManager.updateContainerReplica(containerID1, containerReplica1);
    // Should still have 1 entry in the replica history map
    Assert.assertEquals(1, repHistMap.size());
    // Now last seen time should be larger than first seen time
    Assert.assertTrue(repHist1.getLastSeenTime() > repHist1.getFirstSeenTime());

    // Init DN02
    final UUID uuid2 = UUID.randomUUID();
    final DatanodeDetails datanodeDetails2 = DatanodeDetails.newBuilder()
        .setUuid(uuid2).setHostName("host2").setIpAddress("127.0.0.2").build();
    final ContainerReplica containerReplica2 = ContainerReplica.newBuilder()
        .setContainerID(containerID1).setContainerState(State.OPEN)
        .setDatanodeDetails(datanodeDetails2).build();

    // Add replica to DN02
    containerManager.updateContainerReplica(containerID1, containerReplica2);

    // Should still have 1 container entry in the replica history map
    Assert.assertEquals(1, repHistMap.size());
    // Should have 2 entries for this replica (on DN01 and DN02)
    Assert.assertEquals(2, repHistMap.get(cIDlong1).size());
    ContainerReplicaHistory repHist2 = repHistMap.get(cIDlong1).get(uuid2);
    Assert.assertEquals(uuid2, repHist2.getUuid());
    // Because this is a new entry, first seen time equals last seen time
    assertEquals(repHist2.getLastSeenTime(), repHist2.getFirstSeenTime());

    // Remove replica from DN01
    containerManager.removeContainerReplica(containerID1, containerReplica1);
    // Should still have 1 container entry in the replica history map
    Assert.assertEquals(1, repHistMap.size());
    // Should have 1 entry for this replica
    Assert.assertEquals(1, repHistMap.get(cIDlong1).size());
    // And the only entry should match DN02
    Assert.assertEquals(uuid2,
        repHistMap.get(cIDlong1).keySet().iterator().next());
  }
}
