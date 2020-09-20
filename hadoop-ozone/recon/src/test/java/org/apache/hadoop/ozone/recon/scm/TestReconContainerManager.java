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
import java.util.NavigableSet;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
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
}