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

package org.apache.hadoop.hdds.scm.container;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.container.states.ContainerStateMap;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.event.Level;

/**
 * Tests to verify the functionality of ContainerManager.
 */
public class TestContainerManagerImpl {

  @TempDir
  private File testDir;
  private DBStore dbStore;
  private ContainerManager containerManager;
  private SCMHAManager scmhaManager;
  private SequenceIdGenerator sequenceIdGen;
  private ContainerReplicaPendingOps pendingOpsMock;
  private PipelineManager pipelineManager;

  @BeforeAll
  static void init() {
    // Print container state transition logs
    GenericTestUtils.setLogLevel(ContainerStateMap.class, Level.TRACE);
  }

  @BeforeEach
  void setUp() throws Exception {
    final OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    scmhaManager = SCMHAManagerStub.getInstance(true);
    NodeManager nodeManager = new MockNodeManager(true, 10);
    sequenceIdGen = new SequenceIdGenerator(
        conf, scmhaManager, SCMDBDefinition.SEQUENCE_ID.getTable(dbStore));
    PipelineManager base = new MockPipelineManager(dbStore, scmhaManager, nodeManager);
    pipelineManager = spy(base);

    // Default: allow allocation in tests unless a test overrides it.
    doReturn(true).when(pipelineManager).hasEnoughSpace(any(Pipeline.class), anyLong());

    pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
        ReplicationFactor.THREE));

    pendingOpsMock = mock(ContainerReplicaPendingOps.class);
    containerManager = new ContainerManagerImpl(conf,
        scmhaManager, sequenceIdGen, pipelineManager,
        SCMDBDefinition.CONTAINERS.getTable(dbStore), pendingOpsMock);
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  @Test
  void testAllocateContainer() throws Exception {
    assertTrue(
        containerManager.getContainers().isEmpty());
    final ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "admin");

    assertNotNull(container);
    assertEquals(1, containerManager.getContainers().size());
    assertNotNull(containerManager.getContainer(
        container.containerID()));
  }

  /**
   * getMatchingContainer allocates a new container in some cases. This test verifies that a container is not
   * allocated when nodes in that pipeline don't have enough space for a new container.
   */
  @Test
  public void testGetMatchingContainerReturnsNullWhenNotEnoughSpaceInDatanodes() throws IOException {
    doReturn(false).when(pipelineManager).hasEnoughSpace(any(), anyLong());

    long sizeRequired = 256 * 1024 * 1024; // 256 MB
    Pipeline pipeline = pipelineManager.getPipelines().iterator().next();
    // MockPipelineManager#hasEnoughSpace always returns false
    // the pipeline has no existing containers, so a new container gets allocated in getMatchingContainer
    ContainerInfo container = containerManager
        .getMatchingContainer(sizeRequired, "test", pipeline, Collections.emptySet());
    assertNull(container);

    // create an EC pipeline to test for EC containers
    ECReplicationConfig ecReplicationConfig = new ECReplicationConfig(3, 2);
    pipelineManager.createPipeline(ecReplicationConfig);
    pipeline = pipelineManager.getPipelines(ecReplicationConfig).iterator().next();
    container = containerManager.getMatchingContainer(sizeRequired, "test", pipeline, Collections.emptySet());
    assertNull(container);
  }

  @Test
  public void testGetMatchingContainerReturnsContainerWhenEnoughSpaceInDatanodes() throws IOException {
    long sizeRequired = 256 * 1024 * 1024; // 256 MB

    // create a spy to mock hasEnoughSpace to always return true
    PipelineManager spyPipelineManager = spy(pipelineManager);
    doReturn(true).when(spyPipelineManager)
        .hasEnoughSpace(any(Pipeline.class), anyLong());

    // create a new ContainerManager using the spy
    File tempDir = new File(testDir, "tempDir");
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);
    ContainerManager manager = new ContainerManagerImpl(conf,
        scmhaManager, sequenceIdGen, spyPipelineManager,
        SCMDBDefinition.CONTAINERS.getTable(dbStore), pendingOpsMock);

    Pipeline pipeline = spyPipelineManager.getPipelines().iterator().next();
    // the pipeline has no existing containers, so a new container gets allocated in getMatchingContainer
    ContainerInfo container = manager
        .getMatchingContainer(sizeRequired, "test", pipeline, Collections.emptySet());
    assertNotNull(container);

    // create an EC pipeline to test for EC containers
    ECReplicationConfig ecReplicationConfig = new ECReplicationConfig(3, 2);
    spyPipelineManager.createPipeline(ecReplicationConfig);
    pipeline = spyPipelineManager.getPipelines(ecReplicationConfig).iterator().next();
    container = manager.getMatchingContainer(sizeRequired, "test", pipeline, Collections.emptySet());
    assertNotNull(container);
  }

  @Test
  void testUpdateContainerState() throws Exception {
    final ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "admin");
    final ContainerID cid = container.containerID();
    assertEquals(LifeCycleState.OPEN, containerManager.getContainer(cid).getState());
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.FINALIZE);
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(cid).getState());
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.QUASI_CLOSE);
    assertEquals(LifeCycleState.QUASI_CLOSED, containerManager.getContainer(cid).getState());
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.FORCE_CLOSE);
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(cid).getState());
  }

  @ParameterizedTest
  @EnumSource(value = HddsProtos.LifeCycleState.class,
      names = {"DELETING", "DELETED"})
  void testTransitionDeletingOrDeletedToClosedState(HddsProtos.LifeCycleState desiredState)
      throws IOException, InvalidStateTransitionException {
    // Allocate OPEN Ratis and Ec containers, and do a series of state changes to transition them to DELETING / DELETED
    final ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "admin");
    ContainerInfo ecContainer = containerManager.allocateContainer(new ECReplicationConfig(3, 2), "admin");
    final ContainerID cid = container.containerID();
    final ContainerID ecCid = ecContainer.containerID();
    assertEquals(LifeCycleState.OPEN, containerManager.getContainer(cid).getState());
    assertEquals(LifeCycleState.OPEN, containerManager.getContainer(ecCid).getState());

    // OPEN -> CLOSING
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(ecCid, HddsProtos.LifeCycleEvent.FINALIZE);
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(cid).getState());
    assertEquals(LifeCycleState.CLOSING, containerManager.getContainer(ecCid).getState());

    // CLOSING -> CLOSED
    containerManager.updateContainerState(cid, HddsProtos.LifeCycleEvent.CLOSE);
    containerManager.updateContainerState(ecCid, HddsProtos.LifeCycleEvent.CLOSE);
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(cid).getState());
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(ecCid).getState());

    // CLOSED -> DELETING
    containerManager.updateContainerState(cid, HddsProtos.LifeCycleEvent.DELETE);
    containerManager.updateContainerState(ecCid, HddsProtos.LifeCycleEvent.DELETE);
    assertEquals(LifeCycleState.DELETING, containerManager.getContainer(cid).getState());
    assertEquals(LifeCycleState.DELETING, containerManager.getContainer(ecCid).getState());

    if (desiredState == LifeCycleState.DELETED) {
      // DELETING -> DELETED
      containerManager.updateContainerState(cid, HddsProtos.LifeCycleEvent.CLEANUP);
      containerManager.updateContainerState(ecCid, HddsProtos.LifeCycleEvent.CLEANUP);
      assertEquals(LifeCycleState.DELETED, containerManager.getContainer(cid).getState());
      assertEquals(LifeCycleState.DELETED, containerManager.getContainer(ecCid).getState());
    }

    // DELETING / DELETED -> CLOSED
    containerManager.transitionDeletingOrDeletedToClosedState(cid);
    containerManager.transitionDeletingOrDeletedToClosedState(ecCid);
    // the containers should be back in CLOSED state now
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(cid).getState());
    assertEquals(LifeCycleState.CLOSED, containerManager.getContainer(ecCid).getState());
  }

  @Test
  void testTransitionContainerToClosedStateAllowOnlyDeletingOrDeletedContainers() throws IOException {
    // Negative test for Ratis/EC container OPEN -> CLOSED transition

    // test for RATIS container
    final ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "admin");
    final ContainerID cid = container.containerID();
    assertEquals(LifeCycleState.OPEN, containerManager.getContainer(cid).getState());
    assertThrows(IOException.class, () -> containerManager.transitionDeletingOrDeletedToClosedState(cid));

    // test for EC container
    final ContainerInfo ecContainer = containerManager.allocateContainer(new ECReplicationConfig(3, 2), "admin");
    final ContainerID ecCid = ecContainer.containerID();
    assertEquals(LifeCycleState.OPEN, containerManager.getContainer(ecCid).getState());
    assertThrows(IOException.class, () -> containerManager.transitionDeletingOrDeletedToClosedState(ecCid));
  }

  @Test
  void testGetContainers() throws Exception {
    assertEquals(emptyList(), containerManager.getContainers());

    List<ContainerID> ids = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ContainerInfo container = containerManager.allocateContainer(
          RatisReplicationConfig.getInstance(ReplicationFactor.THREE), "admin");
      ids.add(container.containerID());
    }

    assertIds(ids,
        containerManager.getContainers(ContainerID.MIN, 10));
    assertIds(ids.subList(0, 5),
        containerManager.getContainers(ContainerID.MIN, 5));

    assertIds(ids, containerManager.getContainers(ids.get(0), 10));
    assertIds(ids, containerManager.getContainers(ids.get(0), 100));
    assertIds(ids.subList(5, ids.size()),
        containerManager.getContainers(ids.get(5), 100));
    assertIds(emptyList(),
        containerManager.getContainers(ids.get(5), 100, LifeCycleState.CLOSED));

    containerManager.updateContainerState(ids.get(0),
        HddsProtos.LifeCycleEvent.FINALIZE);
    assertIds(ids.subList(0, 1),
        containerManager.getContainers(LifeCycleState.CLOSING));
    assertIds(ids.subList(1, ids.size()),
        containerManager.getContainers(LifeCycleState.OPEN));

    containerManager.updateContainerState(ids.get(1),
        HddsProtos.LifeCycleEvent.FINALIZE);
    assertIds(ids.subList(0, 2),
        containerManager.getContainers(LifeCycleState.CLOSING));
    assertIds(ids.subList(2, ids.size()),
        containerManager.getContainers(LifeCycleState.OPEN));

    containerManager.updateContainerState(ids.get(1),
        HddsProtos.LifeCycleEvent.QUASI_CLOSE);
    containerManager.updateContainerState(ids.get(2),
        HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(ids.get(2),
        HddsProtos.LifeCycleEvent.CLOSE);
    assertEquals(7, containerManager.
        getContainerStateCount(LifeCycleState.OPEN));
    assertEquals(1, containerManager
        .getContainerStateCount(LifeCycleState.CLOSING));
    assertEquals(1, containerManager
        .getContainerStateCount(LifeCycleState.QUASI_CLOSED));
    assertEquals(1, containerManager
        .getContainerStateCount(LifeCycleState.CLOSED));
  }

  private static void assertIds(
      List<ContainerID> expected,
      List<ContainerInfo> containers
  ) {
    assertEquals(expected, containers.stream()
        .map(ContainerInfo::containerID)
        .collect(toList()));
  }

  @Test
  void testAllocateContainersWithECReplicationConfig() throws Exception {
    final ContainerInfo admin = containerManager
        .allocateContainer(new ECReplicationConfig(3, 2), "admin");
    assertEquals(1, containerManager.getContainers().size());
    assertNotNull(
        containerManager.getContainer(admin.containerID()));
  }

  @Test
  void testUpdateContainerReplicaInvokesPendingOp()
      throws IOException, TimeoutException {
    final ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "admin");
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    containerManager.updateContainerReplica(container.containerID(),
        ContainerReplica.newBuilder()
            .setContainerState(OPEN)
            .setReplicaIndex(0)
            .setContainerID(container.containerID())
            .setDatanodeDetails(dn)
            .setSequenceId(1)
            .setBytesUsed(1234)
            .setKeyCount(123)
            .build());
    verify(pendingOpsMock, times(1)).completeAddReplica(container.containerID(), dn, 0);
  }

  @Test
  void testRemoveContainerReplicaInvokesPendingOp()
      throws IOException, TimeoutException {
    final ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "admin");
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    containerManager.removeContainerReplica(container.containerID(),
        ContainerReplica.newBuilder()
            .setContainerState(OPEN)
            .setReplicaIndex(0)
            .setContainerID(container.containerID())
            .setDatanodeDetails(dn)
            .setSequenceId(1)
            .setBytesUsed(1234)
            .setKeyCount(123)
            .build());
    verify(pendingOpsMock, times(1)).completeDeleteReplica(container.containerID(), dn, 0);
  }

}
