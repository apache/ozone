/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN;


/**
 * Tests to verify the functionality of ContainerManager.
 */
public class TestContainerManagerImpl {

  private File testDir;
  private DBStore dbStore;
  private ContainerManager containerManager;
  private SCMHAManager scmhaManager;
  private SequenceIdGenerator sequenceIdGen;
  private NodeManager nodeManager;
  private ContainerReplicaPendingOps pendingOpsMock;

  @BeforeEach
  public void setUp() throws Exception {
    final OzoneConfiguration conf = SCMTestUtils.getConf();
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    scmhaManager = SCMHAManagerStub.getInstance(true);
    nodeManager = new MockNodeManager(true, 10);
    sequenceIdGen = new SequenceIdGenerator(
        conf, scmhaManager, SCMDBDefinition.SEQUENCE_ID.getTable(dbStore));
    final PipelineManager pipelineManager =
        new MockPipelineManager(dbStore, scmhaManager, nodeManager);
    pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
        ReplicationFactor.THREE));
    pendingOpsMock = Mockito.mock(ContainerReplicaPendingOps.class);
    containerManager = new ContainerManagerImpl(conf,
        scmhaManager, sequenceIdGen, pipelineManager,
        SCMDBDefinition.CONTAINERS.getTable(dbStore), pendingOpsMock);
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (containerManager != null) {
      containerManager.close();
    }

    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  @Test
  void testAllocateContainer() throws Exception {
    Assertions.assertTrue(
        containerManager.getContainers().isEmpty());
    final ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "admin");
    Assertions.assertEquals(1, containerManager.getContainers().size());
    Assertions.assertNotNull(containerManager.getContainer(
        container.containerID()));
  }

  @Test
  void testUpdateContainerState() throws Exception {
    final ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "admin");
    final ContainerID cid = container.containerID();
    Assertions.assertEquals(HddsProtos.LifeCycleState.OPEN,
        containerManager.getContainer(cid).getState());
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.FINALIZE);
    Assertions.assertEquals(HddsProtos.LifeCycleState.CLOSING,
        containerManager.getContainer(cid).getState());
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.QUASI_CLOSE);
    Assertions.assertEquals(HddsProtos.LifeCycleState.QUASI_CLOSED,
        containerManager.getContainer(cid).getState());
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.FORCE_CLOSE);
    Assertions.assertEquals(HddsProtos.LifeCycleState.CLOSED,
        containerManager.getContainer(cid).getState());
  }

  @Test
  void testGetContainers() throws Exception {
    Assertions.assertTrue(
        containerManager.getContainers().isEmpty());

    ContainerID[] cidArray = new ContainerID[10];
    for (int i = 0; i < 10; i++) {
      ContainerInfo container = containerManager.allocateContainer(
          RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE), "admin");
      cidArray[i] = container.containerID();
    }

    Assertions.assertEquals(10,
        containerManager.getContainers(cidArray[0], 10).size());
    Assertions.assertEquals(10,
        containerManager.getContainers(cidArray[0], 100).size());

    containerManager.updateContainerState(cidArray[0],
        HddsProtos.LifeCycleEvent.FINALIZE);
    Assertions.assertEquals(9,
        containerManager.getContainers(HddsProtos.LifeCycleState.OPEN).size());
    Assertions.assertEquals(1, containerManager
        .getContainers(HddsProtos.LifeCycleState.CLOSING).size());
    containerManager.updateContainerState(cidArray[1],
        HddsProtos.LifeCycleEvent.FINALIZE);
    Assertions.assertEquals(8,
        containerManager.getContainers(HddsProtos.LifeCycleState.OPEN).size());
    Assertions.assertEquals(2, containerManager
        .getContainers(HddsProtos.LifeCycleState.CLOSING).size());
    containerManager.updateContainerState(cidArray[1],
        HddsProtos.LifeCycleEvent.QUASI_CLOSE);
    containerManager.updateContainerState(cidArray[2],
        HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(cidArray[2],
        HddsProtos.LifeCycleEvent.CLOSE);
    Assertions.assertEquals(7, containerManager.
        getContainerStateCount(HddsProtos.LifeCycleState.OPEN));
    Assertions.assertEquals(1, containerManager
        .getContainerStateCount(HddsProtos.LifeCycleState.CLOSING));
    Assertions.assertEquals(1, containerManager
        .getContainerStateCount(HddsProtos.LifeCycleState.QUASI_CLOSED));
    Assertions.assertEquals(1, containerManager
        .getContainerStateCount(HddsProtos.LifeCycleState.CLOSED));
  }

  @Test
  void testAllocateContainersWithECReplicationConfig() throws Exception {
    final ContainerInfo admin = containerManager
        .allocateContainer(new ECReplicationConfig(3, 2), "admin");
    Assertions.assertEquals(1, containerManager.getContainers().size());
    Assertions.assertNotNull(
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
    Mockito.verify(pendingOpsMock, Mockito.times(1))
        .completeAddReplica(container.containerID(), dn, 0);
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
    Mockito.verify(pendingOpsMock, Mockito.times(1))
        .completeDeleteReplica(container.containerID(), dn, 0);
  }

}
