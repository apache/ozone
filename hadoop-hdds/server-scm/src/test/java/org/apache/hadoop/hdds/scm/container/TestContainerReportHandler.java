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
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server
    .SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getECContainer;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getReplicas;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getContainer;

/**
 * Test the behaviour of the ContainerReportHandler.
 */
public class TestContainerReportHandler {

  private NodeManager nodeManager;
  private ContainerManager containerManager;
  private ContainerStateManager containerStateManager;
  private EventPublisher publisher;
  private File testDir;
  private DBStore dbStore;
  private SCMHAManager scmhaManager;
  private PipelineManager pipelineManager;

  @BeforeEach
  public void setup() throws IOException, InvalidStateTransitionException,
      TimeoutException {
    final OzoneConfiguration conf = SCMTestUtils.getConf();
    nodeManager = new MockNodeManager(true, 10);
    containerManager = Mockito.mock(ContainerManager.class);
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    scmhaManager = SCMHAManagerStub.getInstance(true);
    nodeManager = new MockNodeManager(true, 10);
    pipelineManager =
        new MockPipelineManager(dbStore, scmhaManager, nodeManager);
    containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setContainerStore(SCMDBDefinition.CONTAINERS.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
    publisher = Mockito.mock(EventPublisher.class);

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer(((ContainerID)invocation
                .getArguments()[0])));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas(((ContainerID)invocation
                .getArguments()[0])));

    Mockito.doAnswer(invocation -> {
      containerStateManager
          .updateContainerState(((ContainerID)invocation
                  .getArguments()[0]).getProtobuf(),
              (HddsProtos.LifeCycleEvent)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerState(
        Mockito.any(ContainerID.class),
        Mockito.any(HddsProtos.LifeCycleEvent.class));

    Mockito.doAnswer(invocation -> {
      containerStateManager.updateContainerReplica(
          ((ContainerID)invocation.getArguments()[0]),
          (ContainerReplica) invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerReplica(
        Mockito.any(ContainerID.class), Mockito.any(ContainerReplica.class));

    Mockito.doAnswer(invocation -> {
      containerStateManager.removeContainerReplica(
          ((ContainerID)invocation.getArguments()[0]),
          (ContainerReplica) invocation.getArguments()[1]);
      return null;
    }).when(containerManager).removeContainerReplica(
        Mockito.any(ContainerID.class), Mockito.any(ContainerReplica.class));

  }

  @AfterEach
  public void tearDown() throws Exception {
    containerStateManager.close();
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  private void testReplicaIndexUpdate(ContainerInfo container,
               DatanodeDetails dn, int replicaIndex,
               Map<DatanodeDetails, Integer> expectedReplicaMap) {
    final ContainerReportsProto containerReport = getContainerReportsProto(
            container.containerID(), ContainerReplicaProto.State.CLOSED,
            dn.getUuidString(), 2000000000L, 100000000L, replicaIndex);
    final ContainerReportFromDatanode containerReportFromDatanode =
            new ContainerReportFromDatanode(dn, containerReport);
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
            nodeManager, containerManager);
    reportHandler.onMessage(containerReportFromDatanode, publisher);
    Assert.assertEquals(containerStateManager
            .getContainerReplicas(container.containerID()).stream()
            .collect(Collectors.toMap(ContainerReplica::getDatanodeDetails,
                    ContainerReplica::getReplicaIndex)), expectedReplicaMap);

  }

  @Test
  public void testECReplicaIndexValidation() throws NodeNotFoundException,
          IOException, TimeoutException {
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
            NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();
    final DatanodeDetails datanodeFour = nodeIterator.next();
    final DatanodeDetails datanodeFive = nodeIterator.next();
    ECReplicationConfig replicationConfig = new ECReplicationConfig(3, 2);
    final ContainerInfo container = getECContainer(LifeCycleState.CLOSED,
            PipelineID.randomId(), replicationConfig);
    nodeManager.addContainer(datanodeOne, container.containerID());
    nodeManager.addContainer(datanodeTwo, container.containerID());
    nodeManager.addContainer(datanodeThree, container.containerID());
    nodeManager.addContainer(datanodeFour, container.containerID());
    nodeManager.addContainer(datanodeFive, container.containerID());
    containerStateManager.addContainer(container.getProtobuf());
    Set<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(container.containerID(),
            ContainerReplicaProto.State.CLOSED,
            HddsTestUtils.CONTAINER_USED_BYTES_DEFAULT,
            HddsTestUtils.CONTAINER_NUM_KEYS_DEFAULT,
            1000000L,
            datanodeOne, datanodeTwo, datanodeThree,
            datanodeFour, datanodeFive);
    Map<DatanodeDetails, Integer> replicaMap = replicas.stream()
            .collect(Collectors.toMap(ContainerReplica::getDatanodeDetails,
                    ContainerReplica::getReplicaIndex));
    replicas.forEach(r -> containerStateManager.updateContainerReplica(
                    container.containerID(), r));
    testReplicaIndexUpdate(container, datanodeOne, 0, replicaMap);
    testReplicaIndexUpdate(container, datanodeOne, 6, replicaMap);
    replicaMap.put(datanodeOne, 2);
    testReplicaIndexUpdate(container, datanodeOne, 2, replicaMap);
  }

  @Test
  public void testUnderReplicatedContainer()
      throws NodeNotFoundException, IOException, TimeoutException {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSED);
    final ContainerInfo containerTwo = getContainer(LifeCycleState.CLOSED);
    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    getReplicas(containerOne.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(r -> containerStateManager.updateContainerReplica(
            containerOne.containerID(), r));

    getReplicas(containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(r -> containerStateManager.updateContainerReplica(
            containerTwo.containerID(), r));


    // SCM expects both containerOne and containerTwo to be in all the three
    // datanodes datanodeOne, datanodeTwo and datanodeThree

    // Now datanodeOne is sending container report in which containerOne is
    // missing.

    // containerOne becomes under replicated.
    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerTwo.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);
    Assertions.assertEquals(2, containerManager.getContainerReplicas(
        containerOne.containerID()).size());

  }

  @Test
  public void testOverReplicatedContainer() throws NodeNotFoundException,
      IOException, TimeoutException {

    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();
    final DatanodeDetails datanodeFour = nodeIterator.next();

    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSED);
    final ContainerInfo containerTwo = getContainer(LifeCycleState.CLOSED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    getReplicas(containerOne.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(r -> containerStateManager.updateContainerReplica(
            containerOne.containerID(), r));

    getReplicas(containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(r -> containerStateManager.updateContainerReplica(
            containerTwo.containerID(), r));


    // SCM expects both containerOne and containerTwo to be in all the three
    // datanodes datanodeOne, datanodeTwo and datanodeThree

    // Now datanodeFour is sending container report which has containerOne.

    // containerOne becomes over replicated.

    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeFour.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeFour, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    Assertions.assertEquals(4, containerManager.getContainerReplicas(
        containerOne.containerID()).size());
  }


  @Test
  public void testClosingToClosed() throws NodeNotFoundException, IOException,
      TimeoutException {
    /*
     * The container is in CLOSING state and all the replicas are in
     * OPEN/CLOSING state.
     *
     * The datanode reports that one of the replica is now CLOSED.
     *
     * In this case SCM should mark the container as CLOSED.
     */

    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSING);
    final ContainerInfo containerTwo = getContainer(LifeCycleState.CLOSED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());

    final Set<ContainerReplica> containerOneReplicas = getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne);

    containerOneReplicas.addAll(getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.OPEN,
        datanodeTwo, datanodeThree));

    final Set<ContainerReplica> containerTwoReplicas = getReplicas(
        containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree);

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    containerOneReplicas.forEach(r ->
        containerStateManager.updateContainerReplica(
        containerTwo.containerID(), r));

    containerTwoReplicas.forEach(r ->
        containerStateManager.updateContainerReplica(
        containerTwo.containerID(), r));


    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    Assertions.assertEquals(LifeCycleState.CLOSED,
        containerManager.getContainer(containerOne.containerID()).getState());
  }

  @Test
  public void testClosingToQuasiClosed()
      throws NodeNotFoundException, IOException, TimeoutException {
    /*
     * The container is in CLOSING state and all the replicas are in
     * OPEN/CLOSING state.
     *
     * The datanode reports that the replica is now QUASI_CLOSED.
     *
     * In this case SCM should move the container to QUASI_CLOSED.
     */

    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSING);
    final ContainerInfo containerTwo = getContainer(LifeCycleState.CLOSED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());

    final Set<ContainerReplica> containerOneReplicas = getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeOne, datanodeTwo);
    containerOneReplicas.addAll(getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.OPEN,
        datanodeThree));
    final Set<ContainerReplica> containerTwoReplicas = getReplicas(
        containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree);

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    containerOneReplicas.forEach(r ->
        containerStateManager.updateContainerReplica(
        containerTwo.containerID(), r));

    containerTwoReplicas.forEach(r ->
        containerStateManager.updateContainerReplica(
        containerTwo.containerID(), r));


    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.QUASI_CLOSED,
        datanodeOne.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    Assertions.assertEquals(LifeCycleState.QUASI_CLOSED,
        containerManager.getContainer(containerOne.containerID()).getState());
  }

  @Test
  public void testQuasiClosedToClosed()
      throws NodeNotFoundException, IOException, TimeoutException {
    /*
     * The container is in QUASI_CLOSED state.
     *  - One of the replica is in QUASI_CLOSED state
     *  - The other two replica are in OPEN/CLOSING state
     *
     * The datanode reports the second replica is now CLOSED.
     *
     * In this case SCM should CLOSE the container.
     */

    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();

    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerInfo containerOne =
        getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerInfo containerTwo =
        getContainer(LifeCycleState.CLOSED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID(), containerTwo.containerID())
        .collect(Collectors.toSet());
    final Set<ContainerReplica> containerOneReplicas = getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.QUASI_CLOSED,
        10000L,
        datanodeOne);
    containerOneReplicas.addAll(getReplicas(
        containerOne.containerID(),
        ContainerReplicaProto.State.CLOSING,
        datanodeTwo, datanodeThree));
    final Set<ContainerReplica> containerTwoReplicas = getReplicas(
        containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree);

    nodeManager.setContainers(datanodeOne, containerIDSet);
    nodeManager.setContainers(datanodeTwo, containerIDSet);
    nodeManager.setContainers(datanodeThree, containerIDSet);

    containerStateManager.addContainer(containerOne.getProtobuf());
    containerStateManager.addContainer(containerTwo.getProtobuf());

    containerOneReplicas.forEach(r ->
        containerStateManager.updateContainerReplica(
        containerTwo.containerID(), r));

    containerTwoReplicas.forEach(r ->
        containerStateManager.updateContainerReplica(
        containerTwo.containerID(), r));


    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString());

    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    Assertions.assertEquals(LifeCycleState.CLOSED,
        containerManager.getContainer(containerOne.containerID()).getState());
  }

  @Test
  public void openContainerKeyAndBytesUsedUpdatedToMinimumOfAllReplicas()
      throws IOException, TimeoutException {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();

    Pipeline pipeline = pipelineManager.createPipeline(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerReplicaProto.State replicaState
        = ContainerReplicaProto.State.OPEN;
    final ContainerInfo containerOne =
        getContainer(LifeCycleState.OPEN, pipeline.getId());

    containerStateManager.addContainer(containerOne.getProtobuf());
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
            .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
            .getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 50L, 60L), publisher);

    // Single replica reported - ensure values are updated
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 50L, 60L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 50L, 60L), publisher);

    // All 3 DNs are reporting the same values. Counts should be as expected.
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Now each DN reports a different lesser value. Counts should be the min
    // reported.
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 1L, 10L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 2L, 11L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 3L, 12L), publisher);

    // All 3 DNs are reporting different values. The actual value should be the
    // minimum.
    assertEquals(1L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(10L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Have the lowest value report a higher value and ensure the new value
    // is the minimum
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 3L, 12L), publisher);

    assertEquals(2L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(11L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());
  }

  @Test
  public void notOpenContainerKeyAndBytesUsedUpdatedToMaximumOfAllReplicas()
      throws IOException, TimeoutException {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();

    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerReplicaProto.State replicaState
        = ContainerReplicaProto.State.CLOSED;
    final ContainerInfo containerOne = getContainer(LifeCycleState.CLOSED);

    containerStateManager.addContainer(containerOne.getProtobuf());
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerOne.getUsedBytes());
    assertEquals(0L, containerOne.getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 50L, 60L), publisher);

    // Single replica reported - ensure values are updated
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 50L, 60L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 50L, 60L), publisher);

    // All 3 DNs are reporting the same values. Counts should be as expected.
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Now each DN reports a different lesser value. Counts should be the max
    // reported.
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 1L, 10L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 2L, 11L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 3L, 12L), publisher);

    // All 3 DNs are reporting different values. The actual value should be the
    // maximum.
    assertEquals(3L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(12L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Have the highest value report a lower value and ensure the new value
    // is the new maximumu
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 1L, 10L), publisher);

    assertEquals(2L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(11L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());
  }

  @Test
  public void openECContainerKeyAndBytesUsedUpdatedToMinimumOfAllReplicas()
      throws IOException, TimeoutException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    Pipeline pipeline = pipelineManager.createPipeline(repConfig);
    Map<Integer, DatanodeDetails> dns = new HashMap<>();
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      dns.put(i, nodeIterator.next());
    }
    final ContainerReplicaProto.State replicaState
        = ContainerReplicaProto.State.OPEN;
    final ContainerInfo containerOne =
        getECContainer(LifeCycleState.OPEN, pipeline.getId(), repConfig);

    containerStateManager.addContainer(containerOne.getProtobuf());
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from data index 2 - should not update stats
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(2), 50L, 60L, 2), publisher);
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from replica 1, it should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(1), 50L, 60L, 1), publisher);
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Parity 1 report a greater value, but as the container is own the stats
    // should be the min value.
    // Report from replica 1, it should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(4), 80L, 90L, 4), publisher);
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Parity 2 reports a lesser value, so the stored values should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(5), 40, 30, 5), publisher);
    assertEquals(40L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(30L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from data index 3 - should not update stats even though it has
    // lesser values
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(2), 10L, 10L, 2), publisher);
    assertEquals(40L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(30L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());
  }

  @Test
  public void closedECContainerKeyAndBytesUsedUpdatedToMinimumOfAllReplicas()
      throws IOException, TimeoutException {
    final ECReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    Pipeline pipeline = pipelineManager.createPipeline(repConfig);
    Map<Integer, DatanodeDetails> dns = new HashMap<>();
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      dns.put(i, nodeIterator.next());
    }
    final ContainerReplicaProto.State replicaState
        = ContainerReplicaProto.State.OPEN;
    final ContainerInfo containerOne =
        getECContainer(LifeCycleState.CLOSED, pipeline.getId(), repConfig);

    containerStateManager.addContainer(containerOne.getProtobuf());
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from data index 2 - should not update stats
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(2), 50L, 60L, 2), publisher);
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(0L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from replica 1, it should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(1), 50L, 60L, 1), publisher);
    assertEquals(50L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(60L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Parity 1 report a greater value, as the container is closed the stats
    // should be the max value.
    // Report from replica 1, it should update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(4), 80L, 90L, 4), publisher);
    assertEquals(80L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(90L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Parity 2 reports a lesser value, so the stored values should not update
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(5), 40, 30, 5), publisher);
    assertEquals(80L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(90L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());

    // Report from data index 3 - should not update stats even though it has
    // greater values
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        dns.get(2), 110L, 120L, 2), publisher);
    assertEquals(80L, containerManager.getContainer(containerOne.containerID())
        .getUsedBytes());
    assertEquals(90L, containerManager.getContainer(containerOne.containerID())
        .getNumberOfKeys());
  }

  @Test
  public void testStaleReplicaOfDeletedContainer() throws NodeNotFoundException,
      IOException, TimeoutException {

    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);

    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();
    final DatanodeDetails datanodeOne = nodeIterator.next();
    final ContainerInfo containerOne = getContainer(LifeCycleState.DELETED);

    final Set<ContainerID> containerIDSet = Stream.of(
        containerOne.containerID())
        .collect(Collectors.toSet());

    nodeManager.setContainers(datanodeOne, containerIDSet);
    containerStateManager.addContainer(containerOne.getProtobuf());

    // Expects the replica will be deleted.
    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString(), 0);
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    Mockito.verify(publisher, Mockito.times(1))
        .fireEvent(Mockito.any(), Mockito.any(CommandForDatanode.class));

    Assertions.assertEquals(0, containerManager.getContainerReplicas(
        containerOne.containerID()).size());
  }

  private ContainerReportFromDatanode getContainerReportFromDatanode(
      ContainerID containerId, ContainerReplicaProto.State state,
      DatanodeDetails dn, long bytesUsed, long keyCount) {
    return getContainerReportFromDatanode(containerId, state, dn, bytesUsed,
        keyCount, 0);
  }

  private ContainerReportFromDatanode getContainerReportFromDatanode(
      ContainerID containerId, ContainerReplicaProto.State state,
      DatanodeDetails dn, long bytesUsed, long keyCount, int replicaIndex) {
    ContainerReportsProto containerReport = getContainerReportsProto(
        containerId, state, dn.getUuidString(), bytesUsed, keyCount,
        replicaIndex);

    return new ContainerReportFromDatanode(dn, containerReport);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId) {
    return getContainerReportsProto(containerId, state, originNodeId,
        2000000000L, 100000000L, 0);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId, int replicaIndex) {
    return getContainerReportsProto(containerId, state, originNodeId,
        2000000000L, 100000000L, replicaIndex);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId, final long usedBytes, final long keyCount,
      final int replicaIndex) {
    final ContainerReportsProto.Builder crBuilder =
        ContainerReportsProto.newBuilder();
    final ContainerReplicaProto replicaProto =
        ContainerReplicaProto.newBuilder()
            .setContainerID(containerId.getId())
            .setState(state)
            .setOriginNodeId(originNodeId)
            .setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
            .setSize(5368709120L)
            .setUsed(usedBytes)
            .setKeyCount(keyCount)
            .setReadCount(100000000L)
            .setWriteCount(100000000L)
            .setReadBytes(2000000000L)
            .setWriteBytes(2000000000L)
            .setBlockCommitSequenceId(10000L)
            .setDeleteTransactionId(0)
            .setReplicaIndex(replicaIndex)
            .build();
    return crBuilder.addReports(replicaProto).build();
  }

}