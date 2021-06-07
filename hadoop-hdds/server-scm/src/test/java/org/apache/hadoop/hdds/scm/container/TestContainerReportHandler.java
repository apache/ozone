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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server
    .SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;
import static org.apache.hadoop.hdds.scm.TestUtils.getReplicas;
import static org.apache.hadoop.hdds.scm.TestUtils.getContainer;

/**
 * Test the behaviour of the ContainerReportHandler.
 */
public class TestContainerReportHandler {

  private NodeManager nodeManager;
  private ContainerManagerV2 containerManager;
  private ContainerStateManager containerStateManager;
  private EventPublisher publisher;

  @Before
  public void setup() throws IOException, InvalidStateTransitionException {
    final ConfigurationSource conf = new OzoneConfiguration();
    this.nodeManager = new MockNodeManager(true, 10);
    this.containerManager = Mockito.mock(ContainerManagerV2.class);
    this.containerStateManager = new ContainerStateManager(conf);
    this.publisher = Mockito.mock(EventPublisher.class);


    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer((ContainerID)invocation.getArguments()[0]));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas((ContainerID)invocation.getArguments()[0]));

    Mockito.doAnswer(invocation -> {
      containerStateManager
          .updateContainerState((ContainerID)invocation.getArguments()[0],
              (HddsProtos.LifeCycleEvent)invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerState(
        Mockito.any(ContainerID.class),
        Mockito.any(HddsProtos.LifeCycleEvent.class));

    Mockito.doAnswer(invocation -> {
      containerStateManager.updateContainerReplica(
          (ContainerID) invocation.getArguments()[0],
          (ContainerReplica) invocation.getArguments()[1]);
      return null;
    }).when(containerManager).updateContainerReplica(
        Mockito.any(ContainerID.class), Mockito.any(ContainerReplica.class));

    Mockito.doAnswer(invocation -> {
      containerStateManager.removeContainerReplica(
          (ContainerID) invocation.getArguments()[0],
          (ContainerReplica) invocation.getArguments()[1]);
      return null;
    }).when(containerManager).removeContainerReplica(
        Mockito.any(ContainerID.class), Mockito.any(ContainerReplica.class));

  }

  @After
  public void tearDown() throws IOException {
    containerStateManager.close();
  }

  @Test
  public void testUnderReplicatedContainer()
      throws NodeNotFoundException, ContainerNotFoundException, SCMException {

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

    containerStateManager.loadContainer(containerOne);
    containerStateManager.loadContainer(containerTwo);

    getReplicas(containerOne.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(r -> {
          try {
            containerStateManager.updateContainerReplica(
                containerOne.containerID(), r);
          } catch (ContainerNotFoundException ignored) {

          }
        });

    getReplicas(containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(r -> {
          try {
            containerStateManager.updateContainerReplica(
                containerTwo.containerID(), r);
          } catch (ContainerNotFoundException ignored) {

          }
        });


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
    Assert.assertEquals(2, containerManager.getContainerReplicas(
        containerOne.containerID()).size());

  }

  @Test
  public void testOverReplicatedContainer() throws NodeNotFoundException,
      SCMException, ContainerNotFoundException {

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

    containerStateManager.loadContainer(containerOne);
    containerStateManager.loadContainer(containerTwo);

    getReplicas(containerOne.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(r -> {
          try {
            containerStateManager.updateContainerReplica(
                containerOne.containerID(), r);
          } catch (ContainerNotFoundException ignored) {

          }
        });

    getReplicas(containerTwo.containerID(),
        ContainerReplicaProto.State.CLOSED,
        datanodeOne, datanodeTwo, datanodeThree)
        .forEach(r -> {
          try {
            containerStateManager.updateContainerReplica(
                containerTwo.containerID(), r);
          } catch (ContainerNotFoundException ignored) {

          }
        });


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

    Assert.assertEquals(4, containerManager.getContainerReplicas(
        containerOne.containerID()).size());
  }


  @Test
  public void testClosingToClosed() throws NodeNotFoundException, IOException {
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

    containerStateManager.loadContainer(containerOne);
    containerStateManager.loadContainer(containerTwo);

    containerOneReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            containerTwo.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });

    containerTwoReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            containerTwo.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });


    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    Assert.assertEquals(LifeCycleState.CLOSED, containerOne.getState());
  }

  @Test
  public void testClosingToQuasiClosed()
      throws NodeNotFoundException, IOException {
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

    containerStateManager.loadContainer(containerOne);
    containerStateManager.loadContainer(containerTwo);

    containerOneReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            containerTwo.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });

    containerTwoReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            containerTwo.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });


    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.QUASI_CLOSED,
        datanodeOne.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    Assert.assertEquals(LifeCycleState.QUASI_CLOSED, containerOne.getState());
  }

  @Test
  public void testQuasiClosedToClosed()
      throws NodeNotFoundException, IOException {
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

    containerStateManager.loadContainer(containerOne);
    containerStateManager.loadContainer(containerTwo);

    containerOneReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            containerTwo.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });

    containerTwoReplicas.forEach(r -> {
      try {
        containerStateManager.updateContainerReplica(
            containerTwo.containerID(), r);
      } catch (ContainerNotFoundException ignored) {

      }
    });


    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString());

    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    Assert.assertEquals(LifeCycleState.CLOSED, containerOne.getState());
  }

  @Test
  public void openContainerKeyAndBytesUsedUpdatedToMinimumOfAllReplicas()
      throws SCMException {
    final ContainerReportHandler reportHandler = new ContainerReportHandler(
        nodeManager, containerManager);
    final Iterator<DatanodeDetails> nodeIterator = nodeManager.getNodes(
        NodeStatus.inServiceHealthy()).iterator();

    final DatanodeDetails datanodeOne = nodeIterator.next();
    final DatanodeDetails datanodeTwo = nodeIterator.next();
    final DatanodeDetails datanodeThree = nodeIterator.next();

    final ContainerReplicaProto.State replicaState
        = ContainerReplicaProto.State.OPEN;
    final ContainerInfo containerOne = getContainer(LifeCycleState.OPEN);

    containerStateManager.loadContainer(containerOne);
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerOne.getUsedBytes());
    assertEquals(0L, containerOne.getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 50L, 60L), publisher);

    // Single replica reported - ensure values are updated
    assertEquals(50L, containerOne.getUsedBytes());
    assertEquals(60L, containerOne.getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 50L, 60L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 50L, 60L), publisher);

    // All 3 DNs are reporting the same values. Counts should be as expected.
    assertEquals(50L, containerOne.getUsedBytes());
    assertEquals(60L, containerOne.getNumberOfKeys());

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
    assertEquals(1L, containerOne.getUsedBytes());
    assertEquals(10L, containerOne.getNumberOfKeys());

    // Have the lowest value report a higher value and ensure the new value
    // is the minimum
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 3L, 12L), publisher);

    assertEquals(2L, containerOne.getUsedBytes());
    assertEquals(11L, containerOne.getNumberOfKeys());
  }

  @Test
  public void notOpenContainerKeyAndBytesUsedUpdatedToMaximumOfAllReplicas()
      throws SCMException {
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

    containerStateManager.loadContainer(containerOne);
    // Container loaded, no replicas reported from DNs. Expect zeros for
    // usage values.
    assertEquals(0L, containerOne.getUsedBytes());
    assertEquals(0L, containerOne.getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeOne, 50L, 60L), publisher);

    // Single replica reported - ensure values are updated
    assertEquals(50L, containerOne.getUsedBytes());
    assertEquals(60L, containerOne.getNumberOfKeys());

    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeTwo, 50L, 60L), publisher);
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 50L, 60L), publisher);

    // All 3 DNs are reporting the same values. Counts should be as expected.
    assertEquals(50L, containerOne.getUsedBytes());
    assertEquals(60L, containerOne.getNumberOfKeys());

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
    assertEquals(3L, containerOne.getUsedBytes());
    assertEquals(12L, containerOne.getNumberOfKeys());

    // Have the highest value report a lower value and ensure the new value
    // is the new maximumu
    reportHandler.onMessage(getContainerReportFromDatanode(
        containerOne.containerID(), replicaState,
        datanodeThree, 1L, 10L), publisher);

    assertEquals(2L, containerOne.getUsedBytes());
    assertEquals(11L, containerOne.getNumberOfKeys());
  }

  @Test
  public void testStaleReplicaOfDeletedContainer() throws NodeNotFoundException,
      SCMException, ContainerNotFoundException {

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
    containerStateManager.loadContainer(containerOne);

    // Expects the replica will be deleted.
    final ContainerReportsProto containerReport = getContainerReportsProto(
        containerOne.containerID(), ContainerReplicaProto.State.CLOSED,
        datanodeOne.getUuidString());
    final ContainerReportFromDatanode containerReportFromDatanode =
        new ContainerReportFromDatanode(datanodeOne, containerReport);
    reportHandler.onMessage(containerReportFromDatanode, publisher);

    Mockito.verify(publisher, Mockito.times(1));

    Assert.assertEquals(0, containerManager.getContainerReplicas(
        containerOne.containerID()).size());
  }

  private ContainerReportFromDatanode getContainerReportFromDatanode(
      ContainerID containerId, ContainerReplicaProto.State state,
      DatanodeDetails dn, long bytesUsed, long keyCount) {
    ContainerReportsProto containerReport = getContainerReportsProto(
        containerId, state, dn.getUuidString(), bytesUsed, keyCount);

    return new ContainerReportFromDatanode(dn, containerReport);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId) {
    return getContainerReportsProto(containerId, state, originNodeId,
        2000000000L, 100000000L);
  }

  protected static ContainerReportsProto getContainerReportsProto(
      final ContainerID containerId, final ContainerReplicaProto.State state,
      final String originNodeId, final long usedBytes, final long keyCount) {
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
            .build();
    return crBuilder.addReports(replicaProto).build();
  }

}