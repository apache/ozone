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

package org.apache.hadoop.hdds.scm.container;

import com.google.common.primitives.Longs;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.container.ReplicationManager
    .ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.lock.LockManager;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.createDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.TestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.TestUtils.getReplicas;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;

/**
 * Test cases to verify the functionality of ReplicationManager.
 */
public class TestReplicationManager {

  private ReplicationManager replicationManager;
  private ContainerStateManager containerStateManager;
  private PlacementPolicy containerPlacementPolicy;
  private EventQueue eventQueue;
  private DatanodeCommandHandler datanodeCommandHandler;
  private SimpleMockNodeManager nodeManager;
  private ContainerManager containerManager;
  private ConfigurationSource conf;
  private SCMNodeManager scmNodeManager;

  @Before
  public void setup()
      throws IOException, InterruptedException, NodeNotFoundException {
    conf = new OzoneConfiguration();
    containerManager = Mockito.mock(ContainerManager.class);
    nodeManager = new SimpleMockNodeManager();
    eventQueue = new EventQueue();
    containerStateManager = new ContainerStateManager(conf);

    datanodeCommandHandler = new DatanodeCommandHandler();
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, datanodeCommandHandler);

    Mockito.when(containerManager.getContainerIDs())
        .thenAnswer(invocation -> containerStateManager.getAllContainerIDs());

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer((ContainerID)invocation.getArguments()[0]));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas((ContainerID)invocation.getArguments()[0]));

    containerPlacementPolicy = Mockito.mock(PlacementPolicy.class);

    Mockito.when(containerPlacementPolicy.chooseDatanodes(
        Mockito.anyListOf(DatanodeDetails.class),
        Mockito.anyListOf(DatanodeDetails.class),
        Mockito.anyInt(), Mockito.anyLong()))
        .thenAnswer(invocation -> {
          int count = (int) invocation.getArguments()[2];
          return IntStream.range(0, count)
              .mapToObj(i -> randomDatanodeDetails())
              .collect(Collectors.toList());
        });

    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.anyListOf(DatanodeDetails.class),
        Mockito.anyInt()
        )).thenAnswer(invocation ->  {
          return new ContainerPlacementStatusDefault(2, 2, 3);
        });

    scmNodeManager = Mockito.mock(SCMNodeManager.class);
    Mockito.when(scmNodeManager.getNodeStatus(
        Mockito.any(DatanodeDetails.class)))
        .thenReturn(NodeStatus.inServiceHealthy());

    replicationManager = new ReplicationManager(
        new ReplicationManagerConfiguration(),
        containerManager,
        containerPlacementPolicy,
        eventQueue,
        new LockManager<>(conf),
        nodeManager);
    replicationManager.start();
    Thread.sleep(100L);
  }

  private void createReplicationManager(ReplicationManagerConfiguration rmConf)
      throws InterruptedException {
    replicationManager = new ReplicationManager(
        rmConf,
        containerManager,
        containerPlacementPolicy,
        eventQueue,
        new LockManager<ContainerID>(conf),
        nodeManager);

    replicationManager.start();
    Thread.sleep(100L);
  }


  /**
   * Checks if restarting of replication manager works.
   */
  @Test
  public void testReplicationManagerRestart() throws InterruptedException {
    Assert.assertTrue(replicationManager.isRunning());
    replicationManager.stop();
    // Stop is a non-blocking call, it might take sometime for the
    // ReplicationManager to shutdown
    Thread.sleep(500);
    Assert.assertFalse(replicationManager.isRunning());
    replicationManager.start();
    Assert.assertTrue(replicationManager.isRunning());
  }

  /**
   * Open containers are not handled by ReplicationManager.
   * This test-case makes sure that ReplicationManages doesn't take
   * any action on OPEN containers.
   */
  @Test
  public void testOpenContainer() throws SCMException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.OPEN);
    containerStateManager.loadContainer(container);
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());

  }

  /**
   * If the container is in CLOSING state we resend close container command
   * to all the datanodes.
   */
  @Test
  public void testClosingContainer() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final ContainerID id = container.containerID();

    containerStateManager.loadContainer(container);

    // Two replicas in CLOSING state
    final Set<ContainerReplica> replicas = getReplicas(id, State.CLOSING,
        randomDatanodeDetails(),
        randomDatanodeDetails());

    // One replica in OPEN state
    final DatanodeDetails datanode = randomDatanodeDetails();
    replicas.addAll(getReplicas(id, State.OPEN, datanode));

    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentCloseCommandCount + 3, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));

    // Update the OPEN to CLOSING
    for (ContainerReplica replica : getReplicas(id, State.CLOSING, datanode)) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentCloseCommandCount + 6, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));
  }


  /**
   * The container is QUASI_CLOSED but two of the replica is still in
   * open state. ReplicationManager should resend close command to those
   * datanodes.
   */
  @Test
  public void testQuasiClosedContainerWithTwoOpenReplica() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.OPEN, 1000L, originNodeId, randomDatanodeDetails());
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final ContainerReplica replicaThree = getReplicas(
        id, State.OPEN, 1000L, datanodeDetails.getUuid(), datanodeDetails);

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);
    // Two of the replicas are in OPEN state
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentCloseCommandCount + 2, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.closeContainerCommand,
        replicaTwo.getDatanodeDetails()));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.closeContainerCommand,
        replicaThree.getDatanodeDetails()));
  }

  /**
   * When the container is in QUASI_CLOSED state and all the replicas are
   * also in QUASI_CLOSED state and doesn't have a quorum to force close
   * the container, ReplicationManager will not do anything.
   */
  @Test
  public void testHealthyQuasiClosedContainer() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);

    // All the QUASI_CLOSED replicas have same originNodeId, so the
    // container will not be closed. ReplicationManager should take no action.
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());
  }

  /**
   * When a container is QUASI_CLOSED and we don't have quorum to force close
   * the container, the container should have all the replicas in QUASI_CLOSED
   * state, else ReplicationManager will take action.
   *
   * In this test case we make one of the replica unhealthy, replication manager
   * will send delete container command to the datanode which has the unhealthy
   * replica.
   */
  @Test
  public void testQuasiClosedContainerWithUnhealthyReplica()
      throws SCMException, ContainerNotFoundException, InterruptedException,
      ContainerReplicaNotFoundException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);
    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    // All the QUASI_CLOSED replicas have same originNodeId, so the
    // container will not be closed. ReplicationManager should take no action.
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());

    // Make the first replica unhealthy
    final ContainerReplica unhealthyReplica = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId,
        replicaOne.getDatanodeDetails());
    containerStateManager.updateContainerReplica(id, unhealthyReplica);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaOne.getDatanodeDetails()));

    // Now we will delete the unhealthy replica from in-memory.
    containerStateManager.removeContainerReplica(id, replicaOne);

    // The container is under replicated as unhealthy replica is removed
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);

    // We should get replicate command
    Assert.assertEquals(currentReplicateCommandCount + 1,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
  }

  /**
   * When a QUASI_CLOSED container is over replicated, ReplicationManager
   * deletes the excess replicas.
   */
  @Test
  public void testOverReplicatedQuasiClosedContainer() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);
    containerStateManager.updateContainerReplica(id, replicaFour);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
  }

  /**
   * When a QUASI_CLOSED container is over replicated, ReplicationManager
   * deletes the excess replicas. While choosing the replica for deletion
   * ReplicationManager should prioritize unhealthy replica over QUASI_CLOSED
   * replica.
   */
  @Test
  public void testOverReplicatedQuasiClosedContainerWithUnhealthyReplica()
      throws SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);
    containerStateManager.updateContainerReplica(id, replicaFour);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaOne.getDatanodeDetails()));
  }

  /**
   * ReplicationManager should replicate an QUASI_CLOSED replica if it is
   * under replicated.
   */
  @Test
  public void testUnderReplicatedQuasiClosedContainer() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);

    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentReplicateCommandCount + 1,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
  }

  /**
   * When a QUASI_CLOSED container is under replicated, ReplicationManager
   * should re-replicate it. If there are any unhealthy replica, it has to
   * be deleted.
   *
   * In this test case, the container is QUASI_CLOSED and is under replicated
   * and also has an unhealthy replica.
   *
   * In the first iteration of ReplicationManager, it should re-replicate
   * the container so that it has enough replicas.
   *
   * In the second iteration, ReplicationManager should delete the unhealthy
   * replica.
   *
   * In the third iteration, ReplicationManager will re-replicate as the
   * container has again become under replicated after the unhealthy
   * replica has been deleted.
   *
   */
  @Test
  public void testUnderReplicatedQuasiClosedContainerWithUnhealthyReplica()
      throws SCMException, ContainerNotFoundException, InterruptedException,
      ContainerReplicaNotFoundException, TimeoutException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);

    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);
    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    GenericTestUtils.waitFor(
        () -> (currentReplicateCommandCount + 1) == datanodeCommandHandler
            .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand),
        50, 5000);

    Optional<CommandForDatanode> replicateCommand = datanodeCommandHandler
        .getReceivedCommands().stream()
        .filter(c -> c.getCommand().getType()
            .equals(SCMCommandProto.Type.replicateContainerCommand))
        .findFirst();

    Assert.assertTrue(replicateCommand.isPresent());

    DatanodeDetails newNode = createDatanodeDetails(
        replicateCommand.get().getDatanodeId());
    ContainerReplica newReplica = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, newNode);
    containerStateManager.updateContainerReplica(id, newReplica);

    /*
     * We have report the replica to SCM, in the next ReplicationManager
     * iteration it should delete the unhealthy replica.
     */

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    // ReplicaTwo should be deleted, that is the unhealthy one
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaTwo.getDatanodeDetails()));

    containerStateManager.removeContainerReplica(id, replicaTwo);

    /*
     * We have now removed unhealthy replica, next iteration of
     * ReplicationManager should re-replicate the container as it
     * is under replicated now
     */

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentReplicateCommandCount + 2,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
  }


  /**
   * When a container is QUASI_CLOSED and it has >50% of its replica
   * in QUASI_CLOSED state with unique origin node id,
   * ReplicationManager should force close the replica(s) with
   * highest BCSID.
   */
  @Test
  public void testQuasiClosedToClosed() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final Set<ContainerReplica> replicas = getReplicas(id, State.QUASI_CLOSED,
        randomDatanodeDetails(),
        randomDatanodeDetails(),
        randomDatanodeDetails());
    containerStateManager.loadContainer(container);
    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);

    // All the replicas have same BCSID, so all of them will be closed.
    Assert.assertEquals(currentCloseCommandCount + 3, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));

  }


  /**
   * ReplicationManager should not take any action if the container is
   * CLOSED and healthy.
   */
  @Test
  public void testHealthyClosedContainer()
      throws SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final Set<ContainerReplica> replicas = getReplicas(id, CLOSED,
        randomDatanodeDetails(),
        randomDatanodeDetails(),
        randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());
  }

  /**
   * ReplicationManager should close the unhealthy OPEN container.
   */
  @Test
  public void testUnhealthyOpenContainer()
      throws SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.OPEN);
    final ContainerID id = container.containerID();
    final Set<ContainerReplica> replicas = getReplicas(id, State.OPEN,
        randomDatanodeDetails(),
        randomDatanodeDetails());
    replicas.addAll(getReplicas(id, State.UNHEALTHY, randomDatanodeDetails()));

    containerStateManager.loadContainer(container);
    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    final CloseContainerEventHandler closeContainerHandler =
        Mockito.mock(CloseContainerEventHandler.class);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);

    replicationManager.processContainersNow();

    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Mockito.verify(closeContainerHandler, Mockito.times(1))
        .onMessage(id, eventQueue);
  }

  @Test
  public void testGeneratedConfig() {
    ReplicationManagerConfiguration rmc =
        OzoneConfiguration.newInstanceOf(ReplicationManagerConfiguration.class);

    //default is not included in ozone-site.xml but generated from annotation
    //to the ozone-site-generated.xml which should be loaded by the
    // OzoneConfiguration.
    Assert.assertEquals(1800000, rmc.getEventTimeout());

  }

  @Test
  public void additionalReplicaScheduledWhenMisReplicated()
      throws SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);

    // Ensure a mis-replicated status is returned for any containers in this
    // test where there are 3 replicas. When there are 2 or 4 replicas
    // the status returned will be healthy.
    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.argThat(new ListOfNElements(3)),
        Mockito.anyInt()
    )).thenAnswer(invocation ->  {
      return new ContainerPlacementStatusDefault(1, 2, 3);
    });

    int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    // At this stage, due to the mocked calls to validteContainerPlacement
    // the mis-replicated racks will not have improved, so expect to see nothing
    // scheduled.
    Assert.assertEquals(currentReplicateCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand));

    // Now make it so that all containers seem mis-replicated no matter how
    // many replicas. This will test replicas are not scheduled if the new
    // replica does not fix the mis-replication.
    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.anyList(),
        Mockito.anyInt()
    )).thenAnswer(invocation ->  {
      return new ContainerPlacementStatusDefault(1, 2, 3);
    });

    currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    // At this stage, due to the mocked calls to validteContainerPlacement
    // the mis-replicated racks will not have improved, so expect to see nothing
    // scheduled.
    Assert.assertEquals(currentReplicateCommandCount, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand));
  }

  @Test
  public void overReplicatedButRemovingMakesMisReplicated()
      throws SCMException, ContainerNotFoundException, InterruptedException {
    // In this test, the excess replica should not be removed.
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFive = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);
    containerStateManager.updateContainerReplica(id, replicaFour);
    containerStateManager.updateContainerReplica(id, replicaFive);

    // Ensure a mis-replicated status is returned for any containers in this
    // test where there are exactly 3 replicas checked.
    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.argThat(new ListOfNElements(3)),
        Mockito.anyInt()
    )).thenAnswer(
        invocation -> new ContainerPlacementStatusDefault(1, 2, 3));

    int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    // The unhealthy replica should be removed, but not the other replica
    // as each time we test with 3 replicas, Mockitor ensures it returns
    // mis-replicated
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));

    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaFive.getDatanodeDetails()));
  }

  @Test
  public void testOverReplicatedAndPolicySatisfied() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);
    containerStateManager.updateContainerReplica(id, replicaFour);

    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.argThat(new ListOfNElements(3)),
        Mockito.anyInt()
    )).thenAnswer(
        invocation -> new ContainerPlacementStatusDefault(2, 2, 3));

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
  }

  @Test
  public void testOverReplicatedAndPolicyUnSatisfiedAndDeleted() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFive = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);
    containerStateManager.updateContainerReplica(id, replicaFour);
    containerStateManager.updateContainerReplica(id, replicaFive);

    Mockito.when(containerPlacementPolicy.validateContainerPlacement(
        Mockito.argThat(new FunctionMatcher(list ->
            list != null && ((List) list).size() <= 4)),
        Mockito.anyInt()
    )).thenAnswer(
        invocation -> new ContainerPlacementStatusDefault(1, 2, 3));

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 2, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
  }

  /**
   * ReplicationManager should replicate an additional replica if there are
   * decommissioned replicas.
   */
  @Test
  public void testUnderReplicatedDueToDecommission() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    assertReplicaScheduled(2);
  }

  /**
   * ReplicationManager should replicate an additional replica when all copies
   * are decommissioning.
   */
  @Test
  public void testUnderReplicatedDueToAllDecommission() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    assertReplicaScheduled(3);
  }

  /**
   * ReplicationManager should not take any action when the container is
   * correctly replicated with decommissioned replicas still present.
   */
  @Test
  public void testCorrectlyReplicatedWithDecommission() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONING, HEALTHY), CLOSED);
    assertReplicaScheduled(0);
  }

  /**
   * ReplicationManager should replicate an additional replica when min rep
   * is not met for maintenance.
   */
  @Test
  public void testUnderReplicatedDueToMaintenance() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(1);
  }

  /**
   * ReplicationManager should not replicate an additional replica when if
   * min replica for maintenance is 1 and another replica is available.
   */
  @Test
  public void testNotUnderReplicatedDueToMaintenanceMinRepOne() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    replicationManager.stop();
    ReplicationManagerConfiguration newConf =
        new ReplicationManagerConfiguration();
    newConf.setMaintenanceReplicaMinimum(1);
    createReplicationManager(newConf);
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(0);
  }

  /**
   * ReplicationManager should replicate an additional replica when all copies
   * are going off line and min rep is 1.
   */
  @Test
  public void testUnderReplicatedDueToMaintenanceMinRepOne() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    replicationManager.stop();
    ReplicationManagerConfiguration newConf =
        new ReplicationManagerConfiguration();
    newConf.setMaintenanceReplicaMinimum(1);
    createReplicationManager(newConf);
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(1);
  }

  /**
   * ReplicationManager should replicate additional replica when all copies
   * are going into maintenance.
   */
  @Test
  public void testUnderReplicatedDueToAllMaintenance() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(2);
  }

  /**
   * ReplicationManager should not replicate additional replica sufficient
   * replica are available.
   */
  @Test
  public void testCorrectlyReplicatedWithMaintenance() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_SERVICE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(0);
  }

  /**
   * ReplicationManager should replicate additional replica when all copies
   * are decommissioning or maintenance.
   */
  @Test
  public void testUnderReplicatedWithDecommissionAndMaintenance() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    assertReplicaScheduled(2);
  }

  /**
   * When a CLOSED container is over replicated, ReplicationManager
   * deletes the excess replicas. While choosing the replica for deletion
   * ReplicationManager should not attempt to remove a DECOMMISSION or
   * MAINTENANCE replica.
   */
  @Test
  public void testOverReplicatedClosedContainerWithDecomAndMaint()
      throws SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, HEALTHY), CLOSED);
    addReplica(container, new NodeStatus(IN_MAINTENANCE, HEALTHY), CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);
    addReplica(container, NodeStatus.inServiceHealthy(), CLOSED);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 2, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    // Get the DECOM and Maint replica and ensure none of them are scheduled
    // for removal
    Set<ContainerReplica> decom =
        containerStateManager.getContainerReplicas(container.containerID())
        .stream()
        .filter(r -> r.getDatanodeDetails().getPersistedOpState() != IN_SERVICE)
        .collect(Collectors.toSet());
    for (ContainerReplica r : decom) {
      Assert.assertFalse(datanodeCommandHandler.received(
          SCMCommandProto.Type.deleteContainerCommand,
          r.getDatanodeDetails()));
    }
  }

  /**
   * Replication Manager should not attempt to replicate from an unhealthy
   * (stale or dead) node. To test this, setup a scenario where a replia needs
   * to be created, but mark all nodes stale. That way, no new replica will be
   * scheduled.
   */
  @Test
  public void testUnderReplicatedNotHealthySource()
      throws SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = createContainer(LifeCycleState.CLOSED);
    addReplica(container, NodeStatus.inServiceStale(), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, STALE), CLOSED);
    addReplica(container, new NodeStatus(DECOMMISSIONED, STALE), CLOSED);
    // There should be replica scheduled, but as all nodes are stale, nothing
    // gets scheduled.
    assertReplicaScheduled(0);
  }

  private ContainerInfo createContainer(LifeCycleState containerState)
      throws SCMException {
    final ContainerInfo container = getContainer(containerState);
    final ContainerID id = container.containerID();
    containerStateManager.loadContainer(container);
    return container;
  }

  private ContainerReplica addReplica(ContainerInfo container,
      NodeStatus nodeStatus, State replicaState)
      throws ContainerNotFoundException {
    DatanodeDetails dn = randomDatanodeDetails();
    dn.setPersistedOpState(nodeStatus.getOperationalState());
    dn.setPersistedOpStateExpiryEpochSec(
        nodeStatus.getOpStateExpiryEpochSeconds());
    nodeManager.register(dn, nodeStatus);
    // Using the same originID for all replica in the container set. If each
    // replica has a unique originID, it causes problems in ReplicationManager
    // when processing over-replicated containers.
    final UUID originNodeId =
        UUID.nameUUIDFromBytes(Longs.toByteArray(container.getContainerID()));
    final ContainerReplica replica = getReplicas(
        container.containerID(), CLOSED, 1000L, originNodeId, dn);
    containerStateManager
        .updateContainerReplica(container.containerID(), replica);
    return replica;
  }

  private void assertReplicaScheduled(int delta) throws InterruptedException {
    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentReplicateCommandCount + delta,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
  }

  @After
  public void teardown() throws IOException {
    containerStateManager.close();
    replicationManager.stop();
  }

  private class DatanodeCommandHandler implements
      EventHandler<CommandForDatanode> {

    private AtomicInteger invocation = new AtomicInteger(0);
    private Map<SCMCommandProto.Type, AtomicInteger> commandInvocation =
        new HashMap<>();
    private List<CommandForDatanode> commands = new ArrayList<>();

    @Override
    public void onMessage(final CommandForDatanode command,
                          final EventPublisher publisher) {
      final SCMCommandProto.Type type = command.getCommand().getType();
      commandInvocation.computeIfAbsent(type, k -> new AtomicInteger(0));
      commandInvocation.get(type).incrementAndGet();
      invocation.incrementAndGet();
      commands.add(command);
    }

    private int getInvocation() {
      return invocation.get();
    }

    private int getInvocationCount(SCMCommandProto.Type type) {
      return commandInvocation.containsKey(type) ?
          commandInvocation.get(type).get() : 0;
    }

    private List<CommandForDatanode> getReceivedCommands() {
      return commands;
    }

    /**
     * Returns true if the command handler has received the given
     * command type for the provided datanode.
     *
     * @param type Command Type
     * @param datanode DatanodeDetails
     * @return True if command was received, false otherwise
     */
    private boolean received(final SCMCommandProto.Type type,
                             final DatanodeDetails datanode) {
      return commands.stream().anyMatch(dc ->
          dc.getCommand().getType().equals(type) &&
              dc.getDatanodeId().equals(datanode.getUuid()));
    }
  }

  class ListOfNElements extends ArgumentMatcher<List> {

    private int expected;

    ListOfNElements(int expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(Object argument) {
      return ((List)argument).size() == expected;
    }
  }

  class FunctionMatcher extends ArgumentMatcher<List> {

    private Function<Object, Boolean> function;

    FunctionMatcher(Function<Object, Boolean> function) {
      this.function = function;
    }

    @Override
    public boolean matches(Object argument) {
      return function.apply(argument);
    }
  }
}