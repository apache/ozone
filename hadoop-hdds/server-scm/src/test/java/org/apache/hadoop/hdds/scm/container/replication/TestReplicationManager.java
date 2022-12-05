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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ozone.test.TestClock;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;

/**
 * Tests for the ReplicationManager.
 */
public class TestReplicationManager {

  private OzoneConfiguration configuration;
  private ReplicationManager replicationManager;
  private LegacyReplicationManager legacyReplicationManager;
  private ContainerManager containerManager;
  private PlacementPolicy ratisPlacementPolicy;
  private PlacementPolicy ecPlacementPolicy;
  private EventPublisher eventPublisher;
  private SCMContext scmContext;
  private NodeManager nodeManager;
  private TestClock clock;
  private ContainerReplicaPendingOps containerReplicaPendingOps;

  private Map<ContainerID, Set<ContainerReplica>> containerReplicaMap;
  private Set<ContainerInfo> containerInfoSet;
  private ReplicationConfig repConfig;
  private ReplicationManagerReport repReport;
  private ReplicationQueue repQueue;

  @Before
  public void setup() throws IOException {
    configuration = new OzoneConfiguration();
    configuration.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");
    containerManager = Mockito.mock(ContainerManager.class);
    ratisPlacementPolicy = Mockito.mock(PlacementPolicy.class);
    ecPlacementPolicy = Mockito.mock(PlacementPolicy.class);
    Mockito.when(ecPlacementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 2, 3));
    eventPublisher = Mockito.mock(EventPublisher.class);
    scmContext = Mockito.mock(SCMContext.class);
    nodeManager = Mockito.mock(NodeManager.class);
    legacyReplicationManager = Mockito.mock(LegacyReplicationManager.class);
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    containerReplicaPendingOps =
        new ContainerReplicaPendingOps(configuration, clock);

    Mockito.when(containerManager
        .getContainerReplicas(Mockito.any(ContainerID.class))).thenAnswer(
          invocation -> {
            ContainerID cid = invocation.getArgument(0);
            return containerReplicaMap.get(cid);
          });

    Mockito.when(containerManager.getContainers()).thenAnswer(
        invocation -> new ArrayList<>(containerInfoSet));

    replicationManager = new ReplicationManager(
        configuration,
        containerManager,
        ratisPlacementPolicy,
        ecPlacementPolicy,
        eventPublisher,
        scmContext,
        nodeManager,
        clock,
        legacyReplicationManager,
        containerReplicaPendingOps);
    containerReplicaMap = new HashMap<>();
    containerInfoSet = new HashSet<>();
    repConfig = new ECReplicationConfig(3, 2);
    repReport = new ReplicationManagerReport();
    repQueue = new ReplicationQueue();

    // Ensure that RM will run when asked.
    Mockito.when(scmContext.isLeaderReady()).thenReturn(true);
    Mockito.when(scmContext.isInSafeMode()).thenReturn(false);
  }

  private void enableProcessAll() {
    SCMServiceManager serviceManager = new SCMServiceManager();
    serviceManager.register(replicationManager);
    serviceManager.notifyStatusChanged();
  }

  @Test
  public void testOpenContainerSkipped() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.OPEN);
    // It is under replicated, but as its still open it is seen as healthy.
    addReplicas(container, ContainerReplicaProto.State.OPEN, 1, 2, 3, 4);
    replicationManager.processContainer(
        container, repQueue, repReport);
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testUnhealthyOpenContainerClosed()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.OPEN);
    // Container is open but replicas are closed, so it is open but unhealthy.
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);
    replicationManager.processContainer(
        container, repQueue, repReport);
    Mockito.verify(eventPublisher, Mockito.times(1))
        .fireEvent(SCMEvents.CLOSE_CONTAINER, container.containerID());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OPEN_UNHEALTHY));
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testHealthyContainer() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);

    replicationManager.processContainer(
        container, repQueue, repReport);
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testUnderReplicatedContainer() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);

    replicationManager.processContainer(
        container, repQueue, repReport);
    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedContainerFixedByPending()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);
    containerReplicaPendingOps.scheduleAddReplica(container.containerID(),
        MockDatanodeDetails.randomDatanodeDetails(), 5);

    replicationManager.processContainer(
        container, repQueue, repReport);
    // As the pending replication fixes the under replication, nothing is added
    // to the under replication list.
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    // As the container is still under replicated, as the pending have not
    // completed yet, the container is still marked as under-replicated in the
    // report.
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  @Test
  public void testUnderReplicatedAndUnrecoverable()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2);

    replicationManager.processContainer(
        container, repQueue, repReport);
    // If it is unrecoverable, there is no point in putting it into the under
    // replication list. It will be checked again on the next RM run.
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.MISSING));
  }

  @Test
  public void testUnderAndOverReplicated()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 5, 5);

    replicationManager.processContainer(
        container, repQueue, repReport);
    // If it is both under and over replicated, we set it to the most important
    // state, which is under-replicated. When that is fixed, over replication
    // will be handled.
    Assert.assertEquals(1, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED));
    Assert.assertEquals(0, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicated() throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4, 5, 5);
    replicationManager.processContainer(
        container, repQueue, repReport);
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    Assert.assertEquals(1, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testOverReplicatedFixByPending()
      throws ContainerNotFoundException {
    ContainerInfo container = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(container, ContainerReplicaProto.State.CLOSED,
        1, 2, 3, 4, 5, 5);
    containerReplicaPendingOps.scheduleDeleteReplica(container.containerID(),
        MockDatanodeDetails.randomDatanodeDetails(), 5);
    replicationManager.processContainer(
        container, repQueue, repReport);
    Assert.assertEquals(0, repQueue.underReplicatedQueueSize());
    // If the pending replication fixes the over-replication, nothing is added
    // to the over replication list.
    Assert.assertEquals(0, repQueue.overReplicatedQueueSize());
    Assert.assertEquals(1, repReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED));
  }

  @Test
  public void testUnderReplicationQueuePopulated() {
    ContainerInfo decomContainer = createContainerInfo(repConfig, 1,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(decomContainer, ContainerReplicaProto.State.CLOSED,
        Pair.of(DECOMMISSIONING, 1),
        Pair.of(DECOMMISSIONING, 2), Pair.of(DECOMMISSIONING, 3),
        Pair.of(DECOMMISSIONING, 4), Pair.of(DECOMMISSIONING, 5));

    ContainerInfo underRep1 = createContainerInfo(repConfig, 2,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(underRep1, ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4);
    ContainerInfo underRep0 = createContainerInfo(repConfig, 3,
        HddsProtos.LifeCycleState.CLOSED);
    addReplicas(underRep0, ContainerReplicaProto.State.CLOSED, 1, 2, 3);

    enableProcessAll();
    replicationManager.processAll();

    // Get the first message off the queue - it should be underRep0.
    ContainerHealthResult.UnderReplicatedHealthResult res
        = replicationManager.dequeueUnderReplicatedContainer();
    Assert.assertEquals(underRep0, res.getContainerInfo());

    // Now requeue it
    replicationManager.requeueUnderReplicatedContainer(res);

    // Now get the next message. It should be underRep1, as it has remaining
    // redundancy 1 + zero retries. UnderRep0 will have remaining redundancy 0
    // and 1 retry. They will have the same weighted redundancy so lesser
    // retries should come first
    res = replicationManager.dequeueUnderReplicatedContainer();
    Assert.assertEquals(underRep1, res.getContainerInfo());

    // Next message is underRep0. It starts with a weighted redundancy of 0 + 1
    // retry. The other message on the queue is a decommission only with a
    // weighted redundancy of 5 + 0. So lets dequeue and requeue the message 4
    // times. Then the weighted redundancy will be equal and the decommission
    // one will be next due to having less retries.
    for (int i = 0; i < 4; i++) {
      res = replicationManager.dequeueUnderReplicatedContainer();
      Assert.assertEquals(underRep0, res.getContainerInfo());
      replicationManager.requeueUnderReplicatedContainer(res);
    }
    res = replicationManager.dequeueUnderReplicatedContainer();
    Assert.assertEquals(decomContainer, res.getContainerInfo());

    res = replicationManager.dequeueUnderReplicatedContainer();
    Assert.assertEquals(underRep0, res.getContainerInfo());

    res = replicationManager.dequeueUnderReplicatedContainer();
    Assert.assertNull(res);
  }

  @Test
  public void testSendDatanodeDeleteCommand() throws NotLeaderException {
    ECReplicationConfig ecRepConfig = new ECReplicationConfig(3, 2);
    ContainerInfo containerInfo =
        ReplicationTestUtil.createContainerInfo(ecRepConfig, 1,
        HddsProtos.LifeCycleState.CLOSED, 10, 20);
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();

    DeleteContainerCommand deleteContainerCommand = new DeleteContainerCommand(
        containerInfo.getContainerID());
    deleteContainerCommand.setReplicaIndex(1);

    replicationManager.sendDatanodeCommand(deleteContainerCommand,
        containerInfo, target);

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    Mockito.verify(eventPublisher).fireEvent(any(), any());
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(ContainerReplicaOp.PendingOpType.DELETE,
        ops.get(0).getOpType());
    Assertions.assertEquals(target, ops.get(0).getTarget());
    Assertions.assertEquals(1, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(1, replicationManager.getMetrics()
            .getEcDeletionCmdsSentTotal());
    Assertions.assertEquals(0, replicationManager.getMetrics()
        .getNumDeletionCmdsSent());

    // Repeat with Ratis container, as different metrics should be incremented
    Mockito.clearInvocations(eventPublisher);
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    containerInfo = ReplicationTestUtil.createContainerInfo(ratisRepConfig, 2,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);

    deleteContainerCommand = new DeleteContainerCommand(
        containerInfo.getContainerID());
    replicationManager.sendDatanodeCommand(deleteContainerCommand,
        containerInfo, target);

    ops = containerReplicaPendingOps.getPendingOps(containerInfo.containerID());
    Mockito.verify(eventPublisher).fireEvent(any(), any());
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(ContainerReplicaOp.PendingOpType.DELETE,
        ops.get(0).getOpType());
    Assertions.assertEquals(target, ops.get(0).getTarget());
    Assertions.assertEquals(0, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getEcDeletionCmdsSentTotal());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getNumDeletionCmdsSent());
    Assertions.assertEquals(20, replicationManager.getMetrics()
        .getNumDeletionBytesTotal());
  }

  @Test
  public void testSendDatanodeReconstructCommand() throws NotLeaderException {
    ECReplicationConfig ecRepConfig = new ECReplicationConfig(3, 2);
    ContainerInfo containerInfo =
        ReplicationTestUtil.createContainerInfo(ecRepConfig, 1,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);

    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
        sourceNodes = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      sourceNodes.add(
          new ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex(
              MockDatanodeDetails.randomDatanodeDetails(), i));
    }
    List<DatanodeDetails> targetNodes = new ArrayList<>();
    DatanodeDetails target4 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails target5 = MockDatanodeDetails.randomDatanodeDetails();
    targetNodes.add(target4);
    targetNodes.add(target5);
    byte[] missingIndexes = {4, 5};

    ReconstructECContainersCommand command = new ReconstructECContainersCommand(
        containerInfo.getContainerID(), sourceNodes, targetNodes,
        missingIndexes, ecRepConfig);

    replicationManager.sendDatanodeCommand(command, containerInfo, target4);

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    Mockito.verify(eventPublisher).fireEvent(any(), any());
    Assertions.assertEquals(2, ops.size());
    Set<DatanodeDetails> cmdTargets = new HashSet<>();
    Set<Integer> cmdIndexes = new HashSet<>();
    for (ContainerReplicaOp op : ops) {
      Assertions.assertEquals(ADD, op.getOpType());
      cmdTargets.add(op.getTarget());
      cmdIndexes.add(op.getReplicaIndex());
    }
    Assertions.assertEquals(2, cmdTargets.size());
    for (DatanodeDetails dn : targetNodes) {
      Assertions.assertTrue(cmdTargets.contains(dn));
    }

    Assertions.assertEquals(2, cmdIndexes.size());
    for (int i : missingIndexes) {
      Assertions.assertTrue(cmdIndexes.contains(i));
    }
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getEcReconstructionCmdsSentTotal());
  }

  @Test
  public void testSendDatanodeReplicateCommand() throws NotLeaderException {
    ECReplicationConfig ecRepConfig = new ECReplicationConfig(3, 2);
    ContainerInfo containerInfo =
        ReplicationTestUtil.createContainerInfo(ecRepConfig, 1,
            HddsProtos.LifeCycleState.CLOSED, 10, 20);
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();

    List<DatanodeDetails> sources = new ArrayList<>();
    sources.add(MockDatanodeDetails.randomDatanodeDetails());
    sources.add(MockDatanodeDetails.randomDatanodeDetails());


    ReplicateContainerCommand command = new ReplicateContainerCommand(
        containerInfo.getContainerID(), sources);
    command.setReplicaIndex(1);

    replicationManager.sendDatanodeCommand(command, containerInfo, target);

    List<ContainerReplicaOp> ops = containerReplicaPendingOps.getPendingOps(
        containerInfo.containerID());
    Mockito.verify(eventPublisher).fireEvent(any(), any());
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(ContainerReplicaOp.PendingOpType.ADD,
        ops.get(0).getOpType());
    Assertions.assertEquals(target, ops.get(0).getTarget());
    Assertions.assertEquals(1, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getEcReplicationCmdsSentTotal());
    Assertions.assertEquals(0, replicationManager.getMetrics()
        .getNumReplicationCmdsSent());

    // Repeat with Ratis container, as different metrics should be incremented
    Mockito.clearInvocations(eventPublisher);
    RatisReplicationConfig ratisRepConfig =
        RatisReplicationConfig.getInstance(THREE);
    containerInfo = ReplicationTestUtil.createContainerInfo(ratisRepConfig, 2,
        HddsProtos.LifeCycleState.CLOSED, 10, 20);

    command = new ReplicateContainerCommand(
        containerInfo.getContainerID(), sources);
    replicationManager.sendDatanodeCommand(command, containerInfo, target);

    ops = containerReplicaPendingOps.getPendingOps(containerInfo.containerID());
    Mockito.verify(eventPublisher).fireEvent(any(), any());
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(ContainerReplicaOp.PendingOpType.ADD,
        ops.get(0).getOpType());
    Assertions.assertEquals(target, ops.get(0).getTarget());
    Assertions.assertEquals(0, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getEcReplicationCmdsSentTotal());
    Assertions.assertEquals(1, replicationManager.getMetrics()
        .getNumReplicationCmdsSent());
  }

  @SafeVarargs
  private final Set<ContainerReplica>  addReplicas(ContainerInfo container,
      ContainerReplicaProto.State replicaState,
      Pair<HddsProtos.NodeOperationalState, Integer>... nodes) {
    final Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), replicaState, nodes);
    storeContainerAndReplicas(container, replicas);
    return replicas;
  }

  private Set<ContainerReplica> addReplicas(ContainerInfo container,
      ContainerReplicaProto.State replicaState, int... indexes) {
    final Set<ContainerReplica> replicas =
        createReplicas(container.containerID(), replicaState, indexes);
    storeContainerAndReplicas(container, replicas);
    return replicas;
  }

  private void storeContainerAndReplicas(ContainerInfo container,
      Set<ContainerReplica> replicas) {
    containerReplicaMap.put(container.containerID(), replicas);
    containerInfoSet.add(container);
  }

}
