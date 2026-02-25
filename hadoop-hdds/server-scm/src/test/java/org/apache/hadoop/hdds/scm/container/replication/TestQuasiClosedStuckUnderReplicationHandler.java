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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.InsufficientDatanodesException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for QuasiClosedStuckUnderReplicationHandler.
 */
public class TestQuasiClosedStuckUnderReplicationHandler {

  private static final RatisReplicationConfig RATIS_REPLICATION_CONFIG = RatisReplicationConfig.getInstance(THREE);
  private ContainerInfo container;
  private NodeManager nodeManager;
  private OzoneConfiguration conf;
  private ReplicationManager replicationManager;
  private Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent;
  private QuasiClosedStuckUnderReplicationHandler handler;

  @BeforeEach
  void setup(@TempDir File testDir) throws NodeNotFoundException,
      CommandTargetOverloadedException, NotLeaderException {
    container = ReplicationTestUtil.createContainer(
        HddsProtos.LifeCycleState.QUASI_CLOSED, RATIS_REPLICATION_CONFIG);

    nodeManager = mock(NodeManager.class);
    conf = SCMTestUtils.getConf(testDir);
    PlacementPolicy policy = ReplicationTestUtil
        .getSimpleTestPlacementPolicy(nodeManager, conf);
    replicationManager = mock(ReplicationManager.class);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setBoolean("hdds.scm.replication.push", true);
    when(replicationManager.getConfig())
        .thenReturn(ozoneConfiguration.getObject(
            ReplicationManager.ReplicationManagerConfiguration.class));
    ReplicationManagerMetrics metrics = ReplicationManagerMetrics.create(replicationManager);
    when(replicationManager.getMetrics()).thenReturn(metrics);
    when(replicationManager.getContainerReplicaPendingOps()).thenReturn(mock(ContainerReplicaPendingOps.class));

    /*
      Return NodeStatus with NodeOperationalState as specified in
      DatanodeDetails, and NodeState as HEALTHY.
    */
    when(
        replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocationOnMock -> {
          DatanodeDetails dn = invocationOnMock.getArgument(0);
          return NodeStatus.valueOf(dn.getPersistedOpState(),
              HddsProtos.NodeState.HEALTHY);
        });

    commandsSent = new HashSet<>();
    ReplicationTestUtil.mockRMSendThrottleReplicateCommand(
        replicationManager, commandsSent, new AtomicBoolean(false));
    ReplicationTestUtil.mockRMSendDatanodeCommand(replicationManager,
        commandsSent);
    ReplicationTestUtil.mockRMSendDeleteCommand(replicationManager,
        commandsSent);
    handler = new QuasiClosedStuckUnderReplicationHandler(policy, conf,
        replicationManager);
  }

  @Test
  public void testReturnsZeroIfNotUnderReplicated() throws IOException {
    final DatanodeID origin = DatanodeID.randomID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        QUASI_CLOSED, Pair.of(origin, IN_SERVICE), Pair.of(origin, IN_SERVICE), Pair.of(origin, IN_SERVICE));

    int count = handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1);
    assertEquals(0, count);
  }

  @Test
  public void testNoCommandsScheduledIfPendingOps() throws IOException {
    final DatanodeID origin = DatanodeID.randomID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        QUASI_CLOSED, Pair.of(origin, IN_SERVICE), Pair.of(origin, IN_SERVICE));
    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    pendingOps.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.ADD, MockDatanodeDetails.randomDatanodeDetails(), 0, null, Long.MAX_VALUE, 0));

    int count = handler.processAndSendCommands(replicas, pendingOps, getUnderReplicatedHealthResult(), 1);
    assertEquals(0, count);
  }

  @Test
  public void testCommandScheduledForUnderReplicatedContainer() throws IOException {
    final DatanodeID origin = DatanodeID.randomID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        QUASI_CLOSED, Pair.of(origin, IN_SERVICE));

    int count = handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1);
    assertEquals(2, count);
    ReplicationTestUtil.mockRMSendThrottleReplicateCommand(replicationManager, commandsSent, new AtomicBoolean(true));
  }

  @Test
  public void testOverloadedExceptionContinuesAndThrows() throws NotLeaderException, CommandTargetOverloadedException {
    final DatanodeID origin1 = DatanodeID.randomID();
    final DatanodeID origin2 = DatanodeID.randomID();
    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(container.containerID(),
        origin1, IN_SERVICE, QUASI_CLOSED, 10));
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(container.containerID(),
        origin1, IN_SERVICE, QUASI_CLOSED, 10));
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(container.containerID(),
        origin2, IN_SERVICE, QUASI_CLOSED, 5));

    ReplicationTestUtil.mockRMSendThrottleReplicateCommand(replicationManager, commandsSent, new AtomicBoolean(true));

    assertThrows(CommandTargetOverloadedException.class, () ->
        handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1));
    assertEquals(1, commandsSent.size());
  }

  @Test
  public void testInsufficientNodesExceptionThrown() {
    final DatanodeID origin1 = DatanodeID.randomID();
    final DatanodeID origin2 = DatanodeID.randomID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        QUASI_CLOSED, Pair.of(origin1, IN_SERVICE), Pair.of(origin2, IN_SERVICE));

    PlacementPolicy policy = ReplicationTestUtil.getNoNodesTestPlacementPolicy(nodeManager, conf);
    handler = new QuasiClosedStuckUnderReplicationHandler(policy, conf, replicationManager);

    assertThrows(SCMException.class, () ->
        handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1));
    assertEquals(0, commandsSent.size());
  }

  @Test
  public void testPartialReplicationExceptionThrown() {
    final DatanodeID origin1 = DatanodeID.randomID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        QUASI_CLOSED, Pair.of(origin1, IN_SERVICE));

    PlacementPolicy policy = ReplicationTestUtil.getInsufficientNodesTestPlacementPolicy(nodeManager, conf, 2);
    handler = new QuasiClosedStuckUnderReplicationHandler(policy, conf, replicationManager);

    assertThrows(InsufficientDatanodesException.class, () ->
        handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1));
    assertEquals(1, commandsSent.size());
  }

  @Test
  public void testOriginCopies() throws IOException {
    final DatanodeID origin1 = DatanodeID.randomID();
    final DatanodeID origin2 = DatanodeID.randomID();

    // Configure default copy counts: best origin gets 3 copies, other origins get 2 copies.
    ReplicationManager.ReplicationManagerConfiguration rmConf =
        new OzoneConfiguration().getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    rmConf.setQuasiClosedStuckBestOriginCopies(3);
    rmConf.setQuasiClosedStuckOtherOriginCopies(2);
    when(replicationManager.getConfig()).thenReturn(rmConf);

    // origin1 is the best origin (BCSID=10, target=3): has 2 copies, needs 1 more.
    // origin2 is an other origin (BCSID=5, target=2): has 2 copies, already at target.
    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(
        container.containerID(), origin1, IN_SERVICE, QUASI_CLOSED, 10));
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(
        container.containerID(), origin1, IN_SERVICE, QUASI_CLOSED, 10));
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(
        container.containerID(), origin2, IN_SERVICE, QUASI_CLOSED, 5));
    replicas.add(ReplicationTestUtil.createReplicaWithOriginAndSeqId(
        container.containerID(), origin2, IN_SERVICE, QUASI_CLOSED, 5));

    int count = handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1);
    assertEquals(1, count);
  }

  private ContainerHealthResult.UnderReplicatedHealthResult getUnderReplicatedHealthResult() {
    ContainerHealthResult.UnderReplicatedHealthResult
        healthResult = mock(ContainerHealthResult.UnderReplicatedHealthResult.class);
    when(healthResult.getContainerInfo()).thenReturn(container);
    return healthResult;
  }

}
