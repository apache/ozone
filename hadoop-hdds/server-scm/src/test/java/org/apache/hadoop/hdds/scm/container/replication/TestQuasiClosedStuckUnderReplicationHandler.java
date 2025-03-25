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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
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
  private ReplicationManagerMetrics metrics;
  private PlacementPolicy policy;
  private Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent;
  private QuasiClosedStuckUnderReplicationHandler handler;


  @BeforeEach
  void setup(@TempDir File testDir) throws NodeNotFoundException,
      CommandTargetOverloadedException, NotLeaderException {
    container = ReplicationTestUtil.createContainer(
        HddsProtos.LifeCycleState.QUASI_CLOSED, RATIS_REPLICATION_CONFIG);

    nodeManager = mock(NodeManager.class);
    conf = SCMTestUtils.getConf(testDir);
    policy = ReplicationTestUtil
        .getSimpleTestPlacementPolicy(nodeManager, conf);
    replicationManager = mock(ReplicationManager.class);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setBoolean("hdds.scm.replication.push", true);
    when(replicationManager.getConfig())
        .thenReturn(ozoneConfiguration.getObject(
            ReplicationManager.ReplicationManagerConfiguration.class));
    metrics = ReplicationManagerMetrics.create(replicationManager);
    when(replicationManager.getMetrics()).thenReturn(metrics);

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
    UUID origin = UUID.randomUUID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin, HddsProtos.NodeOperationalState.IN_SERVICE));

    int count = handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1);
    assertEquals(0, count);
  }

  @Test
  public void testNoCommandsScheduledIfPendingOps() throws IOException {
    UUID origin = UUID.randomUUID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin, HddsProtos.NodeOperationalState.IN_SERVICE));
    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    pendingOps.add(ContainerReplicaOp.create(
        ContainerReplicaOp.PendingOpType.ADD, MockDatanodeDetails.randomDatanodeDetails(), 0));

    int count = handler.processAndSendCommands(replicas, pendingOps, getUnderReplicatedHealthResult(), 1);
    assertEquals(0, count);
  }

  @Test
  public void testCommandScheduledForUnderReplicatedContainer() throws IOException {
    UUID origin = UUID.randomUUID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin, HddsProtos.NodeOperationalState.IN_SERVICE));

    int count = handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1);
    assertEquals(2, count);
    ReplicationTestUtil.mockRMSendThrottleReplicateCommand(replicationManager, commandsSent, new AtomicBoolean(true));
  }

  @Test
  public void testOverloadedExceptionContinuesAndThrows() throws NotLeaderException, CommandTargetOverloadedException {
    UUID origin1 = UUID.randomUUID();
    UUID origin2 = UUID.randomUUID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE));

    ReplicationTestUtil.mockRMSendThrottleReplicateCommand(replicationManager, commandsSent, new AtomicBoolean(true));

    assertThrows(CommandTargetOverloadedException.class, () ->
        handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1));
    assertEquals(1, commandsSent.size());
  }

  @Test
  public void testInsufficientNodesExceptionThrown() {
    UUID origin1 = UUID.randomUUID();
    UUID origin2 = UUID.randomUUID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE));

    policy = ReplicationTestUtil.getNoNodesTestPlacementPolicy(nodeManager, conf);
    handler = new QuasiClosedStuckUnderReplicationHandler(policy, conf, replicationManager);

    assertThrows(SCMException.class, () ->
        handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1));
    assertEquals(0, commandsSent.size());
  }

  @Test
  public void testPartialReplicationExceptionThrown() {
    UUID origin1 = UUID.randomUUID();
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE));

    policy = ReplicationTestUtil.getInsufficientNodesTestPlacementPolicy(nodeManager, conf, 2);
    handler = new QuasiClosedStuckUnderReplicationHandler(policy, conf, replicationManager);

    assertThrows(InsufficientDatanodesException.class, () ->
        handler.processAndSendCommands(replicas, Collections.emptyList(), getUnderReplicatedHealthResult(), 1));
    assertEquals(1, commandsSent.size());
  }

  private ContainerHealthResult.UnderReplicatedHealthResult getUnderReplicatedHealthResult() {
    ContainerHealthResult.UnderReplicatedHealthResult
        healthResult = mock(ContainerHealthResult.UnderReplicatedHealthResult.class);
    when(healthResult.getContainerInfo()).thenReturn(container);
    return healthResult;
  }

}
