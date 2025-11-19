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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for QuasiClosedStuckOverReplicationHandler.
 */
public class TestQuasiClosedStuckOverReplicationHandler {

  private static final RatisReplicationConfig RATIS_REPLICATION_CONFIG = RatisReplicationConfig.getInstance(THREE);
  private ContainerInfo container;
  private ReplicationManager replicationManager;
  private Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent;
  private QuasiClosedStuckOverReplicationHandler handler;
  private final DatanodeID origin1 = DatanodeID.randomID();
  private final DatanodeID origin2 = DatanodeID.randomID();

  @BeforeEach
  void setup() throws NodeNotFoundException,
      CommandTargetOverloadedException, NotLeaderException {
    container = ReplicationTestUtil.createContainer(
        HddsProtos.LifeCycleState.QUASI_CLOSED, RATIS_REPLICATION_CONFIG);

    replicationManager = mock(ReplicationManager.class);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setBoolean("hdds.scm.replication.push", true);
    when(replicationManager.getConfig())
        .thenReturn(ozoneConfiguration.getObject(
            ReplicationManager.ReplicationManagerConfiguration.class));
    ReplicationManagerMetrics metrics = ReplicationManagerMetrics.create(replicationManager);
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
    ReplicationTestUtil.mockRMSendThrottledDeleteCommand(
        replicationManager, commandsSent);
    handler = new QuasiClosedStuckOverReplicationHandler(replicationManager);
  }

  @Test
  public void testReturnsZeroIfNotOverReplicated() throws IOException {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE));

    int count = handler.processAndSendCommands(replicas, Collections.emptyList(), getOverReplicatedHealthResult(), 1);
    assertEquals(0, count);
  }

  @Test
  public void testNoCommandsScheduledIfPendingOps() throws IOException {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE));
    List<ContainerReplicaOp> pendingOps = new ArrayList<>();
    pendingOps.add(new ContainerReplicaOp(
        ContainerReplicaOp.PendingOpType.DELETE,
        MockDatanodeDetails.randomDatanodeDetails(),
        0,
        null,
        Long.MAX_VALUE,
        0));

    int count = handler.processAndSendCommands(replicas, pendingOps, getOverReplicatedHealthResult(), 1);
    assertEquals(0, count);
  }

  @Test
  public void testCommandScheduledForOverReplicatedContainer() throws IOException {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE));

    int count = handler.processAndSendCommands(replicas, Collections.emptyList(), getOverReplicatedHealthResult(), 1);
    assertEquals(1, count);
    SCMCommand<?> command = commandsSent.iterator().next().getRight();
    assertEquals(StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.deleteContainerCommand, command.getType());
  }

  @Test
  public void testOverloadedExceptionContinuesAndThrows() throws NotLeaderException, CommandTargetOverloadedException {
    Set<ContainerReplica> replicas = ReplicationTestUtil.createReplicasWithOriginAndOpState(container.containerID(),
        StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED,
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin1, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE),
        Pair.of(origin2, HddsProtos.NodeOperationalState.IN_SERVICE));

    ReplicationTestUtil.mockRMSendThrottledDeleteCommand(replicationManager, commandsSent, new AtomicBoolean(true));

    assertThrows(CommandTargetOverloadedException.class, () ->
        handler.processAndSendCommands(replicas, Collections.emptyList(), getOverReplicatedHealthResult(), 1));
    assertEquals(1, commandsSent.size());
  }

  private ContainerHealthResult.OverReplicatedHealthResult getOverReplicatedHealthResult() {
    ContainerHealthResult.OverReplicatedHealthResult
        healthResult = mock(ContainerHealthResult.OverReplicatedHealthResult.class);
    when(healthResult.getContainerInfo()).thenReturn(container);
    return healthResult;
  }

}
