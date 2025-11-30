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

package org.apache.hadoop.hdds.scm.container.replication.health;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link VulnerableUnhealthyReplicasHandler}.
 */
public class TestVulnerableUnhealthyReplicasHandler {
  private ReplicationManager replicationManager;
  private ReplicationConfig repConfig;
  private ReplicationQueue repQueue;
  private ContainerCheckRequest.Builder requestBuilder;
  private VulnerableUnhealthyReplicasHandler handler;

  @BeforeEach
  public void setup() throws NodeNotFoundException {
    replicationManager = mock(ReplicationManager.class);
    handler = new VulnerableUnhealthyReplicasHandler(replicationManager);
    repConfig = RatisReplicationConfig.getInstance(THREE);
    repQueue = new ReplicationQueue();
    ReplicationManager.ReplicationManagerConfiguration rmConf =
        mock(ReplicationManager.ReplicationManagerConfiguration.class);
    ReplicationManagerReport report = new ReplicationManagerReport(rmConf.getContainerSampleLimit());
    requestBuilder = new ContainerCheckRequest.Builder()
        .setReplicationQueue(repQueue)
        .setMaintenanceRedundancy(2)
        .setPendingOps(Collections.emptyList())
        .setReport(report);

    when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenReturn(NodeStatus.inServiceHealthy());
  }

  @Test
  public void testReturnsFalseForECContainer() {
    ContainerInfo container = createContainerInfo(new ECReplicationConfig(3, 2));
    Set<ContainerReplica> replicas = createReplicas(container.containerID(), 1, 2, 3, 4);
    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);

    assertFalse(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testReturnsFalseForClosedContainer() {
    ContainerInfo container = createContainerInfo(repConfig, 1, LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(), 0, 0, 0);
    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);

    assertFalse(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testReturnsFalseForQuasiClosedContainerWithNoUnhealthyReplicas() {
    ContainerInfo container = createContainerInfo(repConfig, 1, LifeCycleState.QUASI_CLOSED);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(), State.QUASI_CLOSED, 0, 0, 0);
    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);

    assertFalse(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testReturnsFalseForQuasiClosedContainerWithNoVulnerableReplicas() {
    ContainerInfo container = createContainerInfo(repConfig, 1, LifeCycleState.QUASI_CLOSED);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(), 0, 0, 0);
    // create UNHEALTHY replica with unique origin id on an IN_SERVICE node
    replicas.add(createContainerReplica(container.containerID(), 0, IN_SERVICE, State.UNHEALTHY));
    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);

    assertFalse(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  /**
   * A QUASI_CLOSED container with 3 QUASI_CLOSED replicas with incorrect sequence id. They're on unique origin nodes.
   * There's an UNHEALTHY replica on a Decommissioning node, which has the correct sequence ID and unique origin.
   * It's expected that the UNHEALTHY replica is queued for under replication.
   */
  @Test
  public void testReturnsTrueForQuasiClosedContainerWithVulnerableReplica() throws NodeNotFoundException {
    long sequenceId = 10;
    ContainerInfo container = createContainerInfo(repConfig, 1, LifeCycleState.QUASI_CLOSED, sequenceId);
    Set<ContainerReplica> replicas = new HashSet<>(4);
    for (int i = 0; i < 3; i++) {
      replicas.add(createContainerReplica(container.containerID(), 0, IN_SERVICE, State.QUASI_CLOSED,
          container.getSequenceId() - 1));
    }
    // create UNHEALTHY replica with unique origin id on a DECOMMISSIONING node
    ContainerReplica unhealthy =
        createContainerReplica(container.containerID(), 0, DECOMMISSIONING, State.UNHEALTHY, sequenceId);
    replicas.add(unhealthy);
    when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dn = invocation.getArgument(0);
          if (dn.equals(unhealthy.getDatanodeDetails())) {
            return NodeStatus.valueOf(DECOMMISSIONING, HEALTHY);
          }
          return NodeStatus.inServiceHealthy();
        });
    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);

    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  /**
   * A QUASI_CLOSED container with 3 QUASI_CLOSED replicas with correct sequence id. They're on unique origin nodes.
   * There's an UNHEALTHY replica on a Decommissioning node, which also has the correct sequence ID and unique origin.
   * It's expected that the UNHEALTHY replica is queued for under replication. This is a variation of the situation
   * where the healthy replicas have incorrect sequence id, and the unhealthy ones have the correct sequence id.
   * Here, all the replicas have the correct sequence id but the unhealthy still need to be saved because they're on
   * unique origin nodes.
   * <p>
   * Why do we need to save the UNHEALTHY replicas if we have enough unique QUASI_CLOSED replicas to form a quorum?
   * Simply because we're ensuring redundancy of replicas having unique origin node IDs. When HDDS has the ability to
   * restore UNHEALTHY replicas to a healthy state, they can also be used to create a quorum. In any case, when the
   * container transitions to CLOSED, any UNHEALTHY replicas will be deleted.
   * </p>
   */
  @Test
  public void testReturnsTrueForQuasiClosedContainerWithVulnerableReplicaWhenAllReplicasHaveCorrectSequence()
      throws NodeNotFoundException {
    long sequenceId = 10;
    ContainerInfo container = createContainerInfo(repConfig, 1, LifeCycleState.QUASI_CLOSED, sequenceId);
    Set<ContainerReplica> replicas = new HashSet<>(4);
    for (int i = 0; i < 3; i++) {
      replicas.add(createContainerReplica(container.containerID(), 0, IN_SERVICE, State.QUASI_CLOSED,
          container.getSequenceId()));
    }
    // create UNHEALTHY replica with unique origin id on a DECOMMISSIONING node
    ContainerReplica unhealthy =
        createContainerReplica(container.containerID(), 0, DECOMMISSIONING, State.UNHEALTHY, sequenceId);
    replicas.add(unhealthy);
    when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dn = invocation.getArgument(0);
          if (dn.equals(unhealthy.getDatanodeDetails())) {
            return NodeStatus.valueOf(DECOMMISSIONING, HEALTHY);
          }
          return NodeStatus.inServiceHealthy();
        });
    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);

    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(1, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testReturnsFalseForVulnerableReplicaWithAnotherCopy() throws NodeNotFoundException {
    long sequenceId = 10;
    ContainerInfo container = createContainerInfo(repConfig, 1, LifeCycleState.QUASI_CLOSED, sequenceId);
    Set<ContainerReplica> replicas = new HashSet<>(4);
    for (int i = 0; i < 3; i++) {
      replicas.add(createContainerReplica(container.containerID(), 0, IN_SERVICE, State.QUASI_CLOSED,
          container.getSequenceId() - 1));
    }
    // create UNHEALTHY replica with a non-unique origin id on a DECOMMISSIONING node
    ContainerReplica unhealthy =
        createContainerReplica(container.containerID(), 0, DECOMMISSIONING, State.UNHEALTHY, sequenceId);
    replicas.add(unhealthy);
    when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dn = invocation.getArgument(0);
          if (dn.equals(unhealthy.getDatanodeDetails())) {
            return NodeStatus.valueOf(DECOMMISSIONING, HEALTHY);
          }
          return NodeStatus.inServiceHealthy();
        });
    replicas.add(createContainerReplica(container.containerID(), 0, IN_SERVICE, State.UNHEALTHY,
        container.getNumberOfKeys(), container.getUsedBytes(), MockDatanodeDetails.randomDatanodeDetails(),
        unhealthy.getOriginDatanodeId(), container.getSequenceId()));
    requestBuilder.setContainerReplicas(replicas).setContainerInfo(container);

    assertFalse(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }

  @Test
  public void testDoesNotEnqueueForReadOnlyRequest() throws NodeNotFoundException {
    long sequenceId = 10;
    ContainerInfo container = createContainerInfo(repConfig, 1, LifeCycleState.QUASI_CLOSED, sequenceId);
    Set<ContainerReplica> replicas = new HashSet<>(4);
    for (int i = 0; i < 3; i++) {
      replicas.add(createContainerReplica(container.containerID(), 0, IN_SERVICE, State.QUASI_CLOSED,
          container.getSequenceId() - 1));
    }
    // create UNHEALTHY replica with unique origin id on a DECOMMISSIONING node
    ContainerReplica unhealthy =
        createContainerReplica(container.containerID(), 0, DECOMMISSIONING, State.UNHEALTHY, sequenceId);
    replicas.add(unhealthy);
    when(replicationManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dn = invocation.getArgument(0);
          if (dn.equals(unhealthy.getDatanodeDetails())) {
            return NodeStatus.valueOf(DECOMMISSIONING, HEALTHY);
          }
          return NodeStatus.inServiceHealthy();
        });
    requestBuilder.setContainerReplicas(replicas)
        .setContainerInfo(container)
        .setReadOnly(true);

    assertTrue(handler.handle(requestBuilder.build()));
    assertEquals(0, repQueue.underReplicatedQueueSize());
    assertEquals(0, repQueue.overReplicatedQueueSize());
  }
}
