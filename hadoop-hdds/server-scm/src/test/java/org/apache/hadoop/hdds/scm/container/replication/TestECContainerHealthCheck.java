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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.OverReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.UnderReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.HealthState;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;

/**
 * Tests for the ECContainerHealthCheck class.
 */
public class TestECContainerHealthCheck {

  private ECContainerHealthCheck healthCheck;
  private ECReplicationConfig repConfig;

  @Before
  public void setup() {
    healthCheck = new ECContainerHealthCheck();
    repConfig = new ECReplicationConfig(3, 2);
  }

  @Test
  public void testHealthyContainerIsHealthy() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 1, 2, 3, 4, 5);
    ContainerHealthResult result = healthCheck.checkHealth(container, replicas,
        Collections.emptyList(), 2);
    Assert.assertEquals(HealthState.HEALTHY, result.getHealthState());
  }

  @Test
  public void testUnderReplicatedContainerIsUnderReplicated() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 1, 2, 4, 5);
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(container, replicas,
            Collections.emptyList(), 2);
    Assert.assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    Assert.assertEquals(1, result.getRemainingRedundancy());
    Assert.assertFalse(result.isSufficientlyReplicatedAfterPending());
    Assert.assertFalse(result.underReplicatedDueToDecommission());
  }

  @Test
  public void testUnderReplicatedContainerFixedWithPending() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), 1, 2, 4, 5);
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(ContainerReplicaOp.create(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 3));
    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(container, replicas, pending, 2);
    Assert.assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    Assert.assertEquals(1, result.getRemainingRedundancy());
    Assert.assertTrue(result.isSufficientlyReplicatedAfterPending());
    Assert.assertFalse(result.underReplicatedDueToDecommission());
  }

  @Test
  public void testUnderReplicatedDueToDecommission() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(DECOMMISSIONING, 4),
        Pair.of(DECOMMISSIONED, 5));

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(container, replicas, Collections.emptyList(),
            2);
    Assert.assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    Assert.assertEquals(2, result.getRemainingRedundancy());
    Assert.assertFalse(result.isSufficientlyReplicatedAfterPending());
    Assert.assertTrue(result.underReplicatedDueToDecommission());
  }

  @Test
  public void testUnderReplicatedDueToDecommissionFixedWithPending() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(DECOMMISSIONING, 4),
        Pair.of(IN_SERVICE, 4), Pair.of(DECOMMISSIONED, 5));
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(ContainerReplicaOp.create(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 5));

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(container, replicas, pending, 2);
    Assert.assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    Assert.assertEquals(2, result.getRemainingRedundancy());
    Assert.assertTrue(result.isSufficientlyReplicatedAfterPending());
    Assert.assertTrue(result.underReplicatedDueToDecommission());
  }

  @Test
  public void testUnderReplicatedDueToDecommissionAndMissing() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(DECOMMISSIONING, 4), Pair.of(DECOMMISSIONED, 5));
    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(ContainerReplicaOp.create(
        ADD, MockDatanodeDetails.randomDatanodeDetails(), 3));

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(container, replicas, pending, 2);
    Assert.assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    Assert.assertEquals(1, result.getRemainingRedundancy());
    Assert.assertFalse(result.isSufficientlyReplicatedAfterPending());
    Assert.assertFalse(result.underReplicatedDueToDecommission());
  }

  @Test
  public void testUnderReplicatedAndUnrecoverable() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas = createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2));

    UnderReplicatedHealthResult result = (UnderReplicatedHealthResult)
        healthCheck.checkHealth(container, replicas,
            Collections.emptyList(), 2);
    Assert.assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    Assert.assertEquals(-1, result.getRemainingRedundancy());
    Assert.assertFalse(result.isSufficientlyReplicatedAfterPending());
    Assert.assertFalse(result.underReplicatedDueToDecommission());
    Assert.assertTrue(result.isUnrecoverable());
  }

  @Test
  public void testOverReplicatedContainer() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2));

    List<ContainerReplicaOp> pending = new ArrayList<>();
    pending.add(ContainerReplicaOp.create(
        DELETE, MockDatanodeDetails.randomDatanodeDetails(), 1));
    pending.add(ContainerReplicaOp.create(
        DELETE, MockDatanodeDetails.randomDatanodeDetails(), 2));

    OverReplicatedHealthResult result = (OverReplicatedHealthResult)
        healthCheck.checkHealth(container, replicas, pending, 2);
    Assert.assertEquals(HealthState.OVER_REPLICATED, result.getHealthState());
    Assert.assertEquals(2, result.getExcessRedundancy());
    Assert.assertTrue(result.isSufficientlyReplicatedAfterPending());
  }

  @Test
  public void testOverReplicatedContainerDueToMaintenance() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 5),
        Pair.of(IN_MAINTENANCE, 1), Pair.of(IN_MAINTENANCE, 2));

    ContainerHealthResult result = healthCheck.checkHealth(container, replicas,
        Collections.emptyList(), 2);
    Assert.assertEquals(HealthState.HEALTHY, result.getHealthState());
  }

  @Test
  public void testOverAndUnderReplicated() {
    ContainerInfo container = createContainerInfo(repConfig);
    Set<ContainerReplica> replicas =  createReplicas(container.containerID(),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
        Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
        Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2));

    ContainerHealthResult result = healthCheck.checkHealth(container, replicas,
        Collections.emptyList(), 2);
    Assert.assertEquals(HealthState.UNDER_REPLICATED, result.getHealthState());
    Assert.assertEquals(1,
        ((UnderReplicatedHealthResult)result).getRemainingRedundancy());
  }

}
