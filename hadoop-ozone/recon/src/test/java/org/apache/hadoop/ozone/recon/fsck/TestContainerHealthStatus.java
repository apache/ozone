/*
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
package org.apache.hadoop.ozone.recon.fsck;

import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test to ensure the correct container state is return by a
 * ContainerHealthStatus instance.
 */
public class TestContainerHealthStatus {

  private PlacementPolicy placementPolicy;
  private ContainerInfo container;

  @Before
  public void setup() {
    placementPolicy = mock(PlacementPolicy.class);
    container = mock(ContainerInfo.class);
    when(container.getReplicationFactor())
        .thenReturn(HddsProtos.ReplicationFactor.THREE);
    when(container.containerID()).thenReturn(new ContainerID(123456));
    when(container.getContainerID()).thenReturn((long)123456);
    when(placementPolicy.validateContainerPlacement(
        Mockito.anyList(), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 1, 1));
  }

  @Test
  public void testHealthyContainer() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    assertTrue(status.isHealthy());
    assertFalse(status.isOverReplicated());
    assertFalse(status.isUnderReplicated());
    assertEquals(0, status.replicaDelta());
    assertFalse(status.isMissing());
    assertEquals(false, status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());

    assertEquals(container, status.getContainer());
    assertEquals((long)123456, status.getContainerID());
    assertEquals(3, status.getReplicationFactor());
    assertEquals(3, status.getReplicaCount());
  }

  @Test
  public void testHealthyContainerWithExtraUnhealthyReplica() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.UNHEALTHY);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    assertTrue(status.isHealthy());
    assertFalse(status.isOverReplicated());
    assertFalse(status.isUnderReplicated());
    assertEquals(0, status.replicaDelta());
    assertFalse(status.isMissing());
    assertEquals(false, status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());
  }

  @Test
  public void testMissingContainer() {
    Set<ContainerReplica> replicas = new HashSet<>();
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    assertFalse(status.isHealthy());
    assertFalse(status.isOverReplicated());
    assertFalse(status.isUnderReplicated());
    assertEquals(3, status.replicaDelta());
    assertTrue(status.isMissing());
    assertEquals(false, status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());
  }

  @Test
  public void testUnderReplicatedContainer() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    assertFalse(status.isHealthy());
    assertFalse(status.isMissing());
    assertFalse(status.isOverReplicated());
    assertTrue(status.isUnderReplicated());
    assertEquals(2, status.replicaDelta());
    assertEquals(false, status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());
  }

  @Test
  public void testOverReplicatedContainer() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    assertFalse(status.isHealthy());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertTrue(status.isOverReplicated());
    assertEquals(-1, status.replicaDelta());
    assertEquals(false, status.isMisReplicated());
    assertEquals(0, status.misReplicatedDelta());
  }

  @Test
  public void testMisReplicated() {
    Set<ContainerReplica> replicas = generateReplicas(container,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED,
        ContainerReplicaProto.State.CLOSED);
    when(placementPolicy.validateContainerPlacement(
        Mockito.anyList(), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 5));
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    assertFalse(status.isHealthy());
    assertFalse(status.isMissing());
    assertFalse(status.isUnderReplicated());
    assertFalse(status.isOverReplicated());
    assertEquals(0, status.replicaDelta());
    assertTrue(status.isMisReplicated());
    assertEquals(1, status.misReplicatedDelta());
  }

  private Set<ContainerReplica> generateReplicas(ContainerInfo cont,
      ContainerReplicaProto.State...states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (ContainerReplicaProto.State s : states) {
      replicas.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(cont.containerID())
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
          .setContainerState(s)
          .build());
    }
    return replicas;
  }
}