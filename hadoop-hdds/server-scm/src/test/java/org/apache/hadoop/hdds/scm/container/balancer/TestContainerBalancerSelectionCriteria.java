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

package org.apache.hadoop.hdds.scm.container.balancer;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.QUASI_CLOSED;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ContainerBalancerSelectionCriteria}.
 */
public class TestContainerBalancerSelectionCriteria {

  private ContainerBalancerSelectionCriteria criteria;
  private ContainerBalancerConfiguration balancerConfiguration;
  private ContainerManager containerManager;
  private ReplicationManager replicationManager;
  private NodeManager nodeManager;
  private FindSourceStrategy findSourceStrategy;
  private DatanodeDetails source;
  private ContainerInfo containerInfo;
  private ContainerID containerID;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    balancerConfiguration = conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * OzoneConsts.GB);

    nodeManager = mock(NodeManager.class);
    containerManager = mock(ContainerManager.class);
    replicationManager = mock(ReplicationManager.class);
    findSourceStrategy = mock(FindSourceStrategy.class);

    source = MockDatanodeDetails.randomDatanodeDetails();
    containerInfo = ReplicationTestUtil.createContainerInfo(RatisReplicationConfig.getInstance(THREE), 1L,
        HddsProtos.LifeCycleState.CLOSED, 1L, OzoneConsts.GB);
    containerID = containerInfo.containerID();

    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(ReplicationTestUtil.createContainerReplica(containerID, 0, IN_SERVICE, CLOSED,
        1L, OzoneConsts.GB, source, source.getID()));

    when(containerManager.getContainer(containerID)).thenReturn(containerInfo);
    when(containerManager.getContainerReplicas(containerID)).thenReturn(replicas);
    when(replicationManager.isContainerReplicatingOrDeleting(containerID)).thenReturn(false);
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet()))
        .thenReturn(new ContainerHealthResult.HealthyResult(containerInfo));
    when(findSourceStrategy.canSizeLeaveSource(any(DatanodeDetails.class), anyLong())).thenReturn(true);

    criteria = new ContainerBalancerSelectionCriteria(balancerConfiguration, nodeManager, replicationManager,
        containerManager, findSourceStrategy, new HashMap<>());
  }

  @Test
  public void shouldExcludeUnderReplicatedContainer() {
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet())).thenReturn(
        new ContainerHealthResult.UnderReplicatedHealthResult(containerInfo, 1,
            false, false, false));

    assertTrue(criteria.shouldBeExcluded(containerID, source, 0L));
  }

  @Test
  public void shouldExcludeOverReplicatedContainer() {
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet())).thenReturn(
        new ContainerHealthResult.OverReplicatedHealthResult(containerInfo, 1, false));

    assertTrue(criteria.shouldBeExcluded(containerID, source, 0L));
  }

  @Test
  public void shouldExcludeMisReplicatedContainer() {
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet())).thenReturn(
        new ContainerHealthResult.MisReplicatedHealthResult(containerInfo, false, "test"));

    assertTrue(criteria.shouldBeExcluded(containerID, source, 0L));
  }

  @Test
  public void shouldNotExcludeHealthyContainer() {
    assertFalse(criteria.shouldBeExcluded(containerID, source, 0L));
  }

  @Test
  public void shouldExcludeReplicatingContainer()
      throws Exception {
    when(replicationManager.isContainerReplicatingOrDeleting(containerID)).thenReturn(true);

    assertTrue(criteria.shouldBeExcluded(containerID, source, 0L));

    verify(containerManager, times(1)).getContainerReplicas(containerID);
    verify(replicationManager, times(1)).getContainerReplicationHealth(
        eq(containerInfo), anySet());
  }

  @Test
  public void shouldIncludeOverReplicatedClosedContainers() throws Exception {
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn3 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn4 = MockDatanodeDetails.randomDatanodeDetails();

    // Over-replicated CLOSED container with 3 CLOSED + 1 QUASI_CLOSED replica (non-empty)
    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(ReplicationTestUtil.createContainerReplica(containerID, 0, IN_SERVICE,
        CLOSED, 1L, OzoneConsts.GB, source, source.getID()));
    replicas.add(ReplicationTestUtil.createContainerReplica(containerID, 0, IN_SERVICE,
        CLOSED, 1L, OzoneConsts.GB, dn2, dn2.getID()));
    replicas.add(ReplicationTestUtil.createContainerReplica(containerID, 0, IN_SERVICE,
        CLOSED, 1L, OzoneConsts.GB, dn3, dn3.getID()));
    replicas.add(ReplicationTestUtil.createContainerReplica(containerID, 0, IN_SERVICE,
        QUASI_CLOSED, 1L, OzoneConsts.GB, dn4, dn4.getID()));

    // Update mocks: container is over-replicated
    when(containerManager.getContainerReplicas(containerID)).thenReturn(replicas);
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet()))
        .thenReturn(new ContainerHealthResult.OverReplicatedHealthResult(containerInfo, 1, false));

    // Test 1: Config ENABLED - should allow balancing of over-replicated + quasi-closed
    balancerConfiguration.setIncludeNonStandardContainers(true);
    ContainerBalancerSelectionCriteria configEnabled = new ContainerBalancerSelectionCriteria(
        balancerConfiguration, nodeManager, replicationManager,
        containerManager, findSourceStrategy, new HashMap<>());

    // All replicas (including QUASI_CLOSED) can be moved because:
    // - Config is enabled
    // - Container has minimum 3 CLOSED replicas
    // - Container is over-replicated (allowed by config)
    assertFalse(configEnabled.shouldBeExcluded(containerID, source, 0L));
    assertFalse(configEnabled.shouldBeExcluded(containerID, dn2, 0L));
    assertFalse(configEnabled.shouldBeExcluded(containerID, dn3, 0L));
    assertFalse(configEnabled.shouldBeExcluded(containerID, dn4, 0L));

    // Test 2: Config DISABLED (default) - should exclude over-replicated containers
    balancerConfiguration.setIncludeNonStandardContainers(false);
    ContainerBalancerSelectionCriteria criteriaDisabled = new ContainerBalancerSelectionCriteria(
        balancerConfiguration, nodeManager, replicationManager,
        containerManager, findSourceStrategy, new HashMap<>());

    // Over-replicated containers are excluded when config is disabled
    assertTrue(criteriaDisabled.shouldBeExcluded(containerID, source, 0L));
    assertTrue(criteriaDisabled.shouldBeExcluded(containerID, dn4, 0L));
  }

  @Test
  public void shouldExcludeEmptyQuasiClosedReplicas() throws Exception {
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn3 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn4 = MockDatanodeDetails.randomDatanodeDetails();

    // Over-replicated CLOSED container with 3 CLOSED + 1 QUASI_CLOSED replica (empty)
    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(ReplicationTestUtil.createContainerReplica(containerID, 0, IN_SERVICE,
        CLOSED, 1L, OzoneConsts.GB, source, source.getID()));
    replicas.add(ReplicationTestUtil.createContainerReplica(containerID, 0, IN_SERVICE,
        CLOSED, 1L, OzoneConsts.GB, dn2, dn2.getID()));
    replicas.add(ReplicationTestUtil.createContainerReplica(containerID, 0, IN_SERVICE,
        CLOSED, 1L, OzoneConsts.GB, dn3, dn3.getID()));

    // Empty QUASI_CLOSED replica
    ContainerReplica emptyQuasiClosedReplica = ContainerReplica.newBuilder()
        .setContainerID(containerID)
        .setContainerState(QUASI_CLOSED)
        .setSequenceId(0L)
        .setKeyCount(0)
        .setBytesUsed(0)
        .setReplicaIndex(0)
        .setDatanodeDetails(dn4)
        .setEmpty(true)
        .build();
    replicas.add(emptyQuasiClosedReplica);

    when(containerManager.getContainerReplicas(containerID)).thenReturn(replicas);
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet()))
        .thenReturn(new ContainerHealthResult.OverReplicatedHealthResult(containerInfo, 1, false));

    balancerConfiguration.setIncludeNonStandardContainers(true);
    ContainerBalancerSelectionCriteria configEnabled = new ContainerBalancerSelectionCriteria(
        balancerConfiguration, nodeManager, replicationManager,
        containerManager, findSourceStrategy, new HashMap<>());

    // CLOSED replicas can be moved
    assertFalse(configEnabled.shouldBeExcluded(containerID, source, 0L));
    assertFalse(configEnabled.shouldBeExcluded(containerID, dn2, 0L));
    assertFalse(configEnabled.shouldBeExcluded(containerID, dn3, 0L));

    // Empty QUASI_CLOSED replica should be EXCLUDED
    assertTrue(configEnabled.shouldBeExcluded(containerID, dn4, 0L),
        "Empty QUASI_CLOSED replica should be excluded from balancing");
  }

  @Test
  public void shouldIncludeQuasiClosedContainers() throws Exception {
    DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails dn3 = MockDatanodeDetails.randomDatanodeDetails();

    // QUASI_CLOSED container with all QUASI_CLOSED replicas
    ContainerInfo quasiClosedContainer = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 1L,
        HddsProtos.LifeCycleState.QUASI_CLOSED, 1L, OzoneConsts.GB);
    ContainerID quasiClosedContainerID = quasiClosedContainer.containerID();

    Set<ContainerReplica> quasiClosedReplicas = new HashSet<>();
    quasiClosedReplicas.add(ReplicationTestUtil.createContainerReplica(quasiClosedContainerID, 0,
        IN_SERVICE, QUASI_CLOSED, 1L, OzoneConsts.GB, source, source.getID()));
    quasiClosedReplicas.add(ReplicationTestUtil.createContainerReplica(quasiClosedContainerID, 0,
        IN_SERVICE, QUASI_CLOSED, 1L, OzoneConsts.GB, dn2, dn2.getID()));
    quasiClosedReplicas.add(ReplicationTestUtil.createContainerReplica(quasiClosedContainerID, 0,
        IN_SERVICE, QUASI_CLOSED, 1L, OzoneConsts.GB, dn3, dn3.getID()));

    when(containerManager.getContainer(quasiClosedContainerID)).thenReturn(quasiClosedContainer);
    when(containerManager.getContainerReplicas(quasiClosedContainerID)).thenReturn(quasiClosedReplicas);
    when(replicationManager.isContainerReplicatingOrDeleting(quasiClosedContainerID)).thenReturn(false);
    when(replicationManager.getContainerReplicationHealth(eq(quasiClosedContainer), anySet()))
        .thenReturn(new ContainerHealthResult.OverReplicatedHealthResult(quasiClosedContainer, 1, false));

    // Test 1: Config ENABLED - should allow balancing of QUASI_CLOSED container
    balancerConfiguration.setIncludeNonStandardContainers(true);
    ContainerBalancerSelectionCriteria configEnabled = new ContainerBalancerSelectionCriteria(
        balancerConfiguration, nodeManager, replicationManager,
        containerManager, findSourceStrategy, new HashMap<>());

    // All QUASI_CLOSED replicas can be moved because:
    // - Config is enabled
    // - Container is QUASI_CLOSED
    // - All replicas are non-empty QUASI_CLOSED (consistent state)
    assertFalse(configEnabled.shouldBeExcluded(quasiClosedContainerID, source, 0L));
    assertFalse(configEnabled.shouldBeExcluded(quasiClosedContainerID, dn2, 0L));
    assertFalse(configEnabled.shouldBeExcluded(quasiClosedContainerID, dn3, 0L));

    // Test 2: Config DISABLED (default) - should exclude QUASI_CLOSED containers
    balancerConfiguration.setIncludeNonStandardContainers(false);
    ContainerBalancerSelectionCriteria criteriaDisabled = new ContainerBalancerSelectionCriteria(
        balancerConfiguration, nodeManager, replicationManager,
        containerManager, findSourceStrategy, new HashMap<>());

    // QUASI_CLOSED containers are excluded when config is disabled
    assertTrue(criteriaDisabled.shouldBeExcluded(quasiClosedContainerID, source, 0L));
    assertTrue(criteriaDisabled.shouldBeExcluded(quasiClosedContainerID, dn2, 0L));
  }
}
