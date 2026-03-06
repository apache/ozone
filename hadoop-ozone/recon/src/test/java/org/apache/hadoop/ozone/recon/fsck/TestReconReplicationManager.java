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

package org.apache.hadoop.ozone.recon.fsck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.daos.UnhealthyContainersDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for ReconReplicationManager Local ReplicationManager.
 *
 * These tests verify that:
 * 1. ReconReplicationManager can be instantiated properly
 * 2. processAll() runs without errors
 * 3. Database operations work correctly
 * 4. It doesn't rely on RPC calls to SCM
 *
 * Note: Detailed health state testing requires integration tests with real
 * ContainerManager, PlacementPolicy, and NodeManager implementations, as the
 * health check logic in SCM's ReplicationManager is complex and depends on
 * many factors beyond simple mocking.
 */
public class TestReconReplicationManager extends AbstractReconSqlDBTest {

  private ContainerHealthSchemaManagerV2 schemaManagerV2;
  private UnhealthyContainersDao dao;
  private ContainerManager containerManager;
  private ReconReplicationManager reconRM;

  public TestReconReplicationManager() {
    super();
  }

  @BeforeEach
  public void setUp() throws Exception {
    dao = getDao(UnhealthyContainersDao.class);
    schemaManagerV2 = new ContainerHealthSchemaManagerV2(
        getSchemaDefinition(ContainerSchemaDefinition.class), dao);

    containerManager = mock(ContainerManager.class);
    PlacementPolicy placementPolicy = mock(PlacementPolicy.class);
    SCMContext scmContext = mock(SCMContext.class);
    NodeManager nodeManager = mock(NodeManager.class);

    // Mock SCM context to allow processing
    when(scmContext.isLeader()).thenReturn(true);
    when(scmContext.isInSafeMode()).thenReturn(false);

    // Create ReconReplicationManager
    ReconReplicationManager.InitContext initContext =
        ReconReplicationManager.InitContext.newBuilder()
            .setRmConf(new ReplicationManager.ReplicationManagerConfiguration())
            .setConf(new OzoneConfiguration())
            .setContainerManager(containerManager)
            .setRatisContainerPlacement(placementPolicy)
            .setEcContainerPlacement(placementPolicy)
            .setEventPublisher(new EventQueue())
            .setScmContext(scmContext)
            .setNodeManager(nodeManager)
            .setClock(Clock.system(ZoneId.systemDefault()))
            .build();

    reconRM = new ReconReplicationManager(initContext, schemaManagerV2);
  }

  @Test
  public void testProcessAllStoresEmptyMissingAndNegativeSizeRecords()
      throws Exception {
    final long emptyMissingContainerId = 101L;
    final long negativeSizeContainerId = 202L;

    ContainerInfo emptyMissingContainer = mockContainerInfo(
        emptyMissingContainerId, 0, 1024L, 3);
    ContainerInfo negativeSizeContainer = mockContainerInfo(
        negativeSizeContainerId, 7, -1L, 3);
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(emptyMissingContainer);
    containers.add(negativeSizeContainer);

    Set<ContainerReplica> emptyReplicas = Collections.emptySet();
    Set<ContainerReplica> underReplicatedReplicas = new HashSet<>();
    underReplicatedReplicas.add(mock(ContainerReplica.class));
    underReplicatedReplicas.add(mock(ContainerReplica.class));

    when(containerManager.getContainers()).thenReturn(containers);
    when(containerManager.getContainer(ContainerID.valueOf(emptyMissingContainerId)))
        .thenReturn(emptyMissingContainer);
    when(containerManager.getContainer(ContainerID.valueOf(negativeSizeContainerId)))
        .thenReturn(negativeSizeContainer);
    when(containerManager.getContainerReplicas(ContainerID.valueOf(emptyMissingContainerId)))
        .thenReturn(emptyReplicas);
    when(containerManager.getContainerReplicas(ContainerID.valueOf(negativeSizeContainerId)))
        .thenReturn(underReplicatedReplicas);

    // Deterministically inject health states for this test to verify DB writes.
    reconRM = createStateInjectingReconRM(
        emptyMissingContainerId, negativeSizeContainerId);
    reconRM.processAll();

    List<ContainerHealthSchemaManagerV2.UnhealthyContainerRecordV2> emptyMissing =
        schemaManagerV2.getUnhealthyContainers(
            UnHealthyContainerStates.EMPTY_MISSING, 0, 0, 100);
    assertEquals(1, emptyMissing.size());
    assertEquals(emptyMissingContainerId, emptyMissing.get(0).getContainerId());

    List<ContainerHealthSchemaManagerV2.UnhealthyContainerRecordV2> negativeSize =
        schemaManagerV2.getUnhealthyContainers(
            UnHealthyContainerStates.NEGATIVE_SIZE, 0, 0, 100);
    assertEquals(1, negativeSize.size());
    assertEquals(negativeSizeContainerId, negativeSize.get(0).getContainerId());
  }

  @Test
  public void testProcessAllStoresAllPrimaryV2States() throws Exception {
    final long missingContainerId = 301L;
    final long underReplicatedContainerId = 302L;
    final long overReplicatedContainerId = 303L;
    final long misReplicatedContainerId = 304L;
    final long mismatchContainerId = 305L;

    List<ContainerInfo> containers = Arrays.asList(
        mockContainerInfo(missingContainerId, 10, 1024L, 3),
        mockContainerInfo(underReplicatedContainerId, 5, 1024L, 3),
        mockContainerInfo(overReplicatedContainerId, 5, 1024L, 3),
        mockContainerInfo(misReplicatedContainerId, 5, 1024L, 3),
        mockContainerInfo(mismatchContainerId, 5, 1024L, 3));
    when(containerManager.getContainers()).thenReturn(containers);

    Map<Long, Set<ContainerReplica>> replicasByContainer = new HashMap<>();
    replicasByContainer.put(missingContainerId, Collections.emptySet());
    replicasByContainer.put(underReplicatedContainerId,
        setOfMockReplicasWithChecksums(1000L, 1000L));
    replicasByContainer.put(overReplicatedContainerId,
        setOfMockReplicasWithChecksums(1000L, 1000L, 1000L, 1000L));
    replicasByContainer.put(misReplicatedContainerId,
        setOfMockReplicasWithChecksums(1000L, 1000L, 1000L));
    replicasByContainer.put(mismatchContainerId,
        setOfMockReplicasWithChecksums(1000L, 2000L, 3000L));

    Map<Long, ContainerHealthState> stateByContainer = new HashMap<>();
    stateByContainer.put(missingContainerId, ContainerHealthState.MISSING);
    stateByContainer.put(underReplicatedContainerId,
        ContainerHealthState.UNDER_REPLICATED);
    stateByContainer.put(overReplicatedContainerId,
        ContainerHealthState.OVER_REPLICATED);
    stateByContainer.put(misReplicatedContainerId,
        ContainerHealthState.MIS_REPLICATED);

    for (ContainerInfo container : containers) {
      long containerId = container.getContainerID();
      when(containerManager.getContainer(ContainerID.valueOf(containerId)))
          .thenReturn(container);
      when(containerManager.getContainerReplicas(ContainerID.valueOf(containerId)))
          .thenReturn(replicasByContainer.get(containerId));
    }

    reconRM = createStateInjectingReconRM(stateByContainer);
    reconRM.processAll();

    assertEquals(1, schemaManagerV2.getUnhealthyContainers(
        UnHealthyContainerStates.MISSING, 0, 0, 10).size());
    assertEquals(1, schemaManagerV2.getUnhealthyContainers(
        UnHealthyContainerStates.UNDER_REPLICATED, 0, 0, 10).size());
    assertEquals(1, schemaManagerV2.getUnhealthyContainers(
        UnHealthyContainerStates.OVER_REPLICATED, 0, 0, 10).size());
    assertEquals(1, schemaManagerV2.getUnhealthyContainers(
        UnHealthyContainerStates.MIS_REPLICATED, 0, 0, 10).size());
    assertEquals(1, schemaManagerV2.getUnhealthyContainers(
        UnHealthyContainerStates.REPLICA_MISMATCH, 0, 0, 10).size());
  }

  @Test
  public void testReconReplicationManagerCreation() {
    // Verify ReconReplicationManager was created successfully
    assertNotNull(reconRM);
  }

  @Test
  public void testProcessAllWithNoContainers() throws Exception {
    // Setup: No containers
    when(containerManager.getContainers()).thenReturn(new ArrayList<>());

    // Execute - should not throw any exceptions
    reconRM.processAll();

    // Verify: Method completed without errors
    // No records should be in database since there are no containers
    assertEquals(0, dao.count());
  }

  @Test
  public void testProcessAllRunsMultipleTimes() throws Exception {
    // Setup: No containers
    when(containerManager.getContainers()).thenReturn(new ArrayList<>());

    // Execute multiple times - verify it's idempotent
    reconRM.processAll();
    assertEquals(0, dao.count());

    reconRM.processAll();
    assertEquals(0, dao.count());

    reconRM.processAll();
    assertEquals(0, dao.count());
  }

  @Test
  public void testDatabaseOperationsWork() throws Exception {
    // This test verifies that the database schema and operations work
    // Setup: No containers
    when(containerManager.getContainers()).thenReturn(new ArrayList<>());

    // Insert a test record directly
    org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers record =
        new org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers();
    record.setContainerId(999L);
    record.setContainerState("UNDER_REPLICATED");
    record.setInStateSince(System.currentTimeMillis());
    record.setExpectedReplicaCount(3);
    record.setActualReplicaCount(2);
    record.setReplicaDelta(1);
    record.setReason("Test record");
    dao.insert(record);

    assertEquals(1, dao.count());

    // Run processAll - the old record should persist because container 999
    // is not in containerManager (only records for containers being processed are cleaned up)
    reconRM.processAll();

    // Verify the old record persists (correct behavior - containers not in
    // containerManager should keep their records as they might indicate missing containers)
    assertEquals(1, dao.count());
  }

  @Test
  public void testSchemaManagerIntegration() {
    // Verify the schema manager is properly integrated
    assertNotNull(schemaManagerV2);

    // Verify we can perform batch operations
    // (This is a smoke test to ensure the wiring is correct)
    schemaManagerV2.batchDeleteSCMStatesForContainers(new ArrayList<>());
    schemaManagerV2.insertUnhealthyContainerRecords(new ArrayList<>());

    // No assertion needed - just verify no exceptions thrown
  }

  private ContainerInfo mockContainerInfo(long containerId, long numberOfKeys,
      long usedBytes, int requiredNodes) {
    ContainerInfo containerInfo = mock(ContainerInfo.class);
    ReplicationConfig replicationConfig = mock(ReplicationConfig.class);

    when(containerInfo.getContainerID()).thenReturn(containerId);
    when(containerInfo.containerID()).thenReturn(ContainerID.valueOf(containerId));
    when(containerInfo.getNumberOfKeys()).thenReturn(numberOfKeys);
    when(containerInfo.getUsedBytes()).thenReturn(usedBytes);
    when(containerInfo.getReplicationConfig()).thenReturn(replicationConfig);
    when(containerInfo.getState()).thenReturn(HddsProtos.LifeCycleState.CLOSED);
    when(replicationConfig.getRequiredNodes()).thenReturn(requiredNodes);
    return containerInfo;
  }

  private ReconReplicationManager createStateInjectingReconRM(
      long emptyMissingContainerId, long negativeSizeContainerId)
      throws Exception {
    Map<Long, ContainerHealthState> stateByContainer = new HashMap<>();
    stateByContainer.put(emptyMissingContainerId, ContainerHealthState.MISSING);
    stateByContainer.put(negativeSizeContainerId,
        ContainerHealthState.UNDER_REPLICATED);
    return createStateInjectingReconRM(stateByContainer);
  }

  private ReconReplicationManager createStateInjectingReconRM(
      Map<Long, ContainerHealthState> stateByContainer) throws Exception {
    PlacementPolicy placementPolicy = mock(PlacementPolicy.class);
    SCMContext scmContext = mock(SCMContext.class);
    NodeManager nodeManager = mock(NodeManager.class);
    when(scmContext.isLeader()).thenReturn(true);
    when(scmContext.isInSafeMode()).thenReturn(false);

    ReconReplicationManager.InitContext initContext =
        ReconReplicationManager.InitContext.newBuilder()
            .setRmConf(new ReplicationManager.ReplicationManagerConfiguration())
            .setConf(new OzoneConfiguration())
            .setContainerManager(containerManager)
            .setRatisContainerPlacement(placementPolicy)
            .setEcContainerPlacement(placementPolicy)
            .setEventPublisher(new EventQueue())
            .setScmContext(scmContext)
            .setNodeManager(nodeManager)
            .setClock(Clock.system(ZoneId.systemDefault()))
            .build();

    return new ReconReplicationManager(initContext, schemaManagerV2) {
      @Override
      protected boolean processContainer(ContainerInfo containerInfo,
          ReplicationQueue repQueue, ReplicationManagerReport report,
          boolean readOnly) {
        ReconReplicationManagerReport reconReport =
            (ReconReplicationManagerReport) report;
        ContainerHealthState state =
            stateByContainer.get(containerInfo.getContainerID());
        if (state != null) {
          reconReport.incrementAndSample(state, containerInfo);
          return true;
        }
        return false;
      }
    };
  }

  private Set<ContainerReplica> setOfMockReplicasWithChecksums(Long... checksums) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (Long checksum : checksums) {
      ContainerReplica replica = mock(ContainerReplica.class);
      when(replica.getDataChecksum()).thenReturn(checksum);
      replicas.add(replica);
    }
    return replicas;
  }
}
