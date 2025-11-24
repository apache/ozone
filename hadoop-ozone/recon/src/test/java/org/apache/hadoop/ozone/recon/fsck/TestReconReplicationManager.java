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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2;
import org.apache.ozone.recon.schema.ContainerSchemaDefinitionV2;
import org.apache.ozone.recon.schema.generated.tables.daos.UnhealthyContainersV2Dao;
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
  private UnhealthyContainersV2Dao dao;
  private ContainerManager containerManager;
  private ReconReplicationManager reconRM;

  public TestReconReplicationManager() {
    super();
  }

  @BeforeEach
  public void setUp() throws Exception {
    dao = getDao(UnhealthyContainersV2Dao.class);
    schemaManagerV2 = new ContainerHealthSchemaManagerV2(
        getSchemaDefinition(ContainerSchemaDefinitionV2.class), dao);

    containerManager = mock(ContainerManager.class);
    PlacementPolicy placementPolicy = mock(PlacementPolicy.class);
    SCMContext scmContext = mock(SCMContext.class);
    NodeManager nodeManager = mock(NodeManager.class);

    // Mock SCM context to allow processing
    when(scmContext.isLeader()).thenReturn(true);
    when(scmContext.isInSafeMode()).thenReturn(false);

    // Create ReconReplicationManager
    reconRM = new ReconReplicationManager(
        new ReplicationManager.ReplicationManagerConfiguration(),
        new OzoneConfiguration(),
        containerManager,
        placementPolicy,
        placementPolicy,
        new EventQueue(),
        scmContext,
        nodeManager,
        Clock.system(ZoneId.systemDefault()),
        schemaManagerV2
    );
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
    org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainersV2 record =
        new org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainersV2();
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
}
