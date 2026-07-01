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

package org.apache.hadoop.ozone.recon.persistence;

import static org.apache.ozone.recon.schema.generated.tables.UnhealthyContainersTable.UNHEALTHY_CONTAINERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.ContainerStateKey;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.UnhealthyContainerRecord;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Functional tests for {@link ContainerHealthSchemaManager#syncUnhealthyContainerRecordsAtomically}.
 */
public class TestContainerHealthSchemaManager extends AbstractReconSqlDBTest {

  private static final long ORIGINAL_TIMESTAMP = 1_000L;
  private static final long UPDATED_TIMESTAMP = 2_000L;

  private ContainerSchemaDefinition schemaDefinition;
  private ContainerHealthSchemaManager schemaManager;

  @BeforeEach
  public void setUpSchemaManager() {
    schemaDefinition = getSchemaDefinition(ContainerSchemaDefinition.class);
    schemaManager =
        new ContainerHealthSchemaManager(schemaDefinition, new OzoneConfiguration());
  }

  @Test
  public void testSyncInsertsNewUnhealthyRecords() {
    Map<ContainerStateKey, Long> existing = Collections.emptyMap();
    List<UnhealthyContainerRecord> desired = Arrays.asList(
        record(1L, UnHealthyContainerStates.MISSING, ORIGINAL_TIMESTAMP, 3, 0),
        record(2L, UnHealthyContainerStates.UNDER_REPLICATED, ORIGINAL_TIMESTAMP, 3, 2));

    schemaManager.syncUnhealthyContainerRecordsAtomically(existing, desired);

    assertEquals(Arrays.asList(1L, 2L), remainingContainerIds());
    assertEquals(2L, countRows());
  }

  @Test
  public void testSyncDeletesStaleRowsWhenContainerBecomesHealthy() {
    insert(record(1L, UnHealthyContainerStates.MISSING, ORIGINAL_TIMESTAMP, 3, 0));
    insert(record(1L, UnHealthyContainerStates.UNDER_REPLICATED, ORIGINAL_TIMESTAMP, 3, 2));
    insert(record(2L, UnHealthyContainerStates.MISSING, ORIGINAL_TIMESTAMP, 3, 0));

    Map<ContainerStateKey, Long> existing = existingMap(
        key(1L, UnHealthyContainerStates.MISSING),
        key(1L, UnHealthyContainerStates.UNDER_REPLICATED),
        key(2L, UnHealthyContainerStates.MISSING));

    // Container 1 recovered; container 2 still missing.
    List<UnhealthyContainerRecord> desired = Collections.singletonList(
        record(2L, UnHealthyContainerStates.MISSING, ORIGINAL_TIMESTAMP, 3, 0));

    schemaManager.syncUnhealthyContainerRecordsAtomically(existing, desired);

    assertEquals(Collections.singletonList(2L), remainingContainerIds());
    assertEquals(1L, countRows());
  }

  @Test
  public void testSyncUpdatesExistingRowAndDeletesChangedState() {
    insert(record(1L, UnHealthyContainerStates.MISSING, ORIGINAL_TIMESTAMP, 3, 0));

    Map<ContainerStateKey, Long> existing = existingMap(
        key(1L, UnHealthyContainerStates.MISSING));

    // Same container now under-replicated instead of missing.
    List<UnhealthyContainerRecord> desired = Collections.singletonList(
        record(1L, UnHealthyContainerStates.UNDER_REPLICATED, UPDATED_TIMESTAMP, 3, 2));

    schemaManager.syncUnhealthyContainerRecordsAtomically(existing, desired);

    assertEquals(Collections.singletonList(1L), remainingContainerIds());
    assertEquals(1L, countRows());
    UnhealthyContainerRecord row = fetchRow(1L, UnHealthyContainerStates.UNDER_REPLICATED);
    assertEquals(UPDATED_TIMESTAMP, row.getInStateSince());
    assertEquals(2, row.getActualReplicaCount());
    assertTrue(countByState(UnHealthyContainerStates.MISSING) == 0);
  }

  @Test
  public void testSyncUpdatesExistingRowInPlace() {
    insert(record(1L, UnHealthyContainerStates.UNDER_REPLICATED,
        ORIGINAL_TIMESTAMP, 3, 2, "old reason"));

    Map<ContainerStateKey, Long> existing = existingMap(
        key(1L, UnHealthyContainerStates.UNDER_REPLICATED));

    List<UnhealthyContainerRecord> desired = Collections.singletonList(
        record(1L, UnHealthyContainerStates.UNDER_REPLICATED,
            ORIGINAL_TIMESTAMP, 3, 1, "new reason"));

    schemaManager.syncUnhealthyContainerRecordsAtomically(existing, desired);

    assertEquals(1L, countRows());
    UnhealthyContainerRecord row = fetchRow(1L, UnHealthyContainerStates.UNDER_REPLICATED);
    assertEquals(ORIGINAL_TIMESTAMP, row.getInStateSince());
    assertEquals(1, row.getActualReplicaCount());
    assertEquals("new reason", row.getReason());
  }

  @Test
  public void testBatchDeleteStillRemovesAllScmStatesForContainer() {
    insert(record(1L, UnHealthyContainerStates.MISSING, ORIGINAL_TIMESTAMP, 3, 0));
    insert(record(1L, UnHealthyContainerStates.UNDER_REPLICATED, ORIGINAL_TIMESTAMP, 3, 2));
    insert(record(1L, UnHealthyContainerStates.ALL_REPLICAS_BAD, ORIGINAL_TIMESTAMP, 3, 0));

    schemaManager.batchDeleteSCMStatesForContainers(Collections.singletonList(1L));

    assertEquals(1L, countRows());
    assertEquals(1L, countByState(UnHealthyContainerStates.ALL_REPLICAS_BAD));
  }

  private void insert(UnhealthyContainerRecord record) {
    schemaManager.insertUnhealthyContainerRecords(Collections.singletonList(record));
  }

  private UnhealthyContainerRecord record(long id, UnHealthyContainerStates state,
      long timestamp, int expected, int actual) {
    return record(id, state, timestamp, expected, actual, "test");
  }

  private UnhealthyContainerRecord record(long id, UnHealthyContainerStates state,
      long timestamp, int expected, int actual, String reason) {
    return new UnhealthyContainerRecord(id, state.toString(), timestamp,
        expected, actual, expected - actual, reason);
  }

  private ContainerStateKey key(long id, UnHealthyContainerStates state) {
    return new ContainerStateKey(id, state.toString());
  }

  private Map<ContainerStateKey, Long> existingMap(ContainerStateKey... keys) {
    Map<ContainerStateKey, Long> existing = new HashMap<>();
    for (ContainerStateKey key : keys) {
      existing.put(key, ORIGINAL_TIMESTAMP);
    }
    return existing;
  }

  private List<Long> remainingContainerIds() {
    DSLContext dsl = schemaDefinition.getDSLContext();
    return dsl.selectDistinct(UNHEALTHY_CONTAINERS.CONTAINER_ID)
        .from(UNHEALTHY_CONTAINERS)
        .orderBy(UNHEALTHY_CONTAINERS.CONTAINER_ID.asc())
        .fetch(UNHEALTHY_CONTAINERS.CONTAINER_ID);
  }

  private long countRows() {
    DSLContext dsl = schemaDefinition.getDSLContext();
    return dsl.fetchCount(UNHEALTHY_CONTAINERS);
  }

  private long countByState(UnHealthyContainerStates state) {
    DSLContext dsl = schemaDefinition.getDSLContext();
    return dsl.fetchCount(dsl.selectFrom(UNHEALTHY_CONTAINERS)
        .where(UNHEALTHY_CONTAINERS.CONTAINER_STATE.eq(state.toString())));
  }

  private UnhealthyContainerRecord fetchRow(long id, UnHealthyContainerStates state) {
    List<UnhealthyContainerRecord> rows = schemaManager.getUnhealthyContainers(
        state, 0, 0, 100);
    for (UnhealthyContainerRecord row : rows) {
      if (row.getContainerId() == id) {
        return row;
      }
    }
    throw new AssertionError("Row not found for container " + id + " state " + state);
  }
}
