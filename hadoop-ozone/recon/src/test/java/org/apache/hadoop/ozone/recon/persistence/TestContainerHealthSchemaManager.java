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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.UnhealthyContainerRecord;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Functional tests for {@link ContainerHealthSchemaManager} delete handling.
 *
 * <p>{@code deleteScmStatesForContainers} coalesces long contiguous container
 * id runs into single range ({@code BETWEEN}) deletes while folding shorter
 * runs and scattered ids into chunked {@code IN} deletes. These tests assert
 * that both paths remove exactly the requested rows for contiguous, scattered,
 * and mixed id distributions, and that non-SCM states are never removed.</p>
 */
public class TestContainerHealthSchemaManager extends AbstractReconSqlDBTest {

  private ContainerSchemaDefinition schemaDefinition;
  private ContainerHealthSchemaManager schemaManager;

  @BeforeEach
  public void setUpSchemaManager() {
    schemaDefinition = getSchemaDefinition(ContainerSchemaDefinition.class);
    schemaManager =
        new ContainerHealthSchemaManager(schemaDefinition, new OzoneConfiguration());
  }

  @Test
  public void testDeleteContiguousRangeRemovesExactlyRequestedRows() {
    // A run long enough to be removed through the range-delete path.
    int runLength = ContainerHealthSchemaManager.MIN_RANGE_DELETE_RUN + 200;
    insertRange(1, runLength, UnHealthyContainerStates.UNDER_REPLICATED);
    // Survivors that are not part of the delete list.
    insertIds(UnHealthyContainerStates.UNDER_REPLICATED, 5000L, 5002L, 5004L);

    List<Long> idsToDelete = contiguous(1, runLength);
    idsToDelete.add(1L);              // duplicate -> must be de-duplicated
    Collections.shuffle(idsToDelete); // unsorted -> must be sorted internally

    schemaManager.batchDeleteSCMStatesForContainers(idsToDelete);

    assertEquals(Arrays.asList(5000L, 5002L, 5004L), remainingContainerIds());
  }

  @Test
  public void testDeleteScatteredIdsRemovesExactlyRequestedRows() {
    for (long id = 1; id <= 10; id++) {
      insertIds(UnHealthyContainerStates.MISSING, id);
    }

    // Delete only the odd ids, supplied out of order.
    schemaManager.batchDeleteSCMStatesForContainers(
        new ArrayList<>(Arrays.asList(9L, 1L, 5L, 7L, 3L)));

    assertEquals(Arrays.asList(2L, 4L, 6L, 8L, 10L), remainingContainerIds());
  }

  @Test
  public void testDeleteMixedRunAndScatteredRemovesExactlyRequestedRows() {
    int runLength = ContainerHealthSchemaManager.MIN_RANGE_DELETE_RUN + 100;
    insertRange(1, runLength, UnHealthyContainerStates.UNDER_REPLICATED);
    insertIds(UnHealthyContainerStates.UNDER_REPLICATED, 9001L, 9003L, 9005L);
    // Survivor not present in the delete list.
    insertIds(UnHealthyContainerStates.UNDER_REPLICATED, 9004L);

    List<Long> idsToDelete = contiguous(1, runLength);
    idsToDelete.add(9001L);
    idsToDelete.add(9003L);
    idsToDelete.add(9005L);

    schemaManager.batchDeleteSCMStatesForContainers(idsToDelete);

    assertEquals(Collections.singletonList(9004L), remainingContainerIds());
  }

  @Test
  public void testDeletePreservesNonScmStateInsideRange() {
    int runLength = ContainerHealthSchemaManager.MIN_RANGE_DELETE_RUN + 100;
    insertRange(1, runLength, UnHealthyContainerStates.UNDER_REPLICATED);
    // ALL_REPLICAS_BAD is not an SCM-generated state, so it must survive even
    // though container 50 falls inside the deleted [1, runLength] range.
    insertIds(UnHealthyContainerStates.ALL_REPLICAS_BAD, 50L);

    schemaManager.batchDeleteSCMStatesForContainers(contiguous(1, runLength));

    assertEquals(Collections.singletonList(50L), remainingContainerIds());
    assertEquals(0L, countByState(UnHealthyContainerStates.UNDER_REPLICATED));
    assertEquals(1L, countByState(UnHealthyContainerStates.ALL_REPLICAS_BAD));
  }

  private void insertRange(long startInclusive, long endInclusive,
      UnHealthyContainerStates state) {
    List<UnhealthyContainerRecord> records = new ArrayList<>();
    for (long id = startInclusive; id <= endInclusive; id++) {
      records.add(record(id, state));
    }
    schemaManager.insertUnhealthyContainerRecords(records);
  }

  private void insertIds(UnHealthyContainerStates state, long... ids) {
    List<UnhealthyContainerRecord> records = new ArrayList<>();
    for (long id : ids) {
      records.add(record(id, state));
    }
    schemaManager.insertUnhealthyContainerRecords(records);
  }

  private UnhealthyContainerRecord record(long id, UnHealthyContainerStates state) {
    return new UnhealthyContainerRecord(id, state.toString(), 1L, 3, 2, 1, "test");
  }

  private List<Long> contiguous(long startInclusive, long endInclusive) {
    List<Long> ids = new ArrayList<>();
    for (long id = startInclusive; id <= endInclusive; id++) {
      ids.add(id);
    }
    return ids;
  }

  private List<Long> remainingContainerIds() {
    DSLContext dsl = schemaDefinition.getDSLContext();
    return dsl.selectDistinct(UNHEALTHY_CONTAINERS.CONTAINER_ID)
        .from(UNHEALTHY_CONTAINERS)
        .orderBy(UNHEALTHY_CONTAINERS.CONTAINER_ID.asc())
        .fetch(UNHEALTHY_CONTAINERS.CONTAINER_ID);
  }

  private long countByState(UnHealthyContainerStates state) {
    DSLContext dsl = schemaDefinition.getDSLContext();
    return dsl.fetchCount(dsl.selectFrom(UNHEALTHY_CONTAINERS)
        .where(UNHEALTHY_CONTAINERS.CONTAINER_STATE.eq(state.toString())));
  }
}
