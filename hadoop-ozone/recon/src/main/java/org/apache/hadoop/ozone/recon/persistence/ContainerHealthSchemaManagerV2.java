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

import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME;
import static org.apache.ozone.recon.schema.generated.tables.UnhealthyContainersTable.UNHEALTHY_CONTAINERS;
import static org.jooq.impl.DSL.count;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.daos.UnhealthyContainersDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers;
import org.apache.ozone.recon.schema.generated.tables.records.UnhealthyContainersRecord;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.OrderField;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for UNHEALTHY_CONTAINERS table used by ContainerHealthTaskV2.
 */
@Singleton
public class ContainerHealthSchemaManagerV2 {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerHealthSchemaManagerV2.class);
  private static final int BATCH_INSERT_CHUNK_SIZE = 1000;

  private final UnhealthyContainersDao unhealthyContainersV2Dao;
  private final ContainerSchemaDefinition containerSchemaDefinitionV2;

  @Inject
  public ContainerHealthSchemaManagerV2(
      ContainerSchemaDefinition containerSchemaDefinitionV2,
      UnhealthyContainersDao unhealthyContainersV2Dao) {
    this.unhealthyContainersV2Dao = unhealthyContainersV2Dao;
    this.containerSchemaDefinitionV2 = containerSchemaDefinitionV2;
  }

  /**
   * Insert or update unhealthy container records in V2 table using TRUE batch insert.
   * Uses JOOQ's batch API for optimal performance (single SQL statement for all records).
   * Falls back to individual insert-or-update if batch insert fails (e.g., duplicate keys).
   */
  public void insertUnhealthyContainerRecords(List<UnhealthyContainerRecordV2> recs) {
    if (recs == null || recs.isEmpty()) {
      return;
    }

    if (LOG.isDebugEnabled()) {
      recs.forEach(rec -> LOG.debug("rec.getContainerId() : {}, rec.getContainerState(): {}",
          rec.getContainerId(), rec.getContainerState()));
    }

    DSLContext dslContext = containerSchemaDefinitionV2.getDSLContext();

    try {
      batchInsertInChunks(dslContext, recs);

      LOG.debug("Batch inserted {} unhealthy container records", recs.size());

    } catch (DataAccessException e) {
      // Batch insert failed (likely duplicate key) - fall back to insert-or-update per record
      LOG.warn("Batch insert failed, falling back to individual insert-or-update for {} records",
          recs.size(), e);
      fallbackInsertOrUpdate(recs);
    } catch (Exception e) {
      LOG.error("Failed to batch insert records into {}", UNHEALTHY_CONTAINERS_TABLE_NAME, e);
      throw new RuntimeException("Recon failed to insert " + recs.size() +
          " unhealthy container records.", e);
    }
  }

  private void batchInsertInChunks(DSLContext dslContext,
      List<UnhealthyContainerRecordV2> recs) {
    dslContext.transaction(configuration -> {
      DSLContext txContext = configuration.dsl();
      List<UnhealthyContainersRecord> records =
          new ArrayList<>(BATCH_INSERT_CHUNK_SIZE);

      for (int from = 0; from < recs.size(); from += BATCH_INSERT_CHUNK_SIZE) {
        int to = Math.min(from + BATCH_INSERT_CHUNK_SIZE, recs.size());
        records.clear();
        for (int i = from; i < to; i++) {
          records.add(toJooqRecord(txContext, recs.get(i)));
        }
        txContext.batchInsert(records).execute();
      }
    });
  }

  private void fallbackInsertOrUpdate(List<UnhealthyContainerRecordV2> recs) {
    try (Connection connection = containerSchemaDefinitionV2.getDataSource().getConnection()) {
      connection.setAutoCommit(false);
      try {
        for (UnhealthyContainerRecordV2 rec : recs) {
          UnhealthyContainers jooqRec = toJooqPojo(rec);
          try {
            unhealthyContainersV2Dao.insert(jooqRec);
          } catch (DataAccessException insertEx) {
            // Duplicate key - update existing record
            unhealthyContainersV2Dao.update(jooqRec);
          }
        }
        connection.commit();
      } catch (Exception innerEx) {
        connection.rollback();
        LOG.error("Transaction rolled back during fallback insert", innerEx);
        throw innerEx;
      } finally {
        connection.setAutoCommit(true);
      }
    } catch (Exception fallbackEx) {
      LOG.error("Failed to insert {} records even with fallback", recs.size(), fallbackEx);
      throw new RuntimeException("Recon failed to insert " + recs.size() +
          " unhealthy container records.", fallbackEx);
    }
  }

  private UnhealthyContainersRecord toJooqRecord(DSLContext txContext,
      UnhealthyContainerRecordV2 rec) {
    UnhealthyContainersRecord record = txContext.newRecord(UNHEALTHY_CONTAINERS);
    record.setContainerId(rec.getContainerId());
    record.setContainerState(rec.getContainerState());
    record.setInStateSince(rec.getInStateSince());
    record.setExpectedReplicaCount(rec.getExpectedReplicaCount());
    record.setActualReplicaCount(rec.getActualReplicaCount());
    record.setReplicaDelta(rec.getReplicaDelta());
    record.setReason(rec.getReason());
    return record;
  }

  private UnhealthyContainers toJooqPojo(UnhealthyContainerRecordV2 rec) {
    return new UnhealthyContainers(
        rec.getContainerId(),
        rec.getContainerState(),
        rec.getInStateSince(),
        rec.getExpectedReplicaCount(),
        rec.getActualReplicaCount(),
        rec.getReplicaDelta(),
        rec.getReason());
  }

  /**
   * Batch delete all health states for multiple containers.
   * This deletes all states generated from SCM/Recon health scans:
   * MISSING, EMPTY_MISSING, UNDER_REPLICATED, OVER_REPLICATED,
   * MIS_REPLICATED, NEGATIVE_SIZE and REPLICA_MISMATCH for all containers
   * in the list in a single transaction.
   *
   * <p>REPLICA_MISMATCH is included here because it is re-evaluated on every
   * scan cycle (just like the SCM-sourced states); omitting it would leave
   * stale REPLICA_MISMATCH records in the table after a mismatch is resolved.
   *
   * @param containerIds List of container IDs to delete states for
   */
  public void batchDeleteSCMStatesForContainers(List<Long> containerIds) {
    if (containerIds == null || containerIds.isEmpty()) {
      return;
    }

    DSLContext dslContext = containerSchemaDefinitionV2.getDSLContext();
    try {
      int deleted = dslContext.deleteFrom(UNHEALTHY_CONTAINERS)
          .where(UNHEALTHY_CONTAINERS.CONTAINER_ID.in(containerIds))
          .and(UNHEALTHY_CONTAINERS.CONTAINER_STATE.in(
              UnHealthyContainerStates.MISSING.toString(),
              UnHealthyContainerStates.EMPTY_MISSING.toString(),
              UnHealthyContainerStates.UNDER_REPLICATED.toString(),
              UnHealthyContainerStates.OVER_REPLICATED.toString(),
              UnHealthyContainerStates.MIS_REPLICATED.toString(),
              UnHealthyContainerStates.NEGATIVE_SIZE.toString(),
              UnHealthyContainerStates.REPLICA_MISMATCH.toString()))
          .execute();
      LOG.debug("Batch deleted {} health state records for {} containers",
          deleted, containerIds.size());
    } catch (Exception e) {
      LOG.error("Failed to batch delete health states for {} containers", containerIds.size(), e);
      throw new RuntimeException("Failed to batch delete health states", e);
    }
  }

  /**
   * Get summary of unhealthy containers grouped by state from V2 table.
   */
  public List<UnhealthyContainersSummaryV2> getUnhealthyContainersSummary() {
    DSLContext dslContext = containerSchemaDefinitionV2.getDSLContext();
    List<UnhealthyContainersSummaryV2> result = new ArrayList<>();

    try {
      return dslContext
          .select(UNHEALTHY_CONTAINERS.CONTAINER_STATE.as("containerState"),
              count().as("cnt"))
          .from(UNHEALTHY_CONTAINERS)
          .groupBy(UNHEALTHY_CONTAINERS.CONTAINER_STATE)
          .fetchInto(UnhealthyContainersSummaryV2.class);
    } catch (Exception e) {
      LOG.error("Failed to get summary from V2 table", e);
      return result;
    }
  }

  /**
   * Get unhealthy containers from V2 table.
   */
  public List<UnhealthyContainerRecordV2> getUnhealthyContainers(
      UnHealthyContainerStates state, long minContainerId, long maxContainerId, int limit) {
    DSLContext dslContext = containerSchemaDefinitionV2.getDSLContext();

    SelectQuery<Record> query = dslContext.selectQuery();
    query.addFrom(UNHEALTHY_CONTAINERS);

    Condition containerCondition;
    OrderField[] orderField;

    if (maxContainerId > 0) {
      containerCondition = UNHEALTHY_CONTAINERS.CONTAINER_ID.lessThan(maxContainerId);
      orderField = new OrderField[]{
          UNHEALTHY_CONTAINERS.CONTAINER_ID.desc(),
          UNHEALTHY_CONTAINERS.CONTAINER_STATE.asc()
      };
    } else {
      containerCondition = UNHEALTHY_CONTAINERS.CONTAINER_ID.greaterThan(minContainerId);
      orderField = new OrderField[]{
          UNHEALTHY_CONTAINERS.CONTAINER_ID.asc(),
          UNHEALTHY_CONTAINERS.CONTAINER_STATE.asc()
      };
    }

    if (state != null) {
      query.addConditions(containerCondition.and(
          UNHEALTHY_CONTAINERS.CONTAINER_STATE.eq(state.toString())));
    } else {
      query.addConditions(containerCondition);
    }

    query.addOrderBy(orderField);
    query.addLimit(limit);

    try {
      return query.fetchInto(UnhealthyContainersRecord.class).stream()
          .sorted(Comparator.comparingLong(UnhealthyContainersRecord::getContainerId))
          .map(record -> new UnhealthyContainerRecordV2(
              record.getContainerId(),
              record.getContainerState(),
              record.getInStateSince(),
              record.getExpectedReplicaCount(),
              record.getActualReplicaCount(),
              record.getReplicaDelta(),
              record.getReason()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      LOG.error("Failed to query V2 table", e);
      return new ArrayList<>();
    }
  }

  /**
   * Clear all records from V2 table (for testing).
   */
  @VisibleForTesting
  public void clearAllUnhealthyContainerRecords() {
    DSLContext dslContext = containerSchemaDefinitionV2.getDSLContext();
    try {
      dslContext.deleteFrom(UNHEALTHY_CONTAINERS).execute();
      LOG.info("Cleared all V2 unhealthy container records");
    } catch (Exception e) {
      LOG.error("Failed to clear V2 unhealthy container records", e);
    }
  }

  /**
   * POJO representing a record in UNHEALTHY_CONTAINERS table.
   */
  public static class UnhealthyContainerRecordV2 {
    private final long containerId;
    private final String containerState;
    private final long inStateSince;
    private final int expectedReplicaCount;
    private final int actualReplicaCount;
    private final int replicaDelta;
    private final String reason;

    public UnhealthyContainerRecordV2(long containerId, String containerState,
        long inStateSince, int expectedReplicaCount, int actualReplicaCount,
        int replicaDelta, String reason) {
      this.containerId = containerId;
      this.containerState = containerState;
      this.inStateSince = inStateSince;
      this.expectedReplicaCount = expectedReplicaCount;
      this.actualReplicaCount = actualReplicaCount;
      this.replicaDelta = replicaDelta;
      this.reason = reason;
    }

    public long getContainerId() {
      return containerId;
    }

    public String getContainerState() {
      return containerState;
    }

    public long getInStateSince() {
      return inStateSince;
    }

    public int getExpectedReplicaCount() {
      return expectedReplicaCount;
    }

    public int getActualReplicaCount() {
      return actualReplicaCount;
    }

    public int getReplicaDelta() {
      return replicaDelta;
    }

    public String getReason() {
      return reason;
    }
  }

  /**
   * POJO representing a summary record for unhealthy containers.
   */
  public static class UnhealthyContainersSummaryV2 {
    private final String containerState;
    private final int count;

    public UnhealthyContainersSummaryV2(String containerState, int count) {
      this.containerState = containerState;
      this.count = count;
    }

    public String getContainerState() {
      return containerState;
    }

    public int getCount() {
      return count;
    }
  }
}
