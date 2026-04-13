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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.records.UnhealthyContainersRecord;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.OrderField;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for UNHEALTHY_CONTAINERS table used by ContainerHealthTask.
 */
@Singleton
public class ContainerHealthSchemaManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerHealthSchemaManager.class);
  private static final int BATCH_INSERT_CHUNK_SIZE = 1000;

  /**
   * Maximum number of container IDs to include in a single
   * {@code DELETE … WHERE container_id IN (…)} statement.
   *
   * <p>Derby's SQL compiler translates each prepared statement into a Java
   * class.  A large IN-predicate generates a deeply nested expression tree
   * whose compiled bytecode can exceed the JVM hard limit of 65,535 bytes
   * per method (ERROR XBCM4).  Empirically, 5,000 IDs combined with the
   * 7-state container_state IN-predicate generates ~148 KB — more than
   * twice the limit.  1,000 IDs stays well under ~30 KB, providing a safe
   * 2× margin.</p>
   */
  static final int MAX_IN_CLAUSE_CHUNK_SIZE = 1_000;

  private final ContainerSchemaDefinition containerSchemaDefinition;

  @Inject
  public ContainerHealthSchemaManager(
      ContainerSchemaDefinition containerSchemaDefinition) {
    this.containerSchemaDefinition = containerSchemaDefinition;
  }

  /**
   * Insert unhealthy container records in UNHEALTHY_CONTAINERS table using
   * true batch insert.
   *
   * <p>In the health-task flow, inserts are preceded by delete in the same
   * transaction via {@link #replaceUnhealthyContainerRecordsAtomically(List, List)}.
   * Therefore duplicate-key fallback is not expected and this method fails fast
   * on any insert error.</p>
   */
  @VisibleForTesting
  public void insertUnhealthyContainerRecords(List<UnhealthyContainerRecord> recs) {
    if (recs == null || recs.isEmpty()) {
      return;
    }

    if (LOG.isDebugEnabled()) {
      recs.forEach(rec -> LOG.debug("rec.getContainerId() : {}, rec.getContainerState(): {}",
          rec.getContainerId(), rec.getContainerState()));
    }

    DSLContext dslContext = containerSchemaDefinition.getDSLContext();

    try {
      dslContext.transaction(configuration ->
          batchInsertInChunks(configuration.dsl(), recs));

      LOG.debug("Batch inserted {} unhealthy container records", recs.size());

    } catch (Exception e) {
      LOG.error("Failed to batch insert records into {}", UNHEALTHY_CONTAINERS_TABLE_NAME, e);
      throw new RuntimeException("Recon failed to insert " + recs.size() +
          " unhealthy container records.", e);
    }
  }

  private void batchInsertInChunks(DSLContext dslContext,
      List<UnhealthyContainerRecord> recs) {
    List<UnhealthyContainersRecord> records =
        new ArrayList<>(BATCH_INSERT_CHUNK_SIZE);

    for (int from = 0; from < recs.size(); from += BATCH_INSERT_CHUNK_SIZE) {
      int to = Math.min(from + BATCH_INSERT_CHUNK_SIZE, recs.size());
      records.clear();
      for (int i = from; i < to; i++) {
        records.add(toJooqRecord(dslContext, recs.get(i)));
      }
      dslContext.batchInsert(records).execute();
    }
  }

  private UnhealthyContainersRecord toJooqRecord(DSLContext txContext,
      UnhealthyContainerRecord rec) {
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

  /**
   * Batch delete all health states for multiple containers.
   * This deletes all states generated from SCM/Recon health scans:
   * MISSING, EMPTY_MISSING, UNDER_REPLICATED, OVER_REPLICATED,
   * MIS_REPLICATED, NEGATIVE_SIZE and REPLICA_MISMATCH for all containers
   * in the list.
   *
   * <p>REPLICA_MISMATCH is included here because it is re-evaluated on every
   * scan cycle (just like the SCM-sourced states); omitting it would leave
   * stale REPLICA_MISMATCH records in the table after a mismatch is resolved.
   *
   * <p><b>Derby bytecode limit:</b> Derby translates each SQL statement into
   * a Java class whose methods must each stay under the JVM 64 KB bytecode
   * limit.  A single {@code IN} predicate with more than ~2,000 values (when
   * combined with the 7-state container_state filter) overflows this limit
   * and causes {@code ERROR XBCM4}.  This method automatically partitions
   * {@code containerIds} into chunks of at most
   * {@value #MAX_IN_CLAUSE_CHUNK_SIZE}
   * IDs so callers never need to worry about the limit, regardless of how
   * many containers a scan cycle processes.
   *
   * @param containerIds List of container IDs to delete states for
   */
  public void batchDeleteSCMStatesForContainers(List<Long> containerIds) {
    if (containerIds == null || containerIds.isEmpty()) {
      return;
    }

    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    int totalDeleted = deleteScmStatesForContainers(dslContext, containerIds);
    LOG.debug("Batch deleted {} health state records for {} containers",
        totalDeleted, containerIds.size());
  }

  /**
   * Atomically replaces unhealthy rows for a given set of containers.
   * Delete and insert happen in the same DB transaction.
   */
  public void replaceUnhealthyContainerRecordsAtomically(
      List<Long> containerIdsToDelete,
      List<UnhealthyContainerRecord> recordsToInsert) {
    if ((containerIdsToDelete == null || containerIdsToDelete.isEmpty())
        && (recordsToInsert == null || recordsToInsert.isEmpty())) {
      return;
    }

    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    dslContext.transaction(configuration -> {
      DSLContext txContext = configuration.dsl();
      if (containerIdsToDelete != null && !containerIdsToDelete.isEmpty()) {
        deleteScmStatesForContainers(txContext, containerIdsToDelete);
      }
      if (recordsToInsert != null && !recordsToInsert.isEmpty()) {
        batchInsertInChunks(txContext, recordsToInsert);
      }
    });
  }

  private int deleteScmStatesForContainers(DSLContext dslContext,
      List<Long> containerIds) {
    int totalDeleted = 0;

    for (int from = 0; from < containerIds.size(); from += MAX_IN_CLAUSE_CHUNK_SIZE) {
      int to = Math.min(from + MAX_IN_CLAUSE_CHUNK_SIZE, containerIds.size());
      List<Long> chunk = containerIds.subList(from, to);

      int deleted = dslContext.deleteFrom(UNHEALTHY_CONTAINERS)
          .where(UNHEALTHY_CONTAINERS.CONTAINER_ID.in(chunk))
          .and(UNHEALTHY_CONTAINERS.CONTAINER_STATE.in(
              UnHealthyContainerStates.MISSING.toString(),
              UnHealthyContainerStates.EMPTY_MISSING.toString(),
              UnHealthyContainerStates.UNDER_REPLICATED.toString(),
              UnHealthyContainerStates.OVER_REPLICATED.toString(),
              UnHealthyContainerStates.MIS_REPLICATED.toString(),
              UnHealthyContainerStates.NEGATIVE_SIZE.toString(),
              UnHealthyContainerStates.REPLICA_MISMATCH.toString()))
          .execute();
      totalDeleted += deleted;
    }
    return totalDeleted;
  }

  /**
   * Returns previous in-state-since timestamps for tracked unhealthy states.
   * The key is a stable containerId + state tuple.
   *
   * <p>This method also chunks the container-id predicate internally to stay
   * within Derby's statement compilation limits. Large scan cycles in Recon can
   * easily touch tens of thousands of containers, and expanding all IDs into a
   * single {@code IN (...)} predicate causes Derby to generate bytecode that
   * exceeds the JVM constant-pool / method-size limits.</p>
   */
  public Map<ContainerStateKey, Long> getExistingInStateSinceByContainerIds(
      List<Long> containerIds) {
    if (containerIds == null || containerIds.isEmpty()) {
      return new HashMap<>();
    }

    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    Map<ContainerStateKey, Long> existing = new HashMap<>();
    try {
      for (int from = 0; from < containerIds.size(); from += MAX_IN_CLAUSE_CHUNK_SIZE) {
        int to = Math.min(from + MAX_IN_CLAUSE_CHUNK_SIZE, containerIds.size());
        List<Long> chunk = containerIds.subList(from, to);

        dslContext.select(
                UNHEALTHY_CONTAINERS.CONTAINER_ID,
                UNHEALTHY_CONTAINERS.CONTAINER_STATE,
                UNHEALTHY_CONTAINERS.IN_STATE_SINCE)
            .from(UNHEALTHY_CONTAINERS)
            .where(UNHEALTHY_CONTAINERS.CONTAINER_ID.in(chunk))
            .and(UNHEALTHY_CONTAINERS.CONTAINER_STATE.in(
                UnHealthyContainerStates.MISSING.toString(),
                UnHealthyContainerStates.EMPTY_MISSING.toString(),
                UnHealthyContainerStates.UNDER_REPLICATED.toString(),
                UnHealthyContainerStates.OVER_REPLICATED.toString(),
                UnHealthyContainerStates.MIS_REPLICATED.toString(),
                UnHealthyContainerStates.NEGATIVE_SIZE.toString(),
                UnHealthyContainerStates.REPLICA_MISMATCH.toString()))
            .forEach(record -> existing.put(
                new ContainerStateKey(record.get(UNHEALTHY_CONTAINERS.CONTAINER_ID),
                    record.get(UNHEALTHY_CONTAINERS.CONTAINER_STATE)),
                record.get(UNHEALTHY_CONTAINERS.IN_STATE_SINCE)));
      }
    } catch (Exception e) {
      LOG.warn("Failed to load existing inStateSince records. Falling back to current scan time.", e);
    }
    return existing;
  }

  /**
   * Preserve existing inStateSince values for records that remain in the
   * same unhealthy state across scan cycles.
   */
  public List<UnhealthyContainerRecord> applyExistingInStateSince(
      List<UnhealthyContainerRecord> records,
      List<Long> containerIds) {
    if (records == null || records.isEmpty()
        || containerIds == null || containerIds.isEmpty()) {
      return records;
    }

    Map<ContainerStateKey, Long> existingByContainerAndState =
        getExistingInStateSinceByContainerIds(containerIds);
    if (existingByContainerAndState.isEmpty()) {
      return records;
    }

    List<UnhealthyContainerRecord> withPreservedInStateSince =
        new ArrayList<>(records.size());
    for (UnhealthyContainerRecord record : records) {
      Long existingInStateSince = existingByContainerAndState.get(
          new ContainerStateKey(record.getContainerId(),
              record.getContainerState()));
      if (existingInStateSince == null) {
        withPreservedInStateSince.add(record);
      } else {
        withPreservedInStateSince.add(new UnhealthyContainerRecord(
            record.getContainerId(),
            record.getContainerState(),
            existingInStateSince,
            record.getExpectedReplicaCount(),
            record.getActualReplicaCount(),
            record.getReplicaDelta(),
            record.getReason()));
      }
    }
    return withPreservedInStateSince;
  }

  /**
   * Get summary of unhealthy containers grouped by state from UNHEALTHY_CONTAINERS table.
   */
  public List<UnhealthyContainersSummary> getUnhealthyContainersSummary() {
    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    List<UnhealthyContainersSummary> result = new ArrayList<>();

    try {
      return dslContext
          .select(UNHEALTHY_CONTAINERS.CONTAINER_STATE.as("containerState"),
              count().as("cnt"))
          .from(UNHEALTHY_CONTAINERS)
          .groupBy(UNHEALTHY_CONTAINERS.CONTAINER_STATE)
          .fetchInto(UnhealthyContainersSummary.class);
    } catch (Exception e) {
      LOG.error("Failed to get summary from UNHEALTHY_CONTAINERS table", e);
      return result;
    }
  }

  /**
   * Get unhealthy containers from UNHEALTHY_CONTAINERS table.
   */
  public List<UnhealthyContainerRecord> getUnhealthyContainers(
      UnHealthyContainerStates state, long minContainerId, long maxContainerId, int limit) {
    DSLContext dslContext = containerSchemaDefinition.getDSLContext();

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

    // Pre-buffer `limit` rows per JDBC round-trip instead of Derby's default of 1 row.
    query.fetchSize(limit);

    try {
      Stream<UnhealthyContainersRecord> stream =
          query.fetchInto(UnhealthyContainersRecord.class).stream();

      if (maxContainerId > 0) {
        // Reverse-pagination path: SQL orders DESC (to get the last `limit` rows before
        // maxContainerId); re-sort to ASC so callers always see ascending container IDs.
        stream = stream.sorted(Comparator.comparingLong(UnhealthyContainersRecord::getContainerId));
      }
      // Forward-pagination path: SQL already orders ASC — no Java re-sort needed.

      return stream.map(record -> new UnhealthyContainerRecord(
              record.getContainerId(),
              record.getContainerState(),
              record.getInStateSince(),
              record.getExpectedReplicaCount(),
              record.getActualReplicaCount(),
              record.getReplicaDelta(),
              record.getReason()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      LOG.error("Failed to query UNHEALTHY_CONTAINERS table", e);
      return new ArrayList<>();
    }
  }

  /**
   * Clear all records from UNHEALTHY_CONTAINERS table (for testing).
   */
  @VisibleForTesting
  public void clearAllUnhealthyContainerRecords() {
    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    try {
      dslContext.deleteFrom(UNHEALTHY_CONTAINERS).execute();
      LOG.info("Cleared all UNHEALTHY_CONTAINERS table's unhealthy container records");
    } catch (Exception e) {
      LOG.error("Failed to clear UNHEALTHY_CONTAINERS table's unhealthy container records", e);
    }
  }

  /**
   * POJO representing a record in UNHEALTHY_CONTAINERS table.
   */
  public static class UnhealthyContainerRecord {
    private final long containerId;
    private final String containerState;
    private final long inStateSince;
    private final int expectedReplicaCount;
    private final int actualReplicaCount;
    private final int replicaDelta;
    private final String reason;

    public UnhealthyContainerRecord(long containerId, String containerState,
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
   * Key type for (containerId, state).
   */
  public static final class ContainerStateKey {
    private final long containerId;
    private final String state;

    public ContainerStateKey(long containerId, String state) {
      this.containerId = containerId;
      this.state = state;
    }

    public long getContainerId() {
      return containerId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ContainerStateKey)) {
        return false;
      }
      ContainerStateKey that = (ContainerStateKey) other;
      return containerId == that.containerId && state.equals(that.state);
    }

    @Override
    public int hashCode() {
      return Objects.hash(containerId, state);
    }
  }

  /**
   * POJO representing a summary record for unhealthy containers.
   */
  public static class UnhealthyContainersSummary {
    private final String containerState;
    private final int count;

    public UnhealthyContainersSummary(String containerState, int count) {
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
