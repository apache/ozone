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

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages CRUD operations for the QUASI_CLOSED_CONTAINERS table.
 *
 * <p>This manager tracks containers in the QUASI_CLOSED lifecycle state.
 * Unlike the UNHEALTHY_CONTAINERS table which tracks replication health,
 * this table tracks lifecycle state — containers that have been locally
 * closed by a datanode but not yet force-closed to CLOSED by SCM.
 */
@Singleton
public class QuasiClosedContainerSchemaManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(QuasiClosedContainerSchemaManager.class);

  public static final String QUASI_CLOSED_CONTAINERS_TABLE_NAME =
      "QUASI_CLOSED_CONTAINERS";

  static final int BATCH_INSERT_CHUNK_SIZE = 1_000;

  // Column definitions
  private static final Field<Long>    COL_CONTAINER_ID     =
      field(name("container_id"), SQLDataType.BIGINT);
  private static final Field<String>  COL_PIPELINE_ID      =
      field(name("pipeline_id"), SQLDataType.VARCHAR(64));
  private static final Field<Integer> COL_DATANODE_COUNT   =
      field(name("datanode_count"), SQLDataType.INTEGER);
  private static final Field<Long>    COL_KEY_COUNT        =
      field(name("key_count"), SQLDataType.BIGINT);
  private static final Field<Long>    COL_DATA_SIZE        =
      field(name("data_size"), SQLDataType.BIGINT);
  private static final Field<String>  COL_REPLICATION_TYPE =
      field(name("replication_type"), SQLDataType.VARCHAR(16));
  private static final Field<Integer> COL_REPLICATION_FACTOR =
      field(name("replication_factor"), SQLDataType.INTEGER);
  private static final Field<Long>    COL_STATE_ENTER_TIME =
      field(name("state_enter_time"), SQLDataType.BIGINT);
  private static final Field<Long>    COL_FIRST_SEEN_TIME  =
      field(name("first_seen_time"), SQLDataType.BIGINT);
  private static final Field<Long>    COL_LAST_SCAN_TIME   =
      field(name("last_scan_time"), SQLDataType.BIGINT);

  private final Configuration jooqConfiguration;

  @Inject
  public QuasiClosedContainerSchemaManager(Configuration jooqConfiguration) {
    this.jooqConfiguration = jooqConfiguration;
  }

  private DSLContext dsl() {
    return DSL.using(jooqConfiguration);
  }

  /**
   * Atomically replaces all quasi-closed container records.
   * Deletes all existing rows and inserts the new set in a single transaction.
   *
   * @param records new records to insert (may be empty to clear the table)
   */
  public void replaceAll(List<QuasiClosedContainerRecord> records) {
    dsl().transaction(configuration -> {
      DSLContext txCtx = DSL.using(configuration);
      // 1. Delete all existing records
      txCtx.deleteFrom(table(QUASI_CLOSED_CONTAINERS_TABLE_NAME)).execute();
      // 2. Batch-insert new records
      if (records != null && !records.isEmpty()) {
        batchInsert(txCtx, records);
      }
    });
    LOG.debug("Replaced {} quasi-closed container records",
        records == null ? 0 : records.size());
  }

  private void batchInsert(DSLContext txCtx, List<QuasiClosedContainerRecord> records) {
    for (int from = 0; from < records.size(); from += BATCH_INSERT_CHUNK_SIZE) {
      int to = Math.min(from + BATCH_INSERT_CHUNK_SIZE, records.size());
      List<QuasiClosedContainerRecord> chunk = records.subList(from, to);

      // Build a multi-row INSERT using jOOQ's fluent API
      org.jooq.InsertValuesStep10<Record, Long, String, Integer, Long, Long,
          String, Integer, Long, Long, Long> insert =
          txCtx.insertInto(table(QUASI_CLOSED_CONTAINERS_TABLE_NAME),
              COL_CONTAINER_ID, COL_PIPELINE_ID, COL_DATANODE_COUNT,
              COL_KEY_COUNT, COL_DATA_SIZE, COL_REPLICATION_TYPE,
              COL_REPLICATION_FACTOR, COL_STATE_ENTER_TIME,
              COL_FIRST_SEEN_TIME, COL_LAST_SCAN_TIME);

      for (QuasiClosedContainerRecord rec : chunk) {
        insert = insert.values(
            rec.getContainerId(),
            rec.getPipelineId(),
            rec.getDatanodeCount(),
            rec.getKeyCount(),
            rec.getDataSize(),
            rec.getReplicationType(),
            rec.getReplicationFactor(),
            rec.getStateEnterTime(),
            rec.getFirstSeenTime(),
            rec.getLastScanTime());
      }
      insert.execute();
    }
  }

  /**
   * Returns first_seen_time values for containers already tracked.
   * Used to preserve the timestamp across scan cycles.
   *
   * @param containerIds list of container IDs to look up
   * @return map of containerId to first_seen_time
   */
  public Map<Long, Long> getExistingFirstSeenTimes(List<Long> containerIds) {
    Map<Long, Long> result = new HashMap<>();
    if (containerIds == null || containerIds.isEmpty()) {
      return result;
    }
    try {
      dsl().select(COL_CONTAINER_ID, COL_FIRST_SEEN_TIME)
          .from(table(QUASI_CLOSED_CONTAINERS_TABLE_NAME))
          .where(COL_CONTAINER_ID.in(containerIds))
          .forEach(r -> result.put(
              r.get(COL_CONTAINER_ID),
              r.get(COL_FIRST_SEEN_TIME)));
    } catch (Exception e) {
      LOG.warn("Failed to load existing first_seen_time values. "
          + "Will use current time for all.", e);
    }
    return result;
  }

  /**
   * Paginated forward read of all quasi-closed containers.
   *
   * @param minContainerId cursor — only containers with ID &gt; minContainerId
   * @param limit          max rows to return
   * @return list of records in ascending container_id order
   */
  public List<QuasiClosedContainerRecord> getQuasiClosedContainers(
      long minContainerId, int limit) {
    SelectQuery<Record> query = dsl().selectQuery();
    query.addFrom(table(QUASI_CLOSED_CONTAINERS_TABLE_NAME));
    query.addConditions(COL_CONTAINER_ID.greaterThan(minContainerId));
    query.addOrderBy(COL_CONTAINER_ID.asc());
    query.addLimit(limit);
    query.fetchSize(limit);
    return fetchRecords(query);
  }

  /**
   * Paginated read of quasi-closed containers for a specific pipeline.
   *
   * @param pipelineId     filter by pipeline
   * @param minContainerId cursor
   * @param limit          max rows
   * @return list of records
   */
  public List<QuasiClosedContainerRecord> getByPipeline(
      String pipelineId, long minContainerId, int limit) {
    SelectQuery<Record> query = dsl().selectQuery();
    query.addFrom(table(QUASI_CLOSED_CONTAINERS_TABLE_NAME));
    query.addConditions(
        COL_PIPELINE_ID.eq(pipelineId)
            .and(COL_CONTAINER_ID.greaterThan(minContainerId)));
    query.addOrderBy(COL_CONTAINER_ID.asc());
    query.addLimit(limit);
    query.fetchSize(limit);
    return fetchRecords(query);
  }

  /**
   * Returns total count of quasi-closed containers currently tracked.
   */
  public long getCount() {
    try {
      return dsl().fetchCount(table(QUASI_CLOSED_CONTAINERS_TABLE_NAME));
    } catch (Exception e) {
      LOG.error("Failed to count QUASI_CLOSED_CONTAINERS", e);
      return 0L;
    }
  }

  private List<QuasiClosedContainerRecord> fetchRecords(SelectQuery<Record> query) {
    List<QuasiClosedContainerRecord> result = new ArrayList<>();
    try {
      query.fetch().forEach(r -> result.add(new QuasiClosedContainerRecord(
          r.get(COL_CONTAINER_ID),
          r.get(COL_PIPELINE_ID),
          r.get(COL_DATANODE_COUNT),
          r.get(COL_KEY_COUNT),
          r.get(COL_DATA_SIZE),
          r.get(COL_REPLICATION_TYPE),
          r.get(COL_REPLICATION_FACTOR),
          r.get(COL_STATE_ENTER_TIME),
          r.get(COL_FIRST_SEEN_TIME),
          r.get(COL_LAST_SCAN_TIME))));
    } catch (Exception e) {
      LOG.error("Failed to query QUASI_CLOSED_CONTAINERS", e);
    }
    return result;
  }

  @VisibleForTesting
  public void clearAll() {
    try {
      dsl().deleteFrom(table(QUASI_CLOSED_CONTAINERS_TABLE_NAME)).execute();
    } catch (Exception e) {
      LOG.error("Failed to clear QUASI_CLOSED_CONTAINERS", e);
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // POJO
  // ──────────────────────────────────────────────────────────────────────────

  /**
   * Immutable record representing one row in QUASI_CLOSED_CONTAINERS.
   */
  public static final class QuasiClosedContainerRecord {
    private final long containerId;
    private final String pipelineId;
    private final int datanodeCount;
    private final long keyCount;
    private final long dataSize;
    private final String replicationType;
    private final int replicationFactor;
    private final long stateEnterTime;
    private final long firstSeenTime;
    private final long lastScanTime;

    public QuasiClosedContainerRecord(
        long containerId, String pipelineId, int datanodeCount,
        long keyCount, long dataSize, String replicationType,
        int replicationFactor, long stateEnterTime,
        long firstSeenTime, long lastScanTime) {
      this.containerId = containerId;
      this.pipelineId = pipelineId;
      this.datanodeCount = datanodeCount;
      this.keyCount = keyCount;
      this.dataSize = dataSize;
      this.replicationType = replicationType;
      this.replicationFactor = replicationFactor;
      this.stateEnterTime = stateEnterTime;
      this.firstSeenTime = firstSeenTime;
      this.lastScanTime = lastScanTime;
    }

    public long getContainerId() { return containerId; }
    public String getPipelineId() { return pipelineId; }
    public int getDatanodeCount() { return datanodeCount; }
    public long getKeyCount() { return keyCount; }
    public long getDataSize() { return dataSize; }
    public String getReplicationType() { return replicationType; }
    public int getReplicationFactor() { return replicationFactor; }
    public long getStateEnterTime() { return stateEnterTime; }
    public long getFirstSeenTime() { return firstSeenTime; }
    public long getLastScanTime() { return lastScanTime; }
  }
}
