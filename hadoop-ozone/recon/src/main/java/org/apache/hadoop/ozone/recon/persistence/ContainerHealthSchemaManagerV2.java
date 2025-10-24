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

import static org.apache.ozone.recon.schema.ContainerSchemaDefinitionV2.UNHEALTHY_CONTAINERS_V2_TABLE_NAME;
import static org.apache.ozone.recon.schema.generated.tables.UnhealthyContainersV2Table.UNHEALTHY_CONTAINERS_V2;
import static org.jooq.impl.DSL.count;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ozone.recon.schema.ContainerSchemaDefinitionV2;
import org.apache.ozone.recon.schema.ContainerSchemaDefinitionV2.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.daos.UnhealthyContainersV2Dao;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainersV2;
import org.apache.ozone.recon.schema.generated.tables.records.UnhealthyContainersV2Record;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.OrderField;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for UNHEALTHY_CONTAINERS_V2 table used by ContainerHealthTaskV2.
 * This is independent from ContainerHealthSchemaManager to allow both
 * implementations to run in parallel.
 */
@Singleton
public class ContainerHealthSchemaManagerV2 {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerHealthSchemaManagerV2.class);

  private final UnhealthyContainersV2Dao unhealthyContainersV2Dao;
  private final ContainerSchemaDefinitionV2 containerSchemaDefinitionV2;

  @Inject
  public ContainerHealthSchemaManagerV2(
      ContainerSchemaDefinitionV2 containerSchemaDefinitionV2,
      UnhealthyContainersV2Dao unhealthyContainersV2Dao) {
    this.unhealthyContainersV2Dao = unhealthyContainersV2Dao;
    this.containerSchemaDefinitionV2 = containerSchemaDefinitionV2;
  }

  /**
   * Insert or update unhealthy container records in V2 table.
   * Uses DAO pattern with try-insert-catch-update for Derby compatibility.
   */
  public void insertUnhealthyContainerRecords(List<UnhealthyContainerRecordV2> recs) {
    if (LOG.isDebugEnabled()) {
      recs.forEach(rec -> LOG.debug("rec.getContainerId() : {}, rec.getContainerState(): {}",
          rec.getContainerId(), rec.getContainerState()));
    }

    try (Connection connection = containerSchemaDefinitionV2.getDataSource().getConnection()) {
      connection.setAutoCommit(false); // Turn off auto-commit for transactional control
      try {
        for (UnhealthyContainerRecordV2 rec : recs) {
          UnhealthyContainersV2 jooqRec = new UnhealthyContainersV2(
              rec.getContainerId(),
              rec.getContainerState(),
              rec.getInStateSince(),
              rec.getExpectedReplicaCount(),
              rec.getActualReplicaCount(),
              rec.getReplicaDelta(),
              rec.getReason());

          try {
            unhealthyContainersV2Dao.insert(jooqRec);
          } catch (DataAccessException dataAccessException) {
            // Log the error and update the existing record if ConstraintViolationException occurs
            unhealthyContainersV2Dao.update(jooqRec);
            LOG.debug("Error while inserting unhealthy container record: {}", rec, dataAccessException);
          }
        }
        connection.commit(); // Commit all inserted/updated records
      } catch (Exception innerException) {
        connection.rollback(); // Rollback transaction if an error occurs inside processing
        LOG.error("Transaction rolled back due to error", innerException);
        throw innerException;
      } finally {
        connection.setAutoCommit(true); // Reset auto-commit before the connection is auto-closed
      }
    } catch (Exception e) {
      LOG.error("Failed to insert records into {} ", UNHEALTHY_CONTAINERS_V2_TABLE_NAME, e);
      throw new RuntimeException("Recon failed to insert " + recs.size() + " unhealthy container records.", e);
    }
  }

  /**
   * Delete a specific unhealthy container record from V2 table.
   */
  public void deleteUnhealthyContainer(long containerId, Object state) {
    DSLContext dslContext = containerSchemaDefinitionV2.getDSLContext();
    try {
      String stateStr = (state instanceof UnHealthyContainerStates)
          ? ((UnHealthyContainerStates) state).toString()
          : state.toString();
      dslContext.deleteFrom(UNHEALTHY_CONTAINERS_V2)
          .where(UNHEALTHY_CONTAINERS_V2.CONTAINER_ID.eq(containerId))
          .and(UNHEALTHY_CONTAINERS_V2.CONTAINER_STATE.eq(stateStr))
          .execute();
      LOG.debug("Deleted container {} with state {} from V2 table", containerId, state);
    } catch (Exception e) {
      LOG.error("Failed to delete container {} from V2 table", containerId, e);
    }
  }

  /**
   * Delete all records for a specific container (all states).
   */
  public void deleteAllStatesForContainer(long containerId) {
    DSLContext dslContext = containerSchemaDefinitionV2.getDSLContext();
    try {
      int deleted = dslContext.deleteFrom(UNHEALTHY_CONTAINERS_V2)
          .where(UNHEALTHY_CONTAINERS_V2.CONTAINER_ID.eq(containerId))
          .execute();
      LOG.debug("Deleted {} records for container {} from V2 table", deleted, containerId);
    } catch (Exception e) {
      LOG.error("Failed to delete all states for container {} from V2 table", containerId, e);
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
          .select(UNHEALTHY_CONTAINERS_V2.CONTAINER_STATE.as("containerState"),
              count().as("cnt"))
          .from(UNHEALTHY_CONTAINERS_V2)
          .groupBy(UNHEALTHY_CONTAINERS_V2.CONTAINER_STATE)
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
    query.addFrom(UNHEALTHY_CONTAINERS_V2);

    Condition containerCondition;
    OrderField[] orderField;

    if (maxContainerId > 0) {
      containerCondition = UNHEALTHY_CONTAINERS_V2.CONTAINER_ID.lessThan(maxContainerId);
      orderField = new OrderField[]{
          UNHEALTHY_CONTAINERS_V2.CONTAINER_ID.desc(),
          UNHEALTHY_CONTAINERS_V2.CONTAINER_STATE.asc()
      };
    } else {
      containerCondition = UNHEALTHY_CONTAINERS_V2.CONTAINER_ID.greaterThan(minContainerId);
      orderField = new OrderField[]{
          UNHEALTHY_CONTAINERS_V2.CONTAINER_ID.asc(),
          UNHEALTHY_CONTAINERS_V2.CONTAINER_STATE.asc()
      };
    }

    if (state != null) {
      query.addConditions(containerCondition.and(
          UNHEALTHY_CONTAINERS_V2.CONTAINER_STATE.eq(state.toString())));
    } else {
      query.addConditions(containerCondition);
    }

    query.addOrderBy(orderField);
    query.addLimit(limit);

    try {
      return query.fetchInto(UnhealthyContainersV2Record.class).stream()
          .sorted(Comparator.comparingLong(UnhealthyContainersV2Record::getContainerId))
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
  public void clearAllUnhealthyContainerRecords() {
    DSLContext dslContext = containerSchemaDefinitionV2.getDSLContext();
    try {
      dslContext.deleteFrom(UNHEALTHY_CONTAINERS_V2).execute();
      LOG.info("Cleared all V2 unhealthy container records");
    } catch (Exception e) {
      LOG.error("Failed to clear V2 unhealthy container records", e);
    }
  }

  /**
   * POJO representing a record in UNHEALTHY_CONTAINERS_V2 table.
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
