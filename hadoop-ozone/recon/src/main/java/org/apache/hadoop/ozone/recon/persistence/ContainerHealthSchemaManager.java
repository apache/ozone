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
import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates.ALL_REPLICAS_BAD;
import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates.UNDER_REPLICATED;
import static org.apache.ozone.recon.schema.generated.tables.UnhealthyContainersTable.UNHEALTHY_CONTAINERS;
import static org.jooq.impl.DSL.count;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.sql.Connection;
import java.util.List;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersSummary;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.daos.UnhealthyContainersDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers;
import org.apache.ozone.recon.schema.generated.tables.records.UnhealthyContainersRecord;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide a high level API to access the Container Schema.
 */
@Singleton
public class ContainerHealthSchemaManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerHealthSchemaManager.class);

  private final UnhealthyContainersDao unhealthyContainersDao;
  private final ContainerSchemaDefinition containerSchemaDefinition;

  @Inject
  public ContainerHealthSchemaManager(
      ContainerSchemaDefinition containerSchemaDefinition,
      UnhealthyContainersDao unhealthyContainersDao) {
    this.unhealthyContainersDao = unhealthyContainersDao;
    this.containerSchemaDefinition = containerSchemaDefinition;
  }

  /**
   * Get a batch of unhealthy containers, starting at offset and returning
   * limit records. If a null value is passed for state, then unhealthy
   * containers in all states will be returned. Otherwise, only containers
   * matching the given state will be returned.
   * @param state Return only containers in this state, or all containers if
   *              null
   * @param offset The starting record to return in the result set. The first
   *               record is at zero.
   * @param limit The total records to return
   * @return List of unhealthy containers.
   */
  public List<UnhealthyContainers> getUnhealthyContainers(
      UnHealthyContainerStates state, int offset, int limit) {
    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    SelectQuery<Record> query = dslContext.selectQuery();
    query.addFrom(UNHEALTHY_CONTAINERS);
    if (state != null) {
      if (state.equals(ALL_REPLICAS_BAD)) {
        query.addConditions(UNHEALTHY_CONTAINERS.CONTAINER_STATE
            .eq(UNDER_REPLICATED.toString()));
        query.addConditions(UNHEALTHY_CONTAINERS.ACTUAL_REPLICA_COUNT.eq(0));
      } else {
        query.addConditions(
            UNHEALTHY_CONTAINERS.CONTAINER_STATE.eq(state.toString()));
      }
    }
    query.addOrderBy(UNHEALTHY_CONTAINERS.CONTAINER_ID.asc(),
        UNHEALTHY_CONTAINERS.CONTAINER_STATE.asc());
    query.addOffset(offset);
    query.addLimit(limit);

    return query.fetchInto(UnhealthyContainers.class);
  }

  /**
   * Obtain a count of all containers in each state. If there are no unhealthy
   * containers an empty list will be returned. If there are unhealthy
   * containers for a certain state, no entry will be returned for it.
   * @return Count of unhealthy containers in each state
   */
  public List<UnhealthyContainersSummary> getUnhealthyContainersSummary() {
    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    return dslContext
        .select(UNHEALTHY_CONTAINERS.CONTAINER_STATE.as("containerState"),
            count().as("cnt"))
        .from(UNHEALTHY_CONTAINERS)
        .groupBy(UNHEALTHY_CONTAINERS.CONTAINER_STATE)
        .fetchInto(UnhealthyContainersSummary.class);
  }

  public Cursor<UnhealthyContainersRecord> getAllUnhealthyRecordsCursor() {
    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    return dslContext
        .selectFrom(UNHEALTHY_CONTAINERS)
        .orderBy(UNHEALTHY_CONTAINERS.CONTAINER_ID.asc())
        .fetchLazy();
  }

  public void insertUnhealthyContainerRecords(List<UnhealthyContainers> recs) {
    if (LOG.isDebugEnabled()) {
      recs.forEach(rec -> {
        LOG.debug("rec.getContainerId() : {}, rec.getContainerState(): {} ", rec.getContainerId(),
            rec.getContainerState());
      });
    }

    try (Connection connection = containerSchemaDefinition.getDataSource().getConnection()) {
      connection.setAutoCommit(false); // Turn off auto-commit for transactional control
      for (UnhealthyContainers rec : recs) {
        try {
          unhealthyContainersDao.insert(rec);
        } catch (DataAccessException dataAccessException) {
          // Log the error and just update other fields of the existing record
          // in case of ConstraintViolationException being actually thrown.
          unhealthyContainersDao.update(rec);
          LOG.debug("Error while inserting unhealthy container record: {}", rec,
              dataAccessException);
        }
      }
      // Commit all the records inserted and updated.
      connection.commit();
    } catch (Exception e) {
      LOG.error("Failed to get connection over {} ", UNHEALTHY_CONTAINERS_TABLE_NAME, e);
      throw new RuntimeException("Recon failed to insert " + recs.size() + " num of unhealthy container records.", e);
    }
  }

}
