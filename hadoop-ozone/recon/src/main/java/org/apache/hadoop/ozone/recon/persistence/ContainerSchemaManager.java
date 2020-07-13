/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.persistence;

import static org.hadoop.ozone.recon.schema.tables.ContainerHistoryTable.CONTAINER_HISTORY;
import static org.hadoop.ozone.recon.schema.tables.UnhealthyContainersTable.UNHEALTHY_CONTAINERS;
import static org.jooq.impl.DSL.count;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersSummary;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.hadoop.ozone.recon.schema.tables.daos.ContainerHistoryDao;
import org.hadoop.ozone.recon.schema.tables.daos.UnhealthyContainersDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ContainerHistory;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;
import org.hadoop.ozone.recon.schema.tables.records.UnhealthyContainersRecord;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.SelectQuery;
import java.util.List;

/**
 * Provide a high level API to access the Container Schema.
 */
@Singleton
public class ContainerSchemaManager {
  private ContainerHistoryDao containerHistoryDao;
  private UnhealthyContainersDao unhealthyContainersDao;
  private ContainerSchemaDefinition containerSchemaDefinition;

  @Inject
  public ContainerSchemaManager(ContainerHistoryDao containerHistoryDao,
      ContainerSchemaDefinition containerSchemaDefinition,
      UnhealthyContainersDao unhealthyContainersDao) {
    this.containerHistoryDao = containerHistoryDao;
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
      query.addConditions(
          UNHEALTHY_CONTAINERS.CONTAINER_STATE.eq(state.toString()));
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
    unhealthyContainersDao.insert(recs);
  }

  public void upsertContainerHistory(long containerID, String datanode,
                                     long time) {
    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    Record2<Long, String> recordToFind =
        dslContext.newRecord(
        CONTAINER_HISTORY.CONTAINER_ID,
        CONTAINER_HISTORY.DATANODE_HOST).value1(containerID).value2(datanode);
    ContainerHistory newRecord = new ContainerHistory();
    newRecord.setContainerId(containerID);
    newRecord.setDatanodeHost(datanode);
    newRecord.setLastReportTimestamp(time);
    ContainerHistory record = containerHistoryDao.findById(recordToFind);
    if (record != null) {
      newRecord.setFirstReportTimestamp(record.getFirstReportTimestamp());
      containerHistoryDao.update(newRecord);
    } else {
      newRecord.setFirstReportTimestamp(time);
      containerHistoryDao.insert(newRecord);
    }
  }

  public List<ContainerHistory> getAllContainerHistory(long containerID) {
    return containerHistoryDao.fetchByContainerId(containerID);
  }

  public List<ContainerHistory> getLatestContainerHistory(long containerID,
                                                          int limit) {
    DSLContext dslContext = containerSchemaDefinition.getDSLContext();
    // Get container history sorted in descending order of last report timestamp
    return dslContext.select()
        .from(CONTAINER_HISTORY)
        .where(CONTAINER_HISTORY.CONTAINER_ID.eq(containerID))
        .orderBy(CONTAINER_HISTORY.LAST_REPORT_TIMESTAMP.desc())
        .limit(limit)
        .fetchInto(ContainerHistory.class);
  }
}
