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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersSummary;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaWithTimestamp;
import org.apache.hadoop.ozone.recon.scm.ReconSCMDBDefinition;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.hadoop.ozone.recon.schema.tables.daos.UnhealthyContainersDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ContainerHistory;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;
import org.hadoop.ozone.recon.schema.tables.records.UnhealthyContainersRecord;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Provide a high level API to access the Container Schema.
 */
@Singleton
public class ContainerSchemaManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerSchemaManager.class);

  private final UnhealthyContainersDao unhealthyContainersDao;
  private final ContainerSchemaDefinition containerSchemaDefinition;

  private Map<Long, Map<UUID, ContainerReplicaWithTimestamp>> lastSeenMap;

  private final ContainerDBServiceProvider dbServiceProvider;

  private DBStore scmDBStore;
  private Table<UUID, DatanodeDetails> nodeDB;

  public Table<UUID, DatanodeDetails> getNodeDB() {
    return nodeDB;
  }

  @Inject
  public ContainerSchemaManager(
      ContainerSchemaDefinition containerSchemaDefinition,
      UnhealthyContainersDao unhealthyContainersDao,
      ContainerDBServiceProvider containerDBServiceProvider) {
    this.unhealthyContainersDao = unhealthyContainersDao;
    this.containerSchemaDefinition = containerSchemaDefinition;
    this.dbServiceProvider = containerDBServiceProvider;
    this.lastSeenMap = null;
    this.scmDBStore = null;
    this.nodeDB = null;
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

  public void upsertContainerHistory(long containerID, UUID uuid,
                                     long time) {

    Map<UUID, ContainerReplicaWithTimestamp> tsMap;
    try {
      tsMap = dbServiceProvider.getContainerReplicaHistoryMap(containerID);
      ContainerReplicaWithTimestamp ts = tsMap.get(uuid);
      if (ts == null) {
        tsMap.put(uuid, new ContainerReplicaWithTimestamp(uuid, time));
      } else {
        // Entry exists, update last seen time and put it back to DB.
        ts.setLastSeenTime(time);
      }
      dbServiceProvider.storeContainerReplicaHistoryMap(containerID, tsMap);
    } catch (IOException e) {
      // TODO: Better error handling
      LOG.debug("Error on DB operations.");
    }
  }

  public List<ContainerHistory> getAllContainerHistory(long containerID) {
    Map<UUID, ContainerReplicaWithTimestamp> resMap;
    try {
      resMap = dbServiceProvider.getContainerReplicaHistoryMap(containerID);
    } catch (IOException ex) {
      resMap = new HashMap<>();
      LOG.debug("Unable to retrieve container replica history from RDB.");
    }

    if (lastSeenMap != null) {
      Map<UUID, ContainerReplicaWithTimestamp> replicaLastSeenMap =
          lastSeenMap.get(containerID);
      if (replicaLastSeenMap != null) {
        Map<UUID, ContainerReplicaWithTimestamp> finalResMap = resMap;
        replicaLastSeenMap.forEach((k, v) ->
            finalResMap.merge(k, v, (prev, curr) -> curr));
        resMap = finalResMap;
      }
    }

    List<ContainerHistory> resList = new ArrayList<>();
    for (Map.Entry<UUID, ContainerReplicaWithTimestamp> entry :
        resMap.entrySet()) {
      final UUID uuid = entry.getKey();
      final long firstSeenTime = entry.getValue().getFirstSeenTime();
      final long lastSeenTime = entry.getValue().getLastSeenTime();
      // Retrieve hostname in NODES table
      String hostname = "N/A";
      if (nodeDB != null) {
        try {
          DatanodeDetails dnDetails = nodeDB.get(uuid);
          if (dnDetails != null) {
            hostname = dnDetails.getHostName();
          }
        } catch (IOException ex) {
          LOG.debug("Unable to get DatanodeDetails from NODES table.");
        }
      }
      // TODO: Refrain from using jOOQ class since we use RDB now?
      resList.add(new ContainerHistory(
          containerID, uuid.toString(), hostname, firstSeenTime, lastSeenTime));
    }
    return resList;
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

  public void setLastSeenMap(
      Map<Long, Map<UUID, ContainerReplicaWithTimestamp>> lastSeenMap) {
    this.lastSeenMap = lastSeenMap;
  }

  public DBStore getScmDBStore() {
    return scmDBStore;
  }

  public void setScmDBStore(DBStore scmDBStore) {
    this.scmDBStore = scmDBStore;
    try {
      this.nodeDB = ReconSCMDBDefinition.NODES.getTable(scmDBStore);
    } catch (IOException ex) {
      LOG.debug("Failed to get NODES table.");
    }
  }

  public void flushLastSeenMapToDB(boolean clearMap) {
    if (lastSeenMap == null) {
      return;
    }
    synchronized (lastSeenMap) {
      try {
        for (Map.Entry<Long, Map<UUID, ContainerReplicaWithTimestamp>> entry :
            lastSeenMap.entrySet()) {
          final long containerId = entry.getKey();
          final Map<UUID, ContainerReplicaWithTimestamp> map = entry.getValue();
          dbServiceProvider.storeContainerReplicaHistoryMap(containerId, map);
        }
      } catch (IOException e) {
        LOG.debug("Error flushing container replica history to DB. {}",
            e.getMessage());
      } if (clearMap) {
        lastSeenMap.clear();
      }
    }
  }
}
