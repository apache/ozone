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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ContainerHistoryDao;
import org.hadoop.ozone.recon.schema.tables.daos.MissingContainersDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ContainerHistory;
import org.hadoop.ozone.recon.schema.tables.pojos.MissingContainers;
import org.jooq.DSLContext;
import org.jooq.Record2;
import java.util.List;

/**
 * Provide a high level API to access the Container Schema.
 */
@Singleton
public class ContainerSchemaManager {
  private ContainerHistoryDao containerHistoryDao;
  private MissingContainersDao missingContainersDao;
  private ContainerSchemaDefinition containerSchemaDefinition;

  @Inject
  public ContainerSchemaManager(ContainerHistoryDao containerHistoryDao,
              ContainerSchemaDefinition containerSchemaDefinition,
              MissingContainersDao missingContainersDao) {
    this.containerHistoryDao = containerHistoryDao;
    this.missingContainersDao = missingContainersDao;
    this.containerSchemaDefinition = containerSchemaDefinition;
  }

  public void addMissingContainer(long containerID, long time) {
    MissingContainers record = new MissingContainers(containerID, time);
    missingContainersDao.insert(record);
  }

  public List<MissingContainers> getAllMissingContainers() {
    return missingContainersDao.findAll();
  }

  public boolean isMissingContainer(long containerID) {
    return missingContainersDao.existsById(containerID);
  }

  public void deleteMissingContainer(long containerID) {
    missingContainersDao.deleteById(containerID);
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
