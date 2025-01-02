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

package org.apache.hadoop.ozone.recon.tasks.updater;

import com.google.common.annotations.VisibleForTesting;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides utilities to update/modify Recon Task related data
 * like updating table, incrementing counter etc.
 */
public class ReconTaskStatusUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(ReconTaskStatusUpdater.class);

  private ReconTaskStatus reconTaskStatus;

  private ReconTaskStatusDao reconTaskStatusDao;

  private String taskName;

  public ReconTaskStatusUpdater(ReconTaskStatusDao reconTaskStatusDao,
                                String taskName) {
    this.taskName = taskName;
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.reconTaskStatus = new ReconTaskStatus(taskName, 0L, 0L, 0, 0);
  }

  public ReconTaskStatusUpdater(ReconTaskStatusDao reconTaskStatusDao,
                                String taskName, Long lastUpdatedTimestamp, Long lastUpdatedSeqNum,
                                Integer lastTaskRunStatus, Integer isCurrentTaskRunning) {
    this.taskName = taskName;
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.reconTaskStatus = new ReconTaskStatus(taskName, lastUpdatedTimestamp, lastUpdatedSeqNum,
        lastTaskRunStatus, isCurrentTaskRunning);
  }

  @VisibleForTesting
  public ReconTaskStatusUpdater(String taskName, ReconTaskStatusDao reconTaskStatusDao) {
    this.taskName = taskName;
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.reconTaskStatus = new ReconTaskStatus(taskName, 0L, 0L, 0, 0);
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
    this.reconTaskStatus.setTaskName(taskName);
  }

  public void setLastUpdatedSeqNumber(long lastUpdatedSeqNumber) {
    this.reconTaskStatus.setLastUpdatedSeqNumber(lastUpdatedSeqNumber);
  }

  public void setLastUpdatedTimestamp(long lastUpdatedTimestamp) {
    this.reconTaskStatus.setLastUpdatedTimestamp(lastUpdatedTimestamp);
  }

  public void setLastTaskRunStatus(int lastTaskRunStatus) {
    this.reconTaskStatus.setLastTaskRunStatus(lastTaskRunStatus);
  }

  public void setIsCurrentTaskRunning(int isCurrentTaskRunning) {
    this.reconTaskStatus.setIsCurrentTaskRunning(isCurrentTaskRunning);
  }

  /**
   * Utility function to update table with task details and update the counter if needed.
   */
  public void updateDetails() {
    if (!reconTaskStatusDao.existsById(this.taskName)) {
      // First time getting the task, so insert value
      reconTaskStatusDao.insert(this.reconTaskStatus);
      LOG.info("Registered Task: {}", this.taskName);
    } else {
      // We already have row for the task in the table, update the row
      reconTaskStatusDao.update(this.reconTaskStatus);
    }
  }
}
