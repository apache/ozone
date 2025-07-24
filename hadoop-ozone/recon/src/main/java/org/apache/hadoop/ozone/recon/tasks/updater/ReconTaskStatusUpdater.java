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

package org.apache.hadoop.ozone.recon.tasks.updater;

import com.google.common.annotations.VisibleForTesting;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;
import org.jooq.exception.DataAccessException;
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

  @VisibleForTesting
  public ReconTaskStatusUpdater(ReconTaskStatusDao reconTaskStatusDao,
                                String taskName) {
    this.taskName = taskName;
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.reconTaskStatus = new ReconTaskStatus(taskName, 0L, 0L, 0, 0);
  }

  public ReconTaskStatusUpdater(ReconTaskStatusDao reconTaskStatusDao, ReconTaskStatus task) {
    this.taskName = task.getTaskName();
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.reconTaskStatus = new ReconTaskStatus(taskName, task.getLastUpdatedTimestamp(),
        task.getLastUpdatedSeqNumber(), task.getLastTaskRunStatus(), task.getIsCurrentTaskRunning());
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

  public Long getLastUpdatedSeqNumber() {
    return this.reconTaskStatus.getLastUpdatedSeqNumber();
  }

  public String getTaskName() {
    return this.taskName;
  }

  /**
   * Helper function to update TASK_STATUS table with task start values.
   * Set the isCurrentTaskRunning as true, update the timestamp.
   * Call this function before the actual task processing starts to update table in DB.
   */
  public void recordRunStart() {
    try {
      this.reconTaskStatus.setIsCurrentTaskRunning(1);
      this.reconTaskStatus.setLastUpdatedTimestamp(System.currentTimeMillis());
      updateDetails();
    } catch (DataAccessException e) {
      LOG.error("Failed to update table for start of task: {}", this.reconTaskStatus.getTaskName());
    }
  }

  /**
   * Helper function to update TASK_STATUS table with task end values.
   * Set isCurrentTaskRunning as false, update the timestamp.
   * Call this function after the actual task processing ends to update table in DB.
   * It is expected that the task status result (successful/0, failure/-1) is already set
   * before calling.
   */
  public void recordRunCompletion() {
    try {
      this.reconTaskStatus.setIsCurrentTaskRunning(0);
      this.reconTaskStatus.setLastUpdatedTimestamp(System.currentTimeMillis());
      updateDetails();
    } catch (DataAccessException e) {
      LOG.error("Failed to update table for task: {}", this.reconTaskStatus.getTaskName());
    }
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
