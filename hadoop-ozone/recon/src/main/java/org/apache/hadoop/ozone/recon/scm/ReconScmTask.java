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

package org.apache.hadoop.ozone.recon.scm;

import org.apache.hadoop.ozone.recon.metrics.ReconTaskStatusCounter;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskStatusUpdater;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Any background task that keeps SCM's metadata up to date.
 */
public abstract class ReconScmTask {

  private static final Logger LOG = LoggerFactory.getLogger(ReconScmTask.class);
  private Thread taskThread;
  private volatile boolean running;
  private final ReconTaskStatusUpdater taskStatusUpdater;

  protected ReconScmTask(
      ReconTaskStatusDao reconTaskStatusDao, ReconTaskStatusCounter taskStatusCounter
  ) {
    this.taskStatusUpdater = new ReconTaskStatusUpdater(reconTaskStatusDao, taskStatusCounter, getTaskName());
  }

  /**
   * Start underlying start thread.
   */
  public synchronized void start() {
    try {
      if (!isRunning()) {
        LOG.info("Starting {} Thread.", getTaskName());
        running = true;
        taskThread = new Thread(this::run, "Recon-" + getTaskName());
        taskThread.setName(getTaskName());
        taskThread.setDaemon(true);
        taskThread.start();

        // Insert initial values to DB as thread has started
        taskStatusUpdater.updateDetails();
      } else {
        LOG.info("{} Thread is already running.", getTaskName());
      }
    } catch (Exception e) {
      LOG.error("Failed to start {} thread due to exception", getTaskName(), e);
    }
  }

  /**
   * Stop underlying task thread.
   */
  public synchronized void stop() {
    if (running) {
      LOG.info("Stopping {} Thread.", getTaskName());
      running = false;
      notifyAll();
    } else {
      LOG.info("{} Thread is not running.", getTaskName());
    }
  }

  public boolean isRunning() {
    if (!running) {
      synchronized (this) {
        return taskThread != null
            && taskThread.isAlive();
      }
    }
    return true;
  }

  protected void recordSingleRunCompletion() {
    try {
      taskStatusUpdater.setIsCurrentTaskRunning(0);
      taskStatusUpdater.setLastUpdatedTimestamp(System.currentTimeMillis());
      taskStatusUpdater.updateDetails();
    } catch (DataAccessException e) {
      LOG.error("Failed to update table for task: {}", getTaskName());
    }
  }

  protected void recordRunStart() {
    try {
      taskStatusUpdater.setIsCurrentTaskRunning(1);
      taskStatusUpdater.setLastUpdatedTimestamp(System.currentTimeMillis());
      taskStatusUpdater.updateDetails();
    } catch (DataAccessException e) {
      LOG.error("Failed to update table for start of task: {}", getTaskName());
    }
  }

  protected boolean canRun() {
    return running;
  }

  public String getTaskName() {
    return getClass().getSimpleName();
  }

  public ReconTaskStatusUpdater getTaskStatusUpdater() {
    return this.taskStatusUpdater;
  }

  protected abstract void run();
}
