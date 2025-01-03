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

import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Any background task that keeps SCM's metadata up to date.
 */
public abstract class ReconScmTask {

  private static final Logger LOG = LoggerFactory.getLogger(ReconScmTask.class);
  private Thread taskThread;
  private volatile boolean running;
  private final ReconTaskStatusUpdater taskStatusUpdater;
  private ReadWriteLock lock = new ReentrantReadWriteLock(true);

  protected ReconScmTask(
      ReconTaskStatusUpdaterManager taskStatusUpdaterManager
  ) {
    // In case the task is not already present in the DB, table is updated with initial values for task
    this.taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(getTaskName());
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

  /**
   * Helper function to update TASK_STATUS table with task end values.
   * Set isCurrentTaskRunning as false, update the timestamp.
   * Call this function after the actual task processing ends to update table in DB.
   */
  protected void recordSingleRunCompletion() {
    try {
      taskStatusUpdater.setIsCurrentTaskRunning(0);
      taskStatusUpdater.setLastUpdatedTimestamp(System.currentTimeMillis());
      taskStatusUpdater.updateDetails();
    } catch (DataAccessException e) {
      LOG.error("Failed to update table for task: {}", getTaskName());
    }
  }

  /**
   * Helper function to update TASK_STATUS table with task start values.
   * Set the isCurrentTaskRunning as true, update the timestamp.
   * Call this function before the actual task processing starts to update table in DB.
   */
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

  protected void initializeAndRunTask() {
    lock.writeLock().lock();
    try {
      recordRunStart();
      runTask();
    } catch (Exception e) {
      LOG.error("{} encountered exception. ", getTaskName(), e);
      taskStatusUpdater.setLastTaskRunStatus(-1);
    } finally {
      try {
        recordSingleRunCompletion();
      } catch (Exception e) {
        LOG.error("Exception occurred while trying to record {} completion", getTaskName(), e);
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  /**
   * Override this method for the actual processing logic in child tasks.
   */
  protected abstract void runTask() throws Exception;
}
