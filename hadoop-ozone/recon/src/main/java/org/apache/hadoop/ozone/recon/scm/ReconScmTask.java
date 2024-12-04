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
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Any background task that keeps SCM's metadata up to date.
 */
public abstract class ReconScmTask {

  private static final Logger LOG = LoggerFactory.getLogger(ReconScmTask.class);
  private Thread taskThread;
  private ReconTaskStatusDao reconTaskStatusDao;
  private ReconTaskStatusCounter taskStatusCounter;
  private volatile boolean running;
  private ReconTaskStatus reconTaskStatusRecord;

  protected ReconScmTask(ReconTaskStatusDao reconTaskStatusDao) {
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.taskStatusCounter = ReconTaskStatusCounter.getCurrentInstance();
  }

  private void register() {
    String taskName = getTaskName();
    if (!reconTaskStatusDao.existsById(taskName)) {
      reconTaskStatusRecord = new ReconTaskStatus(
          taskName, 0L, 0L, 0, 0);
      reconTaskStatusDao.insert(reconTaskStatusRecord);
      LOG.info("Registered {} task ", taskName);
    }
  }

  /**
   * Start underlying start thread.
   */
  public synchronized void start() {
    register();
    if (!isRunning()) {
      LOG.info("Starting {} Thread.", getTaskName());
      running = true;
      taskThread = new Thread(this::run, "Recon-" + getTaskName());
      taskThread.setName(getTaskName());
      taskThread.setDaemon(true);
      taskThread.start();
      reconTaskStatusRecord.setCurrentTaskRunStatus(1);
    } else {
      LOG.info("{} Thread is already running.", getTaskName());
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
      reconTaskStatusRecord.setCurrentTaskRunStatus(0);
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
    taskStatusCounter.updateCounter(getTaskName(), true);
    reconTaskStatusRecord.setLastUpdatedTimestamp(System.currentTimeMillis());
    reconTaskStatusRecord.setLastTaskRunStatus(1);
    reconTaskStatusDao.update(reconTaskStatusRecord);
  }

  protected boolean canRun() {
    return running;
  }

  public String getTaskName() {
    return getClass().getSimpleName();
  }

  protected abstract void run();
}
