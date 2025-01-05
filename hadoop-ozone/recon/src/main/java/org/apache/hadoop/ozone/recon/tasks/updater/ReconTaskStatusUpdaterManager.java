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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class provides caching for ReconTaskStatusUpdater instances.
 * For each task we maintain a map of updater instance and provide it to consumers
 * to update.
 * Here we also make a single call to the TASK_STATUS_TABLE to check if previous values are present
 * for a task in the DB to avoid overwrite to initial state
 */
@Singleton
public class ReconTaskStatusUpdaterManager {
  private final ReconTaskStatusDao reconTaskStatusDao;
  // Act as a cache for the task updater instancesF
  private final Map<String, ReconTaskStatusUpdater> updaterCache;

  @Inject
  public ReconTaskStatusUpdaterManager(
      ReconTaskStatusDao reconTaskStatusDao
  ) {
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.updaterCache = new ConcurrentHashMap<>();

    // Fetch the tasks present in the DB already
    List<ReconTaskStatus> tasks = reconTaskStatusDao.findAll();
    for (ReconTaskStatus task: tasks) {
      updaterCache.put(task.getTaskName(),
          new ReconTaskStatusUpdater(reconTaskStatusDao, task));
    }
  }

  /**
   * Gets the updater for the provided task name and updates DB with initial values
   * if the task is not already present in DB.
   * @param taskName The name of the task for which we want to get instance of the updater
   * @return An instance of {@link ReconTaskStatusUpdater} for the provided task name.
   */
  public ReconTaskStatusUpdater getTaskStatusUpdater(String taskName) {
    // If the task is not already present in the DB then we can initialize using initial values
    return updaterCache.computeIfAbsent(taskName, (name) ->
        new ReconTaskStatusUpdater(reconTaskStatusDao, name));
  }
}
