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

package org.apache.hadoop.ozone.recon.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.ReconTaskStatusStat;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_COUNTER_CYCLES_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_COUNTER_CYCLES_LIMIT_DEFAULT;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This class contains definitions and implementation of Recon Task Status counters
 * For each task we maintain a count of the successes and the failures.
 * This count is stored for a configurable {@link org.apache.hadoop.ozone.recon.ReconServerConfigKeys
 * #OZONE_RECON_TASK_STATUS_COUNTER_CYCLES_LIMIT} which defaults
 * to '5' times {@link org.apache.hadoop.ozone.recon.ReconServerConfigKeys#OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY}
 * Each task is mapped to a {@link ReconTaskStatusStat} instance to store the counts.
 */
@Singleton
public class ReconTaskStatusCounter {
  // Stores the configurable timeout duration i.e. the TTL of the counts
  private final long timeoutDuration;

  // Task name is mapped from the enum to a Pair of <count of successful runs, count of failed runs>
  private final Map<String, ReconTaskStatusStat> taskStatusCounter = new ConcurrentHashMap<>();

  @Inject
  public ReconTaskStatusCounter(OzoneConfiguration conf) {
    // Get the number of cycles of OM sync task for which we want to store the count
    int countCycles = conf.getInt(
        OZONE_RECON_TASK_STATUS_COUNTER_CYCLES_LIMIT,
        OZONE_RECON_TASK_STATUS_COUNTER_CYCLES_LIMIT_DEFAULT
    );
    // Get the interval between which the OM sync task is being run
    long taskSyncInterval = conf.getTimeDuration(
        OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY,
        OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS
    );
    timeoutDuration = taskSyncInterval * countCycles;
  }

  @VisibleForTesting
  public long getTimeoutDuration() {
    return this.timeoutDuration;
  }

  /**
   * Checks if the counter has exceeded the number of OM DB sync cycles for which it is configured
   * to store the count. <br>
   * The configuration is: {@link org.apache.hadoop.ozone.recon.ReconServerConfigKeys
   * #OZONE_RECON_TASK_STATUS_COUNTER_CYCLES_LIMIT} <br>
   * Default duration/TTL of the counter is 5 OM DB sync cycles i.e (5 * 10min interval between sync) = 50min.<br>
   * In case the count data TTL is reached, reinitialize the instance to reset the data, else do nothing
   */
  private void checkCountDataExpiry(String taskName) {
    taskStatusCounter.compute(taskName, (key, taskStat) -> {
      if (null == taskStat) {
        return new ReconTaskStatusStat();
      }

      //Since initially the task list is empty, each task will get initialized at different times
      if ((System.currentTimeMillis() - taskStat.getInitializationTime()) > timeoutDuration) {
        // If TTL has expired we need to reset the stats
        taskStat.reset();
      }

      return taskStat;
    });
  }

  /**
   * Update the counter's success/failure count based on the task name passed.
   * @param taskName The task name for which we want to update the counter
   * @param successful Whether the task was successful or not
   */
  public void updateCounter(String taskName, boolean successful) {
    checkCountDataExpiry(taskName);
    taskStatusCounter.computeIfPresent(taskName, (key, taskStat) -> {
      if (successful) {
        taskStat.incrementSuccess();
      } else {
        taskStat.incrementFailure();
      }
      return taskStat;
    });
  }

  /**
   * Get the statistics like number of success/failure count for the task name provided.
   * @param taskName The task for which stats should be fetched
   * @return {@link ReconTaskStatusStat} instance of the statistics for the task
   */
  public ReconTaskStatusStat getTaskStatsFor(String taskName) {
    return taskStatusCounter.getOrDefault(taskName, new ReconTaskStatusStat());
  }
}
