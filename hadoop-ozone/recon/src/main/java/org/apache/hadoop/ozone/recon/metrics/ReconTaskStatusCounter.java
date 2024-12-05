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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.ReconTaskStatusStat;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_STORAGE_DURATION;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_STORAGE_DURATION_DEFAULT;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This class contains definitions and implementation of Recon Task Status counters
 * For each task we maintain a count of the successes and the failures.
 * This count is stored for a configurable
 * {@link org.apache.hadoop.ozone.recon.ReconServerConfigKeys#OZONE_RECON_TASK_STATUS_STORAGE_DURATION}
 * which defaults to 30 minutes.
 * Each task is mapped to a {@link Pair} of <code>{no. of successful runs, no. of failed runs}</code>
 */
public class ReconTaskStatusCounter {
  // Stores an instance of this class to maintain state across calls
  private static ReconTaskStatusCounter instance;
  // Stores the configurable timeout duration i.e. the TTL of the counts
  private final long timeoutDuration;

  // Task name is mapped from the enum to a Pair of <count of successful runs, count of failed runs>
  private static final Map<String, ReconTaskStatusStat> TASK_STATUS_COUNTER = new HashMap<>();

  public ReconTaskStatusCounter() {
    OzoneConfiguration conf = new OzoneConfiguration();
    timeoutDuration = conf.getTimeDuration(
      OZONE_RECON_TASK_STATUS_STORAGE_DURATION,
      OZONE_RECON_TASK_STATUS_STORAGE_DURATION_DEFAULT,
      TimeUnit.MILLISECONDS
    );
  }

  /**
   * Checks if the duration of the counters exceeded
   * the configured {@link org.apache.hadoop.ozone.recon.ReconServerConfigKeys
   *  OZONE_RECON_TASK_STATUS_STORAGE_DURATION} duration.
   * Default duration/TTL of the counter is 30 minutes
   * In case the count data TTL is reached, reinitialize the instance to reset the data, else do nothing
   */
  private void checkCountDataExpiry(String taskName) {
    ReconTaskStatusStat taskStat = TASK_STATUS_COUNTER.get(taskName);
    //Since initially the task list is empty, each task will get initialized at different times
    if ((System.currentTimeMillis() - taskStat.getInitializationTime()) > timeoutDuration) {
      // If the task stat TTL is expired, we want to reset the associated counters
      taskStat.reset();
      // Update the map with the initial state for the task stats
      TASK_STATUS_COUNTER.put(taskName, taskStat);
    }
  }

  /**
   * Update the counter's success/failure count based on the task name passed.
   * @param taskName The task name for which we want to update the counter
   * @param successful Whether the task was successful or not
   */
  public void updateCounter(String taskName, boolean successful) {
    TASK_STATUS_COUNTER.putIfAbsent(taskName, new ReconTaskStatusStat());
    checkCountDataExpiry(taskName);

    if (successful) {
      TASK_STATUS_COUNTER.get(taskName).incrementSuccess();
    } else {
      TASK_STATUS_COUNTER.get(taskName).incrementFailure();
    }
  }

  public Map<String, ReconTaskStatusStat> getTaskCounts() {
    return TASK_STATUS_COUNTER;
  }
}
