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

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_STORAGE_DURATION;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_STATUS_STORAGE_DURATION_DEFAULT;

import java.util.EnumMap;
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

  /**
   * {@link Enum} to store the various tasks that are run in Recon.
   */
  public enum ReconTasks {
    ContainerHealthTask,
    ContainerKeyMapperTask,
    ContainerSizeCountTask,
    FileSizeCountTask,
    NSSummaryTask,
    OmDeltaRequest,
    OmTableInsightTask,
    OmSnapshotRequest,
    PipelineSyncTask,
    ReconScmTask
  }

  private long initializationTime = 0L;

  // Task name is mapped from the enum to a Pair of <count of successful runs, count of failed runs>
  private static final Map<ReconTasks, Pair<Integer, Integer>> TASK_STATUS_COUNTER = new EnumMap<>(ReconTasks.class);

  public ReconTaskStatusCounter() {
    OzoneConfiguration conf = new OzoneConfiguration();
    timeoutDuration = conf.getTimeDuration(
      OZONE_RECON_TASK_STATUS_STORAGE_DURATION,
      OZONE_RECON_TASK_STATUS_STORAGE_DURATION_DEFAULT,
      TimeUnit.MILLISECONDS
    );

    initializationTime = System.currentTimeMillis();
    for (ReconTasks task: ReconTasks.values()) {
      TASK_STATUS_COUNTER.put(task, Pair.of(0, 0));
    }
  }

  /**
   * Get an instance of <code>this</code> {@link ReconTaskStatusCounter} in order to persist state
   * of the task counters between multiple modules/packages.
   * @return an instance of current {@link ReconTaskStatusCounter}
   */
  public static ReconTaskStatusCounter getCurrentInstance() {
    if (null == instance) {
      instance = new ReconTaskStatusCounter();
    }
    return instance;
  }

  /**
   * Update the counter's success/failure count based on the task name passed.
   * @param taskName The task name for which we want to update the counter
   * @param successful Whether the task was successful or not
   */
  public void updateCounter(String taskName, boolean successful) {
    int successes = TASK_STATUS_COUNTER.get(ReconTasks.valueOf(taskName)).getLeft();
    int failures = TASK_STATUS_COUNTER.get(ReconTasks.valueOf(taskName)).getRight();
    if (successful) {
      TASK_STATUS_COUNTER.put(ReconTasks.valueOf(taskName), Pair.of(successes + 1, failures));
    } else {
      TASK_STATUS_COUNTER.put(ReconTasks.valueOf(taskName), Pair.of(successes, failures + 1));
    }
  }

  /**
   * Checks if the duration of the counters exceeded
   * the configured {@link org.apache.hadoop.ozone.recon.ReconServerConfigKeys
   *  OZONE_RECON_TASK_STATUS_STORAGE_DURATION} duration.
   * Default duration/TTL of the counter is 30 minutes
   * In case the count data TTL is reached, reinitialize the instance to reset the data, else do nothing
   */
  private void checkCountDataExpiry() {
    if ((System.currentTimeMillis() - initializationTime) > timeoutDuration) {
      instance = new ReconTaskStatusCounter();
    }
  }

  /**
   * Get the number of successes and failures for a provided task name.
   * @param taskName Stores the task name for which we want to fetch the counts
   * @return A {@link Pair} of <code> {successes, failures} for provided task name </code>
   * @throws NullPointerException if the task name provided is not valid
   */
  public Pair<Integer, Integer> getTaskStatusCounts(String taskName)
      throws NullPointerException {
    checkCountDataExpiry();
    try {
      return TASK_STATUS_COUNTER.get(ReconTasks.valueOf(taskName));
    } catch (NullPointerException npe) {
      throw new NullPointerException("Couldn't find task with name " + taskName);
    }
  }

  public Map<ReconTasks, Pair<Integer, Integer>> getTaskCounts() {
    checkCountDataExpiry();
    return TASK_STATUS_COUNTER;
  }
}
