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

package org.apache.hadoop.ozone.recon.tasks;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;

/**
 * Interface used to denote a Recon task that needs to act on OM DB events.
 */
public interface ReconOmTask {

  /**
   * Return task name.
   * @return task name
   */
  String getTaskName();

  /**
   * Initialize the recon om task with first time initialization of resources.
   */
  default void init() { }

  /**
   * Process a set of OM events on tables that the task is listening on.
   *
   * @param events            The batch of OM update events to be processed.
   * @param subTaskSeekPosMap A map containing the seek positions for
   *                          each sub-task, indicating where processing should start.
   * @return A {@link TaskResult} containing:
   *         - The task name.
   *         - A map of sub-task names to their respective seek positions.
   *         - A boolean indicating whether the task was successful.
   */
  TaskResult process(OMUpdateEventBatch events,
                     Map<String, Integer> subTaskSeekPosMap);

  /**
   * Reprocesses full entries in Recon OM RocksDB tables that the task is listening to.
   *
   * @param omMetadataManager The OM Metadata Manager instance used for accessing metadata.
   * @return A {@link TaskResult} containing:
   *         - The task name.
   *         - A map of sub-task names to their respective seek positions.
   *         - A boolean indicating whether the task was successful.
   */
  TaskResult reprocess(OMMetadataManager omMetadataManager);

  /**
   * Returns a staged task that can be used to reprocess events.
   * @param stagedOmMetadataManager  om metadata manager for staged OM DB
   * @param stagedReconDbStore recon DB store for staged
   * @return task that can be used to reprocess events
   * @throws IOException exception
   */
  default ReconOmTask getStagedTask(ReconOMMetadataManager stagedOmMetadataManager, DBStore stagedReconDbStore)
      throws IOException {
    return this;
  }

  /**
   * Represents the result of a task execution, including the task name,
   * sub-task seek positions, and success status.
   *
   * <p>This class is immutable and uses the Builder pattern for object creation.</p>
   */
  class TaskResult {
    private final String taskName;
    private final Map<String, Integer> subTaskSeekPositions;
    private final boolean taskSuccess;

    /**
     * Private constructor to enforce the use of the {@link Builder}.
     *
     * @param builder The builder instance containing values for initialization.
     */
    private TaskResult(Builder builder) {
      this.taskName = builder.taskName;
      this.subTaskSeekPositions = builder.subTaskSeekPositions != null
          ? builder.subTaskSeekPositions
          : Collections.emptyMap(); // Default value
      this.taskSuccess = builder.taskSuccess;
    }

    // Getters
    public String getTaskName() {
      return taskName;
    }

    public Map<String, Integer> getSubTaskSeekPositions() {
      return subTaskSeekPositions;
    }

    public boolean isTaskSuccess() {
      return taskSuccess;
    }

    /**
     * Builder class for creating instances of {@link TaskResult}.
     */
    public static class Builder {
      private String taskName;
      private Map<String, Integer> subTaskSeekPositions = Collections.emptyMap(); // Default value
      private boolean taskSuccess;

      public Builder setTaskName(String taskName) {
        this.taskName = taskName;
        return this;
      }

      public Builder setSubTaskSeekPositions(Map<String, Integer> subTaskSeekPositions) {
        this.subTaskSeekPositions = subTaskSeekPositions;
        return this;
      }

      public Builder setTaskSuccess(boolean taskSuccess) {
        this.taskSuccess = taskSuccess;
        return this;
      }

      public TaskResult build() {
        return new TaskResult(this);
      }
    }

    // toString Method for debugging
    @Override
    public String toString() {
      return "TaskResult{" +
          "taskName='" + taskName + '\'' +
          ", subTaskSeekPositions=" + subTaskSeekPositions +
          ", taskSuccess=" + taskSuccess +
          '}';
    }
  }

  default TaskResult buildTaskResult(boolean success) {
    return new TaskResult.Builder()
        .setTaskName(getTaskName())
        .setTaskSuccess(success)
        .build();
  }
}
