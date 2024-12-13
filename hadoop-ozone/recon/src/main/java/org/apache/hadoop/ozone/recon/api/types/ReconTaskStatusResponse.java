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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class to represent the API response structure of task status statistics.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReconTaskStatusResponse {

  // The name of the task for which we are getting status
  @JsonProperty("taskName")
  private String taskName;

  // The number of successes associated with the task
  @JsonProperty("successes")
  private int successCount;

  // The number of failures associated with the task
  @JsonProperty("failures")
  private int failureCount;

  // The timestamp at which the counters were last reset
  @JsonProperty("counterStartedAt")
  private long counterStartTime;

  @JsonProperty("lastUpdatedTimestamp")
  private long lastUpdatedTimestamp;

  @JsonProperty("lastUpdatedSeqNumber")
  private long lastUpdatedSeqNumber;

  @JsonProperty("lastTaskRunStatus")
  private int lastTaskRunStatus;

  @JsonProperty("isTaskCurrentlyRunning")
  private int isTaskCurrentlyRunning;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public ReconTaskStatusResponse(String taskName, long lastUpdatedSeqNumber, long lastUpdatedTimestamp,
                                 int isTaskCurrentlyRunning, int lastTaskRunStatus,
                                 int successCount, int failureCount, long counterStartTime) {
    this.taskName = taskName;
    this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    this.lastUpdatedSeqNumber = lastUpdatedSeqNumber;
    this.lastTaskRunStatus = lastTaskRunStatus;
    this.isTaskCurrentlyRunning = isTaskCurrentlyRunning;
    this.successCount = successCount;
    this.failureCount = failureCount;
    this.counterStartTime = counterStartTime;
  }

  public String getTaskName() {
    return taskName;
  }

  public long getLastUpdatedTimestamp() {
    return lastUpdatedTimestamp;
  }

  public long getLastUpdatedSeqNumber() {
    return lastUpdatedSeqNumber;
  }

  public int getIsTaskCurrentlyRunning() {
    return isTaskCurrentlyRunning;
  }

  public int getLastTaskRunStatus() {
    return lastTaskRunStatus;
  }

  public long getSuccessCount() {
    return successCount;
  }

  public long getFailureCount() {
    return failureCount;
  }

  public long getCounterStartTime() {
    return counterStartTime;
  }
}
