/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import java.time.Instant;

/**
 * Abstract class to capture common variables and methods for different types
 * of replication tasks.
 */
public abstract class AbstractReplicationTask {

  /**
   * ENUM representing the different status values a replication task can
   * have.
   */
  public enum Status {
    QUEUED,
    IN_PROGRESS,
    FAILED,
    DONE,
    SKIPPED
  }

  private volatile Status status = Status.QUEUED;

  private final long containerId;

  private final Instant queued = Instant.now();

  private final long deadlineMsSinceEpoch;

  private final long term;

  protected AbstractReplicationTask(long containerID,
      long deadlineMsSinceEpoch, long term) {
    this.containerId = containerID;
    this.deadlineMsSinceEpoch = deadlineMsSinceEpoch;
    this.term = term;
  }

  public long getContainerId() {
    return containerId;
  }
  public Status getStatus() {
    return status;
  }

  protected void setStatus(Status newStatus) {
    this.status = newStatus;
  }

  public Instant getQueued() {
    return queued;
  }

  public long getTerm() {
    return term;
  }

  /**
   * Returns any deadline set on this task, in milliseconds since the epoch.
   * A returned value of zero indicates no deadline.
   */
  public long getDeadline() {
    return deadlineMsSinceEpoch;
  }

  /**
   * Abstract method which needs to be overridden by the sub classes to execute
   * the task.
   */
  public abstract void runTask();

}
