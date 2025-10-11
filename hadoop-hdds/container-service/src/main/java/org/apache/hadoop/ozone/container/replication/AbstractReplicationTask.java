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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority.NORMAL;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority;

/**
 * Abstract class to capture common variables and methods for different types
 * of replication tasks.
 */
public abstract class AbstractReplicationTask {

  private volatile Status status = Status.QUEUED;

  private final long containerId;

  private final Instant queued;

  private final long deadlineMsSinceEpoch;

  private final long term;

  private ReplicationCommandPriority priority = NORMAL;

  private boolean shouldOnlyRunOnInServiceDatanodes = true;

  protected AbstractReplicationTask(long containerID,
      long deadlineMsSinceEpoch, long term) {
    this(containerID, deadlineMsSinceEpoch, term,
        Clock.system(ZoneId.systemDefault()));
  }

  protected AbstractReplicationTask(long containerID,
      long deadlineMsSinceEpoch, long term, Clock clock) {
    this.containerId = containerID;
    this.deadlineMsSinceEpoch = deadlineMsSinceEpoch;
    this.term = term;
    queued = Instant.now(clock);
  }
  
  protected abstract String getMetricName();

  protected abstract String getMetricDescriptionSegment();

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

  /**
   * Set the relative priority of a task. Internally Tasks use the integer value
   * associated with the ENUM parameter. An ENUM with a lower integer value
   * will be sorted earlier in the queue than a larger value.
   * @param priority ENUM representing an integer indicating the priority of
   *                 this task.
   */
  public void setPriority(ReplicationCommandPriority priority) {
    this.priority = priority;
  }

  /**
   * Returns the priority of the task. A lower number indicates a higher
   * priority.
   */
  public ReplicationCommandPriority getPriority() {
    return priority;
  }

  /**
   * Returns true if the task should only run on in service datanodes. False
   * otherwise.
   */
  public boolean shouldOnlyRunOnInServiceDatanodes() {
    return shouldOnlyRunOnInServiceDatanodes;
  }

  /**
   * Set whether the task should only run on in service datanodes. Passing false
   * allows the task to run on out of service datanodes as well.
   * @param runOnInServiceOnly
   */
  protected void setShouldOnlyRunOnInServiceDatanodes(
      boolean runOnInServiceOnly) {
    this.shouldOnlyRunOnInServiceDatanodes = runOnInServiceOnly;
  }

  /**
   * Hook for subclasses to provide info about the command.
   * @return string representation of the command
   */
  protected Object getCommandForDebug() {
    return "";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder()
        .append(getStatus()).append(' ')
        .append(getCommandForDebug());
    if (getStatus() == Status.QUEUED) {
      sb.append(", queued at ").append(getQueued());
    }
    return sb.toString();
  }

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
}
