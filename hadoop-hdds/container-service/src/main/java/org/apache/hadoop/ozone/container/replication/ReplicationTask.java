/**
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
package org.apache.hadoop.ozone.container.replication;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;

/**
 * The task to download a container from the sources.
 */
public class ReplicationTask {

  private volatile Status status = Status.QUEUED;

  private final Instant queued = Instant.now();

  private final ReplicateContainerCommand cmd;

  /**
   * Counter for the transferred bytes.
   */
  private long transferredBytes;

  public ReplicationTask(ReplicateContainerCommand cmd) {
    this.cmd = cmd;
  }

  /**
   * Returns any deadline set on this task, in milliseconds since the epoch.
   * A returned value of zero indicates no deadline.
   */
  public long getDeadline() {
    return cmd.getDeadline();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReplicationTask that = (ReplicationTask) o;
    return getContainerId() == that.getContainerId() &&
        Objects.equals(getTarget(), that.getTarget());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getContainerId(), getTarget());
  }

  public long getContainerId() {
    return cmd.getContainerID();
  }

  public List<DatanodeDetails> getSources() {
    return cmd.getSourceDatanodes();
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  @Override
  public String toString() {
    return "ReplicationTask{" +
        "status=" + status +
        ", cmd={" + cmd + "}" +
        ", queued=" + queued +
        '}';
  }

  public Instant getQueued() {
    return queued;
  }

  public long getTransferredBytes() {
    return transferredBytes;
  }

  public void setTransferredBytes(long transferredBytes) {
    this.transferredBytes = transferredBytes;
  }

  long getTerm() {
    return cmd.getTerm();
  }

  DatanodeDetails getTarget() {
    return cmd.getTargetDatanode();
  }

  ReplicateContainerCommand getCommand() {
    return cmd;
  }

  /**
   * Status of the replication.
   */
  public enum Status {
    QUEUED,
    IN_PROGRESS,
    FAILED,
    DONE
  }
}
