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

package org.apache.hadoop.hdds.scm.container.export;

import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;

/**
 * Snapshot of a container ID export job, as reported by the SCM leader to clients.
 */
public final class ContainerExportStatus {

  private final String jobId;
  private final State state;
  private final LifeCycleState lifeCycleState;
  private final ContainerHealthState healthState;
  private final long maxRows;
  private final int batchSize;
  private final int partSize;
  private final long idsWritten;
  private final long elapsedMs;
  private final String tarPath;
  private final String errorMessage;

  /** Execution state of an export job. */
  public enum State {
    RUNNING,
    SUCCEEDED,
    FAILED
  }

  private ContainerExportStatus(Builder builder) {
    this.jobId = Objects.requireNonNull(builder.jobId, "jobId == null");
    this.state = Objects.requireNonNull(builder.state, "state == null");
    this.lifeCycleState = builder.lifeCycleState;
    this.healthState = builder.healthState;
    this.maxRows = builder.maxRows;
    this.batchSize = builder.batchSize;
    this.partSize = builder.partSize;
    this.idsWritten = builder.idsWritten;
    this.elapsedMs = builder.elapsedMs;
    this.tarPath = builder.tarPath;
    this.errorMessage = builder.errorMessage;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public String getJobId() {
    return jobId;
  }

  public State getState() {
    return state;
  }

  public boolean isDone() {
    return state != State.RUNNING;
  }

  /** @return the lifecycle state filter, or null if the job does not filter by lifecycle state. */
  public LifeCycleState getLifeCycleState() {
    return lifeCycleState;
  }

  /** @return the health state filter, or null if the job does not filter by health state. */
  public ContainerHealthState getHealthState() {
    return healthState;
  }

  /** @return the upper bound on exported container IDs, or 0 if unlimited. */
  public long getMaxRows() {
    return maxRows;
  }

  /** @return the number of container IDs fetched per listing query. */
  public int getBatchSize() {
    return batchSize;
  }

  /** @return the number of container IDs written to each part file. */
  public int getPartSize() {
    return partSize;
  }

  /** @return the number of exported container IDs; only set once the job has succeeded, otherwise 0. */
  public long getIdsWritten() {
    return idsWritten;
  }

  /** @return the time the job has been running, or ran for if it is done. */
  public long getElapsedMs() {
    return elapsedMs;
  }

  /** @return the archive path on the SCM leader, or null unless the job succeeded with at least one ID. */
  public String getTarPath() {
    return tarPath;
  }

  /** @return why the job failed, or null if it did not fail. */
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ContainerExportStatus)) {
      return false;
    }
    final ContainerExportStatus that = (ContainerExportStatus) obj;
    return maxRows == that.maxRows
        && batchSize == that.batchSize
        && partSize == that.partSize
        && idsWritten == that.idsWritten
        && elapsedMs == that.elapsedMs
        && jobId.equals(that.jobId)
        && state == that.state
        && lifeCycleState == that.lifeCycleState
        && healthState == that.healthState
        && Objects.equals(tarPath, that.tarPath)
        && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobId, state, lifeCycleState, healthState, maxRows, batchSize, partSize, idsWritten,
        elapsedMs, tarPath, errorMessage);
  }

  @Override
  public String toString() {
    return "ContainerExportStatus{jobId=" + jobId + ", state=" + state + ", lifeCycleState=" + lifeCycleState
        + ", healthState=" + healthState + ", maxRows=" + maxRows + ", batchSize=" + batchSize
        + ", partSize=" + partSize + ", idsWritten=" + idsWritten + ", elapsedMs=" + elapsedMs
        + ", tarPath=" + tarPath + ", errorMessage=" + errorMessage + '}';
  }

  /** Builder for {@link ContainerExportStatus}. */
  public static final class Builder {
    private String jobId;
    private State state;
    private LifeCycleState lifeCycleState;
    private ContainerHealthState healthState;
    private long maxRows;
    private int batchSize;
    private int partSize;
    private long idsWritten;
    private long elapsedMs;
    private String tarPath;
    private String errorMessage;

    private Builder() {
    }

    public Builder setJobId(String id) {
      this.jobId = id;
      return this;
    }

    public Builder setState(State newState) {
      this.state = newState;
      return this;
    }

    public Builder setLifeCycleState(LifeCycleState newLifeCycleState) {
      this.lifeCycleState = newLifeCycleState;
      return this;
    }

    public Builder setHealthState(ContainerHealthState newHealthState) {
      this.healthState = newHealthState;
      return this;
    }

    public Builder setMaxRows(long newMaxRows) {
      this.maxRows = newMaxRows;
      return this;
    }

    public Builder setBatchSize(int newBatchSize) {
      this.batchSize = newBatchSize;
      return this;
    }

    public Builder setPartSize(int newPartSize) {
      this.partSize = newPartSize;
      return this;
    }

    public Builder setIdsWritten(long newIdsWritten) {
      this.idsWritten = newIdsWritten;
      return this;
    }

    public Builder setElapsedMs(long newElapsedMs) {
      this.elapsedMs = newElapsedMs;
      return this;
    }

    public Builder setTarPath(String newTarPath) {
      this.tarPath = newTarPath;
      return this;
    }

    public Builder setErrorMessage(String newErrorMessage) {
      this.errorMessage = newErrorMessage;
      return this;
    }

    public ContainerExportStatus build() {
      return new ContainerExportStatus(this);
    }
  }
}
