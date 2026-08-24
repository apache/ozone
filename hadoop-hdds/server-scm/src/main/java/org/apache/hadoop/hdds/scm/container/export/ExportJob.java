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

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * In-memory state for a container ID export job.
 * <p>Mutable fields are guarded by a {@link ReadWriteLock} so {@link #toStatus()} returns a
 * consistent snapshot while the worker updates progress.
 */
public final class ExportJob {

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private final Id id;
  private final ExportScope scope;
  private final String jobStartTime;
  private final ContainerID startContainerId;
  private final int batchSize;
  private final int partSize;
  // Planned .tar.gz output path, fixed at job creation; used by the worker to write the archive.
  private final String plannedArchivePath;
  private ExecutionState executionState = ExecutionState.RUNNING;
  private long totalRows;
  private String errorMessage;

  /**
   * Unique job identifier.
   */
  public static final class Id {
    private final String value;

    private Id(String value) {
      this.value = Objects.requireNonNull(value, "value == null");
    }

    public static Id newId() {
      return new Id(UUID.randomUUID().toString());
    }

    public static Id of(String value) {
      return new Id(value);
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return value;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof Id)) {
        return false;
      }
      return value.equals(((Id) obj).value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  /**
   * Immutable snapshot of export progress.
   */
  public static final class Status {
    private final Id id;
    private final ExecutionState executionState;
    private final long totalRows;
    // plannedArchivePath when the job SUCCEEDED with rows; null while running, on failure, or zero matches.
    private final String completedArchivePath;
    private final String errorMessage;

    private Status(Id id, ExecutionState executionState, long totalRows, String completedArchivePath,
        String errorMessage) {
      this.id = id;
      this.executionState = executionState;
      this.totalRows = totalRows;
      this.completedArchivePath = completedArchivePath;
      this.errorMessage = errorMessage;
    }

    public Id getId() {
      return id;
    }

    public ExecutionState getExecutionState() {
      return executionState;
    }

    public long getTotalRows() {
      return totalRows;
    }

    public String getCompletedArchivePath() {
      return completedArchivePath;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }

  /**
   * Job execution state.
   */
  public enum ExecutionState {
    RUNNING(false),
    SUCCEEDED(true),
    FAILED(true);

    private final boolean terminal;

    ExecutionState(boolean terminal) {
      this.terminal = terminal;
    }

    public boolean isTerminal() {
      return terminal;
    }
  }

  ExportJob(Id id, ExportScope scope, String jobStartTime, String plannedArchivePath, ContainerID startContainerId,
      int batchSize, int partSize) {
    this.id = id;
    this.scope = scope;
    this.jobStartTime = jobStartTime;
    this.plannedArchivePath = plannedArchivePath;
    this.startContainerId = startContainerId != null ? startContainerId : ContainerID.valueOf(0);
    this.batchSize = batchSize;
    this.partSize = partSize;
  }

  Id getId() {
    return id;
  }

  LifeCycleState getLifeCycleState() {
    return scope.getLifeCycleState();
  }

  ContainerHealthState getHealthState() {
    return scope.getHealthState();
  }

  String getPlannedArchivePath() {
    return plannedArchivePath;
  }

  ContainerID getStartContainerId() {
    return startContainerId;
  }

  int getBatchSize() {
    return batchSize;
  }

  int getPartSize() {
    return partSize;
  }

  void updateTotalRows(long rows) {
    lock.writeLock().lock();
    try {
      totalRows = rows;
    } finally {
      lock.writeLock().unlock();
    }
  }

  void completeWithNoMatches() {
    lock.writeLock().lock();
    try {
      transitionToTerminal(ExecutionState.SUCCEEDED);
    } finally {
      lock.writeLock().unlock();
    }
  }

  void completeSucceeded() {
    lock.writeLock().lock();
    try {
      transitionToTerminal(ExecutionState.SUCCEEDED);
    } finally {
      lock.writeLock().unlock();
    }
  }

  void fail(String message) {
    lock.writeLock().lock();
    try {
      errorMessage = message;
      transitionToTerminal(ExecutionState.FAILED);
    } finally {
      lock.writeLock().unlock();
    }
  }

  Status toStatus() {
    lock.readLock().lock();
    try {
      String completedArchivePath =
          executionState == ExecutionState.SUCCEEDED && totalRows > 0 ? plannedArchivePath : null;
      return new Status(id, executionState, totalRows, completedArchivePath, errorMessage);
    } finally {
      lock.readLock().unlock();
    }
  }

  private void transitionToTerminal(ExecutionState terminalState) {
    if (executionState.isTerminal()) {
      throw new IllegalStateException("Export job " + id + " is already terminal: " + executionState);
    }
    executionState = terminalState;
  }

  String partFileName(int partIndex) {
    return String.format("container-ids_%s_%s_part%03d.txt",
        scope.getValue(), jobStartTime, partIndex);
  }

  void writeMetadataHeader(BufferedWriter writer, int partNumber, long partStartContainerId)
      throws IOException {
    writer.write("# jobId=" + id.getValue());
    writer.newLine();
    writer.write("# jobStartTime=" + jobStartTime);
    writer.newLine();
    if (scope.getHealthState() != null) {
      writer.write("# healthState=" + scope.getHealthState().name());
      writer.newLine();
    }
    if (scope.getLifeCycleState() != null) {
      writer.write("# lifecycleState=" + scope.getLifeCycleState().name());
      writer.newLine();
    }
    writer.write("# startContainerId=" + partStartContainerId);
    writer.newLine();
    writer.write("# part=" + partNumber);
    writer.newLine();
    writer.write("# format=container-id-per-line");
    writer.newLine();
    writer.newLine();
  }
}
