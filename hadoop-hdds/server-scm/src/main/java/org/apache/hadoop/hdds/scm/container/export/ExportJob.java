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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * In-memory state for a container ID export job.
 */
public final class ExportJob {

  private final Id id;
  private final ExportScope scope;
  private final String jobStartTime;
  private final ContainerID startContainerId;
  private final int pageSize;
  private final int shardSize;
  private String tarPath;
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
   * Snapshot of export progress returned to callers. Reads live fields from the enclosing job.
   */
  public final class Status {
    private Status() {
    }

    public Id getId() {
      return id;
    }

    public ExecutionState getExecutionState() {
      return ExportJob.this.getExecutionState();
    }

    public long getTotalRows() {
      return ExportJob.this.getTotalRows();
    }

    public String getTarPath() {
      return ExportJob.this.getTarPath();
    }

    public String getErrorMessage() {
      return ExportJob.this.getErrorMessage();
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

  ExportJob(Id id, ExportScope scope, String jobStartTime, String tarPath, ContainerID startContainerId,
      int pageSize, int shardSize) {
    this.id = id;
    this.scope = scope;
    this.jobStartTime = jobStartTime;
    this.tarPath = tarPath;
    this.startContainerId = startContainerId != null ? startContainerId : ContainerID.valueOf(0);
    this.pageSize = pageSize;
    this.shardSize = shardSize;
  }

  Id getId() {
    return id;
  }

  ContainerID getStartContainerId() {
    return startContainerId;
  }

  LifeCycleState getLifeCycleState() {
    return scope.getLifeCycleState();
  }

  ContainerHealthState getHealthState() {
    return scope.getHealthState();
  }

  int getPageSize() {
    return pageSize;
  }

  int getShardSize() {
    return shardSize;
  }

  synchronized String getTarPath() {
    return tarPath;
  }

  synchronized ExecutionState getExecutionState() {
    return executionState;
  }

  synchronized long getTotalRows() {
    return totalRows;
  }

  synchronized String getErrorMessage() {
    return errorMessage;
  }

  synchronized void startExecution() {
    if (executionState.isTerminal()) {
      throw new IllegalStateException("Export job " + id + " is already terminal: " + executionState);
    }
  }

  synchronized void updateTotalRows(long rows) {
    totalRows = rows;
  }

  synchronized void completeWithNoMatches() {
    tarPath = null;
    transitionToTerminal(ExecutionState.SUCCEEDED);
  }

  synchronized void completeWithArchive(String archivePath) {
    tarPath = archivePath;
    transitionToTerminal(ExecutionState.SUCCEEDED);
  }

  synchronized void fail(String message) {
    errorMessage = message;
    transitionToTerminal(ExecutionState.FAILED);
  }

  private synchronized void transitionToTerminal(ExecutionState terminalState) {
    if (executionState.isTerminal()) {
      throw new IllegalStateException("Export job " + id + " is already terminal: " + executionState);
    }
    executionState = terminalState;
  }

  Status toStatus() {
    return new Status();
  }

  String shardFileName(int partIndex) {
    return String.format("container-ids_%s_%s_part%03d.txt",
        scope.getValue(), jobStartTime, partIndex);
  }

  void writeMetadataHeader(BufferedWriter writer, int partNumber, ContainerID shardStartContainerId)
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
    writer.write("# startContainerId=" + shardStartContainerId.getProtobuf().getId());
    writer.newLine();
    writer.write("# part=" + partNumber);
    writer.newLine();
    writer.write("# format=container-id-per-line");
    writer.newLine();
    writer.newLine();
  }
}
