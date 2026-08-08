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
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * In-memory state for a container ID export job.
 */
public final class ExportJob {

  private final Id id;
  private final ExportScope scope;
  private final String timestamp;
  private final ContainerID startContainerId;
  private final ExportSizing sizing;
  private volatile String tarPath;
  private volatile ExecutionState executionState = ExecutionState.RUNNING;
  private volatile long totalRows;
  private volatile long startTimeNs;
  private volatile long endTimeNs;
  private volatile String errorMessage;

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
      return executionState;
    }

    public LifeCycleState getLifeCycleState() {
      return scope.getLifeCycleState();
    }

    public ContainerHealthState getHealthState() {
      return scope.getHealthState();
    }

    public long getTotalRows() {
      return totalRows;
    }

    public long getElapsedMs() {
      if (startTimeNs <= 0) {
        return 0;
      }
      long endNs = endTimeNs > 0 ? endTimeNs : System.nanoTime();
      return TimeUnit.NANOSECONDS.toMillis(endNs - startTimeNs);
    }

    public String getTarPath() {
      return tarPath;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    public boolean isTerminal() {
      return executionState == ExecutionState.SUCCEEDED
          || executionState == ExecutionState.FAILED;
    }
  }

  /**
   * Job execution state.
   */
  public enum ExecutionState {
    RUNNING,
    SUCCEEDED,
    FAILED
  }

  ExportJob(Id id, ExportScope scope, String timestamp, String tarPath, ContainerID startContainerId,
      ExportSizing sizing) {
    this.id = id;
    this.scope = scope;
    this.timestamp = timestamp;
    this.tarPath = tarPath;
    this.startContainerId = startContainerId != null ? startContainerId : ContainerID.valueOf(0);
    this.sizing = sizing;
  }

  Id getId() {
    return id;
  }

  ExportScope getScope() {
    return scope;
  }

  String getTimestamp() {
    return timestamp;
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

  long getMaxRows() {
    return sizing.getMaxRows();
  }

  int getPageSize() {
    return sizing.getPageSize();
  }

  int getShardSize() {
    return sizing.getShardSize();
  }

  ExecutionState getExecutionState() {
    return executionState;
  }

  void setExecutionState(ExecutionState newState) {
    this.executionState = newState;
    if ((newState == ExecutionState.SUCCEEDED || newState == ExecutionState.FAILED)
        && endTimeNs == 0) {
      this.endTimeNs = System.nanoTime();
    }
  }

  long getEndTimeNs() {
    return endTimeNs;
  }

  long getTotalRows() {
    return totalRows;
  }

  void setTotalRows(long rows) {
    this.totalRows = rows;
  }

  void setStartTimeNs(long startTimeNs) {
    this.startTimeNs = startTimeNs;
  }

  void setErrorMessage(String message) {
    this.errorMessage = message;
  }

  String getTarPath() {
    return tarPath;
  }

  void setTarPath(String path) {
    this.tarPath = path;
  }

  Status toStatus() {
    return new Status();
  }

  String shardFileName(int partIndex) {
    return String.format("container-ids_%s_%s_part%03d.txt",
        scope.getValue(), timestamp, partIndex);
  }

  String shardEntryName(int partIndex) {
    return shardFileName(partIndex);
  }

  void writeMetadataHeader(BufferedWriter writer, int partNumber, ContainerID shardStartContainerId)
      throws IOException {
    writer.write("# jobId=" + id.getValue());
    writer.newLine();
    writer.write("# timestamp=" + timestamp);
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
