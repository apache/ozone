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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * In-memory state for a container ID export job.
 */
public final class ExportJob {

  private final Id id;
  private final ExportScope scope;
  private final String jobStartTime = ExportFileManager.formatJobStartTime(Instant.now());
  private final ContainerID startContainerId;
  private final int batchSize;
  private final int partSize;
  /** The .tar.gz output path, fixed at job creation; used by the worker to write the archive. */
  private final String fileName;

  private final CompletableFuture<Long> idsWrittenFuture = new CompletableFuture<>();

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
  public final class Status {
    private final Long idsWritten;
    private final Throwable exception;

    private Status() {
      if (!idsWrittenFuture.isDone()) {
        this.idsWritten = null;
        this.exception = null;
      } else {
        long rows = -1;
        Throwable cause = null;
        try {
          rows = idsWrittenFuture.join();
        } catch (CompletionException e) {
          cause = e.getCause();
        }
        this.idsWritten = rows;
        this.exception = cause;
      }
    }

    public ExportJob.Id getId() {
      return id;
    }

    public boolean isDone() {
      return idsWritten != null || exception != null;
    }

    public long getIdsWritten() {
      if (!isDone()) {
        throw new IllegalStateException("Export is not done");
      }
      if (exception != null) {
        throw new IllegalStateException("Export failed", exception);
      }
      return idsWritten;
    }

    public Throwable getException() {
      return exception;
    }
  }

  ExportJob(ExportScope scope, ContainerID startContainerId, int batchSize, int partSize) {
    this.id = Id.newId();
    this.scope = scope;
    this.startContainerId = startContainerId != null ? startContainerId : ContainerID.valueOf(0);
    this.fileName = ExportFileManager.archiveFileName(scope, jobStartTime, id);
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

  String getFileName() {
    return fileName;
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

  CompletableFuture<Long> getFuture() {
    return idsWrittenFuture;
  }

  Status getStatus() {
    return new Status();
  }

  String partFileName(int partNumber) {
    return String.format("container-ids_%s_%s_part%03d.txt", scope, jobStartTime, partNumber);
  }

  void writeMetadataHeader(OutputStream out, int partNumber, ContainerID partStartContainerId) throws IOException {
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    writer.write("# jobId=" + id);
    writer.newLine();
    writer.write("# jobStartTime=" + jobStartTime);
    writer.newLine();
    writer.write("# scope=" + scope);
    writer.newLine();
    writer.write("# partStartContainerId=" + partStartContainerId);
    writer.newLine();
    writer.write("# part=" + partNumber);
    writer.newLine();
    writer.write("# format=container-id-per-line");
    writer.newLine();
    writer.newLine();
    writer.flush();
  }
  
  @Override
  public String toString() {
    return "ExportJob" + id;
  }
}
