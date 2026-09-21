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

/**
 * Metadata for a container ID export job.
 */
public final class ExportJob {

  private final Id id;
  private final ExportScope scope;
  private final String jobStartTime;

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

  ExportJob(Id id, ExportScope scope, String jobStartTime) {
    this.id = id;
    this.scope = scope;
    this.jobStartTime = jobStartTime;
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
