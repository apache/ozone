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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ExportJob}.
 */
public class TestExportJob {

  private static final String TEST_JOB_START_TIME = "2026-01-01-12-00-00";

  @TempDir
  private File tempDir;

  @Test
  public void testPartFileName() {
    ExportJob job = newJob(ContainerHealthState.MISSING, null);
    assertEquals("container-ids_health-MISSING_lifecycle-ANY_2026-01-01-12-00-00_part001.txt", job.partFileName(1));
  }

  @Test
  public void testWriteMetadataHeader() throws Exception {
    ExportJob job = newJob(ContainerHealthState.MISSING, LifeCycleState.OPEN);
    File headerFile = new File(tempDir, "part-header.txt");
    try (BufferedWriter writer = Files.newBufferedWriter(headerFile.toPath())) {
      job.writeMetadataHeader(writer, 2, 42L);
    }
    List<String> lines = Files.readAllLines(headerFile.toPath());
    assertTrue(lines.contains("# jobStartTime=" + TEST_JOB_START_TIME));
    assertTrue(lines.contains("# healthState=MISSING"));
    assertTrue(lines.contains("# lifecycleState=OPEN"));
    assertTrue(lines.contains("# startContainerId=42"));
    assertTrue(lines.contains("# part=2"));
  }

  private static ExportJob newJob(ContainerHealthState healthState, LifeCycleState lifeCycleState) {
    ExportScope scope = ExportScope.of(lifeCycleState, healthState);
    return new ExportJob(ExportJob.Id.newId(), scope, TEST_JOB_START_TIME);
  }
}
