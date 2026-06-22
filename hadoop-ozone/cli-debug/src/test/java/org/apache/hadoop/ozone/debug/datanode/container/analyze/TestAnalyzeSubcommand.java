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

package org.apache.hadoop.ozone.debug.datanode.container.analyze;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Tests for {@code ozone debug datanode container analyze} command.
 */
public class TestAnalyzeSubcommand {

  @TempDir
  private Path tempDir;

  private ContainerAnalyzeTestHelper testHelper;
  private CommandLine cmd;
  private StringWriter outWriter;
  private StringWriter errWriter;

  @BeforeEach
  public void setup() {
    OzoneConfiguration conf = new OzoneConfiguration();
    testHelper = new ContainerAnalyzeTestHelper(tempDir, conf, 
        UUID.randomUUID().toString(), UUID.randomUUID().toString());

    cmd = new OzoneDebug().getCmd();
    outWriter = new StringWriter();
    errWriter = new StringWriter();
    cmd.setOut(new PrintWriter(outWriter));
    cmd.setErr(new PrintWriter(errWriter));
  }

  @Test
  public void testAnalyzeNoDuplicates() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    testHelper.createContainerDirectory(volumeRoot, 6006L, true, 6006L);

    executeAnalyze(volumeRoot.getAbsolutePath());

    String output = outWriter.toString();
    assertThat(output).contains("Number of containers with duplicate container directories on this DataNode: 0");
    assertThat(output).doesNotContain("Container ");
  }

  @Test
  public void testAnalyzeRespectsCount() throws Exception {
    File volumeRoot1 = testHelper.formatVolume("volume0");
    File volumeRoot2 = testHelper.formatVolume("volume1");
    long[] duplicateIds = {9003L, 9001L, 9002L};
    for (long containerId : duplicateIds) {
      testHelper.createContainerDirectory(volumeRoot1, containerId, true, containerId);
      testHelper.createContainerDirectory(volumeRoot2, containerId, true, containerId);
    }

    executeAnalyze(volumeRoot1.getAbsolutePath() + "," + volumeRoot2.getAbsolutePath(),
        "--count", "2");

    String output = outWriter.toString();
    assertThat(output).contains("Number of containers with duplicate container directories on this DataNode: 3");
    assertThat(output).contains("Showing first 2:");
    assertThat(output).contains("Container 9001 (2 occurrences):");
    assertThat(output).contains("Container 9002 (2 occurrences):");
    assertThat(output).doesNotContain("Container 9003");
    assertThat(output.indexOf("Container 9001")).isLessThan(output.indexOf("Container 9002"));
  }

  @Test
  public void testAnalyzeInvalidCount() {
    executeAnalyze(tempDir.toString(), "--count", "0");

    String combined = outWriter.toString() + errWriter.toString();
    assertThat(combined).contains("Count must be an integer greater than 0.");
  }

  @Test
  public void testAnalyzeVolumeScanErrors() throws Exception {
    File healthyVolume = testHelper.formatVolume("volume0");
    File failingVolume = testHelper.formatVolume("volume1");
    testHelper.createContainerDirectory(healthyVolume, 6006L, true, 6006L);
    testHelper.corruptVersionFile(failingVolume);

    executeAnalyze(healthyVolume.getAbsolutePath() + "," + failingVolume.getAbsolutePath());

    String output = outWriter.toString();
    assertThat(output).contains("Number of containers with duplicate container directories on this DataNode: 0");

    String errors = errWriter.toString();
    assertThat(errors).contains("Volumes that failed to scan (1):");
    assertThat(errors).contains(failingVolume.getAbsolutePath());
  }

  @Test
  public void testAnalyzeDuplicateValidAndValid() throws Exception {
    File volumeRoot1 = testHelper.formatVolume("volume0");
    File volumeRoot2 = testHelper.formatVolume("volume1");
    long containerId = 4004L;
    testHelper.createContainerDirectory(volumeRoot1, containerId, true, containerId);
    testHelper.createContainerDirectory(volumeRoot2, containerId, true, containerId);

    assertDuplicateReport(volumeRoot1, volumeRoot2, containerId, "VALID");
  }

  @Test
  public void testAnalyzeDuplicateValidAndMissing() throws Exception {
    File volumeRoot1 = testHelper.formatVolume("volume0");
    File volumeRoot2 = testHelper.formatVolume("volume1");
    long containerId = 7007L;
    testHelper.createContainerDirectory(volumeRoot1, containerId, true, containerId);
    testHelper.createContainerDirectory(volumeRoot2, containerId, false, containerId);

    assertDuplicateReport(volumeRoot1, volumeRoot2, containerId, "MISSING_METADATA");
  }

  @Test
  public void testAnalyzeDuplicateValidAndInvalidIdMismatch() throws Exception {
    File volumeRoot1 = testHelper.formatVolume("volume0");
    File volumeRoot2 = testHelper.formatVolume("volume1");
    long containerId = 3003L;
    testHelper.createContainerDirectory(volumeRoot1, containerId, true, containerId);
    testHelper.createContainerDirectory(volumeRoot2, containerId, true, 9999L);

    assertDuplicateReport(volumeRoot1, volumeRoot2, containerId, "INVALID_METADATA");
  }

  @Test
  public void testAnalyzeDuplicateValidAndInvalidEmptyFile() throws Exception {
    File volumeRoot1 = testHelper.formatVolume("volume0");
    File volumeRoot2 = testHelper.formatVolume("volume1");
    long containerId = 5005L;
    testHelper.createContainerDirectory(volumeRoot1, containerId, true, containerId);
    testHelper.createEmptyContainerFileOnVolume(volumeRoot2, containerId);

    assertDuplicateReport(volumeRoot1, volumeRoot2, containerId, "INVALID_METADATA");
  }

  private void assertDuplicateReport(File volumeRoot1, File volumeRoot2, long containerId,
      String volume2ExpectedStatus) {
    executeAnalyze(volumeRoot1.getAbsolutePath() + "," + volumeRoot2.getAbsolutePath());

    String path1 = testHelper.containerPath(volumeRoot1, containerId);
    String path2 = testHelper.containerPath(volumeRoot2, containerId);
    String output = outWriter.toString();
    assertThat(output).contains("Container " + containerId + " (2 occurrences):");
    assertThat(output).contains("path=" + path1 + "\n  status=" + "VALID");
    assertThat(output).contains("path=" + path2 + "\n  status=" + volume2ExpectedStatus);
  }

  private void executeAnalyze(String datanodeDirs, String... extraArgs) {
    List<String> args = new ArrayList<>();
    args.add("-D");
    args.add(ScmConfigKeys.HDDS_DATANODE_DIR_KEY + "=" + datanodeDirs);
    args.add("datanode");
    args.add("container");
    args.add("analyze");
    args.addAll(Arrays.asList(extraArgs));
    cmd.execute(args.toArray(new String[0]));
  }
}
