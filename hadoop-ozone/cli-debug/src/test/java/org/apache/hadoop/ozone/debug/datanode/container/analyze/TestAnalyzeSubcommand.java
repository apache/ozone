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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
        "--length", "2");

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
    executeAnalyze(tempDir.toString(), "--length", "0");

    String combined = outWriter.toString() + errWriter.toString();
    assertThat(combined).contains("List length should be a positive number");
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

  @ParameterizedTest(name = "{0}")
  @MethodSource("scmOrphanOrDeletedScenarios")
  public void testAnalyzeScmOrphanOrDeletedSingleVolume(String scenarioName, long containerId,
      HddsProtos.LifeCycleState scmState, boolean metadataFilePresent, long metadataContainerId, String expectedStatus)
      throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    testHelper.createContainerDirectory(volumeRoot, containerId, metadataFilePresent, metadataContainerId);

    Map<Long, HddsProtos.LifeCycleState> scmContainers = new HashMap<>();
    if (scmState != null) {
      scmContainers.put(containerId, scmState);
    }
    File scmDb = testHelper.createScmDb(scmContainers);

    executeAnalyze(volumeRoot.getAbsolutePath(), "--scm-db", scmDb.getAbsolutePath());

    String output = outWriter.toString();
    assertScmCounts(output, scmState == null ? 1 : 0, scmState == HddsProtos.LifeCycleState.DELETED ? 1 : 0);
    assertThat(output).contains("Container " + containerId + " (1 occurrence):");
    assertOccurrenceStatus(output, volumeRoot, containerId, expectedStatus);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("scmOrphanOrDeletedScenarios")
  public void testAnalyzeScmOrphanOrDeletedOnTwoVolumes(String scenarioName, long containerId,
      HddsProtos.LifeCycleState scmState, boolean metadataFilePresent, long metadataContainerId, String expectedStatus) 
      throws Exception {
    File volumeRoot1 = testHelper.formatVolume("volume0");
    File volumeRoot2 = testHelper.formatVolume("volume1");
    testHelper.createContainerDirectory(volumeRoot1, containerId, metadataFilePresent, metadataContainerId);
    testHelper.createContainerDirectory(volumeRoot2, containerId, metadataFilePresent, metadataContainerId);

    Map<Long, HddsProtos.LifeCycleState> scmContainers = new HashMap<>();
    if (scmState != null) {
      scmContainers.put(containerId, scmState);
    }
    File scmDb = testHelper.createScmDb(scmContainers);

    executeAnalyze(volumeRoot1.getAbsolutePath() + "," + volumeRoot2.getAbsolutePath(),
        "--scm-db", scmDb.getAbsolutePath());

    String output = outWriter.toString();
    assertScmCounts(output, scmState == null ? 1 : 0, scmState == HddsProtos.LifeCycleState.DELETED ? 1 : 0);
    assertThat(output).contains("Container " + containerId + " (2 occurrences):");
    assertOccurrenceStatus(output, volumeRoot1, containerId, expectedStatus);
    assertOccurrenceStatus(output, volumeRoot2, containerId, expectedStatus);
  }

  @Test
  public void testAnalyzeScmOmitsHealthyContainer() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    long containerId = 8020L;
    testHelper.createContainerDirectory(volumeRoot, containerId, true, containerId);

    Map<Long, HddsProtos.LifeCycleState> scmContainers = new HashMap<>();
    scmContainers.put(containerId, HddsProtos.LifeCycleState.CLOSED);
    File scmDb = testHelper.createScmDb(scmContainers);

    executeAnalyze(volumeRoot.getAbsolutePath(), "--scm-db", scmDb.getAbsolutePath());

    String output = outWriter.toString();
    assertThat(output).contains("Number of orphan containers(wrt SCM) on this DataNode: 0");
    assertThat(output).contains(
        "Number of containers marked DELETED in SCM but present on disk on this DataNode: 0");
  }

  @Test
  public void testAnalyzeScmMixedOrphanDeletedHealthy() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    long orphanId = 8101L;
    long deletedId = 8102L;
    long healthyId = 8103L;
    testHelper.createContainerDirectory(volumeRoot, orphanId, true, orphanId);
    testHelper.createContainerDirectory(volumeRoot, deletedId, true, deletedId);
    testHelper.createContainerDirectory(volumeRoot, healthyId, true, healthyId);

    Map<Long, HddsProtos.LifeCycleState> scmContainers = new HashMap<>();
    scmContainers.put(deletedId, HddsProtos.LifeCycleState.DELETED);
    scmContainers.put(healthyId, HddsProtos.LifeCycleState.CLOSED);
    File scmDb = testHelper.createScmDb(scmContainers);

    executeAnalyze(volumeRoot.getAbsolutePath(), "--scm-db", scmDb.getAbsolutePath());

    String output = outWriter.toString();
    assertThat(output).contains("Number of orphan containers(wrt SCM) on this DataNode: 1");
    assertThat(output).contains(
        "Number of containers marked DELETED in SCM but present on disk on this DataNode: 1");
    assertThat(output).contains("Container " + orphanId + " (1 occurrence):");
    assertOccurrenceStatus(output, volumeRoot, orphanId, "VALID");
    assertThat(output).contains("Container " + deletedId + " (1 occurrence):");
    assertOccurrenceStatus(output, volumeRoot, deletedId, "VALID");
    assertThat(output).doesNotContain("Container " + healthyId);
  }

  @Test
  public void testAnalyzeScmMixedOrphanDeletedDuplicate() throws Exception {
    File volumeRoot1 = testHelper.formatVolume("volume0");
    File volumeRoot2 = testHelper.formatVolume("volume1");
    long orphanId = 8201L;
    long deletedId = 8202L;
    long duplicateId = 8203L;
    testHelper.createContainerDirectory(volumeRoot1, orphanId, true, orphanId);
    testHelper.createContainerDirectory(volumeRoot1, deletedId, true, deletedId);
    testHelper.createContainerDirectory(volumeRoot1, duplicateId, true, duplicateId);
    testHelper.createContainerDirectory(volumeRoot2, duplicateId, true, duplicateId);

    Map<Long, HddsProtos.LifeCycleState> scmContainers = new HashMap<>();
    scmContainers.put(deletedId, HddsProtos.LifeCycleState.DELETED);
    scmContainers.put(duplicateId, HddsProtos.LifeCycleState.CLOSED);
    File scmDb = testHelper.createScmDb(scmContainers);

    executeAnalyze(volumeRoot1.getAbsolutePath() + "," + volumeRoot2.getAbsolutePath(),
        "--scm-db", scmDb.getAbsolutePath());

    String output = outWriter.toString();
    assertThat(output).contains("Number of orphan containers(wrt SCM) on this DataNode: 1");
    assertThat(output).contains("Container " + orphanId + " (1 occurrence):");
    assertOccurrenceStatus(output, volumeRoot1, orphanId, "VALID");
    assertThat(output).contains(
        "Number of containers marked DELETED in SCM but present on disk on this DataNode: 1");
    assertThat(output).contains("Container " + deletedId + " (1 occurrence):");
    assertOccurrenceStatus(output, volumeRoot1, deletedId, "VALID");
    assertThat(output).contains("Number of containers with duplicate container directories on this DataNode: 1");
    assertThat(output).contains("Container " + duplicateId + " (2 occurrences):");
    assertOccurrenceStatus(output, volumeRoot1, duplicateId, "VALID");
    assertOccurrenceStatus(output, volumeRoot2, duplicateId, "VALID");
  }

  @Test
  public void testAnalyzeWithoutScmDb() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    long containerId = 8301L;
    testHelper.createContainerDirectory(volumeRoot, containerId, true, containerId);

    executeAnalyze(volumeRoot.getAbsolutePath());

    String output = outWriter.toString();
    assertThat(output).contains("provide the SCM database path using the --scm-db option");
    assertThat(output).doesNotContain("Number of orphan containers(wrt SCM) on this DataNode:");
    assertThat(output).doesNotContain(
        "Number of containers marked DELETED in SCM but present on disk on this DataNode:");
    assertThat(output).contains("Number of containers with duplicate container directories on this DataNode: 0");
  }

  private static Stream<Arguments> scmOrphanOrDeletedScenarios() {
    return Stream.of(
        arguments("orphan-valid", 8008L, null, true, 8008L, "VALID"),
        arguments("deleted-but-present-valid", 8030L, HddsProtos.LifeCycleState.DELETED, true, 8030L, "VALID"),
        arguments("orphan-missing-metadata", 8401L, null, false, 8401L, "MISSING_METADATA"),
        arguments("deleted-but-present-missing-metadata", 8402L, HddsProtos.LifeCycleState.DELETED, false, 8402L,
            "MISSING_METADATA"),
        arguments("orphan-invalid-metadata", 8403L, null, true, 9999L, "INVALID_METADATA"),
        arguments("deleted-but-present-invalid-metadata", 8404L, HddsProtos.LifeCycleState.DELETED, true, 9999L,
            "INVALID_METADATA"));
  }

  private void assertScmCounts(String output, int expectedOrphans, int expectedDeleted) {
    assertThat(output).contains(
        "Number of orphan containers(wrt SCM) on this DataNode: " + expectedOrphans);
    assertThat(output).contains(
        "Number of containers marked DELETED in SCM but present on disk on this DataNode: " + expectedDeleted);
  }

  private void assertDuplicateReport(File volumeRoot1, File volumeRoot2, long containerId,
      String volume2ExpectedStatus) {
    executeAnalyze(volumeRoot1.getAbsolutePath() + "," + volumeRoot2.getAbsolutePath());

    String output = outWriter.toString();
    assertThat(output).contains("Container " + containerId + " (2 occurrences):");
    assertOccurrenceStatus(output, volumeRoot1, containerId, "VALID");
    assertOccurrenceStatus(output, volumeRoot2, containerId, volume2ExpectedStatus);
  }

  private void assertOccurrenceStatus(String output, File volumeRoot, long containerId, String expectedStatus) {
    assertThat(output).contains(String.format("path=%s%n  status=%s",
        testHelper.containerPath(volumeRoot, containerId), expectedStatus));
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
