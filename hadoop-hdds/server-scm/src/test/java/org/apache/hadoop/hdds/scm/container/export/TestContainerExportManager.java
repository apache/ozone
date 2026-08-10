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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.utils.Archiver;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ContainerExportManager}.
 */
public class TestContainerExportManager {

  private static final int TEST_DEFAULT_PAGE_SIZE = 2;
  private static final int TEST_DEFAULT_SHARD_SIZE = 3;
  private static final int TEST_MAX_TERMINAL_JOBS = 10;
  private static final String TEST_SCM_ID = "test-scm";

  @TempDir
  private File tempDir;

  private ContainerManager containerManager;
  private ContainerExportManager exportManager;

  @BeforeEach
  public void setup() throws Exception {
    containerManager = mock(ContainerManager.class);
    exportManager = newExportManager(TEST_DEFAULT_SHARD_SIZE, TEST_DEFAULT_PAGE_SIZE,
        TEST_MAX_TERMINAL_JOBS, () -> true);
  }

  @AfterEach
  public void teardown() {
    exportManager.shutdown();
  }

  @Test
  public void testMultiShardExportCreatesTar() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1, 2));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(3)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(3, 4));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(5)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob.Id jobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0, 0, 0);
    assertNotNull(jobId);

    ExportJob.Status status = waitForTerminal(jobId);
    if (status.getExecutionState() == ExportJob.ExecutionState.FAILED) {
      fail(status.getErrorMessage());
    }
    assertEquals(ExportJob.ExecutionState.SUCCEEDED, status.getExecutionState());
    assertEquals(4, status.getTotalRows());
    assertNotNull(status.getTarPath());
    assertTrue(status.getTarPath().endsWith(ExportFileManager.EXPORT_ARCHIVE_SUFFIX));
    File archive = new File(status.getTarPath());
    assertTrue(archive.exists());
    assertFalse(Files.exists(tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId))));

    Path extractDir = Files.createTempDirectory("export-archive");
    try {
      extractGzTar(archive, extractDir);
      String part2Name;
      try (java.util.stream.Stream<Path> stream = Files.list(extractDir)) {
        part2Name = stream.map(path -> path.getFileName().toString())
            .filter(name -> name.endsWith("part002.txt"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("part002.txt not found in archive"));
      }
      assertTrue(Files.readAllLines(extractDir.resolve(part2Name)).contains(
          "# startContainerId=4"));
    } finally {
      FileUtils.deleteQuietly(extractDir.toFile());
    }
  }

  @Test
  public void testSingleShardExportCreatesTar() throws Exception {
    exportManager.shutdown();
    exportManager = newExportManager(100, TEST_DEFAULT_PAGE_SIZE,
        TEST_MAX_TERMINAL_JOBS, () -> true);

    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1, 2, 3, 4));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(5)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob.Id jobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0, 0, 0);
    assertNotNull(jobId);

    ExportJob.Status status = waitForTerminal(jobId);
    if (status.getExecutionState() == ExportJob.ExecutionState.FAILED) {
      fail(status.getErrorMessage());
    }
    assertEquals(ExportJob.ExecutionState.SUCCEEDED, status.getExecutionState());
    assertTrue(status.getTarPath().endsWith(ExportFileManager.EXPORT_ARCHIVE_SUFFIX));
    assertTrue(new File(status.getTarPath()).exists());
  }

  @Test
  public void testRejectConcurrentExport() {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(2)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenAnswer(invocation -> {
          Thread.sleep(60_000);
          return Collections.emptyList();
        });

    ExportJob.Id first = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0, 0, 0);
    assertNotNull(first);
    assertNull(exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.EMPTY, 0, 0, 0));
  }

  @Test
  public void testRejectMissingFilters() {
    assertThrows(IllegalArgumentException.class, () ->
        exportManager.submitJob(ContainerID.valueOf(0), null, null, 0, 0, 0));
  }

  @Test
  public void testExportScope() {
    assertEquals("health-MISSING_lifecycle-ANY",
        ExportScope.of(null, ContainerHealthState.MISSING).getValue());
  }

  @Test
  public void testMetadataTimestampIsHumanReadable() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob.Id jobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0, 0, 0);
    assertNotNull(jobId);
    waitForTerminal(jobId);

    ExportJob job = exportManager.getJobMap().get(jobId);
    assertTrue(job.getTimestamp().matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z"));
  }

  @Test
  public void testElapsedMsStableAfterCompletion() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(2)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob.Id jobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0, 0, 0);
    assertNotNull(jobId);
    ExportJob.Status first = waitForTerminal(jobId);
    Thread.sleep(200);
    ExportJob.Status second = exportManager.getExportStatus(jobId);
    assertEquals(first.getElapsedMs(), second.getElapsedMs());
  }

  @Test
  public void testTerminalJobEvictionWhenOverCap() throws Exception {
    exportManager.shutdown();
    exportManager = newExportManager(TEST_DEFAULT_SHARD_SIZE, TEST_DEFAULT_PAGE_SIZE, 2, () -> true);

    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(2)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());
    ExportJob.Id job1 = exportManager.submitJob(ContainerID.valueOf(0), null, ContainerHealthState.MISSING, 0, 0, 0);
    ExportJob.Status status1 = waitForTerminal(job1);
    File tar1 = new File(status1.getTarPath());
    assertTrue(tar1.exists());

    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.EMPTY)))
        .thenReturn(ids(10));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(11)), anyInt(), isNull(), eq(ContainerHealthState.EMPTY)))
        .thenReturn(Collections.emptyList());
    ExportJob.Id job2 = exportManager.submitJob(ContainerID.valueOf(0), null, ContainerHealthState.EMPTY, 0, 0, 0);
    waitForTerminal(job2);

    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.UNDER_REPLICATED)))
        .thenReturn(ids(20));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(21)), anyInt(), isNull(), eq(ContainerHealthState.UNDER_REPLICATED)))
        .thenReturn(Collections.emptyList());
    ExportJob.Id job3 = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.UNDER_REPLICATED, 0, 0, 0);
    waitForTerminal(job3);

    assertNotNull(exportManager.getExportStatus(job2));
    assertNotNull(exportManager.getExportStatus(job3));
    assertNull(exportManager.getExportStatus(job1));
    assertFalse(tar1.exists()); //evicting a terminal job deletes its TAR too
  }

  @Test
  public void testOrphanJobDirRemovedOnStartup() throws Exception {
    ExportJob.Id jobId = ExportJob.Id.newId();
    Path orphan = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId));
    Files.createDirectories(orphan);
    exportManager.shutdown();
    exportManager = newExportManager(TEST_DEFAULT_SHARD_SIZE, TEST_DEFAULT_PAGE_SIZE,
        TEST_MAX_TERMINAL_JOBS, () -> true);
    assertFalse(Files.exists(orphan));
  }

  @Test
  public void testIncompleteExportArtifactsRemovedOnStartup() throws Exception {
    ExportJob.Id jobId = ExportJob.Id.newId();
    Path jobDir = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId));
    Files.createDirectories(jobDir);
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    ExportFileManager fileManager = new ExportFileManager(tempDir.getAbsolutePath());
    File partialArchiveTemp = fileManager.resolveArchiveTempFile(scope, "20260101T000000Z", jobId);
    assertTrue(partialArchiveTemp.createNewFile());

    exportManager.shutdown();
    exportManager = newExportManager(TEST_DEFAULT_SHARD_SIZE, TEST_DEFAULT_PAGE_SIZE,
        TEST_MAX_TERMINAL_JOBS, () -> true);

    assertFalse(Files.exists(jobDir));
    assertFalse(partialArchiveTemp.exists());
  }

  @Test
  public void testCompletedExportTarRetainedOnRestart() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(2)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob.Id jobId = exportManager.submitJob(ContainerID.valueOf(0), null, ContainerHealthState.MISSING, 0, 0, 0);
    ExportJob.Status status = waitForTerminal(jobId);
    assertEquals(ExportJob.ExecutionState.SUCCEEDED, status.getExecutionState());
    File tar = new File(status.getTarPath());
    assertTrue(tar.exists());

    exportManager.shutdown();
    exportManager = newExportManager(TEST_DEFAULT_SHARD_SIZE, TEST_DEFAULT_PAGE_SIZE,
        TEST_MAX_TERMINAL_JOBS, () -> true);
    assertTrue(tar.exists());
    assertNull(exportManager.getExportStatus(jobId));
  }

  @Test
  public void testOrphanJobDirDoesNotDeleteCompletedTar() throws Exception {
    ExportJob.Id jobId = ExportJob.Id.newId();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    ExportFileManager fileManager = new ExportFileManager(tempDir.getAbsolutePath());
    File completedTar = fileManager.resolveArchiveFile(scope, "20260101T000000Z", jobId);
    assertTrue(completedTar.createNewFile());
    Path orphanJobDir = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId));
    Files.createDirectories(orphanJobDir);

    exportManager.shutdown();
    exportManager = newExportManager(TEST_DEFAULT_SHARD_SIZE, TEST_DEFAULT_PAGE_SIZE,
        TEST_MAX_TERMINAL_JOBS, () -> true);

    assertTrue(completedTar.exists());
    assertFalse(Files.exists(orphanJobDir));
  }

  @Test
  public void testRejectSubmitWhenNotLeader() throws Exception {
    exportManager.shutdown();
    exportManager = newExportManager(TEST_DEFAULT_SHARD_SIZE, TEST_DEFAULT_PAGE_SIZE, 2, () -> false);
    assertNull(exportManager.submitJob(ContainerID.valueOf(0), null, ContainerHealthState.MISSING, 0, 0, 0));
  }

  private ContainerExportManager newExportManager(int defaultShardSize, int defaultPageSize, int maxTerminalJobs,
      BooleanSupplier isLeaderReady) throws Exception {
    ContainerExportManager manager = new ContainerExportManager(containerManager, isLeaderReady,
        tempDir.getAbsolutePath(), defaultShardSize, defaultPageSize, maxTerminalJobs, TEST_SCM_ID);
    manager.start();
    return manager;
  }

  private static List<ContainerID> ids(long... values) {
    return Arrays.stream(values).mapToObj(ContainerID::valueOf)
        .collect(Collectors.toList());
  }

  private ExportJob.Status waitForTerminal(ExportJob.Id jobId) throws Exception {
    GenericTestUtils.waitFor(() -> {
      ExportJob.Status status = exportManager.getExportStatus(jobId);
      return status != null && status.getExecutionState().isTerminal();
    }, 100, 30_000);
    return exportManager.getExportStatus(jobId);
  }

  private static void extractGzTar(File archive, Path extractDir) throws Exception {
    Files.createDirectories(extractDir);
    try (InputStream in = new GZIPInputStream(Files.newInputStream(archive.toPath()));
         ArchiveInputStream<TarArchiveEntry> tarIn = Archiver.untar(in)) {
      TarArchiveEntry entry;
      while ((entry = tarIn.getNextEntry()) != null) {
        Archiver.extractEntry(entry, tarIn, entry.getSize(), extractDir, extractDir.resolve(entry.getName()));
      }
    }
  }
}
