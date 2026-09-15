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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
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

  private static final int TEST_BATCH_SIZE = 2;
  private static final int TEST_PART_SIZE = 3;
  private static final String TEST_SCM_ID = "test-scm";

  @TempDir
  private File tempDir;

  private ContainerManager containerManager;
  private ContainerExportManager exportManager;

  @BeforeEach
  public void setup() throws Exception {
    containerManager = mock(ContainerManager.class);
    exportManager = newExportManager(TEST_PART_SIZE, TEST_BATCH_SIZE, () -> true);
  }

  @AfterEach
  public void teardown() {
    if (exportManager != null) {
      exportManager.shutdown();
    }
  }

  @Test
  public void testRejectMissingFilters() {
    assertThrows(IllegalArgumentException.class, () ->
        exportManager.submitJob(ContainerID.valueOf(0), null, null));
  }

  @Test
  public void testRejectSubmitWhenNotLeader() throws Exception {
    exportManager.shutdown();
    exportManager = newExportManager(TEST_PART_SIZE, TEST_BATCH_SIZE, () -> false);
    assertNull(exportManager.submitJob(ContainerID.valueOf(0), null, ContainerHealthState.MISSING));
  }

  @Test
  public void testGetExportStatusUnknownJobReturnsNull() {
    assertNull(exportManager.getExportStatus(ExportJob.Id.newId()));
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

    ExportJob first = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING);
    assertNotNull(first);
    assertNull(exportManager.submitJob(ContainerID.valueOf(0), null, ContainerHealthState.EMPTY));
  }

  @Test
  public void testNullStartDefaultsToZeroCursor() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob job = exportManager.submitJob(null, null, ContainerHealthState.MISSING);
    assertNotNull(job);
    waitForDone(job.getId());

    verify(containerManager).getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING));
  }

  @Test
  public void testExportStartsFromRequestedCursor() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(5)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(5, 6));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(7)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob job = exportManager.submitJob(ContainerID.valueOf(5), null,
        ContainerHealthState.MISSING);
    assertNotNull(job);

    ExportJob.Status status = waitForDone(job.getId());
    assertNull(status.getException());
    assertEquals(2, status.getIdsWritten());

    verify(containerManager).getContainerIDs(
        eq(ContainerID.valueOf(5)), anyInt(), isNull(), eq(ContainerHealthState.MISSING));
  }

  @Test
  public void testExportWithLifeCycleFilter() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), eq(LifeCycleState.OPEN), isNull()))
        .thenReturn(ids(1));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(2)), anyInt(), eq(LifeCycleState.OPEN), isNull()))
        .thenReturn(Collections.emptyList());

    ExportJob job = exportManager.submitJob(ContainerID.valueOf(0), LifeCycleState.OPEN, null);
    assertNotNull(job);
    waitForDone(job.getId());

    verify(containerManager).getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), eq(LifeCycleState.OPEN), isNull());
  }

  @Test
  public void testZeroMatchesSucceedsWithoutArchive() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob job = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING);
    assertNotNull(job);

    ExportJob.Status status = waitForDone(job.getId());
    assertNull(status.getException());
    assertEquals(0, status.getIdsWritten());
    assertFalse(archiveFile(job).exists());
  }

  @Test
  public void testSinglePartExportCreatesTar() throws Exception {
    exportManager.shutdown();
    exportManager = newExportManager(100, TEST_BATCH_SIZE, () -> true);

    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1, 2, 3, 4));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(5)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob job = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING);
    assertNotNull(job);

    ExportJob.Status status = waitForDone(job.getId());
    assertNull(status.getException());
    assertEquals(4, status.getIdsWritten());
    assertTrue(job.getFileName().endsWith(ExportFileManager.EXPORT_ARCHIVE_SUFFIX));
    assertTrue(archiveFile(job).exists());
  }

  @Test
  public void testMultiPartExportCreatesTar() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1, 2));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(3)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(3, 4));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(5)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    ExportJob job = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING);
    assertNotNull(job);

    ExportJob.Status status = waitForDone(job.getId());
    assertNull(status.getException());
    assertEquals(4, status.getIdsWritten());
    assertTrue(job.getFileName().endsWith(ExportFileManager.EXPORT_ARCHIVE_SUFFIX));
    File archive = archiveFile(job);
    assertTrue(archive.exists());
    assertFalse(Files.exists(tempDir.toPath().resolve(ExportFileManager.exportJobDirName(job.getId()))));

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
          "# partStartContainerId=#4"));
    } finally {
      FileUtils.deleteQuietly(extractDir.toFile());
    }
  }

  @Test
  public void testSubmitAfterPreviousJobCompletes() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(2)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.EMPTY)))
        .thenReturn(ids(10));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(11)), anyInt(), isNull(), eq(ContainerHealthState.EMPTY)))
        .thenReturn(Collections.emptyList());

    ExportJob first = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING);
    waitForDone(first.getId());

    ExportJob second = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.EMPTY);
    assertNotNull(second);
    ExportJob.Status status = waitForDone(second.getId());
    assertNull(status.getException());
    assertEquals(1, status.getIdsWritten());
  }

  @Test
  public void testFailsWhenLeadershipLostDuringExport() throws Exception {
    AtomicBoolean leader = new AtomicBoolean(true);
    exportManager.shutdown();
    exportManager = newExportManager(TEST_PART_SIZE, TEST_BATCH_SIZE, leader::get);

    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenAnswer(invocation -> {
          leader.set(false);
          return ids(1);
        });

    ExportJob job = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING);
    assertNotNull(job);

    ExportJob.Status status = waitForDone(job.getId());
    assertNotNull(status.getException());
    assertTrue(status.getException().getMessage().contains("lost leadership"));
  }

  @Test
  public void testFailsWhenContainerManagerThrows() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenThrow(new RuntimeException("container listing failed"));

    ExportJob job = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING);
    assertNotNull(job);

    ExportJob.Status status = waitForDone(job.getId());
    assertNotNull(status.getException());
    assertTrue(status.getException().getMessage().contains("container listing failed"));
    assertFalse(Files.exists(tempDir.toPath().resolve(ExportFileManager.exportJobDirName(job.getId()))));
  }

  @Test
  public void testShutdownCancelsRunningExport() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenAnswer(invocation -> {
          Thread.sleep(60_000);
          return Collections.emptyList();
        });

    ExportJob job = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING);
    assertNotNull(job);

    exportManager.shutdown();
    ExportJob.Status status = waitForDone(job.getId());
    assertNotNull(status.getException());
    exportManager = null;
  }

  private File archiveFile(ExportJob job) {
    return new File(tempDir, job.getFileName());
  }

  private ContainerExportManager newExportManager(int partSize, int batchSize,
      BooleanSupplier isLeaderReady) throws Exception {
    ContainerExportManager manager = new ContainerExportManager(TEST_SCM_ID, containerManager, isLeaderReady,
        tempDir.getAbsolutePath(), partSize, batchSize);
    manager.start();
    return manager;
  }

  private static List<ContainerID> ids(long... values) {
    return Arrays.stream(values).mapToObj(ContainerID::valueOf)
        .collect(Collectors.toList());
  }

  private ExportJob.Status waitForDone(ExportJob.Id jobId) throws Exception {
    GenericTestUtils.waitFor(() -> {
      ExportJob.Status status = exportManager.getExportStatus(jobId);
      return status != null && status.isDone();
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
