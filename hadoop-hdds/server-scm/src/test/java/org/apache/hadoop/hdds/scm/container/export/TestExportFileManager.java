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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.utils.Archiver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ExportFileManager}.
 */
public class TestExportFileManager {

  private static final String TEST_JOB_START_TIME = "2026-01-01-12-00-00";

  @TempDir
  private File tempDir;

  private ExportFileManager fileManager;

  @BeforeEach
  public void setup() throws Exception {
    fileManager = new ExportFileManager(tempDir.getAbsolutePath());
    fileManager.start();
  }

  @Test
  public void testExportScopeUsesAnyForNullFilters() {
    assertEquals("health-MISSING_lifecycle-ANY",
        ExportScope.of(null, ContainerHealthState.MISSING).getValue());
    assertEquals("health-ANY_lifecycle-OPEN",
        ExportScope.of(LifeCycleState.OPEN, null).getValue());
  }

  @Test
  public void testRejectMissingFilters() {
    assertThrows(IllegalArgumentException.class, () -> ExportScope.of(null, null));
  }

  @Test
  public void testResolveArchiveFile() {
    ExportJob.Id jobId = ExportJob.Id.newId();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    File archive = fileManager.resolveArchiveFile(scope, TEST_JOB_START_TIME, jobId);
    assertTrue(archive.getName().contains("health-MISSING_lifecycle-ANY_" + TEST_JOB_START_TIME));
    assertTrue(archive.getName().endsWith(ExportFileManager.EXPORT_ARCHIVE_JOB_INFIX + jobId.getValue()
        + ExportFileManager.EXPORT_ARCHIVE_SUFFIX));
  }

  @Test
  public void testResolveArchiveTempFile() {
    ExportJob.Id jobId = ExportJob.Id.newId();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    File tempFile = fileManager.resolveArchiveTempFile(scope, TEST_JOB_START_TIME, jobId);
    assertTrue(tempFile.getName().endsWith(ExportFileManager.EXPORT_ARCHIVE_TMP_SUFFIX));
  }

  @Test
  public void testJobIdFromArchiveFileName() {
    String jobId = UUID.randomUUID().toString();
    String fileName = "container-ids_health-MISSING_lifecycle-ANY_" + TEST_JOB_START_TIME
        + ExportFileManager.EXPORT_ARCHIVE_JOB_INFIX + jobId + ExportFileManager.EXPORT_ARCHIVE_SUFFIX;
    assertEquals(ExportJob.Id.of(jobId), ExportFileManager.jobIdFromArchiveFileName(fileName));
    assertNull(ExportFileManager.jobIdFromArchiveFileName("container-ids_health-MISSING_lifecycle-ANY_"
        + TEST_JOB_START_TIME + ExportFileManager.EXPORT_ARCHIVE_SUFFIX));
  }

  @Test
  public void testJobStartTimeFromArchiveFileName() {
    String fileName = "container-ids_health-MISSING_lifecycle-ANY_" + TEST_JOB_START_TIME
        + ExportFileManager.EXPORT_ARCHIVE_JOB_INFIX + UUID.randomUUID() + ExportFileManager.EXPORT_ARCHIVE_SUFFIX;
    assertEquals(TEST_JOB_START_TIME, ExportFileManager.jobStartTimeFromArchiveFileName(fileName));
  }

  @Test
  public void testListCompletedArchivePaths() throws Exception {
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    ExportJob.Id olderJobId = ExportJob.Id.newId();
    File olderArchive = fileManager.resolveArchiveFile(scope, "2026-01-01-12-00-00", olderJobId);
    assertTrue(olderArchive.createNewFile());
    assertTrue(olderArchive.setLastModified(2_000L));
    ExportJob.Id newerJobId = ExportJob.Id.newId();
    File newerArchive = fileManager.resolveArchiveFile(scope, "2026-01-01-12-00-01", newerJobId);
    assertTrue(newerArchive.createNewFile());
    assertTrue(newerArchive.setLastModified(1_000L));
    ExportJob.Id tempJobId = ExportJob.Id.newId();
    File tempArchive = fileManager.resolveArchiveTempFile(scope, "2026-01-01-12-00-02", tempJobId);
    assertTrue(tempArchive.createNewFile());

    List<String> completedPaths = fileManager.listCompletedArchivePaths();
    assertEquals(2, completedPaths.size());
    assertEquals(olderArchive.getAbsolutePath(), completedPaths.get(0));
    assertEquals(newerArchive.getAbsolutePath(), completedPaths.get(1));
  }

  @Test
  public void testWriteArchiveFromPartFiles() throws Exception {
    ExportJob.Id jobId = ExportJob.Id.newId();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    ExportJob job = new ExportJob(jobId, scope, TEST_JOB_START_TIME);
    File archive = fileManager.resolveArchiveFile(scope, TEST_JOB_START_TIME, jobId);

    fileManager.createJobDirectory(jobId);
    try (BufferedWriter writer = fileManager.newPartWriter(jobId, job.partFileName(1))) {
      job.writeMetadataHeader(writer, 1, 1L);
      writer.write("1\n2\n");
    }

    fileManager.writeArchive(jobId, archive.getAbsolutePath());
    fileManager.deleteJobDirectory(jobId);

    assertTrue(archive.exists());
    Path extractDir = Files.createTempDirectory("export-archive");
    try {
      extractGzTar(archive, extractDir);
      List<String> partNames;
      try (Stream<Path> stream = Files.list(extractDir)) {
        partNames = stream.map(path -> path.getFileName().toString()).collect(Collectors.toList());
      }
      assertEquals(1, partNames.size());
      assertTrue(partNames.get(0).endsWith("part001.txt"));
    } finally {
      FileUtils.deleteQuietly(extractDir.toFile());
    }
  }

  @Test
  public void testCleanupFailedJobRemovesJobDirAndTempArchive() throws Exception {
    ExportJob.Id jobId = ExportJob.Id.newId();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    File archive = fileManager.resolveArchiveFile(scope, TEST_JOB_START_TIME, jobId);
    File tempArchive = fileManager.resolveArchiveTempFile(scope, TEST_JOB_START_TIME, jobId);
    assertTrue(tempArchive.createNewFile());

    fileManager.createJobDirectory(jobId);
    fileManager.cleanupFailedJob(jobId, archive.getAbsolutePath());

    assertFalse(Files.exists(tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId))));
    assertFalse(tempArchive.exists());
    assertFalse(archive.exists());
  }

  @Test
  public void testOrphanJobDirRemovedOnStartup() throws Exception {
    ExportJob.Id jobId = ExportJob.Id.newId();
    Path orphanJobDir = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId));
    Files.createDirectories(orphanJobDir);

    fileManager.start();

    assertFalse(Files.exists(orphanJobDir));
  }

  @Test
  public void testIncompleteExportArtifactsRemovedOnStartup() throws Exception {
    ExportJob.Id jobId = ExportJob.Id.newId();
    Path jobDir = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId));
    Files.createDirectories(jobDir);
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    File partialArchiveTemp = fileManager.resolveArchiveTempFile(scope, "2026-01-01-00-00-00", jobId);
    assertTrue(partialArchiveTemp.createNewFile());

    fileManager.start();

    assertFalse(Files.exists(jobDir));
    assertFalse(partialArchiveTemp.exists());
  }

  @Test
  public void testOrphanJobDirDoesNotDeleteCompletedTar() throws Exception {
    ExportJob.Id jobId = ExportJob.Id.newId();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    File completedArchive = fileManager.resolveArchiveFile(scope, "2026-01-01-00-00-00", jobId);
    assertTrue(completedArchive.createNewFile());
    Path orphanJobDir = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId));
    Files.createDirectories(orphanJobDir);

    fileManager.start();

    assertTrue(completedArchive.exists());
    assertFalse(Files.exists(orphanJobDir));
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
