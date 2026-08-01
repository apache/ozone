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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ExportFileManager}.
 */
public class TestExportFileManager {

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
  public void testResolveArchiveFile() {
    ExportJob.Id jobId = ExportJob.Id.newId();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    File archive = fileManager.resolveArchiveFile(scope, "20260101T120000Z", jobId);
    assertTrue(archive.getName().contains("health-MISSING_lifecycle-ANY_20260101T120000Z"));
    assertTrue(archive.getName().endsWith(ExportFileManager.EXPORT_ARCHIVE_JOB_INFIX + jobId.getValue()
        + ExportFileManager.EXPORT_ARCHIVE_SUFFIX));
  }

  @Test
  public void testResolveArchiveTempFile() {
    ExportJob.Id jobId = ExportJob.Id.newId();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    File tempFile = fileManager.resolveArchiveTempFile(scope, "20260101T120000Z", jobId);
    assertTrue(tempFile.getName().endsWith(ExportFileManager.EXPORT_ARCHIVE_TMP_SUFFIX));
  }

  @Test
  public void testJobIdFromArchiveFileName() {
    String jobId = UUID.randomUUID().toString();
    String fileName = "container-ids_health-MISSING_lifecycle-ANY_20260101T120000Z"
        + ExportFileManager.EXPORT_ARCHIVE_JOB_INFIX + jobId + ExportFileManager.EXPORT_ARCHIVE_SUFFIX;
    assertEquals(ExportJob.Id.of(jobId), ExportFileManager.jobIdFromArchiveFileName(fileName));
    assertNull(ExportFileManager.jobIdFromArchiveFileName("container-ids_health-MISSING_lifecycle-ANY_20260101T120000Z"
        + ExportFileManager.EXPORT_ARCHIVE_SUFFIX));
  }

  @Test
  public void testArchiveTimestampFromArchiveFileName() {
    String fileName = "container-ids_health-MISSING_lifecycle-ANY_20260101T120000Z"
        + ExportFileManager.EXPORT_ARCHIVE_JOB_INFIX + UUID.randomUUID() + ExportFileManager.EXPORT_ARCHIVE_SUFFIX;
    assertEquals("20260101T120000Z", ExportFileManager.archiveTimestampFromArchiveFileName(fileName));
  }

  @Test
  public void testListCompletedArchivePaths() throws Exception {
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    ExportJob.Id olderJobId = ExportJob.Id.newId();
    File olderArchive = fileManager.resolveArchiveFile(scope, "20260101T120000Z", olderJobId);
    assertTrue(olderArchive.createNewFile());
    assertTrue(olderArchive.setLastModified(2_000L));
    ExportJob.Id newerJobId = ExportJob.Id.newId();
    File newerArchive = fileManager.resolveArchiveFile(scope, "20260101T120001Z", newerJobId);
    assertTrue(newerArchive.createNewFile());
    assertTrue(newerArchive.setLastModified(1_000L));
    ExportJob.Id tempJobId = ExportJob.Id.newId();
    File tempArchive = fileManager.resolveArchiveTempFile(scope, "20260101T120002Z", tempJobId);
    assertTrue(tempArchive.createNewFile());

    List<String> completedPaths = fileManager.listCompletedArchivePaths();
    assertEquals(2, completedPaths.size());
    assertEquals(olderArchive.getAbsolutePath(), completedPaths.get(0));
    assertEquals(newerArchive.getAbsolutePath(), completedPaths.get(1));
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
    File partialArchiveTemp = fileManager.resolveArchiveTempFile(scope, "20260101T000000Z", jobId);
    assertTrue(partialArchiveTemp.createNewFile());

    fileManager.start();

    assertFalse(Files.exists(jobDir));
    assertFalse(partialArchiveTemp.exists());
  }

  @Test
  public void testOrphanJobDirDoesNotDeleteCompletedTar() throws Exception {
    ExportJob.Id jobId = ExportJob.Id.newId();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    File completedArchive = fileManager.resolveArchiveFile(scope, "20260101T000000Z", jobId);
    assertTrue(completedArchive.createNewFile());
    Path orphanJobDir = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId));
    Files.createDirectories(orphanJobDir);

    fileManager.start();

    assertTrue(completedArchive.exists());
    assertFalse(Files.exists(orphanJobDir));
  }
}
