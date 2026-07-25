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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
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
  public void testResolveTarPath() {
    String jobId = UUID.randomUUID().toString();
    ExportScope scope = ExportScope.of(null, ContainerHealthState.MISSING);
    String tarPath = fileManager.resolveTarPath(scope, "20260101T120000Z", jobId);
    assertTrue(tarPath.endsWith("container-ids-health-MISSING-20260101T120000Z-" + jobId + ".tar"));
  }

  @Test
  public void testOrphanWorkDirRemovedOnStartup() throws Exception {
    String jobId = UUID.randomUUID().toString();
    Path orphan = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId)).resolve("work");
    Files.createDirectories(orphan);

    fileManager.start();

    assertFalse(Files.exists(orphan));
  }

  @Test
  public void testIncompleteExportArtifactsRemovedOnStartup() throws Exception {
    String jobId = UUID.randomUUID().toString();
    Path jobDir = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId)).resolve("work");
    Files.createDirectories(jobDir);
    File partialTar = new File(tempDir, "container-ids-health-MISSING-20260101T000000Z-" + jobId + ".tar");
    assertTrue(partialTar.createNewFile());
    File inProgress = new File(tempDir, jobId + ExportFileManager.IN_PROGRESS_MARKER_SUFFIX);
    assertTrue(inProgress.createNewFile());

    fileManager.start();

    assertFalse(Files.exists(jobDir));
    assertFalse(partialTar.exists());
    assertFalse(inProgress.exists());
  }

  @Test
  public void testOrphanWorkDirWithoutMarkerDoesNotDeleteCompletedTar() throws Exception {
    String jobId = UUID.randomUUID().toString();
    File completedTar = new File(tempDir, "container-ids-health-MISSING-20260101T000000Z-" + jobId + ".tar");
    assertTrue(completedTar.createNewFile());
    Path orphanWorkDir = tempDir.toPath().resolve(ExportFileManager.exportJobDirName(jobId));
    Files.createDirectories(orphanWorkDir.resolve("work"));

    fileManager.start();

    assertTrue(completedTar.exists());
    assertFalse(Files.exists(orphanWorkDir));
  }
}
