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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages on-disk paths and artifacts for container ID export jobs.
 * Layout under the export directory ({@code {exportDirectory}}, typically {@code {scm.db.dirs}/exports}):
 * <p>
 * {exportDirectory}/
 *   {jobId}.in-progress                             // marker while a job is running
 *   container-ids-{scope}-{timestamp}-{jobId}.tar   // completed export archive
 *   export-{jobId}/                                 // per-job workspace (removed on success)
 *     work/
 *       container-ids-{scope}-{timestamp}-part001.txt
 *       ...
 * <p>
 * Shard text files are written under {@code export-{jobId}/work/}, appended into the TAR at
 * {@code {exportDirectory}}, then the manager deletes the workspace. The manager clears the
 * {@code .in-progress} marker only after the TAR closes successfully. On startup, the manager
 * removes orphaned markers, workspaces, and partial TAR files for the same job id together.
 */
final class ExportFileManager {

  private static final Logger LOG = LoggerFactory.getLogger(ExportFileManager.class);
  static final String IN_PROGRESS_MARKER_SUFFIX = ".in-progress";
  static final String EXPORT_JOB_DIR_PREFIX = "export-";
  private final String exportDirectory;

  ExportFileManager(String exportDirectory) {
    this.exportDirectory = Objects.requireNonNull(exportDirectory, "exportDirectory == null");
  }

  String getExportDirectory() {
    return exportDirectory;
  }

  void start() throws IOException {
    Files.createDirectories(Paths.get(exportDirectory));
    cleanupOrphanedExportArtifacts();
  }

  String resolveTarPath(ExportScope scope, String fileTimestamp, String jobId) {
    String tarFileName = String.format("container-ids-%s-%s-%s.tar", scope.getValue(), fileTimestamp, jobId);
    return exportDirectory + File.separator + tarFileName;
  }

  void markExportInProgress(String jobId) throws IOException {
    Files.createFile(inProgressMarkerFile(jobId).toPath());
  }

  void clearExportInProgress(String jobId) {
    FileUtils.deleteQuietly(inProgressMarkerFile(jobId));
  }

  void deleteExportTar(String tarPath) {
    if (tarPath == null) {
      return;
    }
    File tar = new File(tarPath);
    if (tar.isFile() && FileUtils.deleteQuietly(tar)) {
      LOG.debug("Removed container export TAR: {}", tar.getName());
    }
  }

  void cleanupFailedArtifacts(Path jobDir, File tarFile, String jobId) {
    if (jobDir != null) {
      FileUtils.deleteQuietly(jobDir.toFile());
    }
    if (tarFile != null) {
      FileUtils.deleteQuietly(tarFile);
    }
    clearExportInProgress(jobId);
  }

  private void cleanupOrphanedExportArtifacts() {
    File exportDir = new File(exportDirectory);
    File[] children = exportDir.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isFile() && child.getName().endsWith(IN_PROGRESS_MARKER_SUFFIX)) {
        String jobId = child.getName().substring(
            0, child.getName().length() - IN_PROGRESS_MARKER_SUFFIX.length());
        if (isUuidDirectoryName(jobId)) {
          removeIncompleteExportArtifacts(jobId);
        }
      }
    }
    for (File child : children) {
      if (child.isDirectory()) {
        String jobId = jobIdFromExportDirName(child.getName());
        if (jobId == null) {
          continue;
        }
        if (inProgressMarkerFile(jobId).exists()) {
          removeIncompleteExportArtifacts(jobId);
        } else {
          FileUtils.deleteQuietly(child);
        }
      }
    }
  }

  private void removeIncompleteExportArtifacts(String jobId) {
    LOG.info("Removing incomplete container export artifacts for job {}", jobId);
    FileUtils.deleteQuietly(inProgressMarkerFile(jobId));
    File tar = findTarForJobId(jobId);
    if (tar != null) {
      FileUtils.deleteQuietly(tar);
      LOG.info("Removed incomplete container export TAR for job {}: {}", jobId, tar.getName());
    }
    File jobWorkDir = new File(exportDirectory, exportJobDirName(jobId));
    if (jobWorkDir.isDirectory()) {
      FileUtils.deleteQuietly(jobWorkDir);
      LOG.info("Removed orphaned container export work directory: {}", jobWorkDir.getAbsolutePath());
    }
  }

  private File findTarForJobId(String jobId) {
    File exportDir = new File(exportDirectory);
    File[] matches = exportDir.listFiles(
        (dir, fileName) -> fileName.endsWith("-" + jobId + ".tar"));
    if (matches == null || matches.length == 0) {
      return null;
    }
    return matches[0];
  }

  private File inProgressMarkerFile(String jobId) {
    return new File(exportDirectory, jobId + IN_PROGRESS_MARKER_SUFFIX);
  }

  static String exportJobDirName(String jobId) {
    return EXPORT_JOB_DIR_PREFIX + jobId;
  }

  private static String jobIdFromExportDirName(String dirName) {
    if (!dirName.startsWith(EXPORT_JOB_DIR_PREFIX)) {
      return null;
    }
    String jobId = dirName.substring(EXPORT_JOB_DIR_PREFIX.length());
    return isUuidDirectoryName(jobId) ? jobId : null;
  }

  private static boolean isUuidDirectoryName(String directoryName) {
    try {
      return directoryName.equals(UUID.fromString(directoryName).toString());
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
