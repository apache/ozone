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
 *
 * <p>The export directory ({@code exportDirectory}, typically {@code {scm.db.dirs}/exports})
 * uses the layout below. The manager gzip-compresses the archive ({@code .tar.gz}) so operators
 * can stream entries with {@code zcat}
 * 
 * <pre>
 * {exportDirectory}/
 * ├── {jobId}.in-progress
 * ├── container-ids-{scope}-{timestamp}-{jobId}.tar.gz
 * └── export_{jobId}/
 *     ├── container-ids-{scope}-{timestamp}-part001.txt
 *     └── ...
 * </pre>
 *
 * <p>{@code export_{jobId}/} holds shard text files while the job appends them into the archive.
 *
 * <p><b>When {@code export_{jobId}/} is deleted:</b> the export manager deletes it after the
 * archive closes successfully, or during {@link #cleanupFailedArtifacts} on failure or cancel.
 * On startup, {@link #start()} deletes a leftover {@code export_{jobId}/} when no in-progress
 * marker remains. If the marker still exists, {@link #start()} deletes {@code export_{jobId}/}
 * together with the marker and any partial archive for that job id.
 *
 * <p><b>When {@code .tar.gz} is deleted:</b> {@link #cleanupFailedArtifacts} deletes partial
 * archives for failed or cancelled jobs. {@link #start()} deletes partial archives for jobs that
 * still have an in-progress marker. Completed archives remain on disk until the export manager
 * evicts the job from memory ({@code maxTerminalJobs} in {@code ContainerExportManager}) or an
 * operator deletes them manually. After SCM restart, in-memory eviction state is lost, so
 * completed archives persist until manual cleanup.
 *
 * <p><b>SCM restart while a job runs:</b> the in-progress marker and {@code export_{jobId}/}
 * remain on disk, but in-memory job status is lost. {@link #start()} treats the job as incomplete,
 * removes the marker, workspace, and any partial {@code .tar.gz} for that job id, and the
 * operator re-submits the export on the new leader.
 */
final class ExportFileManager {

  private static final Logger LOG = LoggerFactory.getLogger(ExportFileManager.class);

  static final String IN_PROGRESS_MARKER_SUFFIX = ".in-progress";
  static final String EXPORT_JOB_DIR_PREFIX = "export_";
  static final String EXPORT_ARCHIVE_SUFFIX = ".tar.gz";

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
    String archiveFileName = String.format("container-ids-%s-%s-%s%s",
        scope.getValue(), fileTimestamp, jobId, EXPORT_ARCHIVE_SUFFIX);
    return exportDirectory + File.separator + archiveFileName;
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
      LOG.debug("Removed container export archive: {}", tar.getName());
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
      LOG.info("Removed incomplete container export archive for job {}: {}", jobId, tar.getName());
    }
    File jobDir = new File(exportDirectory, exportJobDirName(jobId));
    if (jobDir.isDirectory()) {
      FileUtils.deleteQuietly(jobDir);
      LOG.info("Removed orphaned container export job directory: {}", jobDir.getAbsolutePath());
    }
  }

  private File findTarForJobId(String jobId) {
    File exportDir = new File(exportDirectory);
    String suffix = "-" + jobId + EXPORT_ARCHIVE_SUFFIX;
    File[] matches = exportDir.listFiles((dir, fileName) -> fileName.endsWith(suffix));
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
