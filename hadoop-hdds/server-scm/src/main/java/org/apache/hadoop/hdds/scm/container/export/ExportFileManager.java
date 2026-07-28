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
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages on-disk paths and artifacts for container ID export jobs.
 *
 * <p>The export directory ({@code exportDirectory}, typically {@code {scm.db.dirs}/exports})
 * uses the layout below. The manager gzip-compresses the archive ({@code .tar.gz}) so operators
 * can stream entries with {@code zcat}.
 *
 * <p>While a job runs, shard text files are written under {@code export_{jobId}/}. The archive is
 * created only after all shards are written. The export manager writes
 * {@code container-ids-{scope}-{timestamp}.tar.gz.tmp} and atomically renames it to
 * {@code .tar.gz} on close ({@link AtomicFileOutputStream}), so a partial {@code .tar.gz} is
 * never visible. {@link #lock()} is used to exclude concurrent writers.
 *
 * <pre>
 * {exportDirectory}/
 * ├── in_use.lock
 * ├── {jobId}.in-progress
 * ├── container-ids-{scope}-{timestamp}.tar.gz
 * ├── container-ids-{scope}-{timestamp}.tar.gz.tmp
 * └── export_{jobId}/
 *     ├── container-ids-{scope}-{timestamp}-part001.txt
 *     └── ...
 * </pre>
 *
 * <p><b>When {@code export_{jobId}/} is deleted:</b> the export manager deletes it after the
 * archive is committed, or during {@link #cleanupFailedArtifacts} on failure or cancel.
 * On startup, {@link #start()} deletes a leftover {@code export_{jobId}/} when no in-progress
 * marker remains. If the marker still exists, {@link #start()} deletes {@code export_{jobId}/}
 * together with the marker and any {@code .tar.gz.tmp} for that incomplete job.
 *
 * <p><b>When {@code .tar.gz.tmp} is deleted:</b> only the temporary file is removed for
 * incomplete work; a partial {@code .tar.gz} is never written. {@link #cleanupFailedArtifacts}
 * deletes {@code .tar.gz.tmp} for failed or cancelled jobs. {@link #start()} deletes
 * {@code .tar.gz.tmp} for jobs that still have an in-progress marker.
 *
 * <p><b>When completed {@code .tar.gz} is deleted:</b> completed archives remain on disk until
 * the export manager evicts them ({@code maxTerminalJobs} in {@code ContainerExportManager}).
 *
 * <p><b>SCM restart:</b> in-memory job status is lost and {@code jobId} cannot be recovered from
 * the archive file name. {@link #listCompletedArchivePaths()} returns existing {@code tarPath}
 * values (oldest first) so {@code ContainerExportManager} can rebuild terminal-job eviction state.
 * Jobs with an in-progress marker are treated as incomplete: {@link #start()} removes the marker,
 * {@code export_{jobId}/}, and any {@code .tar.gz.tmp}, and the operator re-submits the export
 * on the new leader.
 */
final class ExportFileManager {

  private static final Logger LOG = LoggerFactory.getLogger(ExportFileManager.class);

  static final String IN_PROGRESS_MARKER_SUFFIX = ".in-progress";
  static final String EXPORT_JOB_DIR_PREFIX = "export_";
  static final String EXPORT_ARCHIVE_SUFFIX = ".tar.gz";
  static final String EXPORT_ARCHIVE_TMP_SUFFIX = EXPORT_ARCHIVE_SUFFIX + AtomicFileOutputStream.TMP_EXTENSION;
  static final String EXPORT_LOCK_NAME = "in_use.lock";

  private final String exportDirectory;
  private FileLock exportDirectoryLock;

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

  void lock() throws IOException {
    if (exportDirectoryLock != null) {
      return;
    }
    File lockFile = new File(exportDirectory, EXPORT_LOCK_NAME);
    RandomAccessFile lockAccessFile = new RandomAccessFile(lockFile, "rws");
    try {
      FileLock lock = lockAccessFile.getChannel().tryLock();
      if (lock == null) {
        lockAccessFile.close();
        throw new OverlappingFileLockException();
      }
      exportDirectoryLock = lock;
      LOG.debug("Acquired container export directory lock {}", lockFile.getAbsolutePath());
    } catch (OverlappingFileLockException | IOException e) {
      lockAccessFile.close();
      throw new IOException("Failed to lock container export directory " + exportDirectory, e);
    }
  }

  void unlock() throws IOException {
    if (exportDirectoryLock == null) {
      return;
    }
    exportDirectoryLock.release();
    exportDirectoryLock.channel().close();
    exportDirectoryLock = null;
  }

  File resolveArchiveFile(ExportScope scope, String fileTimestamp) {
    return new File(exportDirectory,
        String.format("container-ids-%s-%s%s", scope.getValue(), fileTimestamp, EXPORT_ARCHIVE_SUFFIX));
  }

  File resolveArchiveTempFile(ExportScope scope, String fileTimestamp) {
    return AtomicFileOutputStream.getTemporaryFile(resolveArchiveFile(scope, fileTimestamp));
  }

  /**
   * Returns completed archive paths ({@code tarPath} in {@code ExportJob.Status}), oldest first.
   */
  List<String> listCompletedArchivePaths() {
    File exportDir = new File(exportDirectory);
    File[] matches = exportDir.listFiles((dir, fileName) -> fileName.endsWith(EXPORT_ARCHIVE_SUFFIX)
        && !fileName.endsWith(EXPORT_ARCHIVE_TMP_SUFFIX));
    if (matches == null || matches.length == 0) {
      return Collections.emptyList();
    }
    Arrays.sort(matches, Comparator.comparingLong(File::lastModified));
    List<String> archivePaths = new ArrayList<>(matches.length);
    for (File archive : matches) {
      archivePaths.add(archive.getAbsolutePath());
    }
    return archivePaths;
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
    File archive = new File(tarPath);
    if (archive.isFile() && FileUtils.deleteQuietly(archive)) {
      LOG.debug("Removed container export archive: {}", archive.getName());
    }
    FileUtils.deleteQuietly(AtomicFileOutputStream.getTemporaryFile(archive));
  }

  void cleanupFailedArtifacts(Path jobDir, File archiveFile, String jobId) {
    if (jobDir != null) {
      FileUtils.deleteQuietly(jobDir.toFile());
    }
    if (archiveFile != null) {
      FileUtils.deleteQuietly(AtomicFileOutputStream.getTemporaryFile(archiveFile));
      FileUtils.deleteQuietly(archiveFile);
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
    deleteOrphanArchiveTempFiles();
  }

  private void removeIncompleteExportArtifacts(String jobId) {
    LOG.info("Removing incomplete container export artifacts for job {}", jobId);
    FileUtils.deleteQuietly(inProgressMarkerFile(jobId));
    deleteOrphanArchiveTempFiles();
    File jobDir = new File(exportDirectory, exportJobDirName(jobId));
    if (jobDir.isDirectory()) {
      FileUtils.deleteQuietly(jobDir);
      LOG.debug("Removed orphaned container export job directory: {}", jobDir.getAbsolutePath());
    }
  }

  private void deleteOrphanArchiveTempFiles() {
    File exportDir = new File(exportDirectory);
    File[] tempFiles = exportDir.listFiles((dir, fileName) -> fileName.endsWith(EXPORT_ARCHIVE_TMP_SUFFIX));
    if (tempFiles == null) {
      return;
    }
    for (File tempFile : tempFiles) {
      if (FileUtils.deleteQuietly(tempFile)) {
        LOG.debug("Removed incomplete container export archive temp file: {}", tempFile.getName());
      }
    }
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
