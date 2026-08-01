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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.ozone.util.UUIDUtil;
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
 * {@code container-ids_{scope}_{timestamp}_job{jobId}.tar.gz.tmp} and atomically renames it to
 * {@code .tar.gz} on close ({@link AtomicFileOutputStream}), so a partial {@code .tar.gz} is
 * never visible. {@link #lock()} uses {@code in_use.lock} to exclude concurrent writers.
 *
 * <pre>
 * {exportDirectory}/
 * ├── in_use.lock
 * ├── container-ids_{scope}_{timestamp}_job{jobId}.tar.gz
 * ├── container-ids_{scope}_{timestamp}_job{jobId}.tar.gz.tmp
 * └── export_{jobId}/
 *     ├── container-ids_{scope}_{metadataTimestamp}_part001.txt
 *     └── ...
 * </pre>
 *
 * <p><b>Incomplete work</b> ({@code export_{jobId}/} and {@code .tar.gz.tmp}) is removed by
 * {@link #cleanupFailedJob(Path, File)} on failure or cancel, and by {@link #start()} for every
 * leftover directory and temp file after SCM restart. Completed {@code .tar.gz} files are kept.
 *
 * <p><b>Completed {@code .tar.gz}</b> remains on disk until the export manager evicts it
 * ({@code maxTerminalJobs} in {@code ContainerExportManager}) via {@link #deleteExportTar(String)}.
 *
 * <p><b>SCM restart:</b> in-memory job status is lost. {@link #start()} clears incomplete work;
 * {@link #listCompletedArchivePaths()} returns existing {@code tarPath} values (oldest first);
 * {@link #jobIdFromArchiveFileName(String)} parses {@code jobId} for terminal-job rebuild in
 * {@code ContainerExportManager}.
 */
final class ExportFileManager {

  private static final Logger LOG = LoggerFactory.getLogger(ExportFileManager.class);

  static final String EXPORT_JOB_DIR_PREFIX = "export_";
  static final String EXPORT_ARCHIVE_JOB_INFIX = "_job";
  static final String EXPORT_ARCHIVE_SUFFIX = ".tar.gz";
  static final String EXPORT_ARCHIVE_TMP_SUFFIX = EXPORT_ARCHIVE_SUFFIX + AtomicFileOutputStream.TMP_EXTENSION;
  static final String EXPORT_LOCK_NAME = "in_use.lock";
  private static final int ARCHIVE_TIMESTAMP_LENGTH = 16;

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
    removeIncompleteWorkOnStartup();
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

  File resolveArchiveFile(ExportScope scope, String archiveTimestamp, ExportJob.Id jobId) {
    return new File(exportDirectory, String.format("container-ids_%s_%s%s%s%s",
        scope.getValue(), archiveTimestamp, EXPORT_ARCHIVE_JOB_INFIX, jobId.getValue(), EXPORT_ARCHIVE_SUFFIX));
  }

  File resolveArchiveTempFile(ExportScope scope, String archiveTimestamp, ExportJob.Id jobId) {
    return AtomicFileOutputStream.getTemporaryFile(resolveArchiveFile(scope, archiveTimestamp, jobId));
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
    Arrays.sort(matches, Comparator.comparing(
        file -> archiveTimestampFromArchiveFileName(file.getName())));
    List<String> archivePaths = new ArrayList<>(matches.length);
    for (File archive : matches) {
      archivePaths.add(archive.getAbsolutePath());
    }
    return archivePaths;
  }

  static String archiveTimestampFromArchiveFileName(String fileName) {
    int jobIndex = fileName.lastIndexOf(EXPORT_ARCHIVE_JOB_INFIX);
    if (jobIndex < ARCHIVE_TIMESTAMP_LENGTH + 1
            || !fileName.endsWith(EXPORT_ARCHIVE_SUFFIX)
            || fileName.endsWith(EXPORT_ARCHIVE_TMP_SUFFIX)) {
      return null;
    }
    return fileName.substring(jobIndex - ARCHIVE_TIMESTAMP_LENGTH, jobIndex);
  }

  static ExportJob.Id jobIdFromArchiveFileName(String fileName) {
    if (!fileName.endsWith(EXPORT_ARCHIVE_SUFFIX)) {
      return null;
    }
    String nameWithoutSuffix = fileName.substring(0, fileName.length() - EXPORT_ARCHIVE_SUFFIX.length());
    int jobIndex = nameWithoutSuffix.lastIndexOf(EXPORT_ARCHIVE_JOB_INFIX);
    if (jobIndex < 0) {
      return null;
    }
    String jobId = nameWithoutSuffix.substring(jobIndex + EXPORT_ARCHIVE_JOB_INFIX.length());
    return UUIDUtil.isValidUuidString(jobId) ? ExportJob.Id.of(jobId) : null;
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

  void cleanupFailedJob(Path jobDir, File archiveFile) {
    if (jobDir != null) {
      FileUtils.deleteQuietly(jobDir.toFile());
    }
    if (archiveFile != null) {
      FileUtils.deleteQuietly(AtomicFileOutputStream.getTemporaryFile(archiveFile));
    }
  }

  private void removeIncompleteWorkOnStartup() {
    File exportDir = new File(exportDirectory);
    File[] children = exportDir.listFiles();
    if (children != null) {
      for (File child : children) {
        if (child.isDirectory() && jobIdFromExportDirName(child.getName()) != null) {
          FileUtils.deleteQuietly(child);
          LOG.debug("Removed incomplete container export job directory: {}", child.getAbsolutePath());
        }
      }
    }
    File[] tempFiles = exportDir.listFiles((dir, fileName) -> fileName.endsWith(EXPORT_ARCHIVE_TMP_SUFFIX));
    if (tempFiles != null) {
      for (File tempFile : tempFiles) {
        if (FileUtils.deleteQuietly(tempFile)) {
          LOG.debug("Removed incomplete container export archive temp file: {}", tempFile.getName());
        }
      }
    }
  }

  static String exportJobDirName(ExportJob.Id jobId) {
    return EXPORT_JOB_DIR_PREFIX + jobId.getValue();
  }

  private static ExportJob.Id jobIdFromExportDirName(String dirName) {
    if (!dirName.startsWith(EXPORT_JOB_DIR_PREFIX)) {
      return null;
    }
    String jobId = dirName.substring(EXPORT_JOB_DIR_PREFIX.length());
    return UUIDUtil.isValidUuidString(jobId) ? ExportJob.Id.of(jobId) : null;
  }
}
