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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.utils.Archiver;
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
 * <p>While a job runs, part text files are written under {@code export_{jobId}/}. The archive is
 * created only after all parts are written. The export manager writes
 * {@code container-ids_{scope}_{jobStartTime}_job{jobId}.tar.gz.tmp} and atomically renames it to
 * {@code .tar.gz} on close ({@link AtomicFileOutputStream}), so a partial {@code .tar.gz} is
 * never visible. {@link #start()} acquires {@code in_use.lock} to exclude concurrent writers.
 *
 * <pre>
 * {exportDirectory}/
 * ├── in_use.lock
 * ├── container-ids_{scope}_{jobStartTime}_job{jobId}.tar.gz
 * ├── container-ids_{scope}_{jobStartTime}_job{jobId}.tar.gz.tmp
 * └── export_{jobId}/
 *     ├── container-ids_{scope}_{jobStartTime}_part001.txt
 *     └── ...
 * </pre>
 *
 * <p><b>Incomplete work</b> ({@code export_{jobId}/} and {@code .tar.gz.tmp}) is removed by
 * {@link #cleanupFailedJob(ExportJob.Id, String)} on failure or cancel, and by {@link #start()} for every
 * leftover directory and temp file after SCM restart. Completed {@code .tar.gz} files are kept.
 *
 * <p><b>SCM restart:</b> {@link #start()} acquires the export directory lock and clears incomplete work.
 * {@link #listCompletedArchivePaths()} returns existing archive paths (oldest first).
 */
final class ExportFileManager {

  private static final Logger LOG = LoggerFactory.getLogger(ExportFileManager.class);

  static final String EXPORT_JOB_DIR_PREFIX = "export_";
  static final String EXPORT_ARCHIVE_JOB_INFIX = "_job";
  static final String EXPORT_ARCHIVE_SUFFIX = ".tar.gz";
  static final String EXPORT_ARCHIVE_TMP_SUFFIX = EXPORT_ARCHIVE_SUFFIX + AtomicFileOutputStream.TMP_EXTENSION;
  static final String EXPORT_LOCK_NAME = "in_use.lock";
  private static final int EXPORT_JOB_START_TIME_LENGTH = 19;

  private final String exportDirectory;
  private FileLock exportDirectoryLock;

  ExportFileManager(String exportDirectory) {
    this.exportDirectory = Objects.requireNonNull(exportDirectory, "exportDirectory == null");
  }

  void start() throws IOException {
    Files.createDirectories(Paths.get(exportDirectory));
    lock();
    removeIncompleteWorkOnStartup();
  }

  private void lock() throws IOException {
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

  File resolveArchiveFile(ExportScope scope, String jobStartTime, ExportJob.Id jobId) {
    return new File(exportDirectory, String.format("container-ids_%s_%s%s%s%s",
        scope.getValue(), jobStartTime, EXPORT_ARCHIVE_JOB_INFIX, jobId.getValue(), EXPORT_ARCHIVE_SUFFIX));
  }

  File resolveArchiveTempFile(ExportScope scope, String jobStartTime, ExportJob.Id jobId) {
    return AtomicFileOutputStream.getTemporaryFile(resolveArchiveFile(scope, jobStartTime, jobId));
  }

  void createJobDirectory(ExportJob.Id jobId) throws IOException {
    Files.createDirectories(jobDirectory(jobId));
  }

  BufferedWriter newPartWriter(ExportJob.Id jobId, String partFileName) throws IOException {
    return Files.newBufferedWriter(jobDirectory(jobId).resolve(partFileName), StandardCharsets.UTF_8);
  }

  void writeArchive(ExportJob.Id jobId, String archivePath) throws IOException {
    Path jobDir = jobDirectory(jobId);
    File[] parts = jobDir.toFile().listFiles((dir, name) -> name.endsWith(".txt"));
    if (parts == null || parts.length == 0) {
      throw new IOException("No part files found for export job " + jobId);
    }
    Arrays.sort(parts, Comparator.comparing(File::getName));
    File archiveFile = new File(archivePath);
    try (AtomicFileOutputStream atomicOut = new AtomicFileOutputStream(archiveFile);
         GZIPOutputStream gzipOut = new GZIPOutputStream(atomicOut);
         ArchiveOutputStream<TarArchiveEntry> tarOut = Archiver.tar(gzipOut)) {
      for (File part : parts) {
        Archiver.includeFile(part, part.getName(), tarOut);
      }
    }
  }

  void deleteJobDirectory(ExportJob.Id jobId) throws IOException {
    Path jobDir = jobDirectory(jobId);
    if (Files.exists(jobDir)) {
      FileUtils.deleteDirectory(jobDir.toFile());
    }
  }

  /**
   * Returns completed archive paths, oldest first.
   */
  List<String> listCompletedArchivePaths() {
    File exportDir = new File(exportDirectory);
    File[] matches = exportDir.listFiles((dir, fileName) -> fileName.endsWith(EXPORT_ARCHIVE_SUFFIX)
        && !fileName.endsWith(EXPORT_ARCHIVE_TMP_SUFFIX));
    if (matches == null || matches.length == 0) {
      return Collections.emptyList();
    }
    Arrays.sort(matches, Comparator.comparing(
        file -> jobStartTimeFromArchiveFileName(file.getName())));
    List<String> archivePaths = new ArrayList<>(matches.length);
    for (File archive : matches) {
      archivePaths.add(archive.getAbsolutePath());
    }
    return archivePaths;
  }

  static String jobStartTimeFromArchiveFileName(String fileName) {
    int jobIndex = fileName.lastIndexOf(EXPORT_ARCHIVE_JOB_INFIX);
    if (jobIndex < EXPORT_JOB_START_TIME_LENGTH + 1
            || !fileName.endsWith(EXPORT_ARCHIVE_SUFFIX)
            || fileName.endsWith(EXPORT_ARCHIVE_TMP_SUFFIX)) {
      return null;
    }
    return fileName.substring(jobIndex - EXPORT_JOB_START_TIME_LENGTH, jobIndex);
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

  void cleanupFailedJob(ExportJob.Id jobId, String archivePath) {
    FileUtils.deleteQuietly(jobDirectory(jobId).toFile());
    if (archivePath != null) {
      File archive = new File(archivePath);
      FileUtils.deleteQuietly(archive);
      FileUtils.deleteQuietly(AtomicFileOutputStream.getTemporaryFile(archive));
    }
  }

  private Path jobDirectory(ExportJob.Id jobId) {
    return Paths.get(exportDirectory, exportJobDirName(jobId));
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
