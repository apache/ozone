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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.utils.Archiver.includeFile;
import static org.apache.hadoop.hdds.utils.Archiver.linkAndIncludeFile;
import static org.apache.hadoop.hdds.utils.Archiver.tar;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.includeRatisSnapshotCompleteFlag;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;
import static org.apache.hadoop.ozone.om.lock.FlatResource.SNAPSHOT_DB_LOCK;
import static org.apache.hadoop.ozone.om.snapshot.OMDBCheckpointUtils.includeSnapshotData;
import static org.apache.hadoop.ozone.om.snapshot.OMDBCheckpointUtils.logEstimatedTarballSize;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.DATA_PREFIX;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.DATA_SUFFIX;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specialized OMDBCheckpointServlet implementation that transfers Ozone Manager
 * database checkpoints using inode-based deduplication.
 * <p>
 * This servlet constructs checkpoint archives by examining file inodes,
 * ensuring that files with the same inode (i.e., hardlinks or duplicates)
 * are only transferred once. It maintains mappings from inode IDs to file
 * paths, manages hardlink information, and enforces snapshot and SST file
 * size constraints as needed.
 * <p>
 * This approach optimizes checkpoint streaming by reducing redundant data
 * transfer, especially in environments where RocksDB and snapshotting result
 * in multiple hardlinks to the same physical data.
 */
public class OMDBCheckpointServletInodeBasedXfer extends DBCheckpointServlet {

  protected static final Logger LOG =
      LoggerFactory.getLogger(OMDBCheckpointServletInodeBasedXfer.class);
  private static final long serialVersionUID = 1L;
  private transient BootstrapStateHandler.Lock lock;

  @Override
  public void init() throws ServletException {
    OzoneManager om = (OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);

    if (om == null) {
      LOG.error("Unable to initialize OMDBCheckpointServlet. OM is null");
      return;
    }

    OzoneConfiguration conf = getConf();
    // Only Ozone Admins and Recon are allowed
    Collection<String> allowedUsers =
        new LinkedHashSet<>(om.getOmAdminUsernames());
    Collection<String> allowedGroups = om.getOmAdminGroups();
    ReconConfig reconConfig = conf.getObject(ReconConfig.class);
    String reconPrincipal = reconConfig.getKerberosPrincipal();
    if (!reconPrincipal.isEmpty()) {
      UserGroupInformation ugi =
          UserGroupInformation.createRemoteUser(reconPrincipal);
      allowedUsers.add(ugi.getShortUserName());
    }

    initialize(om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        allowedUsers,
        allowedGroups,
        om.isSpnegoEnabled());
    lock = new OMDBCheckpointServlet.Lock(om);
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  @Override
  public void processMetadataSnapshotRequest(HttpServletRequest request, HttpServletResponse response,
      boolean isFormData, boolean flush) {
    String[] sstParam = isFormData ?
        parseFormDataParameters(request) : request.getParameterValues(
        OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST);
    Set<String> receivedSstFiles = extractFilesToExclude(sstParam);
    Path tmpdir = null;
    try (BootstrapStateHandler.Lock lock = getBootstrapStateLock().lock()) {
      tmpdir = Files.createTempDirectory(getBootstrapTempData().toPath(),
          "bootstrap-data-");
      if (tmpdir == null) {
        throw new IOException("tmp dir is null");
      }
      String tarName = "om.data-" + System.currentTimeMillis() + ".tar";
      response.setContentType("application/x-tar");
      response.setHeader("Content-Disposition", "attachment; filename=\"" + tarName + "\"");
      Instant start = Instant.now();
      writeDbDataToStream(request, response.getOutputStream(), receivedSstFiles, tmpdir);
      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("Time taken to write the checkpoint to response output " +
          "stream: {} milliseconds", duration);
      logSstFileList(receivedSstFiles,
          "Excluded {} SST files from the latest checkpoint{}: {}", 5);
    } catch (Exception e) {
      LOG.error(
          "Unable to process metadata snapshot request. ", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } finally {
      try {
        if (tmpdir != null) {
          FileUtils.deleteDirectory(tmpdir.toFile());
        }
      } catch (IOException e) {
        LOG.error("unable to delete: " + tmpdir, e.toString());
      }
    }
  }

  Path getSstBackupDir() {
    RocksDBCheckpointDiffer differ = getDbStore().getRocksDBCheckpointDiffer();
    return new File(differ.getSSTBackupDir()).toPath();
  }

  Path getCompactionLogDir() {
    RocksDBCheckpointDiffer differ = getDbStore().getRocksDBCheckpointDiffer();
    return new File(differ.getCompactionLogDir()).toPath();
  }

  /**
   * Streams the Ozone Manager database checkpoint and (optionally) snapshot-related data
   * as a tar archive to the provided output stream. This method handles deduplication
   * based on file inodes to avoid transferring duplicate files (such as hardlinks),
   * supports excluding specific SST files, enforces maximum total SST file size limits,
   * and manages temporary directories for processing.
   *
   * The method processes snapshot directories and backup/compaction logs (if requested),
   * then finally the active OM database. It also writes a hardlink mapping file
   * and includes a completion flag for Ratis snapshot streaming.
   *
   * @param request           The HTTP servlet request containing parameters for the snapshot.
   * @param destination       The output stream to which the tar archive is written.
   * @param sstFilesToExclude Set of SST file identifiers to exclude from the archive.
   * @param tmpdir            Temporary directory for staging files during archiving.
   * @throws IOException if an I/O error occurs during processing or streaming.
   */

  public void writeDbDataToStream(HttpServletRequest request, OutputStream destination,
      Set<String> sstFilesToExclude, Path tmpdir) throws IOException {
    DBCheckpoint checkpoint = null;
    OzoneManager om = (OzoneManager) getServletContext().getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);
    OMMetadataManager omMetadataManager = om.getMetadataManager();
    boolean includeSnapshotData = includeSnapshotData(request);
    AtomicLong maxTotalSstSize = new AtomicLong(getConf().getLong(OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY,
        OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT));

    Set<Path> snapshotPaths = Collections.emptySet();

    if (!includeSnapshotData) {
      maxTotalSstSize.set(Long.MAX_VALUE);
    } else {
      snapshotPaths = getSnapshotDirs(omMetadataManager);
    }

    if (sstFilesToExclude.isEmpty()) {
      logEstimatedTarballSize(getDbStore().getDbLocation().toPath(), snapshotPaths);
    }

    boolean shouldContinue = true;

    Map<String, String> hardLinkFileMap = new HashMap<>();
    try (ArchiveOutputStream<TarArchiveEntry> archiveOutputStream = tar(destination)) {
      if (includeSnapshotData) {
        // Process each snapshot db path and write it to archive
        for (Path snapshotDbPath : snapshotPaths) {
          if (!shouldContinue) {
            break;
          }
          shouldContinue = writeDBToArchive(sstFilesToExclude, snapshotDbPath,
              maxTotalSstSize, archiveOutputStream, tmpdir, hardLinkFileMap, true);
        }


        if (shouldContinue) {
          shouldContinue = writeDBToArchive(sstFilesToExclude, getSstBackupDir(),
              maxTotalSstSize, archiveOutputStream,  tmpdir, hardLinkFileMap, true);
        }

        if (shouldContinue) {
          shouldContinue = writeDBToArchive(sstFilesToExclude, getCompactionLogDir(),
              maxTotalSstSize, archiveOutputStream,  tmpdir, hardLinkFileMap, true);
        }
      }

      if (shouldContinue) {
        // we finished transferring files from snapshot DB's by now and
        // this is the last step where we transfer the active om.db contents
        checkpoint = createAndPrepareCheckpoint(tmpdir, true);
        // unlimited files as we want the Active DB contents to be transferred in a single batch
        maxTotalSstSize.set(Long.MAX_VALUE);
        Path checkpointDir = checkpoint.getCheckpointLocation();
        writeDBToArchive(sstFilesToExclude, checkpointDir,
            maxTotalSstSize, archiveOutputStream, tmpdir, hardLinkFileMap, false);
        if (includeSnapshotData) {
          Path tmpCompactionLogDir = tmpdir.resolve(getCompactionLogDir().getFileName());
          Path tmpSstBackupDir = tmpdir.resolve(getSstBackupDir().getFileName());
          writeDBToArchive(sstFilesToExclude, tmpCompactionLogDir, maxTotalSstSize, archiveOutputStream, tmpdir,
              hardLinkFileMap, getCompactionLogDir(), false);
          writeDBToArchive(sstFilesToExclude, tmpSstBackupDir, maxTotalSstSize, archiveOutputStream, tmpdir,
              hardLinkFileMap, getSstBackupDir(), false);
          // This is done to ensure all data to be copied correctly is flushed in the snapshot DB
          transferSnapshotData(sstFilesToExclude, tmpdir, snapshotPaths, maxTotalSstSize,
              archiveOutputStream, hardLinkFileMap);
        }
        writeHardlinkFile(getConf(), hardLinkFileMap, archiveOutputStream);
        includeRatisSnapshotCompleteFlag(archiveOutputStream);
      }

    } catch (IOException ioe) {
      LOG.error("got exception writing to archive " + ioe);
      throw ioe;
    } finally {
      cleanupCheckpoint(checkpoint);
    }
  }

  /**
   * Transfers the snapshot data from the specified snapshot directories into the archive output stream,
   * handling deduplication and managing resource locking.
   *
   * @param sstFilesToExclude   Set of SST file identifiers to exclude from the archive.
   * @param tmpdir              Temporary directory for intermediate processing.
   * @param snapshotPaths       Set of paths to snapshot directories to be processed.
   * @param maxTotalSstSize     AtomicLong to track the cumulative size of SST files included.
   * @param archiveOutputStream Archive output stream to write the snapshot data.
   * @param hardLinkFileMap     Map of hardlink file paths to their unique identifiers for deduplication.
   * @throws IOException if an I/O error occurs during processing.
   */
  private void transferSnapshotData(Set<String> sstFilesToExclude, Path tmpdir, Set<Path> snapshotPaths,
      AtomicLong maxTotalSstSize, ArchiveOutputStream<TarArchiveEntry> archiveOutputStream,
      Map<String, String> hardLinkFileMap) throws IOException {
    OzoneManager om = (OzoneManager) getServletContext().getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);
    OMMetadataManager omMetadataManager = om.getMetadataManager();
    for (Path snapshotDir : snapshotPaths) {
      String snapshotId = OmSnapshotManager.extractSnapshotIDFromCheckpointDirName(snapshotDir.toString());
      omMetadataManager.getLock().acquireReadLock(SNAPSHOT_DB_LOCK, snapshotId);
      try {
        // invalidate closes the snapshot DB
        om.getOmSnapshotManager().invalidateCacheEntry(UUID.fromString(snapshotId));
        writeDBToArchive(sstFilesToExclude, snapshotDir, maxTotalSstSize, archiveOutputStream, tmpdir,
            hardLinkFileMap, false);
        Path snapshotLocalPropertyYaml = Paths.get(
            OmSnapshotLocalDataManager.getSnapshotLocalPropertyYamlPath(snapshotDir));
        if (Files.exists(snapshotLocalPropertyYaml)) {
          File yamlFile = snapshotLocalPropertyYaml.toFile();
          hardLinkFileMap.put(yamlFile.getAbsolutePath(), yamlFile.getName());
          linkAndIncludeFile(yamlFile, yamlFile.getName(), archiveOutputStream, tmpdir);
        }
      } finally {
        omMetadataManager.getLock().releaseReadLock(SNAPSHOT_DB_LOCK, snapshotId);
      }
    }
  }

  @VisibleForTesting
  boolean writeDBToArchive(Set<String> sstFilesToExclude, Path dir,
      AtomicLong maxTotalSstSize, ArchiveOutputStream<TarArchiveEntry> archiveOutputStream,
      Path tmpdir, Map<String, String> hardLinkFileMap, boolean onlySstFile) throws IOException {
    return writeDBToArchive(sstFilesToExclude, dir, maxTotalSstSize,
        archiveOutputStream, tmpdir, hardLinkFileMap, null, onlySstFile);
  }

  private static void cleanupCheckpoint(DBCheckpoint checkpoint) {
    if (checkpoint != null) {
      try {
        checkpoint.cleanupCheckpoint();
      } catch (IOException e) {
        LOG.error("Error trying to clean checkpoint at {} .",
            checkpoint.getCheckpointLocation().toString());
      }
    }
  }

  /**
   * Writes a hardlink mapping file to the archive, which maps file IDs to their
   * relative paths. This method generates the mapping file based on the provided
   * hardlink metadata and adds it to the archive output stream.
   *
   * @param conf                Ozone configuration for the OM instance.
   * @param hardlinkFileMap     A map where the key is the absolute file path
   *                            and the value is its corresponding file ID.
   * @param archiveOutputStream The archive output stream to which the hardlink
   *                            file should be written.
   * @throws IOException If an I/O error occurs while creating or writing the
   *                     hardlink file.
   */
  private static void writeHardlinkFile(OzoneConfiguration conf, Map<String, String> hardlinkFileMap,
      ArchiveOutputStream<TarArchiveEntry> archiveOutputStream) throws IOException {
    Path data = Files.createTempFile(DATA_PREFIX, DATA_SUFFIX);
    Path metaDirPath = OMStorage.getOmDbDir(conf).toPath();
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<String, String> entry : hardlinkFileMap.entrySet()) {
      Path p = Paths.get(entry.getKey());
      String fileId = entry.getValue();
      Path relativePath = metaDirPath.relativize(p);
      // if the file is in "om.db" directory, strip off the 'o
      // m.db' name from the path
      // and only keep the file name as this would be created in the current dir of the untarred dir
      // on the follower.
      if (relativePath.startsWith(OM_DB_NAME)) {
        relativePath = relativePath.getFileName();
      }
      sb.append(relativePath).append('\t').append(fileId).append('\n');
    }
    Files.write(data, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);
    includeFile(data.toFile(), OmSnapshotManager.OM_HARDLINK_FILE, archiveOutputStream);
  }

  /**
   * Gets the configuration from the OzoneManager context.
   *
   * @return OzoneConfiguration instance
   */
  private OzoneConfiguration getConf() {
    return ((OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .getConfiguration();
  }

  /**
   * Collects paths to all snapshot databases.
   *
   * @param omMetadataManager OMMetadataManager instance
   * @return Set of paths to snapshot databases
   * @throws IOException if an I/O error occurs
   */
  Set<Path> getSnapshotDirs(OMMetadataManager omMetadataManager) throws IOException {
    Set<Path> snapshotPaths = new HashSet<>();
    SnapshotChainManager snapshotChainManager = new SnapshotChainManager(omMetadataManager);
    for (SnapshotChainInfo snapInfo : snapshotChainManager.getGlobalSnapshotChain().values()) {
      String snapshotDir =
          OmSnapshotManager.getSnapshotPath(getConf(), SnapshotInfo.getCheckpointDirName(snapInfo.getSnapshotId()));
      Path path = Paths.get(snapshotDir);
      snapshotPaths.add(path);
    }
    return snapshotPaths;
  }

  /**
   * Writes database files to the archive, handling deduplication based on inode IDs.
   * Here the dbDir could either be a snapshot db directory, the active om.db,
   * compaction log dir, sst backup dir.
   *
   * @param sstFilesToExclude Set of SST file IDs to exclude from the archive
   * @param dbDir Directory containing database files to archive
   * @param maxTotalSstSize Maximum total size of SST files to include
   * @param archiveOutputStream Archive output stream
   * @param tmpDir Temporary directory for processing
   * @param hardLinkFileMap Map of hardlink file paths to their unique identifiers for deduplication
   * @param destDir Destination directory for the archived files. If null,
   * the archived files are not moved to this directory.
   * @param onlySstFile If true, only SST files are processed. If false, all files are processed.
   * <p>
   * This parameter is typically set to {@code true} for initial iterations to
   * prioritize SST file transfer, and then set to {@code false} only for the
   * final iteration to ensure all remaining file types are transferred.
   * @return true if processing should continue, false if size limit reached
   * @throws IOException if an I/O error occurs
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private boolean writeDBToArchive(Set<String> sstFilesToExclude, Path dbDir, AtomicLong maxTotalSstSize,
      ArchiveOutputStream<TarArchiveEntry> archiveOutputStream, Path tmpDir,
      Map<String, String> hardLinkFileMap, Path destDir, boolean onlySstFile) throws IOException {
    if (!Files.exists(dbDir)) {
      LOG.warn("DB directory {} does not exist. Skipping.", dbDir);
      return true;
    }
    long bytesWritten = 0L;
    int filesWritten = 0;
    long lastLoggedTime = Time.monotonicNow();
    try (Stream<Path> files = Files.list(dbDir)) {
      Iterable<Path> iterable = files::iterator;
      for (Path dbFile : iterable) {
        if (!Files.isDirectory(dbFile)) {
          if (onlySstFile && !dbFile.toString().endsWith(ROCKSDB_SST_SUFFIX)) {
            continue;
          }
          String fileId = OmSnapshotUtils.getFileInodeAndLastModifiedTimeString(dbFile);
          String path = dbFile.toFile().getAbsolutePath();
          if (destDir != null) {
            path = destDir.resolve(dbFile.getFileName()).toString();
          }
          // if the file is in the om checkpoint dir, then we need to change the path to point to the OM DB.
          if (path.contains(OM_CHECKPOINT_DIR)) {
            path = getDbStore().getDbLocation().toPath().resolve(dbFile.getFileName()).toAbsolutePath().toString();
          }
          hardLinkFileMap.put(path, fileId);
          if (!sstFilesToExclude.contains(fileId)) {
            long fileSize = Files.size(dbFile);
            if (maxTotalSstSize.get() - fileSize <= 0) {
              return false;
            }
            bytesWritten += linkAndIncludeFile(dbFile.toFile(), fileId, archiveOutputStream, tmpDir);
            filesWritten++;
            maxTotalSstSize.addAndGet(-fileSize);
            sstFilesToExclude.add(fileId);
            if (Time.monotonicNow() - lastLoggedTime >= 30000) {
              LOG.info("Transferred {} KB, #files {} to checkpoint tarball stream...",
                  bytesWritten / (1024), filesWritten);
              lastLoggedTime = Time.monotonicNow();
            }
          }
        }
      }
    }
    return true;
  }

  /**
   * Creates a database checkpoint and copies compaction log and SST backup files
   * into the given temporary directory.
   * The copy to the temporary directory for compaction log and SST backup files
   * is done to maintain a consistent view of the files in these directories.
   *
   * @param tmpdir Temporary directory for storing checkpoint-related files.
   * @param flush  If true, flushes in-memory data to disk before checkpointing.
   * @return The created database checkpoint.
   * @throws IOException If an error occurs during checkpoint creation or file copying.
   */
  private DBCheckpoint createAndPrepareCheckpoint(Path tmpdir, boolean flush) throws IOException {
    // make tmp directories to contain the copies
    Path tmpCompactionLogDir = tmpdir.resolve(getCompactionLogDir().getFileName());
    Path tmpSstBackupDir = tmpdir.resolve(getSstBackupDir().getFileName());

    // Create checkpoint and then copy the files so that it has all the compaction entries and files.
    DBCheckpoint dbCheckpoint = getDbStore().getCheckpoint(flush);
    FileUtils.copyDirectory(getCompactionLogDir().toFile(), tmpCompactionLogDir.toFile());
    OmSnapshotUtils.linkFiles(getSstBackupDir().toFile(), tmpSstBackupDir.toFile());

    return dbCheckpoint;
  }
}
