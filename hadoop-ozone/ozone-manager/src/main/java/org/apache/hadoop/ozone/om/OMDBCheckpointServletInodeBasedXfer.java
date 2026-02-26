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
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_TO_EXCLUDE_SST;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.HierarchicalResourceLockManager.HierarchicalResourceLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
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
    OMDBArchiver omdbArchiver = new OMDBArchiver();
    boolean filesCollected = false;
    try (UncheckedAutoCloseable lock = getBootstrapStateLock().acquireWriteLock()) {
      tmpdir = Files.createTempDirectory(getBootstrapTempData().toPath(),
          "bootstrap-data-");
      if (tmpdir == null) {
        throw new IOException("tmp dir is null");
      }
      omdbArchiver.setTmpDir(tmpdir);
      String tarName = "om.data-" + System.currentTimeMillis() + ".tar";
      response.setContentType("application/x-tar");
      response.setHeader("Content-Disposition", "attachment; filename=\"" + tarName + "\"");
      Instant start = Instant.now();
      collectDbDataToTransfer(request, receivedSstFiles, omdbArchiver);
      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("Time taken to collect the DB data : {} milliseconds", duration);
      logSstFileList(receivedSstFiles,
          "Excluded {} SST files from the latest checkpoint{}: {}", 5);
      filesCollected = true;
    } catch (Exception e) {
      LOG.error(
          "Unable to process metadata snapshot request. ", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
    try {
      if (filesCollected) {
        Instant start = Instant.now();
        OutputStream outputStream = response.getOutputStream();
        omdbArchiver.writeToArchive(getConf(), outputStream);
        Instant end = Instant.now();
        long duration = Duration.between(start, end).toMillis();
        LOG.info("Time taken to write the checkpoint to response output " +
            "stream: {} milliseconds", duration);
      }
    } catch (IOException e) {
      LOG.error("unable to write to archive stream", e);
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
   * @param sstFilesToExclude Set of SST file identifiers to exclude from the archive.
   * @throws IOException if an I/O error occurs during processing or streaming.
   */
  public void collectDbDataToTransfer(HttpServletRequest request,
      Set<String> sstFilesToExclude,  OMDBArchiver omdbArchiver) throws IOException {
    DBCheckpoint checkpoint = null;
    OzoneManager om = (OzoneManager) getServletContext().getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);
    OMMetadataManager omMetadataManager = om.getMetadataManager();
    OmSnapshotLocalDataManager snapshotLocalDataManager = om.getOmSnapshotManager().getSnapshotLocalDataManager();
    boolean includeSnapshotData = includeSnapshotData(request);
    AtomicLong maxTotalSstSize = new AtomicLong(getConf().getLong(OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY,
        OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT));

    Collection<Path> snapshotPaths = Collections.emptySet();

    if (!includeSnapshotData) {
      maxTotalSstSize.set(Long.MAX_VALUE);
    } else {
      snapshotPaths = getSnapshotDirsFromDB(omMetadataManager, omMetadataManager, snapshotLocalDataManager).values();
    }

    if (sstFilesToExclude.isEmpty()) {
      logEstimatedTarballSize(getDbStore().getDbLocation().toPath(), snapshotPaths);
    }

    boolean shouldContinue = true;
    try {
      if (includeSnapshotData) {
        // Process each snapshot db path and write it to archive
        for (Path snapshotDbPath : snapshotPaths) {
          if (!shouldContinue) {
            break;
          }
          shouldContinue = collectFilesFromDir(sstFilesToExclude, snapshotDbPath,
              maxTotalSstSize,  true, omdbArchiver);
        }


        if (shouldContinue) {
          shouldContinue = collectFilesFromDir(sstFilesToExclude, getSstBackupDir(),
              maxTotalSstSize,   true, omdbArchiver);
        }

        if (shouldContinue) {
          shouldContinue = collectFilesFromDir(sstFilesToExclude, getCompactionLogDir(),
              maxTotalSstSize,   true, omdbArchiver);
        }
      }

      if (shouldContinue) {
        // we finished transferring files from snapshot DB's by now and
        // this is the last step where we transfer the active om.db contents
        SnapshotCache snapshotCache = om.getOmSnapshotManager().getSnapshotCache();
        OmSnapshotLocalDataManager localDataManager = om.getOmSnapshotManager().getSnapshotLocalDataManager();
        /*
         * Acquire snapshot cache lock when includeSnapshotData is true to prevent race conditions
         * between checkpoint operations and snapshot purge operations. Without this lock, a purge
         * operation (e.g., from a Ratis transaction on follower OM) could delete snapshot directories
         * while checkpoint is reading snapshot data, leading to FileNotFoundException or corrupted
         * checkpoint data. The lock ensures checkpoint completes reading snapshot data before purge
         * can delete the snapshot directory.
         *
         * When includeSnapshotData is false, lock is set to null and no locking is performed.
         * In this case, the try-with-resources block does not call close() on any resource,
         * which is intentional because snapshot consistency is not required.
         */
        try (UncheckedAutoCloseableSupplier<OMLockDetails> snapshotDBLock =
                 includeSnapshotData ? snapshotCache.lock() : null;
             HierarchicalResourceLock snapshotLocalDataLock = includeSnapshotData ? localDataManager.lock() : null) {
          // get the list of sst files of the checkpoint.
          checkpoint = createAndPrepareCheckpoint(true);
          // unlimited files as we want the Active DB contents to be transferred in a single batch
          maxTotalSstSize.set(Long.MAX_VALUE);
          Path checkpointDir = checkpoint.getCheckpointLocation();
          collectFilesFromDir(sstFilesToExclude, checkpointDir, maxTotalSstSize, false, omdbArchiver);
          if (includeSnapshotData) {
            List<Path> sstBackupFiles = extractSSTFilesFromCompactionLog(checkpoint);
            // get the list of snapshots from the checkpoint
            Map<UUID, Path> snapshotInCheckpoint;
            try (OmMetadataManagerImpl checkpointMetadataManager = OmMetadataManagerImpl
                    .createCheckpointMetadataManager(om.getConfiguration(), checkpoint)) {
              snapshotInCheckpoint = getSnapshotDirsFromDB(omMetadataManager, checkpointMetadataManager,
                  snapshotLocalDataManager);
            }
            collectFilesFromDir(sstFilesToExclude, getCompactionLogDir(), maxTotalSstSize, false, omdbArchiver);
            try (Stream<Path> backupFiles = sstBackupFiles.stream()) {
              collectFilesFromDir(sstFilesToExclude, backupFiles, maxTotalSstSize, false, omdbArchiver, true);
            }
            Collection<Path> snapshotLocalPropertyFiles = getSnapshotLocalDataPaths(localDataManager,
                snapshotInCheckpoint.keySet());
            // This is done to ensure all data to be copied correctly is flushed in the snapshot DB
            collectSnapshotData(sstFilesToExclude, snapshotInCheckpoint.values(), snapshotLocalPropertyFiles,
                maxTotalSstSize, omdbArchiver);
          }
        }
        omdbArchiver.setCompleted(true);
      }

    } catch (IOException ioe) {
      LOG.error("got exception while collecting files to archive ", ioe);
      throw ioe;
    } finally {
      cleanupCheckpoint(checkpoint);
    }
  }

  /**
   * Retrieves the paths to the local property YAML files for the specified snapshot IDs.
   * This method resolves the chain of previous snapshot references for each snapshot ID
   * and gathers their corresponding local property YAML file paths.
   *
   * @param localDataManager The OmSnapshotLocalDataManager instance responsible for managing
   *                         snapshot data and metadata.
   * @param snapshotIds      A set of snapshot IDs for which the local property YAML file
   *                         paths should be resolved.
   * @return A collection of paths to the local property YAML files for the specified
   *         snapshot IDs.
   */
  private Collection<Path> getSnapshotLocalDataPaths(OmSnapshotLocalDataManager localDataManager,
      Set<UUID> snapshotIds) {
    Set<UUID> snapshotLocalDataIds = new HashSet<>();
    Map<UUID, OmSnapshotLocalDataManager.SnapshotVersionsMeta> versionNodeMap =
        localDataManager.getVersionNodeMapUnmodifiable();
    for (UUID snapshot : snapshotIds) {
      UUID id = snapshot;
      // Get the previous snapshot id for the current snapshot id until we reach null or the first snapshot id which
      // is already in the snapshotLocalDataIds set.
      while (id != null && !snapshotLocalDataIds.contains(id)) {
        snapshotLocalDataIds.add(id);
        id = versionNodeMap.get(id).getPreviousSnapshotId();
      }
    }
    return snapshotLocalDataIds.stream().map(localDataManager::getSnapshotLocalPropertyYamlPath)
        .map(Paths::get).collect(Collectors.toList());
  }

  /**
   * Collects the snapshots to be transferred from the specified snapshot directories
   * into the archive output stream.
   *
   * @param sstFilesToExclude   Set of SST file identifiers to exclude from the archive.
   * @param snapshotPaths       Set of paths to snapshot directories to be processed.
   * @param maxTotalSstSize     AtomicLong to track the cumulative size of SST files included.
   * @param omdbArchiver     helper to archive the OM DB.
   * @throws IOException if an I/O error occurs during processing.
   */
  @VisibleForTesting
  void collectSnapshotData(Set<String> sstFilesToExclude, Collection<Path> snapshotPaths,
      Collection<Path> snapshotLocalPropertyFiles, AtomicLong maxTotalSstSize,
      OMDBArchiver omdbArchiver)
      throws IOException {
    for (Path snapshotDir : snapshotPaths) {
      collectFilesFromDir(sstFilesToExclude, snapshotDir, maxTotalSstSize,
          false, omdbArchiver);
    }
    for (Path snapshotLocalPropertyYaml : snapshotLocalPropertyFiles) {
      File yamlFile = snapshotLocalPropertyYaml.toFile();
      omdbArchiver.recordHardLinkMapping(yamlFile.getAbsolutePath(), yamlFile.getName());
      omdbArchiver.recordFileEntry(yamlFile, yamlFile.getName());
    }
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
  static void writeHardlinkFile(OzoneConfiguration conf, Map<String, String> hardlinkFileMap,
      ArchiveOutputStream<TarArchiveEntry> archiveOutputStream) throws IOException {
    Path data = Files.createTempFile(DATA_PREFIX, DATA_SUFFIX);
    try {
      Path metaDirPath = OMStorage.getOmDbDir(conf).toPath()
          .toAbsolutePath().normalize();
      StringBuilder sb = new StringBuilder();

      for (Map.Entry<String, String> entry : hardlinkFileMap.entrySet()) {
        Path p = new File(entry.getKey()).toPath();
        if (!p.isAbsolute()) {
          p = metaDirPath.resolve(p);
        }
        p = p.toAbsolutePath().normalize();
        String fileId = entry.getValue();
        Path relativePath = metaDirPath.relativize(p);
        sb.append(relativePath).append('\t').append(fileId).append('\n');
      }
      Files.write(data, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);
      includeFile(data.toFile(), OmSnapshotManager.OM_HARDLINK_FILE, archiveOutputStream);
    } finally {
      Files.deleteIfExists(data);
    }
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
   * Collects paths to all snapshot databases from the OM DB.
   *
   * @param activeOMMetadataManager OMMetadataManager instance
   * @return Map of paths to snapshot databases with snapshot IDs as keys.
   * @throws IOException if an I/O error occurs
   */
  Map<UUID, Path> getSnapshotDirsFromDB(OMMetadataManager activeOMMetadataManager, OMMetadataManager omMetadataManager,
      OmSnapshotLocalDataManager localDataManager) throws IOException {
    Map<UUID, Path> snapshotPaths = new HashMap<>();
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>> iter =
        omMetadataManager.getSnapshotInfoTable().iterator()) {
      while (iter.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> kv = iter.next();
        SnapshotInfo snapshotInfo = kv.getValue();
        try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataMetaProvider snapLocalMeta =
                 localDataManager.getOmSnapshotLocalDataMeta(snapshotInfo.getSnapshotId())) {
          Path snapshotDir = getSnapshotPath(activeOMMetadataManager, snapshotInfo.getSnapshotId(),
              snapLocalMeta.getMeta().getVersion());
          snapshotPaths.put(snapshotInfo.getSnapshotId(), snapshotDir);
        }
      }
    }
    return snapshotPaths;
  }

  @VisibleForTesting
  boolean collectFilesFromDir(Set<String> sstFilesToExclude, Path dbDir, AtomicLong maxTotalSstSize,
      boolean onlySstFile, OMDBArchiver omdbArchiver) throws IOException {
    if (!Files.exists(dbDir)) {
      LOG.warn("DB directory {} does not exist. Skipping.", dbDir);
      return true;
    }
    try (Stream<Path> files = Files.list(dbDir)) {
      return collectFilesFromDir(sstFilesToExclude, files, maxTotalSstSize, onlySstFile, omdbArchiver);
    }
  }

  private boolean collectFilesFromDir(Set<String> sstFilesToExclude, Stream<Path> files,
      AtomicLong maxTotalSstSize, boolean onlySstFile, OMDBArchiver omdbArchiver) throws IOException {
    return collectFilesFromDir(sstFilesToExclude, files, maxTotalSstSize,
        onlySstFile, omdbArchiver, false);
  }

  /**
   * Collects database files to the archive, handling deduplication based on inode IDs.
   * Here the dbDir could either be a snapshot db directory, the active om.db,
   * compaction log dir, sst backup dir.
   *
   * @param sstFilesToExclude Set of SST file IDs to exclude from the archive
   * @param files Stream of files to archive
   * @param maxTotalSstSize Maximum total size of SST files to include
   * the archived files are not moved to this directory.
   * @param onlySstFile If true, only SST files are processed. If false, all files are processed.
   * <p>
   * This parameter is typically set to {@code true} for initial iterations to
   * prioritize SST file transfer, and then set to {@code false} only for the
   * final iteration to ensure all remaining file types are transferred.
   * @return true if processing should continue, false if size limit reached
   * @throws IOException if an I/O error occurs
   */
  boolean collectFilesFromDir(Set<String> sstFilesToExclude, Stream<Path> files,
      AtomicLong maxTotalSstSize, boolean onlySstFile, OMDBArchiver omdbArchiver,
      boolean ignoreNoSuchFileException) throws IOException {
    long bytesRecorded = 0L;
    int filesWritten = 0;
    Iterable<Path> iterable = files::iterator;
    for (Path dbFile : iterable) {
      if (!Files.isDirectory(dbFile)) {
        if (onlySstFile && !dbFile.toString().endsWith(ROCKSDB_SST_SUFFIX)) {
          continue;
        }
        String fileId;
        try {
          fileId = OmSnapshotUtils.getFileInodeAndLastModifiedTimeString(dbFile);
        } catch (NoSuchFileException nsfe) {
          if (ignoreNoSuchFileException) {
            LOG.warn("File {} not found.", dbFile);
            logFileNoLongerExists(dbFile);
            continue;
          }
          throw nsfe;
        }
        String path = dbFile.toFile().getAbsolutePath();
        // if the file is in the om checkpoint dir, then we need to change the path to point to the OM DB.
        if (path.contains(OM_CHECKPOINT_DIR)) {
          path = getDbStore().getDbLocation().toPath().resolve(dbFile.getFileName()).toAbsolutePath().toString();
        }
        omdbArchiver.recordHardLinkMapping(path, fileId);
        if (!sstFilesToExclude.contains(fileId)) {
          try {
            long fileSize = Files.size(dbFile);
            if (maxTotalSstSize.get() - fileSize <= 0) {
              return false;
            }
            bytesRecorded += omdbArchiver.recordFileEntry(dbFile.toFile(), fileId);
            filesWritten++;
            maxTotalSstSize.addAndGet(-fileSize);
            sstFilesToExclude.add(fileId);
          } catch (NoSuchFileException e) {
            if (ignoreNoSuchFileException) {
              logFileNoLongerExists(dbFile);
              omdbArchiver.removeHardLinkMapping(path);
            } else {
              throw e;
            }
          }
        }
      }
    }
    LOG.info("Collected {} KB, #files {} to write to checkpoint tarball stream...",
        bytesRecorded / (1024), filesWritten);
    return true;
  }

  private void logFileNoLongerExists(Path dbFile) {
    LOG.warn("Not writing DB file : {} to archive as it no longer exists", dbFile);
  }

  /**
   * Creates a database checkpoint and copies compaction log and SST backup files
   * into the given temporary directory.
   * The copy to the temporary directory for compaction log and SST backup files
   * is done to maintain a consistent view of the files in these directories.
   *
   * @param flush  If true, flushes in-memory data to disk before checkpointing.
   * @throws IOException If an error occurs during checkpoint creation or file copying.
   */
  DBCheckpoint createAndPrepareCheckpoint(boolean flush) throws IOException {
    // Create & return the checkpoint.
    return getDbStore().getCheckpoint(flush);
  }

  private List<Path> extractSSTFilesFromCompactionLog(DBCheckpoint dbCheckpoint) throws IOException {
    List<Path> sstFiles = new ArrayList<>();
    try (OmMetadataManagerImpl checkpointMetadataManager =
             OmMetadataManagerImpl.createCheckpointMetadataManager(getConf(), dbCheckpoint)) {
      try (Table.KeyValueIterator<String, CompactionLogEntry>
               iterator = checkpointMetadataManager.getCompactionLogTable().iterator()) {
        iterator.seekToFirst();

        Path sstBackupDir = getSstBackupDir();

        while (iterator.hasNext()) {
          CompactionLogEntry logEntry = iterator.next().getValue();
          logEntry.getInputFileInfoList().forEach(f ->
              sstFiles.add(sstBackupDir.resolve(f.getFileName() + ROCKSDB_SST_SUFFIX)));
        }
      }
    } catch (Exception e) {
      throw new IOException("Error reading compaction log from checkpoint", e);
    }
    return sstFiles;
  }
}
