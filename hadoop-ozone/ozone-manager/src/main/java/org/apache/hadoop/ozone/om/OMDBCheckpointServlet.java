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
import static org.apache.hadoop.hdds.utils.Archiver.tar;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.includeRatisSnapshotCompleteFlag;
import static org.apache.hadoop.hdds.utils.IOUtils.getINode;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.BOOTSTRAP_LOCK;
import static org.apache.hadoop.ozone.om.snapshot.OMDBCheckpointUtils.includeSnapshotData;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.createHardLinkList;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.truncateFileName;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OMDBCheckpointUtils;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the current checkpoint Snapshot of the OM DB. (tar.gz)
 *
 * When Ozone ACL is enabled (`ozone.acl.enabled`=`true`), only users/principals
 * configured in `ozone.administrator` (along with the user that starts OM,
 * which automatically becomes an Ozone administrator but not necessarily in
 * the config) are allowed to access this endpoint.
 *
 * If Kerberos is enabled, the principal should be appended to
 * `ozone.administrator`, e.g. `scm/scm@EXAMPLE.COM`
 * If Kerberos is not enabled, simply append the login username to
 * `ozone.administrator`, e.g. `scm`
 */
public class OMDBCheckpointServlet extends DBCheckpointServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBCheckpointServlet.class);
  private static final long serialVersionUID = 1L;
  private transient BootstrapStateHandler.Lock lock;
  private long maxTotalSstSize = 0;

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

    lock = new Lock(om);
  }

  @Override
  public void writeDbDataToStream(DBCheckpoint checkpoint,
                                  HttpServletRequest request,
                                  OutputStream destination,
                                  Set<String> toExcludeList,
                                  Path tmpdir)
      throws IOException, InterruptedException {
    Objects.requireNonNull(toExcludeList);

    // copyFiles is a map of files to be added to tarball.  The keys
    // are the src path of the file, (where they are copied from on
    // the leader.) The values are the dest path of the file, (where
    // they are copied to on the follower.)  In most cases these are
    // the same.  For synchronization purposes, some files are copied
    // to a temp directory on the leader.  In those cases the source
    // and dest won't be the same.
    Map<String, Map<Path, Path>> copyFiles = new HashMap<>();

    // Map of link to path.
    Map<Path, Path> hardLinkFiles = new HashMap<>();

    try (ArchiveOutputStream<TarArchiveEntry> archiveOutputStream = tar(destination)) {
      RocksDBCheckpointDiffer differ =
          getDbStore().getRocksDBCheckpointDiffer();
      DirectoryData sstBackupDir = new DirectoryData(tmpdir,
          differ.getSSTBackupDir());
      DirectoryData compactionLogDir = new DirectoryData(tmpdir,
          differ.getCompactionLogDir());

      // Files to be excluded from tarball
      Map<String, Map<Path, Path>> sstFilesToExclude = normalizeExcludeList(toExcludeList,
          checkpoint.getCheckpointLocation(), sstBackupDir);

      boolean completed = getFilesForArchive(checkpoint, copyFiles,
          hardLinkFiles, sstFilesToExclude, includeSnapshotData(request),
          sstBackupDir, compactionLogDir);
      Map<Path, Path> flatCopyFiles = copyFiles.values().stream().flatMap(map -> map.entrySet().stream())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      writeFilesToArchive(flatCopyFiles, hardLinkFiles, archiveOutputStream,
          completed, checkpoint.getCheckpointLocation());
    } catch (Exception e) {
      LOG.error("got exception writing to archive " + e);
      throw e;
    }
  }

  /**
   * Format the list of excluded sst files from follower to match data
   * on leader.
   * @param  toExcludeList - list of excluded sst files from follower
   * @param  checkpointLocation -  location of checkpoint for this tarball
   * @param  sstBackupDir - location info about sstBackupDir
   * @return A Map of src and dest paths for the entries in the toExcludeList.
   *         Formatted in a manner analogous to the copyFiles data structure
   *         described above.  Because this structure only points to sst files,
   *         the implementation ignores the compactionLog dir, (which doesn't
   *         include sst files.)
   */
  @VisibleForTesting
  public static Map<String, Map<Path, Path>> normalizeExcludeList(
      Collection<String> toExcludeList,
      Path checkpointLocation,
      DirectoryData sstBackupDir) {
    Map<String, Map<Path, Path>> paths = new HashMap<>();
    Path metaDirPath = getMetaDirPath(checkpointLocation);
    for (String s : toExcludeList) {
      Path fileName = Paths.get(s).getFileName();
      if (fileName == null) {
        continue;
      }
      Path destPath = Paths.get(metaDirPath.toString(), s);
      Map<Path, Path> fileMap = paths.computeIfAbsent(fileName.toString(), (k) -> new HashMap<>());
      if (destPath.toString().startsWith(
          sstBackupDir.getOriginalDir().toString())) {
        // The source of the sstBackupDir is a temporary directory and needs
        // to be adjusted accordingly.
        int truncateLength =
            sstBackupDir.getOriginalDir().toString().length() + 1;
        Path srcPath = Paths.get(sstBackupDir.getTmpDir().toString(),
            truncateFileName(truncateLength, destPath));
        fileMap.put(srcPath, destPath);
      } else if (!s.startsWith(OM_SNAPSHOT_DIR)) {
        Path fixedPath = Paths.get(checkpointLocation.toString(), s);
        fileMap.put(fixedPath, fixedPath);
      } else {
        fileMap.put(destPath, destPath);
      }
    }
    return paths;
  }

  /**
   * Copies compaction logs and hard links of sst backups to tmpDir.
   * @param  tmpdir - Place to create copies/links
   * @param  flush -  Whether to flush the db or not.
   * @return Checkpoint containing snapshot entries expected.
   */
  @Override
  public DBCheckpoint getCheckpoint(Path tmpdir, boolean flush) throws IOException {
    // make tmp directories to contain the copies
    RocksDBCheckpointDiffer differ = getDbStore().getRocksDBCheckpointDiffer();
    DirectoryData sstBackupDir = new DirectoryData(tmpdir, differ.getSSTBackupDir());
    DirectoryData compactionLogDir = new DirectoryData(tmpdir, differ.getCompactionLogDir());

    // Create checkpoint and then copy the files so that it has all the compaction entries and files.
    DBCheckpoint dbCheckpoint = getDbStore().getCheckpoint(flush);
    FileUtils.copyDirectory(compactionLogDir.getOriginalDir(), compactionLogDir.getTmpDir());
    OmSnapshotUtils.linkFiles(sstBackupDir.getOriginalDir(), sstBackupDir.getTmpDir());

    return dbCheckpoint;
  }

  // Convenience class for keeping track of the tmp dirs.
  static class DirectoryData {
    private final File originalDir;
    private final File tmpDir;

    DirectoryData(Path tmpdir, String dirStr) throws IOException {
      originalDir = new File(dirStr);
      tmpDir = new File(tmpdir.toString(), getOriginalDir().getName());
      if (!tmpDir.exists() && !tmpDir.mkdirs()) {
        throw new IOException("mkdirs failed: " + tmpDir);
      }
    }

    public File getOriginalDir() {
      return originalDir;
    }

    public File getTmpDir() {
      return tmpDir;
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private boolean getFilesForArchive(DBCheckpoint checkpoint,
      Map<String, Map<Path, Path>> copyFiles,
      Map<Path, Path> hardLinkFiles,
      Map<String, Map<Path, Path>> sstFilesToExclude,
      boolean includeSnapshotData,
      DirectoryData sstBackupDir,
      DirectoryData compactionLogDir)
      throws IOException {

    maxTotalSstSize = getConf().getLong(
        OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY,
        OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT);

    // Tarball limits are not implemented for processes that don't
    // include snapshots.  Currently, this is just for recon.
    if (!includeSnapshotData) {
      maxTotalSstSize = Long.MAX_VALUE;
    }

    AtomicLong copySize = new AtomicLong(0L);

    // Log estimated total data transferred on first request.
    if (sstFilesToExclude.isEmpty()) {
      logEstimatedTarballSize(checkpoint, includeSnapshotData);
    }

    // Get the active fs files.
    Path dir = checkpoint.getCheckpointLocation();
    if (!processDir(dir, copyFiles, hardLinkFiles, sstFilesToExclude,
        new HashSet<>(), copySize, null)) {
      return false;
    }

    if (!includeSnapshotData) {
      return true;
    }

    // Get the snapshot files.
    Set<Path> snapshotPaths = getSnapshotDirs(checkpoint, true);
    Path snapshotDir = getSnapshotDir();
    if (!processDir(snapshotDir, copyFiles, hardLinkFiles, sstFilesToExclude,
        snapshotPaths, copySize, null)) {
      return false;
    }
    // Process the tmp sst compaction dir.
    if (!processDir(sstBackupDir.getTmpDir().toPath(), copyFiles, hardLinkFiles,
        sstFilesToExclude, new HashSet<>(), copySize,
        sstBackupDir.getOriginalDir().toPath())) {
      return false;
    }

    // Process the tmp compaction log dir.
    return processDir(compactionLogDir.getTmpDir().toPath(), copyFiles,
        hardLinkFiles, sstFilesToExclude,
        new HashSet<>(), copySize,
        compactionLogDir.getOriginalDir().toPath());
  }

  private void logEstimatedTarballSize(DBCheckpoint checkpoint, boolean includeSnapshotData)
      throws IOException {
    Set<Path> snapshotPaths = new HashSet<>();
    if (includeSnapshotData) {
      // since this is an estimate we can avoid waiting for dir to exist.
      snapshotPaths = getSnapshotDirs(checkpoint, false);
    }
    OMDBCheckpointUtils.logEstimatedTarballSize(checkpoint.getCheckpointLocation(), snapshotPaths);
  }

  /**
   * The snapshotInfo table may contain a snapshot that
   * doesn't yet exist on the fs, so wait a few seconds for it.
   * @param checkpoint Checkpoint containing snapshot entries expected.
   * @param waitForDir Wait for dir to exist on fs.
   * @return Set of expected snapshot dirs.
   */
  private Set<Path> getSnapshotDirs(DBCheckpoint checkpoint, boolean waitForDir)
      throws IOException {

    OzoneConfiguration conf = getConf();

    Set<Path> snapshotPaths = new HashSet<>();
    OzoneManager om = (OzoneManager) getServletContext().getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);
    OmSnapshotLocalDataManager snapshotLocalDataManager = om.getOmSnapshotManager().getSnapshotLocalDataManager();
    // get snapshotInfo entries
    try (OmMetadataManagerImpl checkpointMetadataManager =
        OmMetadataManagerImpl.createCheckpointMetadataManager(
            conf, checkpoint);
        TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
            iterator = checkpointMetadataManager
            .getSnapshotInfoTable().iterator()) {

      // For each entry, wait for corresponding directory.
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> entry = iterator.next();
        try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataMetaProvider snapMetaProvider =
                 snapshotLocalDataManager.getOmSnapshotLocalDataMeta(entry.getValue())) {
          Path path = Paths.get(getSnapshotPath(conf, entry.getValue(), snapMetaProvider.getMeta().getVersion()));
          if (waitForDir) {
            waitForDirToExist(path);
          }
          snapshotPaths.add(path);
        }
      }
    }
    return snapshotPaths;
  }

  private void waitForDirToExist(Path dir) throws IOException {
    if (!RDBCheckpointUtils.waitForCheckpointDirectoryExist(dir.toFile())) {
      throw new IOException("snapshot dir doesn't exist: " + dir);
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private boolean processDir(Path dir, Map<String, Map<Path, Path>> copyFiles,
                          Map<Path, Path> hardLinkFiles,
                          Map<String, Map<Path, Path>> sstFilesToExclude,
                          Set<Path> snapshotPaths,
                          AtomicLong copySize,
                          Path destDir)
      throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : files.collect(Collectors.toList())) {
        File f = file.toFile();
        if (f.isDirectory()) {
          // Skip any unexpected snapshot files.
          String parent = f.getParent();
          if (parent != null && parent.contains(OM_SNAPSHOT_CHECKPOINT_DIR)
              && !snapshotPaths.contains(file)) {
            LOG.debug("Skipping unneeded file: " + file);
            continue;
          }

          // Skip the real compaction log dir.
          File compactionLogDir = new File(getDbStore().
              getRocksDBCheckpointDiffer().getCompactionLogDir());
          if (f.equals(compactionLogDir)) {
            LOG.debug("Skipping compaction log dir");
            continue;
          }

          // Skip the real compaction sst backup dir.
          File sstBackupDir = new File(getDbStore().
              getRocksDBCheckpointDiffer().getSSTBackupDir());
          if (f.equals(sstBackupDir)) {
            LOG.debug("Skipping sst backup dir");
            continue;
          }
          // findbugs nonsense
          Path filename = file.getFileName();
          if (filename == null) {
            throw new IOException("file has no filename:" + file);
          }

          // Update the dest dir to point to the sub dir
          Path destSubDir = null;
          if (destDir != null) {
            destSubDir = Paths.get(destDir.toString(),
                filename.toString());
          }
          if (!processDir(file, copyFiles, hardLinkFiles, sstFilesToExclude,
                          snapshotPaths, copySize, destSubDir)) {
            return false;
          }
        } else {
          long fileSize = processFile(file, copyFiles, hardLinkFiles,
              sstFilesToExclude, destDir);
          if (copySize.get() + fileSize > maxTotalSstSize) {
            return false;
          } else {
            copySize.addAndGet(fileSize);
          }
        }
      }
    }
    return true;
  }

  /**
   * Takes a db file and determines whether it should be included in
   * the tarball, or added as a link, or excluded altogether.
   * Uses the sstFilesToExclude list to know what already
   * exists on the follower.
   * @param file The db file to be processed.
   * @param copyFiles The db files to be added to tarball.
   * @param hardLinkFiles The db files to be added as hard links.
   * @param sstFilesToExclude The db files to be excluded from tarball.
   */
  @VisibleForTesting
  public static long processFile(Path file, Map<String, Map<Path, Path>> copyFiles,
                                 Map<Path, Path> hardLinkFiles,
                                 Map<String, Map<Path, Path>> sstFilesToExclude,
                                 Path destDir)
      throws IOException {
    long fileSize = 0;
    Path destFile = file;

    // findbugs nonsense
    Path fileNamePath = file.getFileName();
    if (fileNamePath == null) {
      throw new IOException("file has no filename:" + file);
    }
    String fileName = fileNamePath.toString();

    // if the dest dir is not null then the file needs to be copied/linked
    // to the dest dir on the follower.
    if (destDir != null) {
      destFile = Paths.get(destDir.toString(), fileName);
    }
    if (!sstFilesToExclude.getOrDefault(fileNamePath.toString(), Collections.emptyMap()).containsKey(file)) {
      if (fileName.endsWith(ROCKSDB_SST_SUFFIX)) {
        // If same as existing excluded file, add a link for it.
        Path linkPath = findLinkPath(sstFilesToExclude, file);
        if (linkPath != null) {
          hardLinkFiles.put(destFile, linkPath);
        } else {
          // If already in tarball add a link for it.
          linkPath = findLinkPath(copyFiles, file);
          if (linkPath != null) {
            hardLinkFiles.put(destFile, linkPath);
          } else {
            // Add to tarball.
            copyFiles.computeIfAbsent(fileNamePath.toString(), (k) -> new HashMap<>()).put(file, destFile);
            fileSize = Files.size(file);
          }
        }
      } else {
        // Not sst file.
        copyFiles.computeIfAbsent(fileNamePath.toString(), (k) -> new HashMap<>()).put(file, destFile);
      }
    }
    return fileSize;
  }

  /**
   * Find a valid hard link path for file.  If fileName exists in
   * "files" parameter, file should be linked to the corresponding
   * dest path in files.
   * @param files - Map of src/dest path to files available for
   * linking.
   * @param  file - File to be linked.
   * @return dest path of file to be linked to.
   */
  private static Path findLinkPath(Map<String, Map<Path, Path>> files, Path file)
      throws IOException {
    // findbugs nonsense
    Path fileNamePath = file.getFileName();
    if (fileNamePath == null) {
      throw new IOException("file has no filename:" + file);
    }
    String fileName = fileNamePath.toString();

    for (Map.Entry<Path, Path> entry : files.getOrDefault(fileName, Collections.emptyMap()).entrySet()) {
      Path srcPath = entry.getKey();
      Path destPath = entry.getValue();
      if (!srcPath.toString().endsWith(fileName)) {
        continue;
      }
      if (!srcPath.toFile().exists()) {
        continue;
      }
      // Check if the files are hard linked to each other.
      // Note comparison must be done against srcPath, because
      // destPath may only exist on Follower.
      if (getINode(srcPath).equals(getINode(file))) {
        return destPath;
      } else {
        LOG.info("Found non linked sst files with the same name: {}, {}",
            srcPath, file);
      }
    }
    return null;
  }

  private void writeFilesToArchive(
      Map<Path, Path> copyFiles,
      Map<Path, Path> hardLinkFiles,
      ArchiveOutputStream<TarArchiveEntry> archiveOutputStream,
      boolean completed,
      Path checkpointLocation)
      throws IOException {
    Path metaDirPath = getMetaDirPath(checkpointLocation);
    int truncateLength = metaDirPath.toString().length() + 1;

    Map<Path, Path> filteredCopyFiles = completed ? copyFiles :
        copyFiles.entrySet().stream().filter(e ->
        e.getKey().getFileName().toString().toLowerCase().endsWith(".sst")).
        collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    long bytesWritten = 0L;
    int filesWritten = 0;
    long lastLoggedTime = Time.monotonicNow();

    // Go through each of the files to be copied and add to archive.
    for (Map.Entry<Path, Path> entry : filteredCopyFiles.entrySet()) {
      Path path = entry.getValue();

      // Confirm the data is in the right place.
      if (!path.toString().startsWith(metaDirPath.toString())) {
        throw new IOException("tarball file not in metadata dir: "
            + path + ": " + metaDirPath);
      }

      String fixedFile = truncateFileName(truncateLength, path);
      if (fixedFile.startsWith(OM_CHECKPOINT_DIR)) {
        // checkpoint files go to root of tarball
        Path f = Paths.get(fixedFile).getFileName();
        if (f != null) {
          fixedFile = f.toString();
        }
      }
      File file = entry.getKey().toFile();
      if (!file.isDirectory()) {
        filesWritten++;
      }
      bytesWritten += includeFile(file, fixedFile, archiveOutputStream);
      // Log progress every 30 seconds
      if (Time.monotonicNow() - lastLoggedTime >= 30000) {
        LOG.info("Transferred {} KB, #files {} to checkpoint tarball stream...",
            bytesWritten / (1024), filesWritten);
        lastLoggedTime = Time.monotonicNow();
      }
    }

    if (completed) {
      // Only create the hard link list for the last tarball.
      if (!hardLinkFiles.isEmpty()) {
        Path hardLinkFile = null;
        try {
          hardLinkFile = createHardLinkList(truncateLength, hardLinkFiles);
          includeFile(hardLinkFile.toFile(), OmSnapshotManager.OM_HARDLINK_FILE,
              archiveOutputStream);
        } finally {
          if (Objects.nonNull(hardLinkFile)) {
            try {
              Files.delete(hardLinkFile);
            } catch (Exception e) {
              LOG.error("Exception during hard link file: {} deletion",
                  hardLinkFile, e);
            }
          }
        }
      }
      // Mark tarball completed.
      includeRatisSnapshotCompleteFlag(archiveOutputStream);
    }
    LOG.info("Completed transfer of {} KB, #files {} " +
            "to checkpoint tarball stream.{}",
        bytesWritten / (1024), filesWritten, (completed) ?
            " Checkpoint tarball is complete." : "");
  }

  @Nonnull
  private static Path getMetaDirPath(Path checkpointLocation) {
    // This check is done to take care of findbugs else below getParent()
    // should not be null.
    Path locationParent = checkpointLocation.getParent();
    if (null == locationParent) {
      throw new RuntimeException(
          "checkpoint location's immediate parent is null.");
    }
    Path parent = locationParent.getParent();
    if (null == parent) {
      throw new RuntimeException(
          "checkpoint location's path is invalid and could not be verified.");
    }
    return parent;
  }

  private OzoneConfiguration getConf() {
    return ((OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .getConfiguration();
  }

  private Path getSnapshotDir() {
    OzoneManager om = (OzoneManager) getServletContext().getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);
    RDBStore store = (RDBStore) om.getMetadataManager().getStore();
    // store.getSnapshotsParentDir() returns path to checkpointState (e.g. <om-data-dir>/db.snapshots/checkpointState)
    // But we need to return path till db.snapshots which contains checkpointState and diffState.
    // So that whole snapshots and compaction information can be transferred to follower.
    return Paths.get(store.getSnapshotsParentDir()).getParent();
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  static class Lock extends BootstrapStateHandler.Lock {
    private final OzoneManager om;

    Lock(OzoneManager om) {
      super((readLock) -> {
        return om.getMetadataManager().getLock().acquireResourceLock(BOOTSTRAP_LOCK);
      });
      this.om = om;
    }

    @Override
    public UncheckedAutoCloseable acquireWriteLock() throws InterruptedException {
      // Then wait for the double buffer to be flushed.
      om.awaitDoubleBufferFlush();
      return super.acquireWriteLock();
    }

    @Override
    public UncheckedAutoCloseable acquireReadLock() {
      throw new UnsupportedOperationException("Read locks are not supported for OMDBCheckpointServlet");
    }
  }
}
