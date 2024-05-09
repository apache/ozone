/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
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
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.includeFile;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.includeRatisSnapshotCompleteFlag;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.createHardLinkList;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.truncateFileName;

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
  private static final AtomicLong PAUSE_COUNTER = new AtomicLong(0);

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
                                  List<String> toExcludeList,
                                  List<String> excludedList,
                                  Path tmpdir)
      throws IOException, InterruptedException {
    Objects.requireNonNull(toExcludeList);
    Objects.requireNonNull(excludedList);

    // copyFiles is a map of files to be added to tarball.  The keys
    // are the src path of the file, (where they are copied from on
    // the leader.) The values are the dest path of the file, (where
    // they are copied to on the follower.)  In most cases these are
    // the same.  For synchronization purposes, some files are copied
    // to a temp directory on the leader.  In those cases the source
    // and dest won't be the same.
    Map<Path, Path> copyFiles = new HashMap<>();

    // Map of link to path.
    Map<Path, Path> hardLinkFiles = new HashMap<>();

    try (TarArchiveOutputStream archiveOutputStream =
             new TarArchiveOutputStream(destination)) {
      archiveOutputStream
          .setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      archiveOutputStream
          .setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
      RocksDBCheckpointDiffer differ =
          getDbStore().getRocksDBCheckpointDiffer();
      DirectoryData sstBackupDir = new DirectoryData(tmpdir,
          differ.getSSTBackupDir());
      DirectoryData compactionLogDir = new DirectoryData(tmpdir,
          differ.getCompactionLogDir());

      // Files to be excluded from tarball
      Map<Path, Path> sstFilesToExclude = normalizeExcludeList(toExcludeList,
          checkpoint.getCheckpointLocation(), sstBackupDir);
      boolean completed = getFilesForArchive(checkpoint, copyFiles,
          hardLinkFiles, sstFilesToExclude, includeSnapshotData(request),
          excludedList, sstBackupDir, compactionLogDir);
      writeFilesToArchive(copyFiles, hardLinkFiles, archiveOutputStream,
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
  public static Map<Path, Path> normalizeExcludeList(
      List<String> toExcludeList,
      Path checkpointLocation,
      DirectoryData sstBackupDir) {
    Map<Path, Path> paths = new HashMap<>();
    Path metaDirPath = getMetaDirPath(checkpointLocation);
    for (String s : toExcludeList) {
      Path destPath = Paths.get(metaDirPath.toString(), s);
      if (destPath.toString().startsWith(
          sstBackupDir.getOriginalDir().toString())) {
        // The source of the sstBackupDir is a temporary directory and needs
        // to be adjusted accordingly.
        int truncateLength =
            sstBackupDir.getOriginalDir().toString().length() + 1;
        Path srcPath = Paths.get(sstBackupDir.getTmpDir().toString(),
            truncateFileName(truncateLength, destPath));
        paths.put(srcPath, destPath);
      } else if (!s.startsWith(OM_SNAPSHOT_DIR)) {
        Path fixedPath = Paths.get(checkpointLocation.toString(), s);
        paths.put(fixedPath, fixedPath);
      } else {
        paths.put(destPath, destPath);
      }
    }
    return paths;
  }

  /**
   * Pauses rocksdb compaction threads while creating copies of
   * compaction logs and hard links of sst backups.
   * @param  tmpdir - Place to create copies/links
   * @param  flush -  Whether to flush the db or not.
   * @return Checkpoint containing snapshot entries expected.
   */
  @Override
  public DBCheckpoint getCheckpoint(Path tmpdir, boolean flush)
      throws IOException {
    DBCheckpoint checkpoint;

    // make tmp directories to contain the copies
    RocksDBCheckpointDiffer differ = getDbStore().getRocksDBCheckpointDiffer();
    DirectoryData sstBackupDir = new DirectoryData(tmpdir,
        differ.getSSTBackupDir());
    DirectoryData compactionLogDir = new DirectoryData(tmpdir,
        differ.getCompactionLogDir());

    long startTime = System.currentTimeMillis();
    long pauseCounter = PAUSE_COUNTER.incrementAndGet();

    try {
      LOG.info("Compaction pausing {} started.", pauseCounter);
      // Pause compactions, Copy/link files and get checkpoint.
      differ.incrementTarballRequestCount();
      FileUtils.copyDirectory(compactionLogDir.getOriginalDir(),
          compactionLogDir.getTmpDir());
      OmSnapshotUtils.linkFiles(sstBackupDir.getOriginalDir(),
          sstBackupDir.getTmpDir());
      checkpoint = getDbStore().getCheckpoint(flush);
    } finally {
      // Unpause the compaction threads.
      differ.decrementTarballRequestCountAndNotify();
      long elapsedTime = System.currentTimeMillis() - startTime;
      LOG.info("Compaction pausing {} ended. Elapsed ms: {}", pauseCounter, elapsedTime);
    }
    return checkpoint;
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
                                  Map<Path, Path> copyFiles,
                                  Map<Path, Path> hardLinkFiles,
                                  Map<Path, Path> sstFilesToExclude,
                                  boolean includeSnapshotData,
                                  List<String> excluded,
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
    // Get the active fs files.
    Path dir = checkpoint.getCheckpointLocation();
    if (!processDir(dir, copyFiles, hardLinkFiles, sstFilesToExclude,
        new HashSet<>(), excluded, copySize, null)) {
      return false;
    }

    if (!includeSnapshotData) {
      return true;
    }

    // Get the snapshot files.
    Set<Path> snapshotPaths = waitForSnapshotDirs(checkpoint);
    Path snapshotDir = getSnapshotDir();
    if (!processDir(snapshotDir, copyFiles, hardLinkFiles, sstFilesToExclude,
        snapshotPaths, excluded, copySize, null)) {
      return false;
    }
    // Process the tmp sst compaction dir.
    if (!processDir(sstBackupDir.getTmpDir().toPath(), copyFiles, hardLinkFiles,
        sstFilesToExclude, new HashSet<>(), excluded, copySize,
        sstBackupDir.getOriginalDir().toPath())) {
      return false;
    }

    // Process the tmp compaction log dir.
    return processDir(compactionLogDir.getTmpDir().toPath(), copyFiles,
        hardLinkFiles, sstFilesToExclude,
        new HashSet<>(), excluded, copySize,
        compactionLogDir.getOriginalDir().toPath());

  }

  /**
   * The snapshotInfo table may contain a snapshot that
   * doesn't yet exist on the fs, so wait a few seconds for it.
   * @param checkpoint Checkpoint containing snapshot entries expected.
   * @return Set of expected snapshot dirs.
   */
  private Set<Path> waitForSnapshotDirs(DBCheckpoint checkpoint)
      throws IOException {

    OzoneConfiguration conf = getConf();

    Set<Path> snapshotPaths = new HashSet<>();

    // get snapshotInfo entries
    OmMetadataManagerImpl checkpointMetadataManager =
        OmMetadataManagerImpl.createCheckpointMetadataManager(
            conf, checkpoint);
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
        iterator = checkpointMetadataManager
        .getSnapshotInfoTable().iterator()) {

      // For each entry, wait for corresponding directory.
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> entry = iterator.next();
        Path path = Paths.get(getSnapshotPath(conf, entry.getValue()));
        waitForDirToExist(path);
        snapshotPaths.add(path);
      }
    } finally {
      checkpointMetadataManager.stop();
    }
    return snapshotPaths;
  }

  private void waitForDirToExist(Path dir) throws IOException {
    if (!RDBCheckpointUtils.waitForCheckpointDirectoryExist(dir.toFile())) {
      throw new IOException("snapshot dir doesn't exist: " + dir);
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private boolean processDir(Path dir, Map<Path, Path> copyFiles,
                          Map<Path, Path> hardLinkFiles,
                          Map<Path, Path> sstFilesToExclude,
                          Set<Path> snapshotPaths,
                          List<String> excluded,
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
                          snapshotPaths, excluded, copySize, destSubDir)) {
            return false;
          }
        } else {
          long fileSize = processFile(file, copyFiles, hardLinkFiles,
              sstFilesToExclude, excluded, destDir);
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
   * @param excluded The list of db files that actually were excluded.
   */
  @VisibleForTesting
  public static long processFile(Path file, Map<Path, Path> copyFiles,
                                 Map<Path, Path> hardLinkFiles,
                                 Map<Path, Path> sstFilesToExclude,
                                 List<String> excluded,
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
    if (sstFilesToExclude.containsKey(file)) {
      excluded.add(destFile.toString());
    } else {
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
            copyFiles.put(file, destFile);
            fileSize = Files.size(file);
          }
        }
      } else {
        // Not sst file.
        copyFiles.put(file, destFile);
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
  private static Path findLinkPath(Map<Path, Path> files, Path file)
      throws IOException {
    // findbugs nonsense
    Path fileNamePath = file.getFileName();
    if (fileNamePath == null) {
      throw new IOException("file has no filename:" + file);
    }
    String fileName = fileNamePath.toString();

    for (Map.Entry<Path, Path> entry: files.entrySet()) {
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
      if (OmSnapshotUtils.getINode(srcPath).equals(
          OmSnapshotUtils.getINode(file))) {
        return destPath;
      } else {
        LOG.info("Found non linked sst files with the same name: {}, {}",
            srcPath, file);
      }
    }
    return null;
  }

  // Returns value of http request parameter.
  private boolean includeSnapshotData(HttpServletRequest request) {
    String includeParam =
        request.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA);
    return Boolean.parseBoolean(includeParam);
  }

  private void writeFilesToArchive(
      Map<Path, Path> copyFiles,
      Map<Path, Path> hardLinkFiles,
      ArchiveOutputStream archiveOutputStream,
      boolean completed,
      Path checkpointLocation)
      throws IOException {
    Path metaDirPath = getMetaDirPath(checkpointLocation);
    int truncateLength = metaDirPath.toString().length() + 1;

    Map<Path, Path> filteredCopyFiles = completed ? copyFiles :
        copyFiles.entrySet().stream().filter(e ->
        e.getKey().getFileName().toString().toLowerCase().endsWith(".sst")).
        collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Go through each of the files to be copied and add to archive.
    for (Map.Entry<Path, Path> entry : filteredCopyFiles.entrySet()) {
      Path file = entry.getValue();

      // Confirm the data is in the right place.
      if (!file.toString().startsWith(metaDirPath.toString())) {
        throw new IOException("tarball file not in metadata dir: "
            + file + ": " + metaDirPath);
      }

      String fixedFile = truncateFileName(truncateLength, file);
      if (fixedFile.startsWith(OM_CHECKPOINT_DIR)) {
        // checkpoint files go to root of tarball
        Path f = Paths.get(fixedFile).getFileName();
        if (f != null) {
          fixedFile = f.toString();
        }
      }
      includeFile(entry.getKey().toFile(), fixedFile, archiveOutputStream);
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
  }

  @NotNull
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
    private final List<BootstrapStateHandler.Lock> locks;
    private final OzoneManager om;

    Lock(OzoneManager om) {
      Preconditions.checkNotNull(om);
      Preconditions.checkNotNull(om.getKeyManager());
      Preconditions.checkNotNull(om.getMetadataManager());
      Preconditions.checkNotNull(om.getMetadataManager().getStore());

      this.om = om;

      locks = Stream.of(
          om.getKeyManager().getDeletingService(),
          om.getKeyManager().getSnapshotSstFilteringService(),
          om.getMetadataManager().getStore().getRocksDBCheckpointDiffer(),
          om.getKeyManager().getSnapshotDeletingService()
      )
          .filter(Objects::nonNull)
          .map(BootstrapStateHandler::getBootstrapStateLock)
          .collect(Collectors.toList());
    }

    @Override
    public BootstrapStateHandler.Lock lock()
        throws InterruptedException {
      // First lock all the handlers.
      for (BootstrapStateHandler.Lock lock : locks) {
        lock.lock();
      }

      // Then wait for the double buffer to be flushed.
      om.awaitDoubleBufferFlush();
      return this;
    }

    @Override
    public void unlock() {
      locks.forEach(BootstrapStateHandler.Lock::unlock);
    }
  }
}
