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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.security.UserGroupInformation;
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
                                  List<String> excludedList)
      throws IOException, InterruptedException {
    Objects.requireNonNull(toExcludeList);
    Objects.requireNonNull(excludedList);

    // Files to be added to tarball
    Set<Path> copyFiles = new HashSet<>();
    // Map of link to path.
    Map<Path, Path> hardLinkFiles = new HashMap<>();

    try (TarArchiveOutputStream archiveOutputStream =
             new TarArchiveOutputStream(destination)) {
      archiveOutputStream
          .setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      archiveOutputStream
          .setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
      // Files to be excluded from tarball
      Set<Path> toExcludeFiles = normalizeExcludeList(toExcludeList,
          checkpoint.getCheckpointLocation().toString(),
          ServerUtils.getOzoneMetaDirPath(getConf()).toString());
      boolean completed = getFilesForArchive(checkpoint, copyFiles,
          hardLinkFiles, toExcludeFiles, includeSnapshotData(request),
          excludedList);
      writeFilesToArchive(copyFiles, hardLinkFiles, archiveOutputStream,
          completed);
    } catch (Exception e) {
      LOG.error("got exception writing to archive " + e);
      throw e;
    }
  }

  // Format list from follower to match data on leader.
  @VisibleForTesting
  public static Set<Path> normalizeExcludeList(List<String> toExcludeList,
      String checkpointLocation, String metaDirPath) {
    Set<Path> paths = new HashSet<>();
    for (String s: toExcludeList) {
      if (!s.startsWith(OM_SNAPSHOT_DIR)) {
        Path fixedPath = Paths.get(checkpointLocation, s);
        paths.add(fixedPath);
      } else {
        paths.add(Paths.get(metaDirPath, s));
      }
    }
    return paths;
  }

  private boolean getFilesForArchive(DBCheckpoint checkpoint,
                                  Set<Path> copyFiles,
                                  Map<Path, Path> hardLinkFiles,
                                  Set<Path> toExcludeFiles,
                                  boolean includeSnapshotData,
                                  List<String> excluded)
      throws IOException {

    maxTotalSstSize = getConf().getLong(
        OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY,
        OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT);

    AtomicLong copySize = new AtomicLong(0L);
    // Get the active fs files.
    Path dir = checkpoint.getCheckpointLocation();
    if (!processDir(dir, copyFiles, hardLinkFiles, toExcludeFiles,
        new HashSet<>(), excluded, copySize)) {
      return false;
    }

    if (!includeSnapshotData) {
      return true;
    }

    // Get the snapshot files.
    Set<Path> snapshotPaths = waitForSnapshotDirs(checkpoint);
    Path snapshotDir = Paths.get(OMStorage.getOmDbDir(getConf()).toString(),
        OM_SNAPSHOT_DIR);
    return processDir(snapshotDir, copyFiles, hardLinkFiles, toExcludeFiles,
        snapshotPaths, excluded, copySize);
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

  private boolean processDir(Path dir, Set<Path> copyFiles,
                          Map<Path, Path> hardLinkFiles,
                          Set<Path> toExcludeFiles,
                          Set<Path> snapshotPaths,
                          List<String> excluded,
                          AtomicLong copySize)
      throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : files.collect(Collectors.toList())) {
        File f = file.toFile();
        if (f.isDirectory()) {
          // Skip any unexpected snapshot files.
          String parent = f.getParent();
          if (parent != null && parent.endsWith(OM_SNAPSHOT_CHECKPOINT_DIR)
              && !snapshotPaths.contains(file)) {
            LOG.debug("Skipping unneeded file: " + file);
            continue;
          }
          if (!processDir(file, copyFiles, hardLinkFiles, toExcludeFiles,
                          snapshotPaths, excluded, copySize)) {
            return false;
          }
        } else {
          long fileSize = processFile(file, copyFiles, hardLinkFiles,
              toExcludeFiles, excluded);
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
   * Uses the toExcludeFiles list to know what already
   * exists on the follower.
   * @param file The db file to be processed.
   * @param copyFiles The db files to be added to tarball.
   * @param hardLinkFiles The db files to be added as hard links.
   * @param toExcludeFiles The db files to be excluded from tarball.
   * @param excluded The list of db files that actually were excluded.
   */
  @VisibleForTesting
  public static long processFile(Path file, Set<Path> copyFiles,
                                 Map<Path, Path> hardLinkFiles,
                                 Set<Path> toExcludeFiles,
                                 List<String> excluded) throws IOException {
    long fileSize = 0;
    if (toExcludeFiles.contains(file)) {
      excluded.add(file.toString());
    } else {
      Path fileNamePath = file.getFileName();
      if (fileNamePath == null) {
        throw new NullPointerException("Bad file: " + file);
      }
      String fileName = fileNamePath.toString();
      if (fileName.endsWith(ROCKSDB_SST_SUFFIX)) {
        // If same as existing excluded file, add a link for it.
        Path linkPath = findLinkPath(toExcludeFiles, fileName);
        if (linkPath != null) {
          hardLinkFiles.put(file, linkPath);
        } else {
          // If already in tarball add a link for it.
          linkPath = findLinkPath(copyFiles, fileName);
          if (linkPath != null) {
            hardLinkFiles.put(file, linkPath);
          } else {
            // Add to tarball.
            copyFiles.add(file);
            fileSize = Files.size(file);
          }
        }
      } else {
        // Not sst file.
        copyFiles.add(file);
      }
    }
    return fileSize;
  }

  // If fileName exists in "files" parameter,
  // it should be linked to path in files.
  private static Path findLinkPath(Set<Path> files, String fileName) {
    for (Path p: files) {
      if (p.toString().endsWith(fileName)) {
        return p;
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

  private void writeFilesToArchive(Set<Path> copyFiles,
                                   Map<Path, Path> hardLinkFiles,
                                   ArchiveOutputStream archiveOutputStream,
                                   boolean completed)
      throws IOException {

    File metaDirPath = ServerUtils.getOzoneMetaDirPath(getConf());
    int truncateLength = metaDirPath.toString().length() + 1;

    Set<Path> filteredCopyFiles = completed ? copyFiles :
        copyFiles.stream().filter(path ->
          path.getFileName().toString().toLowerCase().endsWith(".sst")).
          collect(Collectors.toSet());

    // Go through each of the files to be copied and add to archive.
    for (Path file : filteredCopyFiles) {
      String fixedFile = truncateFileName(truncateLength, file);
      if (fixedFile.startsWith(OM_CHECKPOINT_DIR)) {
        // checkpoint files go to root of tarball
        Path f = Paths.get(fixedFile).getFileName();
        if (f != null) {
          fixedFile = f.toString();
        }
      }
      includeFile(file.toFile(), fixedFile, archiveOutputStream);
    }

    if (completed) {
      // Only create the hard link list for the last tarball.
      if (!hardLinkFiles.isEmpty()) {
        Path hardLinkFile = createHardLinkList(truncateLength, hardLinkFiles);
        includeFile(hardLinkFile.toFile(), OmSnapshotManager.OM_HARDLINK_FILE,
            archiveOutputStream);
      }
      // Mark tarball completed.
      includeRatisSnapshotCompleteFlag(archiveOutputStream);
    }
  }

  private OzoneConfiguration getConf() {
    return ((OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .getConfiguration();
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  static class Lock extends BootstrapStateHandler.Lock {
    private final BootstrapStateHandler keyDeletingService;
    private final BootstrapStateHandler sstFilteringService;
    private final BootstrapStateHandler rocksDbCheckpointDiffer;
    private final BootstrapStateHandler snapshotDeletingService;
    private final OzoneManager om;

    Lock(OzoneManager om) {
      this.om = om;
      keyDeletingService = om.getKeyManager().getDeletingService();
      sstFilteringService = om.getKeyManager().getSnapshotSstFilteringService();
      rocksDbCheckpointDiffer = om.getMetadataManager().getStore()
          .getRocksDBCheckpointDiffer();
      snapshotDeletingService = om.getKeyManager().getSnapshotDeletingService();
    }

    @Override
    public BootstrapStateHandler.Lock lock()
        throws InterruptedException {
      // First lock all the handlers.
      keyDeletingService.getBootstrapStateLock().lock();
      sstFilteringService.getBootstrapStateLock().lock();
      rocksDbCheckpointDiffer.getBootstrapStateLock().lock();
      snapshotDeletingService.getBootstrapStateLock().lock();

      // Then wait for the double buffer to be flushed.
      om.getOmRatisServer().getOmStateMachine().awaitDoubleBufferFlush();
      return this;
    }

    @Override
    public void unlock() {
      snapshotDeletingService.getBootstrapStateLock().unlock();
      rocksDbCheckpointDiffer.getBootstrapStateLock().unlock();
      sstFilteringService.getBootstrapStateLock().unlock();
      keyDeletingService.getBootstrapStateLock().unlock();
    }
  }
}
