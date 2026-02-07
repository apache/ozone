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

import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RATIS_SNAPSHOT_DIR;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for managing OM DB checkpoint backup and replacement on disk.
 * Contains only filesystem/DB related operations – lifecycle handling
 * (stopping services, reloading OM state, restarting RPC, exit, etc.)
 * is expected to be handled by {@link OzoneManager}.
 *
 * <p>Placed in package {@code org.apache.hadoop.ozone.om} so that it can use
 * package-private methods on OzoneManager without exposing them as public API.
 */
public final class OmRatisCheckpointInstaller {

  private static final Logger LOG =
      LoggerFactory.getLogger(OmRatisCheckpointInstaller.class);

  /**
   * Replaces OM DB with checkpoint data from leader.
   * Only backs up/replaces items present in checkpointLocation.
   *
   * This method is intentionally free of OM lifecycle concerns – the caller is
   * responsible for stopping services, handling failures and rollback of OM
   * process state. In case of IO failures, an {@link IOException} is thrown.
   *
   * @param lastAppliedIndex last applied transaction index
   * @param oldDB current DB directory
   * @param checkpointLocation checkpoint data directory
   * @param ratisLogDirName name of the Ratis log directory to exclude (may be null)
   * @return backup directory with original state
   * @throws IOException if file operations fail
   */
  public File replaceOMDBWithCheckpoint(long lastAppliedIndex,
      File oldDB, Path checkpointLocation, String ratisLogDirName)
      throws IOException {

    // Create backup directory
    String dbBackupName = OzoneConsts.OM_DB_BACKUP_PREFIX + lastAppliedIndex
        + "_" + System.currentTimeMillis();
    File dbDir = oldDB.getParentFile();
    if (dbDir == null) {
      throw new IOException("Failed to get parent directory of: " + oldDB);
    }
    File dbBackupDir = new File(dbDir, dbBackupName);

    if (!dbBackupDir.mkdirs()) {
      throw new IOException("Failed to create backup directory: " + dbBackupDir);
    }

    // Backup only items present in checkpointLocation
    Set<String> backedUpItems = new HashSet<>();

    if (Files.exists(checkpointLocation) && Files.isDirectory(checkpointLocation)) {
      try (Stream<Path> checkpointContents = Files.list(checkpointLocation)) {
        for (Path checkpointItem : checkpointContents.collect(Collectors.toList())) {
          Path fileName = checkpointItem.getFileName();
          if (fileName == null) {
            LOG.warn("Skipping path with no file name: {}", checkpointItem);
            continue;
          }
          String itemName = fileName.toString();
          // Skip backup dirs, raft logs, and marker files
          if (itemName.startsWith(OzoneConsts.OM_DB_BACKUP_PREFIX)
              || itemName.equals(DB_TRANSIENT_MARKER) ||
              itemName.equals(OZONE_RATIS_SNAPSHOT_DIR) ||
              (ratisLogDirName != null &&
                  itemName.equals(new File(ratisLogDirName).getName()))) {
            continue;
          }

          // Backup only if item exists in dbDir
          Path existingItem = dbDir.toPath().resolve(itemName);
          if (Files.exists(existingItem)) {
            Path backupTarget = dbBackupDir.toPath().resolve(itemName);
            Files.move(existingItem, backupTarget);
            backedUpItems.add(itemName);
          }
        }
      }
    }

    moveCheckpointFiles(oldDB, checkpointLocation, dbDir, dbBackupDir,
        backedUpItems, ratisLogDirName);

    return dbBackupDir;
  }

  /**
   * Moves checkpoint files from checkpointLocation to dbDir.
   * Uses backup for rollback on failure.
   *
   * @param oldDB old DB directory
   * @param checkpointLocation source checkpoint directory
   * @param dbDir target directory (parent of oldDB)
   * @param dbBackupDir backup directory
   * @param backedUpItems set of backed up item names for selective restore
   * @param ratisLogDirName name of the Ratis log directory to exclude (may be null)
   * @throws IOException if file operations fail
   */
  private void moveCheckpointFiles(File oldDB,
      Path checkpointLocation, File dbDir, File dbBackupDir,
      Set<String> backedUpItems, String ratisLogDirName) throws IOException {
    Path markerFile = new File(dbDir, DB_TRANSIENT_MARKER).toPath();
    try {
      Files.createFile(markerFile);
      if (!Files.exists(checkpointLocation) ||
          !Files.isDirectory(checkpointLocation)) {
        throw new IOException("Checkpoint data directory does not exist: " +
            checkpointLocation);
      }
      try (Stream<Path> checkpointContents = Files.list(checkpointLocation)) {
        for (Path sourcePath : checkpointContents.collect(Collectors.toList())) {
          Path fileName = sourcePath.getFileName();
          if (fileName == null) {
            LOG.warn("Skipping path with no file name: {}", sourcePath);
            continue;
          }
          String itemName = fileName.toString();
          // Skip backup dirs, raft logs, and marker files
          if (itemName.startsWith(OzoneConsts.OM_DB_BACKUP_PREFIX) ||
              itemName.equals(DB_TRANSIENT_MARKER) ||
              itemName.equals(OZONE_RATIS_SNAPSHOT_DIR) ||
              (ratisLogDirName != null &&
                  itemName.equals(new File(ratisLogDirName).getName()))) {
            continue;
          }
          Path targetPath = dbDir.toPath().resolve(itemName);
          if (Files.exists(targetPath)) {
            throw new IOException("Cannot move checkpoint data into target." +
                "Checkpoint data already exists: " + targetPath);
          }
          Files.move(sourcePath, targetPath);
        }
      }
      Files.deleteIfExists(markerFile);
    } catch (IOException e) {
      LOG.error("Failed to move checkpoint data from {} to {}. " +
          "Restoring from backup.", checkpointLocation, dbDir, e);
      // Rollback: restore backed up items. Since this class is responsible only
      // for filesystem/DB state, it makes a best-effort attempt to restore and
      // then rethrows the original exception to the caller.
      for (String itemName : backedUpItems) {
        Path targetPath = dbDir.toPath().resolve(itemName);
        if (Files.exists(targetPath)) {
          if (Files.isDirectory(targetPath)) {
            FileUtil.fullyDelete(targetPath.toFile());
          } else {
            Files.delete(targetPath);
          }
        }
      }
      // Restore from backup
      if (dbBackupDir.exists() && dbBackupDir.isDirectory()) {
        File[] backupContents = dbBackupDir.listFiles();
        if (backupContents != null) {
          for (File backupItem : backupContents) {
            String itemName = backupItem.getName();
            if (backedUpItems.contains(itemName)) {
              Path targetPath = dbDir.toPath().resolve(itemName);
              Files.move(backupItem.toPath(), targetPath);
            }
          }
        }
      }
      Files.deleteIfExists(markerFile);
      throw e;
    }
  }

  /**
   * Install checkpoint obtained from leader using the dbCheckpoint endpoint.
   * The unpacked directory after installing hardlinks would comprise of
   * both active OM DB dir and the db.snapshots directory.
   * If the checkpoint snapshot index is greater than
   * OM's last applied transaction index, then re-initialize the OM
   * state via this checkpoint. Before re-initializing OM state, the OM Ratis
   * server should be stopped so that no new transactions can be applied
   *
   * @param om OzoneManager instance
   * @param leaderId leader OM node ID
   * @param checkpointLocation checkpoint data directory
   * @return TermIndex if installation succeeds, null otherwise
   * @throws Exception if installation fails
   */
  public TermIndex installCheckpoint(OzoneManager om, String leaderId,
      Path checkpointLocation) throws Exception {
    Path omDbPath = Paths.get(checkpointLocation.toString(), OM_DB_NAME);
    TransactionInfo checkpointTrxnInfo = OzoneManagerRatisUtils
        .getTrxnInfoFromCheckpoint(om.getConfiguration(), omDbPath);
    LOG.info("Installing checkpoint with OMTransactionInfo {}",
        checkpointTrxnInfo);
    return installCheckpoint(om, leaderId, checkpointLocation, checkpointTrxnInfo);
  }

  /**
   * Install checkpoint obtained from leader using the dbCheckpoint endpoint.
   * The unpacked directory after installing hardlinks would comprise of
   * both active OM DB dir and the db.snapshots directory.
   * If the checkpoint snapshot index is greater than
   * OM's last applied transaction index, then re-initialize the OM
   * state via this checkpoint. Before re-initializing OM state, the OM Ratis
   * server should be stopped so that no new transactions can be applied
   *
   * @param om OzoneManager instance
   * @param leaderId leader OM node ID
   * @param checkpointLocation checkpoint data directory
   * @param checkpointTrxnInfo transaction info from checkpoint
   * @return TermIndex if installation succeeds, null otherwise
   * @throws Exception if installation fails
   */
  public TermIndex installCheckpoint(OzoneManager om, String leaderId,
      Path checkpointLocation, TransactionInfo checkpointTrxnInfo) throws Exception {
    long startTime = Time.monotonicNow();
    File oldDBLocation = om.getMetadataManager().getStore().getDbLocation();
    Path omDbPath = Paths.get(checkpointLocation.toString(), OM_DB_NAME);
    try {
      // Stop Background services
      om.getKeyManager().stop();
      om.stopSecretManager();
      om.stopTrashEmptier();
      om.getOmSnapshotManager().invalidateCache();
      // Pause the State Machine so that no new transactions can be applied.
      // This action also clears the OM Double Buffer so that if there are any
      // pending transactions in the buffer, they are discarded.
      om.getOmRatisServer().getOmStateMachine().pause();
    } catch (Exception e) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with " +
          "installing the new checkpoint.");
      // Stop the checkpoint install process and restart the services.
      om.getKeyManager().start(om.getConfiguration());
      om.startSecretManagerIfNecessary();
      om.startTrashEmptier(om.getConfiguration());
      throw e;
    }

    File dbBackup = null;
    TermIndex termIndex = om.getOmRatisServer().getLastAppliedTermIndex();
    long term = termIndex.getTerm();
    long lastAppliedIndex = termIndex.getIndex();

    // Check if current applied log index is smaller than the downloaded
    // checkpoint transaction index. If yes, proceed by stopping the ratis
    // server so that the OM state can be re-initialized. If no then do not
    // proceed with installSnapshot.
    boolean canProceed = OzoneManagerRatisUtils.verifyTransactionInfo(
        checkpointTrxnInfo, lastAppliedIndex, leaderId, omDbPath);

    boolean oldOmMetadataManagerStopped = false;
    boolean newMetadataManagerStarted = false;
    boolean omRpcServerStopped = false;
    long time = Time.monotonicNow();
    if (canProceed) {
      // Stop RPC server before stop metadataManager
      om.getOmRpcServer().stop();
      om.setOmRpcServerRunning(false);
      omRpcServerStopped = true;
      LOG.info("RPC server is stopped. Spend {} ms.", Time.monotonicNow() - time);
      try {
        // Stop old metadataManager before replacing DB Dir
        time = Time.monotonicNow();
        om.getMetadataManager().stop();
        oldOmMetadataManagerStopped = true;
        LOG.info("metadataManager is stopped. Spend {} ms.", Time.monotonicNow() - time);
      } catch (Exception e) {
        String errorMsg = "Failed to stop metadataManager. Cannot proceed " +
            "with installing the new checkpoint.";
        LOG.error(errorMsg);
        om.getExitManager().exitSystem(1, errorMsg, e, LOG);
      }
      try {
        time = Time.monotonicNow();
        dbBackup = replaceOMDBWithCheckpoint(
            lastAppliedIndex, oldDBLocation, checkpointLocation,
            om.getRatisLogDirectory());
        term = checkpointTrxnInfo.getTerm();
        lastAppliedIndex = checkpointTrxnInfo.getTransactionIndex();
        LOG.info("Replaced DB with checkpoint from OM: {}, term: {}, " +
            "index: {}, time: {} ms", leaderId, term, lastAppliedIndex,
            Time.monotonicNow() - time);
      } catch (Exception e) {
        LOG.error("Failed to install Snapshot from {} as OM failed to replace" +
            " DB with downloaded checkpoint. Reloading old OM state.",
            leaderId, e);
      }
    } else {
      LOG.warn("Cannot proceed with InstallSnapshot as OM is at TermIndex {} " +
          "and checkpoint has lower TermIndex {}. Reloading old state of OM.",
          termIndex, checkpointTrxnInfo.getTermIndex());
    }

    if (oldOmMetadataManagerStopped) {
      // Close snapDiff's rocksDB instance only if metadataManager gets closed.
      om.getOmSnapshotManager().close();
    }

    // Reload the OM DB store with the new checkpoint.
    // Restart (unpause) the state machine and update its last applied index
    // to the installed checkpoint's snapshot index.
    try {
      if (oldOmMetadataManagerStopped) {
        time = Time.monotonicNow();
        om.reloadOMState();
        om.setTransactionInfo(TransactionInfo.valueOf(termIndex));
        om.getOmRatisServer().getOmStateMachine().unpause(lastAppliedIndex, term);
        newMetadataManagerStarted = true;
        LOG.info("Reloaded OM state with Term: {} and Index: {}. Spend {} ms",
            term, lastAppliedIndex, Time.monotonicNow() - time);
      } else {
        // OM DB is not stopped. Start the services.
        om.getKeyManager().start(om.getConfiguration());
        om.startSecretManagerIfNecessary();
        om.startTrashEmptier(om.getConfiguration());
        om.getOmRatisServer().getOmStateMachine().unpause(lastAppliedIndex, term);
        LOG.info("OM DB is not stopped. Started services with Term: {} and " +
            "Index: {}", term, lastAppliedIndex);
      }
    } catch (Exception ex) {
      String errorMsg = "Failed to reload OM state and instantiate services.";
      om.getExitManager().exitSystem(1, errorMsg, ex, LOG);
    }

    if (omRpcServerStopped && newMetadataManagerStarted) {
      // Start the RPC server. RPC server start requires metadataManager
      try {
        time = Time.monotonicNow();
        om.setOmRpcServer(om.getRpcServer(om.getConfiguration()));
        om.getOmRpcServer().start();
        om.setOmRpcServerRunning(true);
        LOG.info("RPC server is re-started. Spend {} ms.", Time.monotonicNow() - time);
      } catch (Exception e) {
        String errorMsg = "Failed to start RPC Server.";
        om.getExitManager().exitSystem(1, errorMsg, e, LOG);
      }
    }
    om.buildDBCheckpointInstallAuditLog(leaderId, term, lastAppliedIndex);

    // Delete the backup DB
    try {
      if (dbBackup != null) {
        FileUtils.deleteFully(dbBackup);
      }
    } catch (Exception e) {
      LOG.error("Failed to delete the backup of the original DB {}",
          dbBackup, e);
    }

    if (lastAppliedIndex != checkpointTrxnInfo.getTransactionIndex()) {
      // Install Snapshot failed and old state was reloaded. Return null to
      // Ratis to indicate that installation failed.
      return null;
    }

    // TODO: We should only return the snpashotIndex to the leader.
    //  Should be fixed after RATIS-586
    TermIndex newTermIndex = TermIndex.valueOf(term, lastAppliedIndex);
    LOG.info("Install Checkpoint is finished with Term: {} and Index: {}. " +
        "Spend {} ms.", newTermIndex.getTerm(), newTermIndex.getIndex(),
        (Time.monotonicNow() - startTime));
    return newTermIndex;
  }
}
