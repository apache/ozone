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

package org.apache.hadoop.ozone.om.ratis_snapshot;

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
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Installs checkpoints from leader OM for Ratis follower bootstrap.
 * Extracted from OzoneManager to reduce class complexity.
 */
public final class OmRatisCheckpointInstaller {

  // Use OzoneManager.class logger for test compatibility
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManager.class);

  /**
   * Context interface for accessing OzoneManager operations
   * without exposing private methods/fields.
   */
  public interface Context {
    /**
     * Stops background services and pauses the state machine.
     *
     * @throws Exception if stopping services fails
     */
    void stopBackgroundServicesAndPause() throws Exception;

    /**
     * Starts background services.
     *
     * @throws Exception if starting services fails
     */
    void startBackgroundServices() throws Exception;

    /**
     * Gets the last applied term and index.
     *
     * @return last applied TermIndex
     */
    TermIndex getLastAppliedTermIndex();

    /**
     * Gets the current DB location.
     *
     * @return current DB directory
     */
    File getCurrentDbLocation();

    /**
     * Stops the RPC server.
     */
    void stopRpcServer();

    /**
     * Stops the metadata manager.
     *
     * @throws Exception if stopping metadata manager fails
     */
    void stopMetadataManager() throws Exception;

    /**
     * Closes the snapshot manager.
     */
    void closeSnapshotManager();

    /**
     * Reloads OM state with new checkpoint.
     *
     * @throws Exception if reloading state fails
     */
    void reloadOMState() throws Exception;

    /**
     * Sets transaction info.
     *
     * @param info transaction info to set
     */
    void setTransactionInfo(TransactionInfo info);

    /**
     * Unpauses the state machine.
     *
     * @param index transaction index
     * @param term term number
     */
    void unpauseStateMachine(long index, long term);

    /**
     * Restarts the RPC server.
     *
     * @throws Exception if restarting RPC server fails
     */
    void restartRpcServer() throws Exception;

    /**
     * Audits checkpoint installation.
     *
     * @param leaderId leader OM node ID
     * @param term term number
     * @param index transaction index
     */
    void auditCheckpointInstall(String leaderId, long term, long index);

    /**
     * Exits the system.
     *
     * @param code exit code
     * @param msg exit message
     * @param e exception that caused exit
     * @param log logger for logging
     * @throws IOException if exit operation fails
     */
    void exitSystem(int code, String msg, Exception e, Logger log)
        throws IOException;

    /**
     * Gets the Ratis log directory name.
     *
     * @return Ratis log directory name, or null if not set
     */
    String getRatisLogDirName();
  }

  /**
   * Installs checkpoint from leader OM.
   * Re-initializes OM state if checkpoint index is greater than current
   * applied index.
   *
   * @param ctx OzoneManager context
   * @param leaderId leader OM node ID
   * @param checkpointLocation checkpoint directory path
   * @param checkpointTrxnInfo checkpoint transaction info
   * @return installed checkpoint TermIndex, or null if failed
   * @throws Exception if installation fails
   */
  public TermIndex installCheckpoint(Context ctx, String leaderId,
      Path checkpointLocation, TransactionInfo checkpointTrxnInfo)
      throws Exception {
    long startTime = Time.monotonicNow();
    File oldDBLocation = ctx.getCurrentDbLocation();
    Path omDbPath = Paths.get(checkpointLocation.toString(), OM_DB_NAME);

    // Stop background services and pause state machine
    try {
      ctx.stopBackgroundServicesAndPause();
    } catch (Exception e) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with " +
          "installing the new checkpoint.");
      ctx.startBackgroundServices();
      throw e;
    }

    File dbBackup = null;
    TermIndex termIndex = ctx.getLastAppliedTermIndex();
    long term = termIndex.getTerm();
    long lastAppliedIndex = termIndex.getIndex();

    // Verify checkpoint transaction info
    boolean canProceed = OzoneManagerRatisUtils.verifyTransactionInfo(
        checkpointTrxnInfo, lastAppliedIndex, leaderId, omDbPath);

    boolean oldOmMetadataManagerStopped = false;
    boolean newMetadataManagerStarted = false;
    boolean omRpcServerStopped = false;
    long time = Time.monotonicNow();

    if (canProceed) {
      // Stop RPC server before stop metadataManager
      ctx.stopRpcServer();
      omRpcServerStopped = true;
      LOG.info("RPC server is stopped. Spend {} ms.", Time.monotonicNow() - time);

      try {
        // Stop old metadataManager before replacing DB Dir
        time = Time.monotonicNow();
        ctx.stopMetadataManager();
        oldOmMetadataManagerStopped = true;
        LOG.info("metadataManager is stopped. Spend {} ms.",
            Time.monotonicNow() - time);
      } catch (Exception e) {
        String errorMsg = "Failed to stop metadataManager. Cannot proceed " +
            "with installing the new checkpoint.";
        LOG.error(errorMsg);
        try {
          ctx.exitSystem(1, errorMsg, e, LOG);
        } catch (IOException ex) {
          // exitSystem terminates the process, this should never be reached
          LOG.error("Unexpected exception from exitSystem", ex);
        }
      }

      try {
        time = Time.monotonicNow();
        dbBackup = replaceOMDBWithCheckpoint(ctx, lastAppliedIndex, oldDBLocation,
            checkpointLocation);
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
      ctx.closeSnapshotManager();
    }

    // Reload OM state with new checkpoint
    try {
      if (oldOmMetadataManagerStopped) {
        time = Time.monotonicNow();
        ctx.reloadOMState();
        ctx.setTransactionInfo(TransactionInfo.valueOf(termIndex));
        ctx.unpauseStateMachine(lastAppliedIndex, term);
        newMetadataManagerStarted = true;
        LOG.info("Reloaded OM state with Term: {} and Index: {}. Spend {} ms",
            term, lastAppliedIndex, Time.monotonicNow() - time);
      } else {
        // DB not replaced, just restart services
        ctx.startBackgroundServices();
        ctx.unpauseStateMachine(lastAppliedIndex, term);
        LOG.info("OM DB is not stopped. Started services with Term: {} and " +
            "Index: {}", term, lastAppliedIndex);
      }
    } catch (Exception ex) {
      String errorMsg = "Failed to reload OM state and instantiate services.";
      try {
        ctx.exitSystem(1, errorMsg, ex, LOG);
      } catch (IOException e) {
        // exitSystem terminates the process, this should never be reached
        LOG.error("Unexpected exception from exitSystem", e);
      }
    }

    // Restart RPC server if needed
    if (omRpcServerStopped && newMetadataManagerStarted) {
      try {
        time = Time.monotonicNow();
        ctx.restartRpcServer();
        LOG.info("RPC server is re-started. Spend {} ms.",
            Time.monotonicNow() - time);
      } catch (Exception e) {
        String errorMsg = "Failed to start RPC Server.";
        try {
          ctx.exitSystem(1, errorMsg, e, LOG);
        } catch (IOException ex) {
          // exitSystem terminates the process, this should never be reached
          LOG.error("Unexpected exception from exitSystem", ex);
        }
      }
    }

    ctx.auditCheckpointInstall(leaderId, term, lastAppliedIndex);

    // Cleanup backup directory
    try {
      if (dbBackup != null) {
        FileUtils.deleteFully(dbBackup);
      }
    } catch (Exception e) {
      LOG.error("Failed to delete the backup of the original DB {}",
          dbBackup, e);
    }

    if (lastAppliedIndex != checkpointTrxnInfo.getTransactionIndex()) {
      // Installation failed, old state reloaded
      return null;
    }

    // TODO: Return only snapshotIndex to leader (RATIS-586)
    TermIndex newTermIndex = TermIndex.valueOf(term, lastAppliedIndex);
    LOG.info("Install Checkpoint is finished with Term: {} and Index: {}. " +
        "Spend {} ms.", newTermIndex.getTerm(), newTermIndex.getIndex(),
        (Time.monotonicNow() - startTime));
    return newTermIndex;
  }

  /**
   * Replaces OM DB with checkpoint data from leader.
   * Only backs up/replaces items present in checkpointLocation.
   *
   * @param ctx OzoneManager context
   * @param lastAppliedIndex last applied transaction index
   * @param oldDB current DB directory
   * @param checkpointLocation checkpoint data directory
   * @return backup directory with original state
   * @throws IOException if file operations fail
   */
  private File replaceOMDBWithCheckpoint(Context ctx, long lastAppliedIndex,
      File oldDB, Path checkpointLocation) throws IOException {

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
          String ratisLogDirName = ctx.getRatisLogDirName();
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

    moveCheckpointFiles(ctx, oldDB, checkpointLocation, dbDir, dbBackupDir,
        backedUpItems);

    return dbBackupDir;
  }

  /**
   * Moves checkpoint files from checkpointLocation to dbDir.
   * Uses backup for rollback on failure.
   *
   * @param ctx OzoneManager context
   * @param oldDB old DB directory
   * @param checkpointLocation source checkpoint directory
   * @param dbDir target directory (parent of oldDB)
   * @param dbBackupDir backup directory
   * @param backedUpItems set of backed up item names for selective restore
   * @throws IOException if file operations fail
   */
  private void moveCheckpointFiles(Context ctx, File oldDB,
      Path checkpointLocation, File dbDir, File dbBackupDir,
      Set<String> backedUpItems) throws IOException {
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
          String ratisLogDirName = ctx.getRatisLogDirName();
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
      // Rollback: restore backed up items
      try {
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
      } catch (IOException ex) {
        try {
          ctx.exitSystem(1, "Failed to restore from backup. OM is in an " +
              "inconsistent state.", ex, LOG);
        } catch (IOException ioEx) {
          LOG.error("Unexpected exception from exitSystem", ioEx);
        }
      }
      throw e;
    }
  }
}

