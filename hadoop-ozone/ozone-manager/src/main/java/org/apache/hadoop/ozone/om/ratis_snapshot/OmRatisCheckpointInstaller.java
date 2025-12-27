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
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RATIS_SNAPSHOT_DIR;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for managing OM DB checkpoint backup and replacement on disk.
 * Contains only filesystem/DB related operations – lifecycle handling
 * (stopping services, reloading OM state, restarting RPC, exit, etc.)
 * is expected to be handled by {@link org.apache.hadoop.ozone.om.OzoneManager}.
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
}

