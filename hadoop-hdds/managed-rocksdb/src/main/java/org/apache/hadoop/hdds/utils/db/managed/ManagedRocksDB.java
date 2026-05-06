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

package org.apache.hadoop.hdds.utils.db.managed;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Managed {@link RocksDB}.
 */
public class ManagedRocksDB extends ManagedObject<RocksDB> {
  public static final Class<RocksDB> ORIGINAL_CLASS = RocksDB.class;
  public static final int NOT_FOUND = RocksDB.NOT_FOUND;

  static final Logger LOG = LoggerFactory.getLogger(ManagedRocksDB.class);

  ManagedRocksDB(RocksDB original) {
    super(original);
  }

  public static ManagedRocksDB openReadOnly(
      final ManagedOptions options,
      final String path)
      throws RocksDBException {
    return new ManagedRocksDB(RocksDB.openReadOnly(options, path));
  }

  public static ManagedRocksDB openReadOnly(
      final ManagedDBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.openReadOnly(options, path,
            columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  public static ManagedRocksDB openReadOnly(
      final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.openReadOnly(path, columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  /**
   * Opens a RocksDB at {@code dbPath} as a <b>secondary</b> instance.
   * It is safe to use a secondary instance while a primary writer
   * is active on the same DB.
   *
   * <p>Secondary mode is RocksDB's supported way to attach an extra reader
   * to a DB that has a live primary writer. If a DB is simultaneously opened
   * by with the primary writer and as a read-only instance,
   * it has <i>undefined</i> behavior. It often succeeds if the read-only instance
   * closes quickly, but the contract is unsafe.
   *
   * <p><b>Catch-up semantics.</b> A secondary's view does not auto-refresh; it
   * stays at the snapshot captured at open time. The only way to advance it
   * is to call {@code tryCatchUpWithPrimary()}, a user-triggered operation
   * that rebuilds the in-memory memtable from new MANIFEST / WAL entries and
   * never writes anything to disk.
   *
   * <p><b>The secondary log directory.</b> Secondary mode requires its own
   * directory at {@code secondaryDbLogFilePath} for the RocksDB info
   * {@code LOG} file. That directory is used <i>only</i> for log files. No
   * important data lives there. The previous {@code LOG} file is rotated to
   * {@code LOG.old.<ts>} on each subsequent open, so callers that reopen the
   * secondary repeatedly should periodically clean these up. Note that the
   * open will <b>fail</b> if the {@code LOG} cannot be created or written
   * (directory missing, not writable, or out of space).
   *
   * @param options                DB options for the secondary instance.
   * @param dbPath                 path to the primary DB.
   * @param secondaryDbLogFilePath directory for the secondary's info log
   *                               files; must be writable and on a
   *                               filesystem with at least a small amount
   *                               of free space.
   * @return an open secondary {@link ManagedRocksDB}.
   * @throws RocksDBException if the underlying native open fails for any
   *                          reason, including an unwritable / full
   *                          {@code secondaryDbLogFilePath}.
   */
  public static ManagedRocksDB openAsSecondary(
      final ManagedOptions options,
      final String dbPath,
      final String secondaryDbLogFilePath)
      throws RocksDBException {
    return new ManagedRocksDB(RocksDB.openAsSecondary(options, dbPath, secondaryDbLogFilePath));
  }

  /**
   * True iff the throwable (or any cause in its chain) is a
   * {@link RocksDBException} whose status is {@code IOError(NoSpace)}.
   * RocksDB sets that subcode specifically when the underlying syscall
   * returns {@code ENOSPC}, so this is a precise signal that the failed
   * operation hit a full disk — distinct from {@code IOError} causes such
   * as permission denied, missing path, or DB corruption.
   *
   * <p>Callers wanting to consult the {@link Status} on a
   * {@link RocksDBException} from outside this module would otherwise have
   * to import {@code org.rocksdb.Status} directly, which is restricted by
   * the project's {@code banned-rocksdb-imports} enforcer rule. Use this
   * helper instead.
   *
   * @param t the throwable to inspect; the entire cause chain is walked.
   * @return {@code true} iff a {@code RocksDBException} with status
   * {@code IOError(NoSpace)} is found.
   */
  public static boolean isNoSpaceFailure(Throwable t) {
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur instanceof RocksDBException) {
        Status status = ((RocksDBException) cur).getStatus();
        if (status != null
            && status.getCode() == Status.Code.IOError
            && status.getSubCode() == Status.SubCode.NoSpace) {
          return true;
        }
      }
    }
    return false;
  }

  public static ManagedRocksDB open(
      final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.open(options, path,
            columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  /**
   * Open a RocksDB option with the latest options. Other than {@link ManagedConfigOptions} and
   * path, the other options and handles should be empty since it will be populated with
   * loadLatestOptions. Nevertheless, the caller is still responsible in cleaning up / closing the resources.
   * @param configOptions Config options.
   * @param options DBOptions. This would be modified based on the latest options.
   * @param path DB path.
   * @param columnFamilyDescriptors Column family descriptors. These would be modified based on the latest options.
   * @param columnFamilyHandles Column family handles of the underlying column family
   * @return A RocksDB instance.
   * @throws RocksDBException thrown if error happens in underlying native library.
   */
  public static ManagedRocksDB openWithLatestOptions(
      final ManagedConfigOptions configOptions,
      final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    // Preserve all the previous DB options
    OptionsUtil.loadLatestOptions(configOptions, path, options, columnFamilyDescriptors);
    return new ManagedRocksDB(
        RocksDB.open(options, path, columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  /**
   * Delete liveMetaDataFile from rocks db using RocksDB#deleteFile Api.
   * This function makes the RocksDB#deleteFile Api synchronized by waiting
   * for the deletes to happen.
   * @param fileToBeDeleted File to be deleted.
   * @throws RocksDatabaseException if the underlying db throws an exception
   *                                or the file is not deleted within a time limit.
   */
  public void deleteFile(LiveFileMetaData fileToBeDeleted) throws RocksDatabaseException {
    String sstFileName = fileToBeDeleted.fileName();
    File file = new File(fileToBeDeleted.path(), fileToBeDeleted.fileName());
    try {
      get().deleteFile(sstFileName);
    } catch (RocksDBException e) {
      throw new RocksDatabaseException("Failed to delete " + file, e);
    }
    ManagedRocksObjectUtils.waitForFileDelete(file, Duration.ofSeconds(60));
  }

  public static Map<String, LiveFileMetaData> getLiveMetadataForSSTFiles(RocksDB db) {
    return db.getLiveFilesMetaData().stream().collect(
            Collectors.toMap(liveFileMetaData -> FilenameUtils.getBaseName(liveFileMetaData.fileName()),
                liveFileMetaData -> liveFileMetaData));
  }

  public Map<String, LiveFileMetaData> getLiveMetadataForSSTFiles() {
    return getLiveMetadataForSSTFiles(this.get());
  }
}
