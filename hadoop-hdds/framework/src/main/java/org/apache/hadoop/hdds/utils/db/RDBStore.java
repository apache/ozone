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

package org.apache.hadoop.hdds.utils.db;

import static org.apache.hadoop.ozone.OzoneConsts.COMPACTION_LOG_TABLE;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_LOG_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_SST_BACKUP_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.RocksDBStoreMetrics;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedStatistics;
import org.apache.hadoop.hdds.utils.db.managed.ManagedTransactionLogIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.RocksDBCheckpointDifferHolder;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionLogIterator.BatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB Store that supports creating Tables in DB.
 */
public class RDBStore implements DBStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStore.class);
  private final RocksDatabase db;
  private final File dbLocation;
  private RocksDBStoreMetrics metrics;
  private final RDBCheckpointManager checkPointManager;
  private final String checkpointsParentDir;
  private final String snapshotsParentDir;
  private final RDBMetrics rdbMetrics;
  private final RocksDBCheckpointDiffer rocksDBCheckpointDiffer;

  // this is to track the total size of dbUpdates data since sequence
  // number in request to avoid increase in heap memory.
  private final long maxDbUpdatesSizeThreshold;
  private final ManagedDBOptions dbOptions;
  private final ManagedStatistics statistics;
  private final boolean readOnly;

  @SuppressWarnings("parameternumber")
  RDBStore(File dbFile, ManagedDBOptions dbOptions, ManagedStatistics statistics,
                  ManagedWriteOptions writeOptions, Set<TableConfig> families,
                  boolean readOnly,
                  String dbJmxBeanName,
                  boolean enableCompactionDag, Function<Boolean, UncheckedAutoCloseable> differLockSupplier,
                  long maxDbUpdatesSizeThreshold,
                  boolean createCheckpointDirs,
                  ConfigurationSource configuration,
                  boolean enableRocksDBMetrics)
      throws RocksDatabaseException {
    Objects.requireNonNull(dbFile, "DB file location cannot be null");
    Objects.requireNonNull(families, "families == null");
    Preconditions.checkArgument(!families.isEmpty());
    this.maxDbUpdatesSizeThreshold = maxDbUpdatesSizeThreshold;
    dbLocation = dbFile;
    this.dbOptions = dbOptions;
    this.statistics = statistics;

    try {
      if (enableCompactionDag) {
        Objects.requireNonNull(differLockSupplier, "Differ Lock supplier cannot be null when " +
            "compaction dag is enabled");
        rocksDBCheckpointDiffer = RocksDBCheckpointDifferHolder.getInstance(
            getSnapshotMetadataDir(),
            DB_COMPACTION_SST_BACKUP_DIR,
            DB_COMPACTION_LOG_DIR,
            dbLocation.toString(),
            configuration, differLockSupplier);
        rocksDBCheckpointDiffer.setRocksDBForCompactionTracking(dbOptions);
      } else {
        rocksDBCheckpointDiffer = null;
      }

      db = RocksDatabase.open(dbFile, dbOptions, writeOptions,
          families, readOnly);
      this.readOnly = readOnly;

      // dbOptions.statistics() only contribute to part of RocksDB metrics in
      // Ozone. Enable RocksDB metrics even dbOptions.statistics() is off.
      if (dbJmxBeanName == null) {
        dbJmxBeanName = dbFile.getName();
      }
      // Use statistics instead of dbOptions.statistics() to avoid repeated init.
      if (!enableRocksDBMetrics) {
        LOG.debug("Skipped Metrics registration during RocksDB init, " +
            "db path :{}", dbJmxBeanName);
      } else {
        metrics = RocksDBStoreMetrics.create(statistics, db, dbJmxBeanName);
        if (metrics == null) {
          LOG.warn("Metrics registration failed during RocksDB init, " +
              "db path :{}", dbJmxBeanName);
        } else {
          LOG.debug("Metrics registration succeed during RocksDB init, " +
              "db path :{}", dbJmxBeanName);
        }
      }

      // Create checkpoints and snapshot directories if not exists.
      if (!createCheckpointDirs) {
        checkpointsParentDir = null;
        snapshotsParentDir = null;
      } else {
        Path checkpointsParentDirPath =
            Paths.get(dbLocation.getParent(), OM_CHECKPOINT_DIR);
        checkpointsParentDir = checkpointsParentDirPath.toString();
        Files.createDirectories(checkpointsParentDirPath);

        Path snapshotsParentDirPath =
            Paths.get(dbLocation.getParent(), OM_SNAPSHOT_CHECKPOINT_DIR);
        snapshotsParentDir = snapshotsParentDirPath.toString();
        Files.createDirectories(snapshotsParentDirPath);
      }

      if (enableCompactionDag) {
        ColumnFamily ssInfoTableCF = db.getColumnFamily(SNAPSHOT_INFO_TABLE);
        Objects.requireNonNull(ssInfoTableCF,
            "SnapshotInfoTable column family handle should not be null");
        // Set CF handle in differ to be used in DB listener
        rocksDBCheckpointDiffer.setSnapshotInfoTableCFHandle(
            ssInfoTableCF.getHandle());
        // Set CF handle in differ to be store compaction log entry.
        ColumnFamily compactionLogTableCF =
            db.getColumnFamily(COMPACTION_LOG_TABLE);
        Objects.requireNonNull(compactionLogTableCF,
            "CompactionLogTable column family handle should not be null.");
        rocksDBCheckpointDiffer.setCompactionLogTableCFHandle(
            compactionLogTableCF.getHandle());
        // Set activeRocksDB in differ to access compaction log CF.
        rocksDBCheckpointDiffer.setActiveRocksDB(db.getManagedRocksDb());
        // Load all previous compaction logs
        rocksDBCheckpointDiffer.loadAllCompactionLogs();
      }

      //Initialize checkpoint manager
      checkPointManager = new RDBCheckpointManager(db, dbLocation.getName());
      rdbMetrics = RDBMetrics.create();
    } catch (IOException | RuntimeException e) {
      try {
        close();
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw new RocksDatabaseException("Failed to create RDBStore from " + dbFile, e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("RocksDB successfully opened.");
      LOG.debug("[Option] dbLocation= {}", dbLocation.getAbsolutePath());
      LOG.debug("[Option] createIfMissing = {}", dbOptions.createIfMissing());
      LOG.debug("[Option] maxOpenFiles= {}", dbOptions.maxOpenFiles());
    }
  }

  public String getSnapshotMetadataDir() {
    return dbLocation.getParent() + OM_KEY_PREFIX + OM_SNAPSHOT_DIFF_DIR;
  }

  @Override
  public String getSnapshotsParentDir() {
    return snapshotsParentDir;
  }

  @Override
  public RocksDBCheckpointDiffer getRocksDBCheckpointDiffer() {
    return rocksDBCheckpointDiffer;
  }

  /**
   * Returns the RocksDB's DBOptions.
   */
  public ManagedDBOptions getDbOptions() {
    return dbOptions;
  }

  @Override
  public void compactDB() throws RocksDatabaseException {
    try (ManagedCompactRangeOptions options =
             new ManagedCompactRangeOptions()) {
      db.compactDB(options);
    }
  }

  @Override
  public void compactTable(String tableName) throws RocksDatabaseException {
    try (ManagedCompactRangeOptions options = new ManagedCompactRangeOptions()) {
      compactTable(tableName, options);
    }
  }

  @Override
  public void compactTable(String tableName, ManagedCompactRangeOptions options) throws RocksDatabaseException {
    RocksDatabase.ColumnFamily columnFamily = db.getColumnFamily(tableName);
    if (columnFamily == null) {
      throw new RocksDatabaseException("Table not found: " + tableName);
    }
    db.compactRange(columnFamily, null, null, options);
  }

  @Override
  public void close() {
    if (metrics != null) {
      metrics.unregister();
      metrics = null;
    }

    RDBMetrics.unRegister();
    IOUtils.close(LOG, checkPointManager);
    if (rocksDBCheckpointDiffer != null) {
      RocksDBCheckpointDifferHolder
          .invalidateCacheEntry(rocksDBCheckpointDiffer.getMetadataDir());
    }
    if (statistics != null) {
      IOUtils.close(LOG, statistics);
    }
    if (!readOnly) {
      try {
        // Flush to ensure all data is persisted to disk before closing.
        flushDB();
        LOG.debug("Successfully flushed DB before close");
      } catch (Exception e) {
        LOG.warn("Failed to flush DB before close", e);
        // Continue with close even if flush fails
      }
    }
    IOUtils.close(LOG, db);
  }

  @Override
  public long getEstimatedKeyCount() throws RocksDatabaseException {
    return db.estimateNumKeys();
  }

  @Override
  public BatchOperation initBatchOperation() {
    return RDBBatchOperation.newAtomicOperation();
  }

  @Override
  public void commitBatchOperation(BatchOperation operation)
      throws RocksDatabaseException {
    ((RDBBatchOperation) operation).commit(db);
  }

  @Override
  public RDBTable getTable(String name) throws RocksDatabaseException {
    final ColumnFamily handle = db.getColumnFamily(name);
    if (handle == null) {
      throw new RocksDatabaseException("No such table in this DB. TableName : " + name);
    }
    return new RDBTable(this.db, handle, rdbMetrics);
  }

  @Override
  public <K, V> TypedTable<K, V> getTable(
      String name, Codec<K> keyCodec, Codec<V> valueCodec, TableCache.CacheType cacheType)
      throws RocksDatabaseException, CodecException {
    return new TypedTable<>(getTable(name), keyCodec, valueCodec, cacheType);
  }

  @Override
  public List<Table<?, ?>> listTables() {
    final List<Table<?, ?>> returnList = new ArrayList<>();
    for (ColumnFamily family : getColumnFamilies()) {
      returnList.add(new RDBTable(db, family, rdbMetrics));
    }
    return returnList;
  }

  @Override
  public void flushDB() throws RocksDatabaseException {
    db.flush();
  }

  @Override
  public void flushLog(boolean sync) throws RocksDatabaseException {
    // for RocksDB it is sufficient to flush the WAL as entire db can
    // be reconstructed using it.
    db.flushWal(sync);
  }

  @Override
  public DBCheckpoint getCheckpoint(boolean flush) throws RocksDatabaseException {
    if (flush) {
      this.flushDB();
    }
    return checkPointManager.createCheckpoint(checkpointsParentDir);
  }

  @Override
  public DBCheckpoint getCheckpoint(String parentPath, boolean flush) throws RocksDatabaseException {
    if (flush) {
      this.flushDB();
    }
    return checkPointManager.createCheckpoint(parentPath, null);
  }

  public DBCheckpoint getSnapshot(String name) throws RocksDatabaseException {
    this.flushLog(true);
    return checkPointManager.createCheckpoint(snapshotsParentDir, name);
  }

  @Override
  public File getDbLocation() {
    return dbLocation;
  }

  @Override
  public Map<Integer, String> getTableNames() {
    return db.getColumnFamilyNames();
  }

  /**
  /**
   * Drops a table from the database by removing its associated column family.
   * <p>
   * <b>Warning:</b> This operation should be used with extreme caution. If the table needs to be used again,
   * it is recommended to reinitialize the entire DB store, as the column family will be permanently
   * removed from the database. This method is suitable for truncating a RocksDB column family in a single operation.
   *
   * @param tableName the name of the table to be dropped
   * @throws RocksDatabaseException if an error occurs while attempting to drop the table
   */
  @Override
  public void dropTable(String tableName) throws RocksDatabaseException {
    db.dropColumnFamily(tableName);
  }

  public Collection<ColumnFamily> getColumnFamilies() {
    return db.getExtraColumnFamilies();
  }

  @Override
  public DBUpdatesWrapper getUpdatesSince(long sequenceNumber)
      throws SequenceNumberNotFoundException {
    return getUpdatesSince(sequenceNumber, Long.MAX_VALUE);
  }

  @Override
  public DBUpdatesWrapper getUpdatesSince(long sequenceNumber, long limitCount)
      throws SequenceNumberNotFoundException {
    if (limitCount <= 0) {
      throw new IllegalArgumentException("Illegal count for getUpdatesSince.");
    }
    long cumulativeDBUpdateLogBatchSize = 0L;
    DBUpdatesWrapper dbUpdatesWrapper = new DBUpdatesWrapper();
    try (ManagedTransactionLogIterator logIterator =
        db.getUpdatesSince(sequenceNumber)) {

      // If Recon's sequence number is out-of-date and the iterator is invalid,
      // throw SNNFE and let Recon fall back to full snapshot.
      // This could happen after OM restart.
      if (db.getLatestSequenceNumber() != sequenceNumber &&
          !logIterator.get().isValid()) {
        throw new SequenceNumberNotFoundException(
            "Invalid transaction log iterator when getting updates since "
                + "sequence number " + sequenceNumber);
      }

      // Only the first record needs to be checked if its seq number <
      // ( 1 + passed_in_sequence_number). For example, if seqNumber passed
      // in is 100, then we can read from the WAL ONLY if the first sequence
      // number is <= 101. If it is 102, then 101 may already be flushed to
      // SST. If it 99, we can skip 99 and 100, and then read from 101.

      boolean checkValidStartingSeqNumber = true;

      while (logIterator.get().isValid()) {
        BatchResult result = logIterator.get().getBatch();
        try {
          long currSequenceNumber = result.sequenceNumber();
          if (checkValidStartingSeqNumber &&
              currSequenceNumber > 1 + sequenceNumber) {
            throw new SequenceNumberNotFoundException("Unable to read full data"
                + " from RocksDB wal to get delta updates. It may have"
                + " partially been flushed to SSTs. Requested sequence number"
                + " is " + sequenceNumber + " and first available sequence" +
                " number is " + currSequenceNumber + " in wal.");
          }
          // If the above condition was not satisfied, then it is OK to reset
          // the flag.
          checkValidStartingSeqNumber = false;
          if (currSequenceNumber <= sequenceNumber) {
            logIterator.get().next();
            continue;
          }
          dbUpdatesWrapper.addWriteBatch(result.writeBatch().data(),
              result.sequenceNumber());
          if (currSequenceNumber - sequenceNumber >= limitCount) {
            break;
          }
          cumulativeDBUpdateLogBatchSize += result.writeBatch().getDataSize();
          if (cumulativeDBUpdateLogBatchSize >= maxDbUpdatesSizeThreshold) {
            break;
          }
        } finally {
          result.writeBatch().close();
        }
        logIterator.get().next();
      }
      dbUpdatesWrapper.setLatestSequenceNumber(db.getLatestSequenceNumber());
    } catch (SequenceNumberNotFoundException e) {
      LOG.warn("Unable to get delta updates since sequenceNumber {}. "
              + "This exception will be thrown to the client",
          sequenceNumber, e);
      dbUpdatesWrapper.setDBUpdateSuccess(false);
      // Throw the exception back to Recon. Expect Recon to fall back to
      // full snapshot.
      throw e;
    } catch (RocksDBException | RocksDatabaseException e) {
      LOG.error("Unable to get delta updates since sequenceNumber {}. "
              + "This exception will not be thrown to the client ",
          sequenceNumber, e);
      dbUpdatesWrapper.setDBUpdateSuccess(false);
    } finally {
      if (!dbUpdatesWrapper.getData().isEmpty()) {
        rdbMetrics.incWalUpdateDataSize(cumulativeDBUpdateLogBatchSize);
        rdbMetrics.incWalUpdateSequenceCount(
            dbUpdatesWrapper.getCurrentSequenceNumber() - sequenceNumber);
      }
    }
    if (!dbUpdatesWrapper.isDBUpdateSuccess()) {
      LOG.warn("Returned DBUpdates isDBUpdateSuccess: {}",
          dbUpdatesWrapper.isDBUpdateSuccess());
    }
    return dbUpdatesWrapper;
  }

  @Override
  public boolean isClosed() {
    return db.isClosed();
  }

  public RocksDatabase getDb() {
    return db;
  }

  public String getProperty(String property) throws RocksDatabaseException {
    return db.getProperty(property);
  }

  public String getProperty(ColumnFamily family, String property)
      throws RocksDatabaseException {
    return db.getProperty(family, property);
  }

  public RDBMetrics getMetrics() {
    return rdbMetrics;
  }
}
