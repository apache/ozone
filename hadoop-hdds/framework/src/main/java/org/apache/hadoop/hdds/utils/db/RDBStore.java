/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.utils.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.RocksDBStoreMetrics;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedTransactionLogIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.rocksdb.RocksDBException;

import org.rocksdb.TransactionLogIterator.BatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_LOG_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_SST_BACKUP_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;

/**
 * RocksDB Store that supports creating Tables in DB.
 */
public class RDBStore implements DBStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStore.class);
  private final RocksDatabase db;
  private final File dbLocation;
  private final CodecRegistry codecRegistry;
  private RocksDBStoreMetrics metrics;
  private final RDBCheckpointManager checkPointManager;
  private final String checkpointsParentDir;
  private final String snapshotsParentDir;
  private final RDBMetrics rdbMetrics;
  private final RocksDBCheckpointDiffer rocksDBCheckpointDiffer;
  private final String dbJmxBeanName;

  // this is to track the total size of dbUpdates data since sequence
  // number in request to avoid increase in heap memory.
  private long maxDbUpdatesSizeThreshold;

  @VisibleForTesting
  public RDBStore(File dbFile, ManagedDBOptions options,
                  Set<TableConfig> families, long maxDbUpdatesSizeThreshold)
      throws IOException {
    this(dbFile, options, new ManagedWriteOptions(), families,
        new CodecRegistry(), false, 1000, null, false,
        TimeUnit.DAYS.toMillis(1), TimeUnit.HOURS.toMillis(1),
        maxDbUpdatesSizeThreshold, true);
  }

  @SuppressWarnings("parameternumber")
  public RDBStore(File dbFile, ManagedDBOptions dbOptions,
                  ManagedWriteOptions writeOptions, Set<TableConfig> families,
                  CodecRegistry registry, boolean readOnly, int maxFSSnapshots,
                  String dbJmxBeanNameName, boolean enableCompactionLog,
                  long maxTimeAllowedForSnapshotInDag,
                  long compactionDagDaemonInterval,
                  long maxDbUpdatesSizeThreshold,
                  boolean createCheckpointDirs)

      throws IOException {
    Preconditions.checkNotNull(dbFile, "DB file location cannot be null");
    Preconditions.checkNotNull(families);
    Preconditions.checkArgument(!families.isEmpty());
    this.maxDbUpdatesSizeThreshold = maxDbUpdatesSizeThreshold;
    codecRegistry = registry;
    dbLocation = dbFile;
    dbJmxBeanName = dbJmxBeanNameName == null ? dbFile.getName() :
        dbJmxBeanNameName;

    try {
      if (enableCompactionLog) {
        rocksDBCheckpointDiffer = new RocksDBCheckpointDiffer(
            dbLocation.getParent() + OM_KEY_PREFIX + OM_SNAPSHOT_DIFF_DIR,
            DB_COMPACTION_SST_BACKUP_DIR, DB_COMPACTION_LOG_DIR,
            dbLocation.toString(),
            maxTimeAllowedForSnapshotInDag, compactionDagDaemonInterval);
        rocksDBCheckpointDiffer.setRocksDBForCompactionTracking(dbOptions);
      } else {
        rocksDBCheckpointDiffer = null;
      }

      db = RocksDatabase.open(dbFile, dbOptions, writeOptions,
          families, readOnly);

      // dbOptions.statistics() only contribute to part of RocksDB metrics in
      // Ozone. Enable RocksDB metrics even dbOptions.statistics() is off.
      metrics = RocksDBStoreMetrics.create(dbOptions.statistics(), db,
          dbJmxBeanName);
      if (metrics == null) {
        LOG.warn("Metrics registration failed during RocksDB init, " +
            "db path :{}", dbJmxBeanName);
      } else {
        LOG.debug("Metrics registration succeed during RocksDB init, " +
            "db path :{}", dbJmxBeanName);
      }

      //create checkpoints directory if not exists.
      if (!createCheckpointDirs) {
        checkpointsParentDir = null;
      } else {
        Path checkpointsParentDirPath =
            Paths.get(dbLocation.getParent(), OM_CHECKPOINT_DIR);
        checkpointsParentDir = checkpointsParentDirPath.toString();
        Files.createDirectories(checkpointsParentDirPath);
      }
      //create snapshot checkpoint directory if does not exist.
      if (!createCheckpointDirs) {
        snapshotsParentDir = null;
      } else {
        Path snapshotsParentDirPath =
            Paths.get(dbLocation.getParent(), OM_SNAPSHOT_CHECKPOINT_DIR);
        snapshotsParentDir = snapshotsParentDirPath.toString();
        Files.createDirectories(snapshotsParentDirPath);
      }

      if (enableCompactionLog) {
        ColumnFamily ssInfoTableCF = db.getColumnFamily(SNAPSHOT_INFO_TABLE);
        Preconditions.checkNotNull(ssInfoTableCF,
            "SnapshotInfoTable column family handle should not be null");
        // Set CF handle in differ to be used in DB listener
        rocksDBCheckpointDiffer.setSnapshotInfoTableCFHandle(
            ssInfoTableCF.getHandle());
        // Finish the initialization of compaction DAG tracker by setting the
        // sequence number as current compaction log filename.
        rocksDBCheckpointDiffer.setCurrentCompactionLog(
            db.getLatestSequenceNumber());
        // Load all previous compaction logs
        rocksDBCheckpointDiffer.loadAllCompactionLogs();
      }

      //Initialize checkpoint manager
      checkPointManager = new RDBCheckpointManager(db, dbLocation.getName());
      rdbMetrics = RDBMetrics.create();

    } catch (IOException | RocksDBException e) {
      String msg = "Failed init RocksDB, db path : " + dbFile.getAbsolutePath()
          + ", " + "exception :" + (e.getCause() == null ?
          e.getClass().getCanonicalName() + " " + e.getMessage() :
          e.getCause().getClass().getCanonicalName() + " " +
              e.getCause().getMessage());

      throw new IOException(msg, e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("RocksDB successfully opened.");
      LOG.debug("[Option] dbLocation= {}", dbLocation.getAbsolutePath());
      LOG.debug("[Option] createIfMissing = {}", dbOptions.createIfMissing());
      LOG.debug("[Option] maxOpenFiles= {}", dbOptions.maxOpenFiles());
    }
  }

  public String getSnapshotsParentDir() {
    return snapshotsParentDir;
  }

  public RocksDBCheckpointDiffer getRocksDBCheckpointDiffer() {
    return rocksDBCheckpointDiffer;
  }

  @Override
  public void compactDB() throws IOException {
    try (ManagedCompactRangeOptions options =
             new ManagedCompactRangeOptions()) {
      db.compactDB(options);
    }
  }

  @Override
  public void close() throws IOException {
    if (metrics != null) {
      metrics.unregister();
      metrics = null;
    }

    RDBMetrics.unRegister();
    checkPointManager.close();
    IOUtils.closeQuietly(rocksDBCheckpointDiffer);
    db.close();
  }

  @Override
  public <K, V> void move(K key, Table<K, V> source,
                                Table<K, V> dest) throws IOException {
    try (BatchOperation batchOperation = initBatchOperation()) {

      V value = source.get(key);
      dest.putWithBatch(batchOperation, key, value);
      source.deleteWithBatch(batchOperation, key);
      commitBatchOperation(batchOperation);
    }
  }

  @Override
  public <K, V> void move(K key, V value, Table<K, V> source,
                                Table<K, V> dest) throws IOException {
    move(key, key, value, source, dest);
  }

  @Override
  public <K, V> void move(K sourceKey, K destKey, V value,
                                Table<K, V> source,
                                Table<K, V> dest) throws IOException {
    try (BatchOperation batchOperation = initBatchOperation()) {
      dest.putWithBatch(batchOperation, destKey, value);
      source.deleteWithBatch(batchOperation, sourceKey);
      commitBatchOperation(batchOperation);
    }
  }

  @Override
  public long getEstimatedKeyCount() throws IOException {
    return db.estimateNumKeys();
  }

  @Override
  public BatchOperation initBatchOperation() {
    return new RDBBatchOperation();
  }

  @Override
  public void commitBatchOperation(BatchOperation operation)
      throws IOException {
    ((RDBBatchOperation) operation).commit(db);
  }

  @Override
  public Table<byte[], byte[]> getTable(String name) throws IOException {
    final ColumnFamily handle = db.getColumnFamily(name);
    if (handle == null) {
      throw new IOException("No such table in this DB. TableName : " + name);
    }
    return new RDBTable(this.db, handle, rdbMetrics);
  }

  @Override
  public <K, V> Table<K, V> getTable(String name,
      Class<K> keyType, Class<V> valueType) throws IOException {
    return new TypedTable<>(getTable(name), codecRegistry, keyType,
        valueType);
  }

  @Override
  public <K, V> Table<K, V> getTable(String name,
      Class<K> keyType, Class<V> valueType,
      TableCache.CacheType cacheType) throws IOException {
    return new TypedTable<>(getTable(name), codecRegistry, keyType,
        valueType, cacheType);
  }

  @Override
  public ArrayList<Table> listTables() {
    ArrayList<Table> returnList = new ArrayList<>();
    for (ColumnFamily family : getColumnFamilies()) {
      returnList.add(new RDBTable(db, family, rdbMetrics));
    }
    return returnList;
  }

  @Override
  public void flushDB() throws IOException {
    db.flush();
  }

  @Override
  public void flushLog(boolean sync) throws IOException {
    // for RocksDB it is sufficient to flush the WAL as entire db can
    // be reconstructed using it.
    db.flushWal(sync);
  }

  @Override
  public DBCheckpoint getCheckpoint(boolean flush) throws IOException {
    if (flush) {
      this.flushDB();
    }
    return checkPointManager.createCheckpoint(checkpointsParentDir);
  }

  public DBCheckpoint getSnapshot(String name) throws IOException {
    this.flushLog(true);
    return checkPointManager.createCheckpoint(snapshotsParentDir, name);
  }

  @Override
  public File getDbLocation() {
    return dbLocation;
  }

  @Override
  public Map<Integer, String> getTableNames() {
    Map<Integer, String> tableNames = new HashMap<>();
    StringCodec stringCodec = new StringCodec();

    for (ColumnFamily columnFamily : getColumnFamilies()) {
      tableNames.put(columnFamily.getID(), columnFamily.getName(stringCodec));
    }
    return tableNames;
  }

  public Collection<ColumnFamily> getColumnFamilies() {
    return db.getExtraColumnFamilies();
  }

  @Override
  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
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
    } catch (SequenceNumberNotFoundException e) {
      LOG.warn("Unable to get delta updates since sequenceNumber {}. "
              + "This exception will be thrown to the client",
          sequenceNumber, e);
      dbUpdatesWrapper.setDBUpdateSuccess(false);
      // Throw the exception back to Recon. Expect Recon to fall back to
      // full snapshot.
      throw e;
    } catch (RocksDBException | IOException e) {
      LOG.error("Unable to get delta updates since sequenceNumber {}. "
              + "This exception will not be thrown to the client ",
          sequenceNumber, e);
      dbUpdatesWrapper.setDBUpdateSuccess(false);
    } finally {
      if (dbUpdatesWrapper.getData().size() > 0) {
        rdbMetrics.incWalUpdateDataSize(cumulativeDBUpdateLogBatchSize);
        rdbMetrics.incWalUpdateSequenceCount(
            dbUpdatesWrapper.getCurrentSequenceNumber() - sequenceNumber);
      }
    }
    dbUpdatesWrapper.setLatestSequenceNumber(db.getLatestSequenceNumber());
    return dbUpdatesWrapper;
  }

  @Override
  public boolean isClosed() {
    return db.isClosed();
  }

  @VisibleForTesting
  public RocksDatabase getDb() {
    return db;
  }

  public String getProperty(String property) throws IOException {
    return db.getProperty(property);
  }

  public String getProperty(ColumnFamily family, String property)
      throws IOException {
    return db.getProperty(family, property);
  }

  public RDBMetrics getMetrics() {
    return rdbMetrics;
  }
}
