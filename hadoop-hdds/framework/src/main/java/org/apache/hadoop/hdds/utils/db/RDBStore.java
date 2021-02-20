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

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.RocksDBStoreMBean;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.metrics2.util.MBeans;

import com.google.common.base.Preconditions;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.toIOException;

/**
 * RocksDB Store that supports creating Tables in DB.
 */
public class RDBStore implements DBStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStore.class);
  private RocksDB db;
  private File dbLocation;
  private final WriteOptions writeOptions;
  private final DBOptions dbOptions;
  private final CodecRegistry codecRegistry;
  private final Map<String, ColumnFamilyHandle> handleTable;
  private ObjectName statMBeanName;
  private RDBCheckpointManager checkPointManager;
  private String checkpointsParentDir;
  private List<ColumnFamilyHandle> columnFamilyHandles;
  private RDBMetrics rdbMetrics;

  @VisibleForTesting
  public RDBStore(File dbFile, DBOptions options,
                  Set<TableConfig> families) throws IOException {
    this(dbFile, options, new WriteOptions(), families, new CodecRegistry(),
        false);
  }

  public RDBStore(File dbFile, DBOptions options,
      WriteOptions writeOptions, Set<TableConfig> families,
                  CodecRegistry registry, boolean readOnly)
      throws IOException {
    Preconditions.checkNotNull(dbFile, "DB file location cannot be null");
    Preconditions.checkNotNull(families);
    Preconditions.checkArgument(!families.isEmpty());
    handleTable = new HashMap<>();
    codecRegistry = registry;
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        new ArrayList<>();
    columnFamilyHandles = new ArrayList<>();

    for (TableConfig family : families) {
      columnFamilyDescriptors.add(family.getDescriptor());
    }

    dbOptions = options;
    dbLocation = dbFile;
    this.writeOptions = writeOptions;

    try {
      // This logic has been added to support old column families that have
      // been removed, or those that may have been created in a future version.
      // TODO : Revisit this logic during upgrade implementation.
      List<TableConfig> columnFamiliesInDb = getColumnFamiliesInExistingDb();
      List<TableConfig> extraCf = columnFamiliesInDb.stream().filter(
          cf -> !families.contains(cf)).collect(Collectors.toList());
      if (!extraCf.isEmpty()) {
        LOG.info("Found the following extra column families in existing DB : " +
                "{}", extraCf);
        extraCf.forEach(cf -> columnFamilyDescriptors.add(cf.getDescriptor()));
      }

      if (readOnly) {
        db = RocksDB.openReadOnly(dbOptions, dbLocation.getAbsolutePath(),
            columnFamilyDescriptors, columnFamilyHandles);
      } else {
        db = RocksDB.open(dbOptions, dbLocation.getAbsolutePath(),
            columnFamilyDescriptors, columnFamilyHandles);
      }

      for (int x = 0; x < columnFamilyHandles.size(); x++) {
        handleTable.put(
            StringUtils.bytes2String(columnFamilyHandles.get(x).getName()),
            columnFamilyHandles.get(x));
      }

      if (dbOptions.statistics() != null) {
        Map<String, String> jmxProperties = new HashMap<>();
        jmxProperties.put("dbName", dbFile.getName());
        statMBeanName = HddsUtils.registerWithJmxProperties(
            "Ozone", "RocksDbStore", jmxProperties,
            RocksDBStoreMBean.create(dbOptions.statistics(),
                dbFile.getName()));
        if (statMBeanName == null) {
          LOG.warn("jmx registration failed during RocksDB init, db path :{}",
              dbFile.getAbsolutePath());
        }
      }

      //create checkpoints directory if not exists.
      checkpointsParentDir =
              Paths.get(dbLocation.getParent(), "db.checkpoints").toString();
      File checkpointsDir = new File(checkpointsParentDir);
      if (!checkpointsDir.exists()) {
        boolean success = checkpointsDir.mkdir();
        if (!success) {
          LOG.warn("Unable to create RocksDB checkpoint directory");
        }
      }

      //Initialize checkpoint manager
      checkPointManager = new RDBCheckpointManager(db, dbLocation.getName());
      rdbMetrics = RDBMetrics.create();

    } catch (RocksDBException e) {
      String msg = "Failed init RocksDB, db path : " + dbFile.getAbsolutePath()
          + ", " + "exception :" + (e.getCause() == null ?
          e.getClass().getCanonicalName() + " " + e.getMessage() :
          e.getCause().getClass().getCanonicalName() + " " +
              e.getCause().getMessage());

      throw toIOException(msg, e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("RocksDB successfully opened.");
      LOG.debug("[Option] dbLocation= {}", dbLocation.getAbsolutePath());
      LOG.debug("[Option] createIfMissing = {}", options.createIfMissing());
      LOG.debug("[Option] maxOpenFiles= {}", options.maxOpenFiles());
    }
  }

  /**
   * Read DB and return existing column families.
   * @return List of column families
   * @throws RocksDBException on Error.
   */
  private List<TableConfig> getColumnFamiliesInExistingDb()
      throws RocksDBException {
    List<byte[]> bytes = RocksDB.listColumnFamilies(new Options(),
        dbLocation.getAbsolutePath());
    List<TableConfig> columnFamiliesInDb = bytes.stream()
        .map(cfbytes -> new TableConfig(StringUtils.bytes2String(cfbytes),
            DBStoreBuilder.HDDS_DEFAULT_DB_PROFILE.getColumnFamilyOptions()))
        .collect(Collectors.toList());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found column Families in DB : {}",
          columnFamiliesInDb);
    }
    return columnFamiliesInDb;
  }

  @Override
  public void compactDB() throws IOException {
    if (db != null) {
      try {
        db.compactRange();
      } catch (RocksDBException e) {
        throw toIOException("Failed to compact db", e);
      }
    }
  }

  @Override
  public void close() throws IOException {

    for (final ColumnFamilyHandle handle : handleTable.values()) {
      handle.close();
    }

    if (statMBeanName != null) {
      MBeans.unregister(statMBeanName);
      statMBeanName = null;
    }

    RDBMetrics.unRegister();
    if (db != null) {
      db.close();
    }

    if (dbOptions != null) {
      dbOptions.close();
    }

    if (writeOptions != null) {
      writeOptions.close();
    }
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
    try {
      return db.getLongProperty("rocksdb.estimate-num-keys");
    } catch (RocksDBException e) {
      throw toIOException("Unable to get the estimated count.", e);
    }
  }

  @Override
  public BatchOperation initBatchOperation() {
    return new RDBBatchOperation();
  }

  @Override
  public void commitBatchOperation(BatchOperation operation)
      throws IOException {
    ((RDBBatchOperation) operation).commit(db, writeOptions);
  }


  @VisibleForTesting
  protected ObjectName getStatMBeanName() {
    return statMBeanName;
  }

  @Override
  public Table<byte[], byte[]> getTable(String name) throws IOException {
    ColumnFamilyHandle handle = handleTable.get(name);
    if (handle == null) {
      throw new IOException("No such table in this DB. TableName : " + name);
    }
    return new RDBTable(this.db, handle, this.writeOptions, rdbMetrics);
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
    for (ColumnFamilyHandle handle : handleTable.values()) {
      returnList.add(new RDBTable(db, handle, writeOptions, rdbMetrics));
    }
    return returnList;
  }

  @Override
  public void flushDB() throws IOException {
    try (FlushOptions flushOptions = new FlushOptions()) {
      flushOptions.setWaitForFlush(true);
      db.flush(flushOptions);
    } catch (RocksDBException e) {
      throw toIOException("Unable to Flush RocksDB data", e);
    }
  }

  @Override
  public void flushLog(boolean sync) throws IOException {
    if (db != null) {
      try {
        // for RocksDB it is sufficient to flush the WAL as entire db can
        // be reconstructed using it.
        db.flushWal(sync);
      } catch (RocksDBException e) {
        throw toIOException("Failed to flush db", e);
      }
    }
  }

  @Override
  public DBCheckpoint getCheckpoint(boolean flush) throws IOException {
    if (flush) {
      this.flushDB();
    }
    return checkPointManager.createCheckpoint(checkpointsParentDir);
  }

  @Override
  public File getDbLocation() {
    return dbLocation;
  }

  @Override
  public Map<Integer, String> getTableNames() {
    Map<Integer, String> tableNames = new HashMap<>();
    StringCodec stringCodec = new StringCodec();

    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
      try {
        tableNames.put(columnFamilyHandle.getID(), stringCodec
            .fromPersistedFormat(columnFamilyHandle.getName()));
      } catch (RocksDBException | IOException e) {
        LOG.error("Unexpected exception while reading column family handle " +
            "name", e);
      }
    }
    return tableNames;
  }

  @Override
  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  @Override
  public DBUpdatesWrapper getUpdatesSince(long sequenceNumber)
      throws SequenceNumberNotFoundException {

    DBUpdatesWrapper dbUpdatesWrapper = new DBUpdatesWrapper();
    try {
      TransactionLogIterator transactionLogIterator =
          db.getUpdatesSince(sequenceNumber);

      // Only the first record needs to be checked if its seq number <
      // ( 1 + passed_in_sequence_number). For example, if seqNumber passed
      // in is 100, then we can read from the WAL ONLY if the first sequence
      // number is <= 101. If it is 102, then 101 may already be flushed to
      // SST. If it 99, we can skip 99 and 100, and then read from 101.

      boolean checkValidStartingSeqNumber = true;

      while (transactionLogIterator.isValid()) {
        TransactionLogIterator.BatchResult result =
            transactionLogIterator.getBatch();
        long currSequenceNumber = result.sequenceNumber();
        if (checkValidStartingSeqNumber &&
            currSequenceNumber > 1 + sequenceNumber) {
          throw new SequenceNumberNotFoundException("Unable to read data from" +
              " RocksDB wal to get delta updates. It may have already been" +
              "flushed to SSTs.");
        }
        // If the above condition was not satisfied, then it is OK to reset
        // the flag.
        checkValidStartingSeqNumber = false;
        if (currSequenceNumber <= sequenceNumber) {
          transactionLogIterator.next();
          continue;
        }
        dbUpdatesWrapper.addWriteBatch(result.writeBatch().data(),
            result.sequenceNumber());
        transactionLogIterator.next();
      }
    } catch (RocksDBException e) {
      LOG.error("Unable to get delta updates since sequenceNumber {} ",
          sequenceNumber, e);
    }
    return dbUpdatesWrapper;
  }

  @VisibleForTesting
  public RocksDB getDb() {
    return db;
  }

  public RDBMetrics getMetrics() {
    return rdbMetrics;
  }
}
