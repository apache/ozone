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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.utils.db.cache.TableCacheImpl;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LevelDB Store implementation of {@link DBStore} that uses a single default table to be
 * compatible with
 * stores designed to use RocksDB tables. All operations on the default table actually happen to
 * the one tableless database underneath.
 */
public class LDBStore implements DBStore {
  private static final Logger LOG =
          LoggerFactory.getLogger(LDBStore.class);
  private DB db;
  private File dbLocation;
  private final WriteOptions writeOptions;
  private final Options dbOptions;
  private final CodecRegistry codecRegistry;
//  private RDBCheckpointManager checkPointManager;
  private String checkpointsParentDir;

  @VisibleForTesting
  public LDBStore(File dbFile, Options options) throws IOException {
    this(dbFile, options, new WriteOptions(), new CodecRegistry());
  }

  public LDBStore(File dbFile, Options options,
                  WriteOptions writeOptions, CodecRegistry registry)
          throws IOException {
    Preconditions.checkNotNull(dbFile, "DB file location cannot be null");
    codecRegistry = registry;

    dbOptions = options;
    dbLocation = dbFile;
    this.writeOptions = writeOptions;

    try {
      db = JniDBFactory.factory.open(dbLocation, options);

      // TODO : Figure out if this can be mapped to leveldb
//      if (dbOptions.statistics() != null) {
//        Map<String, String> jmxProperties = new HashMap<>();
//        jmxProperties.put("dbName", dbFile.getName());
//        statMBeanName = HddsUtils.registerWithJmxProperties(
//                "Ozone", "RocksDbStore", jmxProperties,
//                RocksDBStoreMBean.create(dbOptions.statistics(),
//                        dbFile.getName()));
//        if (statMBeanName == null) {
//          LOG.warn("jmx registration failed during RocksDB init, db path :{}",
//                  dbFile.getAbsolutePath());
//        }
//      }

      //create checkpoints directory if not exists.
      checkpointsParentDir =
              Paths.get(dbLocation.getParent(), "db.checkpoints").toString();
      File checkpointsDir = new File(checkpointsParentDir);
      if (!checkpointsDir.exists()) {
        boolean success = checkpointsDir.mkdir();
        if (!success) {
          LOG.warn("Unable to create LevelDB checkpoint directory");
        }
      }

      //Initialize checkpoint manager
      checkPointManager = new RDBCheckpointManager(db, "rdb");
    } catch (DBException e) {
      String msg = "Failed init LevelDB, db path : " + dbFile.getAbsolutePath()
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

  public static IOException toIOException(String msg, DBException e) {
    String errMessage = e.getMessage() == null ? "Unknown error" :
            e.getMessage();
    String output = msg + "; message : " + errMessage;
    return new IOException(output, e);
  }

  @Override
  public void compactDB() throws IOException {
    if (db != null) {
      try {
        // From LevelDB docs : begin == null and end == null means the whole DB.
        db.compactRange(null, null);
      } catch (DBException e) {
        throw toIOException("Failed to compact db", e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (db != null) {
      db.close();
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
    throw new UnsupportedOperationException("LevelDB does not support estimating key count.");
  }

  @Override
  public BatchOperation initBatchOperation() {
    return new LDBBatchOperation(db);
  }

  @Override
  public void commitBatchOperation(BatchOperation operation)
          throws IOException {
    ((LDBBatchOperation) operation).commit(db, writeOptions);
  }

  /**
   * @param name - Unused in this implementation, since there is only one dummy table. Can be null.
   * @return A generic table that can be used to write directly to this LevelDB instance as if
   * the write were happening via a table.
   * @throws IOException
   */
  @Override
  public Table<byte[], byte[]> getTable(String name) {
    return new LDBTable(this.db, this.writeOptions);
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
                                     TableCacheImpl.CacheCleanupPolicy cleanupPolicy) throws IOException {
    return new TypedTable<>(getTable(name), codecRegistry, keyType,
            valueType, cleanupPolicy);
  }

  @Override
  public ArrayList<Table> listTables() {
    // TODO : Determine how to implement (or remove this since its only used in testing)
    ArrayList<Table> tables = new ArrayList<>();
    tables.add(getTable(null));
    return tables;
  }

  @Override
  public void flush() throws IOException {
    // TODO : Determine whether throwing exception or doing nothing is better.
    throw new UnsupportedOperationException("Flush not supported for LevelDB");
  }

  @Override
  public DBCheckpoint getCheckpoint(boolean flush) throws IOException {
    // TODO : Determine how to implement (Seems to only be used for OM)
    //  LevelDB does not have checkpoints. It has snapshots, but those appear to be different.
//    if (flush) {
//      this.flush();
//    }
//    return checkPointManager.createCheckpoint(checkpointsParentDir);
    throw new UnsupportedOperationException("Flush not supported for LevelDB");
  }

  @Override
  public File getDbLocation() {
    return dbLocation;
  }

  /**
   * @return An empty map, since LevelDB does not use tables/column families.
   */
  @Override
  public Map<Integer, String> getTableNames() {
    return new HashMap<>();
  }

  @Override
  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  @Override
  public DBUpdatesWrapper getUpdatesSince(long sequenceNumber) {
    throw new UnsupportedOperationException("Sequence numbers not supported for LevelDB");
  }

  @VisibleForTesting
  public DB getDb() {
    return db;
  }
}
