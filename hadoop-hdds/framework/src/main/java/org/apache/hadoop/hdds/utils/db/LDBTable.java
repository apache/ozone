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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.ReadOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LevelDB implementation of ozone metadata store. This class should be only
 * used as part of TypedTable as it's underlying implementation to access the
 * metadata store content. All other user's using Table should use TypedTable.
 *
 * Although LevelDB does not have tables, this class can be used as if it has one default
 * table, to make LevelDB and RocksDB compatible with the same interfaces.
 */
@InterfaceAudience.Private
class LDBTable implements Table<byte[], byte[]> {

  private static final Logger LOG =
          LoggerFactory.getLogger(LDBTable.class);

  private final DB db;
  private final WriteOptions writeOptions;
  // TODO : Determine if there is an equivalent class for LDB.
  // private final RDBMetrics rdbMetrics;

  /**
   * Constructs a TableStore.
   *
   * @param db - DBstore that we are using.
   * @param writeOptions - RocksDB write Options.
   */
  LDBTable(DB db, WriteOptions writeOptions) {
    this.db = db;
    this.writeOptions = writeOptions;
  }

  /**
   * Converts RocksDB exception to IOE.
   * @param msg  - Message to add to exception.
   * @param e - Original Exception.
   * @return  IOE.
   */
  public static IOException toIOException(String msg, DBException e) {
    String errMessage = e.getMessage() == null ? "Unknown error" :
            e.getMessage();
    String output = msg + "; message : " + errMessage;
    return new IOException(output, e);
 }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    try {
      db.put(key, value, writeOptions);
    } catch (DBException e) {
      LOG.error("Failed to write to DB. Key: {}", new String(key,
              StandardCharsets.UTF_8));
      throw toIOException("Failed to put key-value to metadata "
              + "store", e);
    }
  }

  @Override
  public void putWithBatch(BatchOperation batch, byte[] key, byte[] value)
          throws IOException {
    // TODO : Create LDBBatchOperation class.
    if (batch instanceof LDBBatchOperation) {
      ((LDBBatchOperation) batch).put(key, value);
    } else {
      throw new IllegalArgumentException("batch should be LDBBatchOperation");
    }
  }


  @Override
  public boolean isEmpty() throws IOException {
    try (TableIterator<byte[], ByteArrayKeyValue> keyIter = iterator()) {
      keyIter.seekToFirst();
      return !keyIter.hasNext();
    }
  }

  @Override
  public boolean isExist(byte[] key) throws IOException {
    // LevelDB does not have a "may exist" check like RocksDB to optimize this method.
    try {
      // TODO : Determine if there are metrics for LDB.
      // rdbMetrics.incNumDBKeyMayExistChecks();
      return db.get(key) != null;
      //        if (!keyExists) {
      //          rdbMetrics.incNumDBKeyMayExistMisses();
      //        }
    } catch (DBException e) {
      throw toIOException("Error in accessing DB. ", e);
    }
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    try {
      return db.get(key);
    } catch (DBException e) {
      throw toIOException("Failed to get the value for the given key", e);
    }
  }

  @Override
  public byte[] getIfExist(byte[] key) throws IOException {
    try {
      return db.get(key);
    } catch (DBException e) {
      throw toIOException("Error in accessing DB. ", e);
    }
  }

  @Override
  public void delete(byte[] key) throws IOException {
    try {
      db.delete(key);
    } catch (DBException e) {
      throw toIOException("Failed to delete the given key", e);
    }
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, byte[] key)
          throws IOException {
    if (batch instanceof LDBBatchOperation) {
      ((LDBBatchOperation) batch).delete(key);
    } else {
      throw new IllegalArgumentException("batch should be LDBBatchOperation");
    }

  }

  @Override
  public TableIterator<byte[], ByteArrayKeyValue> iterator() {
    ReadOptions readOptions = new ReadOptions();
    readOptions.fillCache(false);
    return new LDBStoreIterator(db.iterator(readOptions));
  }

  @Override
  public String getName() {
    // Since LevelDB does not have tables, make it appear as though we are using the default
    // table from a RocksDB instance.
    return "default";
  }

  @Override
  public void close() throws Exception {
    // Nothing do for a Column Family.
  }

  @Override
  public long getEstimatedKeyCount() {
    // LevelDB does not provide a way to get an estimated (or exact) number of keys like RocksDB.
    // Only way is to iterate over all the keys.
    throw new UnsupportedOperationException("Cannot estimate key count in LevelDB");
  }
}
