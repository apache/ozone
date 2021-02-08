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
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.StringUtils;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.rocksdb.RocksIterator;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.toIOException;

/**
 * RocksDB implementation of ozone metadata store. This class should be only
 * used as part of TypedTable as it's underlying implementation to access the
 * metadata store content. All other user's using Table should use TypedTable.
 */
@InterfaceAudience.Private
class RDBTable implements Table<byte[], byte[]> {


  private static final Logger LOG =
      LoggerFactory.getLogger(RDBTable.class);

  private final RocksDB db;
  private final ColumnFamilyHandle handle;
  private final WriteOptions writeOptions;
  private final RDBMetrics rdbMetrics;

  /**
   * Constructs a TableStore.
   *
   * @param db - DBstore that we are using.
   * @param handle - ColumnFamily Handle.
   * @param writeOptions - RocksDB write Options.
   */
  RDBTable(RocksDB db, ColumnFamilyHandle handle,
      WriteOptions writeOptions, RDBMetrics rdbMetrics) {
    this.db = db;
    this.handle = handle;
    this.writeOptions = writeOptions;
    this.rdbMetrics = rdbMetrics;
  }

  /**
   * Returns the Column family Handle.
   *
   * @return ColumnFamilyHandle.
   */
  public ColumnFamilyHandle getHandle() {
    return handle;
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    try {
      db.put(handle, writeOptions, key, value);
    } catch (RocksDBException e) {
      LOG.error("Failed to write to DB. Key: {}", new String(key,
          StandardCharsets.UTF_8));
      throw toIOException("Failed to put key-value to metadata "
          + "store", e);
    }
  }

  @Override
  public void putWithBatch(BatchOperation batch, byte[] key, byte[] value)
      throws IOException {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).put(getHandle(), key, value);
    } else {
      throw new IllegalArgumentException("batch should be RDBBatchOperation");
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
    try {
      // RocksDB#keyMayExist
      // If the key definitely does not exist in the database, then this
      // method returns false, else true.
      rdbMetrics.incNumDBKeyMayExistChecks();
      Holder<byte[]> outValue = new Holder<>();
      boolean keyMayExist = db.keyMayExist(handle, key, outValue);
      if (keyMayExist) {
        boolean keyExists =
            (outValue.getValue() != null && outValue.getValue().length > 0) ||
                (db.get(handle, key) != null);
        if (!keyExists) {
          rdbMetrics.incNumDBKeyMayExistMisses();
        }
        return keyExists;
      }
      return false;
    } catch (RocksDBException e) {
      throw toIOException(
          "Error in accessing DB. ", e);
    }
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    try {
      return db.get(handle, key);
    } catch (RocksDBException e) {
      throw toIOException(
          "Failed to get the value for the given key", e);
    }
  }

  /**
   * Skip checking cache and get the value mapped to the given key in byte
   * array or returns null if the key is not found.
   *
   * @param bytes metadata key
   * @return value in byte array or null if the key is not found.
   * @throws IOException on Failure
   */
  @Override
  public byte[] getSkipCache(byte[] bytes) throws IOException {
    return get(bytes);
  }

  @Override
  public byte[] getIfExist(byte[] key) throws IOException {
    try {
      // RocksDB#keyMayExist
      // If the key definitely does not exist in the database, then this
      // method returns false, else true.
      rdbMetrics.incNumDBKeyGetIfExistChecks();
      boolean keyMayExist = db.keyMayExist(handle, key, null);
      if (keyMayExist) {
        // Not using out value from string builder, as that is causing
        // IllegalArgumentException during protobuf parsing.
        rdbMetrics.incNumDBKeyGetIfExistGets();
        byte[] val;
        val = db.get(handle, key);
        if (val == null) {
          rdbMetrics.incNumDBKeyGetIfExistMisses();
        }
        return val;
      }
      return null;
    } catch (RocksDBException e) {
      throw toIOException("Error in accessing DB. ", e);
    }
  }

  @Override
  public void delete(byte[] key) throws IOException {
    try {
      db.delete(handle, key);
    } catch (RocksDBException e) {
      throw toIOException("Failed to delete the given key", e);
    }
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, byte[] key)
      throws IOException {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).delete(getHandle(), key);
    } else {
      throw new IllegalArgumentException("batch should be RDBBatchOperation");
    }

  }

  @Override
  public TableIterator<byte[], ByteArrayKeyValue> iterator() {
    ReadOptions readOptions = new ReadOptions();
    readOptions.setFillCache(false);
    return new RDBStoreIterator(db.newIterator(handle, readOptions), this);
  }

  @Override
  public String getName() throws IOException {
    try {
      return StringUtils.bytes2String(this.getHandle().getName());
    } catch (RocksDBException rdbEx) {
      throw toIOException("Unable to get the table name.", rdbEx);
    }
  }

  @Override
  public void close() throws Exception {
    // Nothing do for a Column Family.
  }

  @Override
  public long getEstimatedKeyCount() throws IOException {
    try {
      return db.getLongProperty(handle, "rocksdb.estimate-num-keys");
    } catch (RocksDBException e) {
      throw toIOException(
          "Failed to get estimated key count of table " + getName(), e);
    }
  }

  @Override
  public List<ByteArrayKeyValue> getRangeKVs(byte[] startKey,
      int count, MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(startKey, count, false, filters);
  }

  @Override
  public List<ByteArrayKeyValue> getSequentialRangeKVs(byte[] startKey,
      int count, MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(startKey, count, true, filters);
  }

  private List<ByteArrayKeyValue> getRangeKVs(byte[] startKey,
      int count, boolean sequential,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    List<ByteArrayKeyValue> result = new ArrayList<>();
    long start = System.currentTimeMillis();
    if (count < 0) {
      throw new IllegalArgumentException(
            "Invalid count given " + count + ", count must be greater than 0");
    }
    try (RocksIterator it = db.newIterator(handle)) {
      if (startKey == null) {
        it.seekToFirst();
      } else {
        if (get(startKey) == null) {
          // Key not found, return empty list
          return result;
        }
        it.seek(startKey);
      }
      while (it.isValid() && result.size() < count) {
        byte[] currentKey = it.key();
        byte[] currentValue = it.value();

        it.prev();
        final byte[] prevKey = it.isValid() ? it.key() : null;

        it.seek(currentKey);
        it.next();
        final byte[] nextKey = it.isValid() ? it.key() : null;

        if (filters == null) {
          result.add(ByteArrayKeyValue
                  .create(currentKey, currentValue));
        } else {
          if (Arrays.stream(filters)
                  .allMatch(entry -> entry.filterKey(prevKey,
                          currentKey, nextKey))) {
            result.add(ByteArrayKeyValue
                    .create(currentKey, currentValue));
          } else {
            if (result.size() > 0 && sequential) {
              // if the caller asks for a sequential range of results,
              // and we met a dis-match, abort iteration from here.
              // if result is empty, we continue to look for the first match.
              break;
            }
          }
        }
      }
    } finally {
      long end = System.currentTimeMillis();
      long timeConsumed = end - start;
      if (LOG.isDebugEnabled()) {
        if (filters != null) {
          for (MetadataKeyFilters.MetadataKeyFilter filter : filters) {
            int scanned = filter.getKeysScannedNum();
            int hinted = filter.getKeysHintedNum();
            if (scanned > 0 || hinted > 0) {
              LOG.debug(
                  "getRangeKVs ({}) numOfKeysScanned={}, numOfKeysHinted={}",
                  filter.getClass().getSimpleName(), filter.getKeysScannedNum(),
                  filter.getKeysHintedNum());
            }
          }
        }
        LOG.debug("Time consumed for getRangeKVs() is {}ms,"
                + " result length is {}.", timeConsumed, result.size());
      }
    }
    return result;
  }
}
