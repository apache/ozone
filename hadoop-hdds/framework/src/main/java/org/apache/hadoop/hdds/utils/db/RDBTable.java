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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB implementation of ozone metadata store. This class should be only
 * used as part of TypedTable as it's underlying implementation to access the
 * metadata store content. All other user's using Table should use TypedTable.
 */
@InterfaceAudience.Private
class RDBTable implements Table<byte[], byte[]> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDBTable.class);

  private final RocksDatabase db;
  private final ColumnFamily family;
  private final RDBMetrics rdbMetrics;

  /**
   * Constructs a TableStore.
   *
   * @param db - DBstore that we are using.
   * @param family - ColumnFamily Handle.
   */
  RDBTable(RocksDatabase db, ColumnFamily family,
      RDBMetrics rdbMetrics) {
    this.db = db;
    this.family = family;
    this.rdbMetrics = rdbMetrics;
  }

  public ColumnFamily getColumnFamily() {
    return family;
  }

  void put(ByteBuffer key, ByteBuffer value) throws RocksDatabaseException {
    db.put(family, key, value);
  }

  @Override
  public void put(byte[] key, byte[] value) throws RocksDatabaseException {
    db.put(family, key, value);
  }

  void putWithBatch(BatchOperation batch, CodecBuffer key, CodecBuffer value) {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).put(family, key, value);
    } else {
      throw new IllegalArgumentException("Unexpected batch class: "
          + batch.getClass().getSimpleName());
    }
  }

  @Override
  public void putWithBatch(BatchOperation batch, byte[] key, byte[] value) {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).put(family, key, value);
    } else {
      throw new IllegalArgumentException("batch should be RDBBatchOperation");
    }
  }

  @Override
  public boolean isEmpty() throws RocksDatabaseException {
    try (KeyValueIterator<byte[], byte[]> keyIter = iterator((byte[])null, IteratorType.NEITHER)) {
      keyIter.seekToFirst();
      return !keyIter.hasNext();
    }
  }

  @Override
  public boolean isExist(byte[] key) throws RocksDatabaseException {
    rdbMetrics.incNumDBKeyMayExistChecks();
    final Supplier<byte[]> holder = db.keyMayExist(family, key);
    if (holder == null) {
      return false;  // definitely not exists
    }
    final byte[] value = holder.get();
    if (value != null) {
      return true; // definitely exists
    }

    // inconclusive: the key may or may not exist
    final boolean exists = get(key) != null;
    if (!exists) {
      rdbMetrics.incNumDBKeyMayExistMisses();
    }
    return exists;
  }

  @Override
  public byte[] get(byte[] key) throws RocksDatabaseException {
    rdbMetrics.incNumDBKeyGets();
    return db.get(family, key);
  }

  Integer get(ByteBuffer key, ByteBuffer outValue) throws RocksDatabaseException {
    return db.get(family, key, outValue);
  }

  /**
   * Skip checking cache and get the value mapped to the given key in byte
   * array or returns null if the key is not found.
   *
   * @param bytes metadata key
   * @return value in byte array or null if the key is not found.
   */
  @Override
  public byte[] getSkipCache(byte[] bytes) throws RocksDatabaseException {
    return get(bytes);
  }

  @Override
  public byte[] getIfExist(byte[] key) throws RocksDatabaseException {
    rdbMetrics.incNumDBKeyGetIfExistChecks();
    final Supplier<byte[]> value = db.keyMayExist(family, key);
    if (value == null) {
      return null; // definitely not exists
    }
    if (value.get() != null) {
      return value.get(); // definitely exists
    }

    // inconclusive: the key may or may not exist
    rdbMetrics.incNumDBKeyGetIfExistGets();
    final byte[] val = get(key);
    if (val == null) {
      rdbMetrics.incNumDBKeyGetIfExistMisses();
    }
    return val;
  }

  Integer getIfExist(ByteBuffer key, ByteBuffer outValue) throws RocksDatabaseException {
    rdbMetrics.incNumDBKeyGetIfExistChecks();
    final Supplier<Integer> value = db.keyMayExist(
        family, key, outValue.duplicate());
    if (value == null) {
      return null; // definitely not exists
    }
    if (value.get() != null) {
      // definitely exists, return value size.
      return value.get();
    }

    // inconclusive: the key may or may not exist
    rdbMetrics.incNumDBKeyGetIfExistGets();
    final Integer val = get(key, outValue);
    if (val == null) {
      rdbMetrics.incNumDBKeyGetIfExistMisses();
    }
    return val;
  }

  @Override
  public void delete(byte[] key) throws RocksDatabaseException {
    db.delete(family, key);
  }

  public void delete(ByteBuffer key) throws RocksDatabaseException {
    db.delete(family, key);
  }

  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey) throws RocksDatabaseException {
    db.deleteRange(family, beginKey, endKey);
  }

  void deleteWithBatch(BatchOperation batch, CodecBuffer key) {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).delete(family, key);
    } else {
      throw new IllegalArgumentException("Unexpected batch class: " + batch.getClass().getSimpleName());
    }
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, byte[] key) {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).delete(family, key);
    } else {
      throw new IllegalArgumentException("batch should be RDBBatchOperation");
    }

  }

  @Override
  public KeyValueIterator<byte[], byte[]> iterator(byte[] prefix, IteratorType type)
      throws RocksDatabaseException {
    return new RDBStoreByteArrayIterator(readOptions -> db.newIterator(family, readOptions),
        this, prefix, type);
  }

  KeyValueIterator<CodecBuffer, CodecBuffer> newCodecBufferIterator(
      byte[] prefix, IteratorType type) throws RocksDatabaseException {
    return new RDBStoreCodecBufferIterator(readOptions -> db.newIterator(family, readOptions),
        this, prefix, type);
  }

  @Override
  public String getName() {
    return family.getName();
  }

  @Override
  public long getEstimatedKeyCount() throws RocksDatabaseException {
    return db.estimateNumKeys(family);
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, byte[] prefix)
      throws RocksDatabaseException, CodecException {
    try (KeyValueIterator<byte[], byte[]> iter = iterator(prefix)) {
      while (iter.hasNext()) {
        deleteWithBatch(batch, iter.next().getKey());
      }
    }
  }

  @Override
  public void dumpToFileWithPrefix(File externalFile, byte[] prefix) throws RocksDatabaseException {
    try (KeyValueIterator<CodecBuffer, CodecBuffer> iter = newCodecBufferIterator(prefix, IteratorType.KEY_AND_VALUE);
         RDBSstFileWriter fileWriter = new RDBSstFileWriter(externalFile)) {
      while (iter.hasNext()) {
        final KeyValue<CodecBuffer, CodecBuffer> entry = iter.next();
        fileWriter.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public void loadFromFile(File externalFile) throws RocksDatabaseException {
    RDBSstFileLoader.load(db, family, externalFile);
  }

  @Override
  public List<KeyValue<byte[], byte[]>> getRangeKVs(
      byte[] startKey, int count, byte[] prefix, KeyPrefixFilter filter, boolean sequential)
      throws RocksDatabaseException, CodecException {
    long start = Time.monotonicNow();

    if (count < 0) {
      throw new IllegalArgumentException(
            "Invalid count given " + count + ", count must be greater than 0");
    }
    final List<KeyValue<byte[], byte[]>> result = new ArrayList<>();
    try (KeyValueIterator<byte[], byte[]> it = iterator(prefix)) {
      if (startKey == null) {
        it.seekToFirst();
      } else {
        if ((prefix == null || startKey.length > prefix.length)
            && get(startKey) == null) {
          // Key not found, return empty list
          return result;
        }
        it.seek(startKey);
      }

      while (it.hasNext() && result.size() < count) {
        final KeyValue<byte[], byte[]> currentEntry = it.next();
        if (filter == null || filter.filterKey(currentEntry.getKey())) {
          result.add(currentEntry);
        } else if (!result.isEmpty() && sequential) {
          // if the caller asks for a sequential range of results,
          // and we met a dis-match, abort iteration from here.
          // if result is empty, we continue to look for the first match.
          break;
        }
      }
    } finally {
      long end = Time.monotonicNow();
      long timeConsumed = end - start;
      if (LOG.isDebugEnabled()) {
        if (filter != null) {
          final int scanned = filter.getKeysScannedNum();
          final int hinted = filter.getKeysHintedNum();
          if (scanned > 0 || hinted > 0) {
            LOG.debug("getRangeKVs ({}) numOfKeysScanned={}, numOfKeysHinted={}",
                filter.getClass().getSimpleName(), scanned, hinted);
          }
        }
        LOG.debug("Time consumed for getRangeKVs() is {}ms, result length is {}.", timeConsumed, result.size());
      }
    }
    return result;
  }
}
