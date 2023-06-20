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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
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

  void put(ByteBuffer key, ByteBuffer value) throws IOException {
    db.put(family, key, value);
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    db.put(family, key, value);
  }

  void putWithBatch(BatchOperation batch, CodecBuffer key, CodecBuffer value)
      throws IOException {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).put(family, key, value);
    } else {
      throw new IllegalArgumentException("Unexpected batch class: "
          + batch.getClass().getSimpleName());
    }
  }

  @Override
  public void putWithBatch(BatchOperation batch, byte[] key, byte[] value)
      throws IOException {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).put(family, key, value);
    } else {
      throw new IllegalArgumentException("batch should be RDBBatchOperation");
    }
  }


  @Override
  public boolean isEmpty() throws IOException {
    try (TableIterator<byte[], KeyValue<byte[], byte[]>> keyIter = iterator()) {
      keyIter.seekToFirst();
      return !keyIter.hasNext();
    }
  }

  @Override
  public boolean isExist(byte[] key) throws IOException {
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
  public byte[] get(byte[] key) throws IOException {
    rdbMetrics.incNumDBKeyGets();
    return db.get(family, key);
  }

  Integer get(ByteBuffer key, ByteBuffer outValue) throws IOException {
    return db.get(family, key, outValue);
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

  Integer getIfExist(ByteBuffer key, ByteBuffer outValue) throws IOException {
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
  public void delete(byte[] key) throws IOException {
    db.delete(family, key);
  }

  public void delete(ByteBuffer key) throws IOException {
    db.delete(family, key);
  }


  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey) throws IOException {
    db.deleteRange(family, beginKey, endKey);
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, byte[] key)
      throws IOException {
    if (batch instanceof RDBBatchOperation) {
      ((RDBBatchOperation) batch).delete(family, key);
    } else {
      throw new IllegalArgumentException("batch should be RDBBatchOperation");
    }

  }

  @Override
  public TableIterator<byte[], KeyValue<byte[], byte[]>> iterator()
      throws IOException {
    return iterator((byte[])null);
  }

  @Override
  public TableIterator<byte[], KeyValue<byte[], byte[]>> iterator(byte[] prefix)
      throws IOException {
    return new RDBStoreByteArrayIterator(db.newIterator(family, false), this,
        prefix);
  }

  TableIterator<CodecBuffer, KeyValue<CodecBuffer, CodecBuffer>> iterator(
      CodecBuffer prefix) throws IOException {
    return new RDBStoreCodecBufferIterator(db.newIterator(family, false),
        this, prefix);
  }

  @Override
  public String getName() {
    return family.getName();
  }

  @Override
  public void close() throws Exception {
    // Nothing do for a Column Family.
  }

  @Override
  public long getEstimatedKeyCount() throws IOException {
    return db.estimateNumKeys(family);
  }

  @Override
  public List<KeyValue<byte[], byte[]>> getRangeKVs(byte[] startKey,
      int count, byte[] prefix,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(startKey, count, false, prefix, filters);
  }

  @Override
  public List<KeyValue<byte[], byte[]>> getSequentialRangeKVs(byte[] startKey,
      int count, byte[] prefix,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return getRangeKVs(startKey, count, true, prefix, filters);
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, byte[] prefix)
      throws IOException {
    try (TableIterator<byte[], KeyValue<byte[], byte[]>> iter
             = iterator(prefix)) {
      while (iter.hasNext()) {
        deleteWithBatch(batch, iter.next().getKey());
      }
    }
  }

  @Override
  public void dumpToFileWithPrefix(File externalFile, byte[] prefix)
      throws IOException {
    try (TableIterator<byte[], KeyValue<byte[], byte[]>> iter
             = iterator(prefix);
         DumpFileWriter fileWriter = new RDBSstFileWriter()) {
      fileWriter.open(externalFile);
      while (iter.hasNext()) {
        final KeyValue<byte[], byte[]> entry = iter.next();
        fileWriter.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public void loadFromFile(File externalFile) throws IOException {
    try (DumpFileLoader fileLoader = new RDBSstFileLoader(db, family)) {
      fileLoader.load(externalFile);
    }
  }

  private List<KeyValue<byte[], byte[]>> getRangeKVs(byte[] startKey,
      int count, boolean sequential, byte[] prefix,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    long start = System.currentTimeMillis();

    if (count < 0) {
      throw new IllegalArgumentException(
            "Invalid count given " + count + ", count must be greater than 0");
    }
    final List<KeyValue<byte[], byte[]>> result = new ArrayList<>();
    try (TableIterator<byte[], KeyValue<byte[], byte[]>> it
             = iterator(prefix)) {
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
        byte[] currentKey = currentEntry.getKey();

        if (filters == null) {
          result.add(currentEntry);
        } else {
          // NOTE: the preKey and nextKey are never checked
          // in all existing underlying filters, so they could
          // be safely as null here.
          if (Arrays.stream(filters)
                  .allMatch(entry -> entry.filterKey(null,
                          currentKey, null))) {
            result.add(currentEntry);
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
