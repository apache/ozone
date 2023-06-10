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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.TableCacheMetrics;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheResult;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hdds.utils.db.cache.FullTableCache;
import org.apache.hadoop.hdds.utils.db.cache.PartialTableCache;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.utils.db.cache.CacheResult.CacheStatus.EXISTS;
import static org.apache.hadoop.hdds.utils.db.cache.CacheResult.CacheStatus.NOT_EXIST;
import static org.apache.ratis.util.JavaUtils.getClassSimpleName;
/**
 * Strongly typed table implementation.
 * <p>
 * Automatically converts values and keys using a raw byte[] based table
 * implementation and registered converters.
 *
 * @param <KEY>   type of the keys in the store.
 * @param <VALUE> type of the values in the store.
 */
public class TypedTable<KEY, VALUE> implements Table<KEY, VALUE> {
  static final Logger LOG = LoggerFactory.getLogger(TypedTable.class);

  private static final long EPOCH_DEFAULT = -1L;
  static final int BUFFER_SIZE_DEFAULT = 4 << 10; // 4 KB

  private final RDBTable rawTable;

  private final Class<KEY> keyType;
  private final Codec<KEY> keyCodec;
  private final Class<VALUE> valueType;
  private final Codec<VALUE> valueCodec;

  private final boolean supportCodecBuffer;
  private final AtomicInteger bufferSize
      = new AtomicInteger(BUFFER_SIZE_DEFAULT);
  private final TableCache<CacheKey<KEY>, CacheValue<VALUE>> cache;

  /**
   * The same as this(rawTable, codecRegistry, keyType, valueType,
   *                  CacheType.PARTIAL_CACHE).
   */
  public TypedTable(RDBTable rawTable,
      CodecRegistry codecRegistry, Class<KEY> keyType,
      Class<VALUE> valueType) throws IOException {
    this(rawTable, codecRegistry, keyType, valueType,
        CacheType.PARTIAL_CACHE);
  }

  /**
   * Create an TypedTable from the raw table with specified cache type.
   *
   * @param rawTable The underlying (untyped) table in RocksDB.
   * @param codecRegistry To look up codecs.
   * @param keyType The key type.
   * @param valueType The value type.
   * @param cacheType How to cache the entries?
   * @throws IOException if failed to iterate the raw table.
   */
  public TypedTable(RDBTable rawTable,
      CodecRegistry codecRegistry, Class<KEY> keyType,
      Class<VALUE> valueType,
      CacheType cacheType) throws IOException {
    this.rawTable = Objects.requireNonNull(rawTable, "rawTable==null");
    Objects.requireNonNull(codecRegistry, "codecRegistry == null");

    this.keyType = Objects.requireNonNull(keyType, "keyType == null");
    this.keyCodec = codecRegistry.getCodecFromClass(keyType);
    Objects.requireNonNull(keyCodec, "keyCodec == null");

    this.valueType = Objects.requireNonNull(valueType, "valueType == null");
    this.valueCodec = codecRegistry.getCodecFromClass(valueType);
    Objects.requireNonNull(valueCodec, "valueCodec == null");

    this.supportCodecBuffer = keyCodec.supportCodecBuffer()
        && valueCodec.supportCodecBuffer();

    if (cacheType == CacheType.FULL_CACHE) {
      cache = new FullTableCache<>();
      //fill cache
      try (TableIterator<KEY, ? extends KeyValue<KEY, VALUE>> tableIterator =
              iterator()) {

        while (tableIterator.hasNext()) {
          KeyValue< KEY, VALUE > kv = tableIterator.next();

          // We should build cache after OM restart when clean up policy is
          // NEVER. Setting epoch value -1, so that when it is marked for
          // delete, this will be considered for cleanup.
          cache.loadInitial(new CacheKey<>(kv.getKey()),
              CacheValue.get(EPOCH_DEFAULT, kv.getValue()));
        }
      }
    } else {
      cache = new PartialTableCache<>();
    }
  }

  private byte[] encodeKey(KEY key) throws IOException {
    return key == null ? null : keyCodec.toPersistedFormat(key);
  }

  private byte[] encodeValue(VALUE value) throws IOException {
    return value == null ? null : valueCodec.toPersistedFormat(value);
  }

  private KEY decodeKey(byte[] key) throws IOException {
    return key == null ? null : keyCodec.fromPersistedFormat(key);
  }

  private VALUE decodeValue(byte[] value) throws IOException {
    return value == null ? null : valueCodec.fromPersistedFormat(value);
  }

  @Override
  public void put(KEY key, VALUE value) throws IOException {
    if (supportCodecBuffer) {
      try (CodecBuffer k = keyCodec.toDirectCodecBuffer(key);
           CodecBuffer v = valueCodec.toDirectCodecBuffer(value)) {
        rawTable.put(k.asReadOnlyByteBuffer(), v.asReadOnlyByteBuffer());
      }
    } else {
      rawTable.put(encodeKey(key), encodeValue(value));
    }
  }

  @Override
  public void putWithBatch(BatchOperation batch, KEY key, VALUE value)
      throws IOException {
    if (supportCodecBuffer) {
      // The buffers will be released after commit.
      rawTable.putWithBatch(batch,
          keyCodec.toDirectCodecBuffer(key),
          valueCodec.toDirectCodecBuffer(value));
    } else {
      rawTable.putWithBatch(batch, encodeKey(key), encodeValue(value));
    }
  }

  @Override
  public boolean isEmpty() throws IOException {
    return rawTable.isEmpty();
  }

  @Override
  public boolean isExist(KEY key) throws IOException {

    CacheResult<CacheValue<VALUE>> cacheResult =
        cache.lookup(new CacheKey<>(key));

    if (cacheResult.getCacheStatus() == EXISTS) {
      return true;
    } else if (cacheResult.getCacheStatus() == NOT_EXIST) {
      return false;
    } else if (keyCodec.supportCodecBuffer()) {
      // keyCodec.supportCodecBuffer() is enough since value is not needed.
      try (CodecBuffer inKey = keyCodec.toDirectCodecBuffer(key)) {
        // Use zero capacity buffer since value is not needed.
        try (CodecBuffer outValue = CodecBuffer.allocateDirect(0)) {
          return getFromTableIfExist(inKey, outValue) != null;
        }
      }
    } else {
      return rawTable.isExist(encodeKey(key));
    }
  }

  /**
   * Get the value mapped to the given key.
   * <p>
   * Caller's of this method should use synchronization mechanism, when
   * accessing. First it will check from cache, if it has entry return the
   * cloned cache value, otherwise get from the RocksDB table.
   *
   * @param key metadata key
   * @return the mapped value; or null if the key is not found.
   * @throws IOException when {@link #getFromTable(Object)} throw an exception.
   */
  @Override
  public VALUE get(KEY key) throws IOException {
    // Here the metadata lock will guarantee that cache is not updated for same
    // key during get key.

    CacheResult<CacheValue<VALUE>> cacheResult =
        cache.lookup(new CacheKey<>(key));

    if (cacheResult.getCacheStatus() == EXISTS) {
      return valueCodec.copyObject(cacheResult.getValue().getCacheValue());
    } else if (cacheResult.getCacheStatus() == NOT_EXIST) {
      return null;
    } else {
      return getFromTable(key);
    }
  }

  /**
   * Skip checking cache and get the value mapped to the given key in byte
   * array or returns null if the key is not found.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   * @throws IOException on Failure
   */
  @Override
  public VALUE getSkipCache(KEY key) throws IOException {
    return getFromTable(key);
  }

  /**
   * This method returns the value if it exists in cache, if it 
   * does not, get the value from the underlying RockDB table. If it
   * exists in cache, it returns the same reference of the cached value.
   * <p>
   * Caller's of this method should use synchronization mechanism, when
   * accessing. First it will check from cache, if it has entry return the
   * cached value, otherwise get from the RocksDB table. It is caller
   * responsibility not to use the returned object outside the lock.
   * <p>
   * One example use case of this method is, when validating volume exists in
   * bucket requests and also where we need actual value of volume info. Once 
   * bucket response is added to the double buffer, only bucket info is 
   * required to flush to DB. So, there is no case of concurrent threads 
   * modifying the same cached object.
   * @param key metadata key
   * @return VALUE
   * @throws IOException when {@link #getFromTable(Object)} throw an exception.
   */
  @Override
  public VALUE getReadCopy(KEY key) throws IOException {
    // Here the metadata lock will guarantee that cache is not updated for same
    // key during get key.

    CacheResult<CacheValue<VALUE>> cacheResult =
        cache.lookup(new CacheKey<>(key));

    if (cacheResult.getCacheStatus() == EXISTS) {
      return cacheResult.getValue().getCacheValue();
    } else if (cacheResult.getCacheStatus() == NOT_EXIST) {
      return null;
    } else {
      return getFromTable(key);
    }
  }

  @Override
  public VALUE getIfExist(KEY key) throws IOException {
    // Here the metadata lock will guarantee that cache is not updated for same
    // key during get key.

    CacheResult<CacheValue<VALUE>> cacheResult =
        cache.lookup(new CacheKey<>(key));

    if (cacheResult.getCacheStatus() == EXISTS) {
      return valueCodec.copyObject(cacheResult.getValue().getCacheValue());
    } else if (cacheResult.getCacheStatus() == NOT_EXIST) {
      return null;
    } else {
      return getFromTableIfExist(key);
    }
  }

  private static int nextBufferSize(int n) {
    // round up to the next power of 2.
    final long roundUp = Long.highestOneBit(n) << 1;
    return roundUp > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) roundUp;
  }

  private void increaseBufferSize(int required) {
    final MemoizedSupplier<Integer> newBufferSize = MemoizedSupplier.valueOf(
        () -> nextBufferSize(required));
    final int previous = bufferSize.getAndUpdate(
        current -> required <= current ? current : newBufferSize.get());
    if (newBufferSize.isInitialized()) {
      LOG.info("{}: increaseBufferSize {} -> {}",
          this, previous, newBufferSize.get());
    }
  }

  /**
   * Use {@link RDBTable#get(ByteBuffer, ByteBuffer)}
   * to get a value mapped to the given key.
   *
   * @param key the buffer containing the key.
   * @param outValue the buffer to write the output value.
   *                 When the buffer capacity is smaller than the value size,
   *                 partial value may be written.
   * @return null if the key is not found;
   *         otherwise, return the size of the value.
   * @throws IOException in case is an error reading from the db.
   */
  private Integer getFromTable(CodecBuffer key, CodecBuffer outValue)
      throws IOException {
    return outValue.putFromSource(
        buffer -> rawTable.get(key.asReadOnlyByteBuffer(), buffer));
  }

  private VALUE getFromTable(KEY key) throws IOException {
    if (supportCodecBuffer) {
      return getFromTable(key, this::getFromTable);
    } else {
      final byte[] keyBytes = encodeKey(key);
      byte[] valueBytes = rawTable.get(keyBytes);
      return decodeValue(valueBytes);
    }
  }

  /**
   * Similar to {@link #getFromTable(CodecBuffer, CodecBuffer)} except that
   * this method use {@link RDBTable#getIfExist(ByteBuffer, ByteBuffer)}.
   */
  private Integer getFromTableIfExist(CodecBuffer key, CodecBuffer outValue)
      throws IOException {
    return outValue.putFromSource(
        buffer -> rawTable.getIfExist(key.asReadOnlyByteBuffer(), buffer));
  }

  private VALUE getFromTable(KEY key,
      CheckedBiFunction<CodecBuffer, CodecBuffer, Integer, IOException> get)
      throws IOException {
    try (CodecBuffer inKey = keyCodec.toDirectCodecBuffer(key)) {
      for (; ;) {
        final Integer required;
        final int initial = -bufferSize.get(); // allocate a resizable buffer
        try (CodecBuffer outValue = CodecBuffer.allocateDirect(initial)) {
          required = get.apply(inKey, outValue);
          if (required == null) {
            // key not found
            return null;
          } else if (required < 0) {
            throw new IllegalStateException("required = " + required + " < 0");
          }

          for (; ;) {
            if (required == outValue.readableBytes()) {
              // buffer size is big enough
              return valueCodec.fromCodecBuffer(outValue);
            }
            // buffer size too small, try increasing the capacity.
            if (!outValue.setCapacity(required)) {
              break;
            }

            // retry with the new capacity
            outValue.clear();
            final int retried = get.apply(inKey, outValue);
            Preconditions.assertSame(required.intValue(), retried, "required");
          }
        }

        // buffer size too small, reallocate a new buffer.
        increaseBufferSize(required);
      }
    }
  }

  private VALUE getFromTableIfExist(KEY key) throws IOException {
    if (supportCodecBuffer) {
      return getFromTable(key, this::getFromTableIfExist);
    } else {
      final byte[] keyBytes = encodeKey(key);
      final byte[] valueBytes = rawTable.getIfExist(keyBytes);
      return decodeValue(valueBytes);
    }
  }

  @Override
  public void delete(KEY key) throws IOException {
    rawTable.delete(encodeKey(key));
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key)
      throws IOException {
    rawTable.deleteWithBatch(batch, encodeKey(key));
  }

  @Override
  public void deleteRange(KEY beginKey, KEY endKey) throws IOException {
    rawTable.deleteRange(encodeKey(beginKey), encodeKey(endKey));
  }

  @Override
  public TableIterator<KEY, TypedKeyValue> iterator() throws IOException {
    return new TypedTableIterator(rawTable.iterator());
  }

  @Override
  public TableIterator<KEY, TypedKeyValue> iterator(KEY prefix)
      throws IOException {
    final byte[] prefixBytes = encodeKey(prefix);
    return new TypedTableIterator(rawTable.iterator(prefixBytes));
  }

  @Override
  public String getName() {
    return rawTable.getName();
  }

  @Override
  public String toString() {
    return getClassSimpleName(getClass()) + "-" + getName()
        + "(" + getClassSimpleName(keyType)
        + "->" + getClassSimpleName(valueType) + ")";
  }

  @Override
  public long getEstimatedKeyCount() throws IOException {
    return rawTable.getEstimatedKeyCount();
  }

  @Override
  public void close() throws Exception {
    rawTable.close();

  }

  @Override
  public void addCacheEntry(CacheKey<KEY> cacheKey,
      CacheValue<VALUE> cacheValue) {
    // This will override the entry if there is already entry for this key.
    cache.put(cacheKey, cacheValue);
  }

  @Override
  public CacheValue<VALUE> getCacheValue(CacheKey<KEY> cacheKey) {
    return cache.get(cacheKey);
  }

  @Override
  public Iterator<Map.Entry<CacheKey<KEY>, CacheValue<VALUE>>> cacheIterator() {
    return cache.iterator();
  }

  @Override
  public TableCacheMetrics createCacheMetrics() {
    return TableCacheMetrics.create(cache, getName());
  }

  @Override
  public List<TypedKeyValue> getRangeKVs(
          KEY startKey, int count, KEY prefix,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    // A null start key means to start from the beginning of the table.
    // Cannot convert a null key to bytes.
    final byte[] startKeyBytes = encodeKey(startKey);
    final byte[] prefixBytes = encodeKey(prefix);

    List<? extends KeyValue<byte[], byte[]>> rangeKVBytes =
        rawTable.getRangeKVs(startKeyBytes, count, prefixBytes, filters);

    List<TypedKeyValue> rangeKVs = new ArrayList<>();
    rangeKVBytes.forEach(byteKV -> rangeKVs.add(new TypedKeyValue(byteKV)));

    return rangeKVs;
  }

  @Override
  public List<TypedKeyValue> getSequentialRangeKVs(
          KEY startKey, int count, KEY prefix,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    // A null start key means to start from the beginning of the table.
    // Cannot convert a null key to bytes.
    final byte[] startKeyBytes = encodeKey(startKey);
    final byte[] prefixBytes = encodeKey(prefix);

    List<? extends KeyValue<byte[], byte[]>> rangeKVBytes =
        rawTable.getSequentialRangeKVs(startKeyBytes, count,
            prefixBytes, filters);

    List<TypedKeyValue> rangeKVs = new ArrayList<>();
    rangeKVBytes.forEach(byteKV -> rangeKVs.add(new TypedKeyValue(byteKV)));

    return rangeKVs;
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, KEY prefix)
      throws IOException {
    rawTable.deleteBatchWithPrefix(batch, encodeKey(prefix));
  }

  @Override
  public void dumpToFileWithPrefix(File externalFile, KEY prefix)
      throws IOException {
    rawTable.dumpToFileWithPrefix(externalFile, encodeKey(prefix));
  }

  @Override
  public void loadFromFile(File externalFile) throws IOException {
    rawTable.loadFromFile(externalFile);
  }

  @Override
  public void cleanupCache(List<Long> epochs) {
    cache.cleanup(epochs);
  }

  @VisibleForTesting
  TableCache<CacheKey<KEY>, CacheValue<VALUE>> getCache() {
    return cache;
  }

  /**
   * Key value implementation for strongly typed tables.
   */
  public class TypedKeyValue implements KeyValue<KEY, VALUE> {

    private final KeyValue<byte[], byte[]> rawKeyValue;

    public TypedKeyValue(KeyValue<byte[], byte[]> rawKeyValue) {
      this.rawKeyValue = rawKeyValue;
    }

    @Override
    public KEY getKey() throws IOException {
      return decodeKey(rawKeyValue.getKey());
    }

    @Override
    public VALUE getValue() throws IOException {
      return decodeValue(rawKeyValue.getValue());
    }
  }

  /**
   * Table Iterator implementation for strongly typed tables.
   */
  public class TypedTableIterator implements TableIterator<KEY, TypedKeyValue> {

    private final TableIterator<byte[], KeyValue<byte[], byte[]>> rawIterator;

    public TypedTableIterator(
        TableIterator<byte[], KeyValue<byte[], byte[]>> rawIterator) {
      this.rawIterator = rawIterator;
    }

    @Override
    public void seekToFirst() {
      rawIterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
      rawIterator.seekToLast();
    }

    @Override
    public TypedKeyValue seek(KEY key) throws IOException {
      final byte[] keyBytes = encodeKey(key);
      KeyValue<byte[], byte[]> result = rawIterator.seek(keyBytes);
      if (result == null) {
        return null;
      }
      return new TypedKeyValue(result);
    }

    @Override
    public void close() throws IOException {
      rawIterator.close();
    }

    @Override
    public boolean hasNext() {
      return rawIterator.hasNext();
    }

    @Override
    public TypedKeyValue next() {
      return new TypedKeyValue(rawIterator.next());
    }

    @Override
    public void removeFromDB() throws IOException {
      rawIterator.removeFromDB();
    }
  }
}
