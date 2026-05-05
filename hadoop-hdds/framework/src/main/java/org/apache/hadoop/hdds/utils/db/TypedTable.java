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

import static org.apache.hadoop.hdds.utils.db.cache.CacheResult.CacheStatus.EXISTS;
import static org.apache.hadoop.hdds.utils.db.cache.CacheResult.CacheStatus.NOT_EXIST;
import static org.apache.ratis.util.JavaUtils.getClassSimpleName;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.TableCacheMetrics;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheResult;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hdds.utils.db.cache.FullTableCache;
import org.apache.hadoop.hdds.utils.db.cache.PartialTableCache;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.hdds.utils.db.cache.TableNoCache;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedBiFunction;

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
  private static final long EPOCH_DEFAULT = -1L;
  static final int BUFFER_SIZE_DEFAULT = 4 << 10; // 4 KB

  private final RDBTable rawTable;
  private final String info;

  private final Codec<KEY> keyCodec;
  private final Codec<VALUE> valueCodec;

  private final boolean supportCodecBuffer;
  private final CodecBuffer.Capacity bufferCapacity
      = new CodecBuffer.Capacity(this, BUFFER_SIZE_DEFAULT);
  private final TableCache<KEY, VALUE> cache;

  /**
   * Create an TypedTable from the raw table with specified cache type.
   *
   * @param rawTable The underlying (untyped) table in RocksDB.
   * @param keyCodec The key codec.
   * @param valueCodec The value codec.
   * @param cacheType How to cache the entries?
   */
  TypedTable(RDBTable rawTable, Codec<KEY> keyCodec, Codec<VALUE> valueCodec, CacheType cacheType)
      throws RocksDatabaseException, CodecException {
    this.rawTable = Objects.requireNonNull(rawTable, "rawTable==null");
    this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec == null");
    this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec == null");

    this.info = getClassSimpleName(getClass()) + "-" + getName() + "(" + getClassSimpleName(keyCodec.getTypeClass())
        + "->" + getClassSimpleName(valueCodec.getTypeClass()) + ")";

    this.supportCodecBuffer = keyCodec.supportCodecBuffer()
        && valueCodec.supportCodecBuffer();

    final String threadNamePrefix = rawTable.getName() + "_";
    if (cacheType == CacheType.FULL_CACHE) {
      cache = new FullTableCache<>(threadNamePrefix);
      //fill cache
      try (KeyValueIterator<KEY, VALUE> tableIterator = iterator()) {

        while (tableIterator.hasNext()) {
          KeyValue< KEY, VALUE > kv = tableIterator.next();

          // We should build cache after OM restart when clean up policy is
          // NEVER. Setting epoch value -1, so that when it is marked for
          // delete, this will be considered for cleanup.
          cache.loadInitial(new CacheKey<>(kv.getKey()),
              CacheValue.get(EPOCH_DEFAULT, kv.getValue()));
        }
      }
    } else if (cacheType == CacheType.PARTIAL_CACHE) {
      cache = new PartialTableCache<>(threadNamePrefix);
    } else {
      cache = TableNoCache.instance();
    }
  }

  private CodecBuffer encodeKeyCodecBuffer(KEY key) throws CodecException {
    return key == null ? null : keyCodec.toDirectCodecBuffer(key);
  }

  private byte[] encodeKey(KEY key) throws CodecException {
    return key == null ? null : keyCodec.toPersistedFormat(key);
  }

  private byte[] encodeValue(VALUE value) throws CodecException {
    return value == null ? null : valueCodec.toPersistedFormat(value);
  }

  private KEY decodeKey(byte[] key) throws CodecException {
    return key != null ? keyCodec.fromPersistedFormat(key) : null;
  }

  private VALUE decodeValue(byte[] value) throws CodecException {
    return value != null ? valueCodec.fromPersistedFormat(value) : null;
  }

  @Override
  public void put(KEY key, VALUE value) throws RocksDatabaseException, CodecException {
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
  public void putWithBatch(BatchOperation batch, KEY key, VALUE value) throws RocksDatabaseException, CodecException {
    if (supportCodecBuffer) {
      CodecBuffer keyBuffer = null;
      CodecBuffer valueBuffer = null;
      try {
        keyBuffer = keyCodec.toDirectCodecBuffer(key);
        valueBuffer = valueCodec.toDirectCodecBuffer(value);
        // The buffers will be released after commit.
        rawTable.putWithBatch(batch, keyBuffer, valueBuffer);
      } catch (Exception e) {
        IOUtils.closeQuietly(valueBuffer, keyBuffer);
        throw e;
      }
    } else {
      rawTable.putWithBatch(batch, encodeKey(key), encodeValue(value));
    }
  }

  @Override
  public boolean isEmpty() throws RocksDatabaseException {
    return rawTable.isEmpty();
  }

  @Override
  public boolean isExist(KEY key) throws RocksDatabaseException, CodecException  {

    CacheResult<VALUE> cacheResult =
        cache.lookup(new CacheKey<>(key));

    if (cacheResult.getCacheStatus() == EXISTS) {
      return true;
    } else if (cacheResult.getCacheStatus() == NOT_EXIST) {
      return false;
    } else if (keyCodec.supportCodecBuffer()) {
      // keyCodec.supportCodecBuffer() is enough since value is not needed.
      try (CodecBuffer inKey = keyCodec.toDirectCodecBuffer(key)) {
        // Use zero capacity buffer since value is not needed.
        try (CodecBuffer outValue = CodecBuffer.getEmptyBuffer()) {
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
   */
  @Override
  public VALUE get(KEY key) throws RocksDatabaseException, CodecException {
    // Here the metadata lock will guarantee that cache is not updated for same
    // key during get key.

    CacheResult<VALUE> cacheResult =
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
   */
  @Override
  public VALUE getSkipCache(KEY key) throws RocksDatabaseException, CodecException {
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
   */
  @Override
  public VALUE getReadCopy(KEY key) throws RocksDatabaseException, CodecException {
    // Here the metadata lock will guarantee that cache is not updated for same
    // key during get key.

    CacheResult<VALUE> cacheResult =
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
  public VALUE getIfExist(KEY key) throws RocksDatabaseException, CodecException {
    // Here the metadata lock will guarantee that cache is not updated for same
    // key during get key.

    CacheResult<VALUE> cacheResult =
        cache.lookup(new CacheKey<>(key));

    if (cacheResult.getCacheStatus() == EXISTS) {
      return valueCodec.copyObject(cacheResult.getValue().getCacheValue());
    } else if (cacheResult.getCacheStatus() == NOT_EXIST) {
      return null;
    } else {
      return getFromTableIfExist(key);
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
   */
  private Integer getFromTable(CodecBuffer key, CodecBuffer outValue) throws RocksDatabaseException {
    return outValue.putFromSource(
        buffer -> rawTable.get(key.asReadOnlyByteBuffer(), buffer));
  }

  private VALUE getFromTable(KEY key) throws RocksDatabaseException, CodecException {
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
  private Integer getFromTableIfExist(CodecBuffer key, CodecBuffer outValue) throws RocksDatabaseException {
    return outValue.putFromSource(
        buffer -> rawTable.getIfExist(key.asReadOnlyByteBuffer(), buffer));
  }

  private VALUE getFromTable(KEY key,
      CheckedBiFunction<CodecBuffer, CodecBuffer, Integer, RocksDatabaseException> get)
      throws RocksDatabaseException, CodecException {
    try (CodecBuffer inKey = keyCodec.toDirectCodecBuffer(key)) {
      for (; ;) {
        final Integer required;
        final int initial = -bufferCapacity.get(); // resizable
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
        bufferCapacity.increase(required);
      }
    }
  }

  private VALUE getFromTableIfExist(KEY key) throws RocksDatabaseException, CodecException {
    if (supportCodecBuffer) {
      return getFromTable(key, this::getFromTableIfExist);
    } else {
      final byte[] keyBytes = encodeKey(key);
      final byte[] valueBytes = rawTable.getIfExist(keyBytes);
      return decodeValue(valueBytes);
    }
  }

  @Override
  public void delete(KEY key) throws RocksDatabaseException, CodecException {
    if (keyCodec.supportCodecBuffer()) {
      try (CodecBuffer buffer = keyCodec.toDirectCodecBuffer(key)) {
        rawTable.delete(buffer.asReadOnlyByteBuffer());
      }
    } else {
      rawTable.delete(encodeKey(key));
    }
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key) throws CodecException {
    if (supportCodecBuffer) {
      CodecBuffer keyBuffer = null;
      try {
        keyBuffer = keyCodec.toDirectCodecBuffer(key);
        // The buffers will be released after commit.
        rawTable.deleteWithBatch(batch, keyBuffer);
      } catch (Exception e) {
        IOUtils.closeQuietly(keyBuffer);
        throw e;
      }
    } else {
      rawTable.deleteWithBatch(batch, encodeKey(key));
    }
  }

  @Override
  public void deleteRange(KEY beginKey, KEY endKey) throws RocksDatabaseException, CodecException {
    rawTable.deleteRange(encodeKey(beginKey), encodeKey(endKey));
  }

  @Override
  public KeyValueIterator<KEY, VALUE> iterator(KEY prefix, IteratorType type)
      throws RocksDatabaseException, CodecException {
    if (supportCodecBuffer) {
      return newCodecBufferTableIterator(prefix, type);
    } else {
      final byte[] prefixBytes = encodeKey(prefix);
      return new TypedTableIterator(rawTable.iterator(prefixBytes, type));
    }
  }

  @Override
  public String getName() {
    return rawTable.getName();
  }

  @Override
  public String toString() {
    return info;
  }

  @Override
  public long getEstimatedKeyCount() throws RocksDatabaseException {
    if (cache.getCacheType() == CacheType.FULL_CACHE) {
      return cache.size();
    }
    return rawTable.getEstimatedKeyCount();
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
  public List<KeyValue<KEY, VALUE>> getRangeKVs(
      KEY startKey, int count, KEY prefix, KeyPrefixFilter filter, boolean isSequential)
      throws RocksDatabaseException, CodecException {
    // TODO use CodecBuffer if the key codec supports

    // A null start key means to start from the beginning of the table.
    // Cannot convert a null key to bytes.
    final byte[] startKeyBytes = encodeKey(startKey);
    final byte[] prefixBytes = encodeKey(prefix);

    List<KeyValue<byte[], byte[]>> rangeKVBytes =
        rawTable.getRangeKVs(startKeyBytes, count, prefixBytes, filter, isSequential);
    final List<KeyValue<KEY, VALUE>> rangeKVs = new ArrayList<>();
    for (KeyValue<byte[], byte[]> kv : rangeKVBytes) {
      rangeKVs.add(Table.newKeyValue(decodeKey(kv.getKey()), decodeValue(kv.getValue())));
    }
    return rangeKVs;
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, KEY prefix) throws RocksDatabaseException, CodecException {
    rawTable.deleteBatchWithPrefix(batch, encodeKey(prefix));
  }

  @Override
  public void dumpToFileWithPrefix(File externalFile, KEY prefix) throws RocksDatabaseException, CodecException {
    rawTable.dumpToFileWithPrefix(externalFile, encodeKey(prefix));
  }

  @Override
  public void loadFromFile(File externalFile) throws RocksDatabaseException {
    rawTable.loadFromFile(externalFile);
  }

  @Override
  public void cleanupCache(List<Long> epochs) {
    cache.cleanup(epochs);
  }

  @VisibleForTesting
  TableCache<KEY, VALUE> getCache() {
    return cache;
  }

  private RawIterator<CodecBuffer> newCodecBufferTableIterator(KEY prefix, IteratorType type)
      throws RocksDatabaseException, CodecException {
    final CodecBuffer encoded = encodeKeyCodecBuffer(prefix);
    final CodecBuffer prefixBuffer;
    if (encoded != null && encoded.readableBytes() == 0) {
      encoded.release();
      prefixBuffer = null;
    } else {
      prefixBuffer = encoded;
    }

    try {
      return newCodecBufferTableIterator(rawTable.iterator(prefixBuffer, type));
    } catch (Throwable t) {
      if (prefixBuffer != null) {
        prefixBuffer.release();
      }
      throw t;
    }
  }

  private RawIterator<CodecBuffer> newCodecBufferTableIterator(KeyValueIterator<CodecBuffer, CodecBuffer> i) {
    return new RawIterator<CodecBuffer>(i) {
      @Override
      AutoCloseSupplier<CodecBuffer> convert(KEY key) throws CodecException {
        final CodecBuffer buffer = encodeKeyCodecBuffer(key);
        return new AutoCloseSupplier<CodecBuffer>() {
          @Override
          public void close() {
            buffer.release();
          }

          @Override
          public CodecBuffer get() {
            return buffer;
          }
        };
      }

      @Override
      KeyValue<KEY, VALUE> convert(KeyValue<CodecBuffer, CodecBuffer> raw) throws CodecException {
        final CodecBuffer keyBuffer = raw.getKey();
        final KEY key = keyBuffer != null ? keyCodec.fromCodecBuffer(keyBuffer) : null;

        final CodecBuffer valueBuffer = raw.getValue();
        return valueBuffer == null ? Table.newKeyValue(key, null)
            : Table.newKeyValue(key, valueCodec.fromCodecBuffer(valueBuffer), valueBuffer.readableBytes());
      }
    };
  }

  /**
   * Table Iterator implementation for strongly typed tables.
   */
  public class TypedTableIterator extends RawIterator<byte[]> {
    TypedTableIterator(KeyValueIterator<byte[], byte[]> rawIterator) {
      super(rawIterator);
    }

    @Override
    AutoCloseSupplier<byte[]> convert(KEY key) throws CodecException {
      final byte[] keyArray = encodeKey(key);
      return () -> keyArray;
    }

    @Override
    KeyValue<KEY, VALUE> convert(KeyValue<byte[], byte[]> raw) throws CodecException {
      final KEY key = decodeKey(raw.getKey());
      final byte[] valueBytes = raw.getValue();
      return valueBytes == null ? Table.newKeyValue(key, null)
          : Table.newKeyValue(key, decodeValue(valueBytes), valueBytes.length);
    }
  }

  /**
   * A {@link Table.KeyValueIterator} backed by a raw iterator.
   *
   * @param <RAW> The raw type.
   */
  abstract class RawIterator<RAW>
      implements Table.KeyValueIterator<KEY, VALUE> {
    private final KeyValueIterator<RAW, RAW> rawIterator;

    RawIterator(KeyValueIterator<RAW, RAW> rawIterator) {
      this.rawIterator = rawIterator;
    }

    /** Covert the given key to the {@link RAW} type. */
    abstract AutoCloseSupplier<RAW> convert(KEY key) throws CodecException;

    /**
     * Covert the given {@link Table.KeyValue}
     * from ({@link RAW}, {@link RAW}) to ({@link KEY}, {@link VALUE}).
     */
    abstract KeyValue<KEY, VALUE> convert(KeyValue<RAW, RAW> raw) throws CodecException;

    @Override
    public void seekToFirst() {
      rawIterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
      rawIterator.seekToLast();
    }

    @Override
    public KeyValue<KEY, VALUE> seek(KEY key) throws RocksDatabaseException, CodecException {
      try (AutoCloseSupplier<RAW> rawKey = convert(key)) {
        final KeyValue<RAW, RAW> result = rawIterator.seek(rawKey.get());
        return result == null ? null : convert(result);
      }
    }

    @Override
    public void close() throws RocksDatabaseException {
      rawIterator.close();
    }

    @Override
    public boolean hasNext() {
      return rawIterator.hasNext();
    }

    @Override
    public KeyValue<KEY, VALUE> next() {
      try {
        return convert(rawIterator.next());
      } catch (CodecException e) {
        throw new IllegalStateException("Failed next() in " + TypedTable.this, e);
      }
    }

    @Override
    public void removeFromDB() throws RocksDatabaseException, CodecException {
      rawIterator.removeFromDB();
    }
  }
}
