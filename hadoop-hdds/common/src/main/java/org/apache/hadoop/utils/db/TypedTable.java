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
package org.apache.hadoop.utils.db;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;
import org.apache.hadoop.utils.db.cache.PartialTableCache;
import org.apache.hadoop.utils.db.cache.TableCache;

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

  private final Table<byte[], byte[]> rawTable;

  private final CodecRegistry codecRegistry;

  private final Class<KEY> keyType;

  private final Class<VALUE> valueType;

  private final TableCache<CacheKey<KEY>, CacheValue<VALUE>> cache;


  public TypedTable(
      Table<byte[], byte[]> rawTable,
      CodecRegistry codecRegistry, Class<KEY> keyType,
      Class<VALUE> valueType) {
    this.rawTable = rawTable;
    this.codecRegistry = codecRegistry;
    this.keyType = keyType;
    this.valueType = valueType;
    cache = new PartialTableCache<>();
  }

  @Override
  public void put(KEY key, VALUE value) throws IOException {
    byte[] keyData = codecRegistry.asRawData(key);
    byte[] valueData = codecRegistry.asRawData(value);
    rawTable.put(keyData, valueData);
  }

  @Override
  public void putWithBatch(BatchOperation batch, KEY key, VALUE value)
      throws IOException {
    byte[] keyData = codecRegistry.asRawData(key);
    byte[] valueData = codecRegistry.asRawData(value);
    rawTable.putWithBatch(batch, keyData, valueData);
  }

  @Override
  public boolean isEmpty() throws IOException {
    return rawTable.isEmpty();
  }

  @Override
  public boolean isExist(KEY key) throws IOException {
    CacheValue<VALUE> cacheValue= cache.get(new CacheKey<>(key));
    return (cacheValue != null && cacheValue.getCacheValue() != null) ||
        rawTable.isExist(codecRegistry.asRawData(key));
  }

  /**
   * Returns the value mapped to the given key in byte array or returns null
   * if the key is not found.
   *
   * Caller's of this method should use synchronization mechanism, when
   * accessing. First it will check from cache, if it has entry return the
   * value, otherwise get from the RocksDB table.
   *
   * @param key metadata key
   * @return VALUE
   * @throws IOException
   */
  @Override
  public VALUE get(KEY key) throws IOException {
    // Here the metadata lock will guarantee that cache is not updated for same
    // key during get key.
    CacheValue< VALUE > cacheValue = cache.get(new CacheKey<>(key));
    if (cacheValue == null) {
      // If no cache for the table or if it does not exist in cache get from
      // RocksDB table.
      return getFromTable(key);
    } else {
      // We have a value in cache, return the value.
      return cacheValue.getCacheValue();
    }
  }

  private VALUE getFromTable(KEY key) throws IOException {
    byte[] keyBytes = codecRegistry.asRawData(key);
    byte[] valueBytes = rawTable.get(keyBytes);
    return codecRegistry.asObject(valueBytes, valueType);
  }

  @Override
  public void delete(KEY key) throws IOException {
    rawTable.delete(codecRegistry.asRawData(key));
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key)
      throws IOException {
    rawTable.deleteWithBatch(batch, codecRegistry.asRawData(key));

  }

  @Override
  public TableIterator<KEY, TypedKeyValue> iterator() {
    TableIterator<byte[], ? extends KeyValue<byte[], byte[]>> iterator =
        rawTable.iterator();
    return new TypedTableIterator(iterator, keyType, valueType);
  }

  @Override
  public String getName() throws IOException {
    return rawTable.getName();
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

  public Iterator<Map.Entry<CacheKey<KEY>, CacheValue<VALUE>>> cacheIterator() {
    return cache.iterator();
  }

  @Override
  public void cleanupCache(long epoch) {
    cache.cleanup(epoch);
  }

  @VisibleForTesting
  TableCache<CacheKey<KEY>, CacheValue<VALUE>> getCache() {
    return cache;
  }

  public Table<byte[], byte[]> getRawTable() {
    return rawTable;
  }

  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  public Class<KEY> getKeyType() {
    return keyType;
  }

  public Class<VALUE> getValueType() {
    return valueType;
  }

  /**
   * Key value implementation for strongly typed tables.
   */
  public class TypedKeyValue implements KeyValue<KEY, VALUE> {

    private KeyValue<byte[], byte[]> rawKeyValue;

    public TypedKeyValue(KeyValue<byte[], byte[]> rawKeyValue) {
      this.rawKeyValue = rawKeyValue;
    }

    public TypedKeyValue(KeyValue<byte[], byte[]> rawKeyValue,
        Class<KEY> keyType, Class<VALUE> valueType) {
      this.rawKeyValue = rawKeyValue;
    }

    @Override
    public KEY getKey() throws IOException {
      return codecRegistry.asObject(rawKeyValue.getKey(), keyType);
    }

    @Override
    public VALUE getValue() throws IOException {
      return codecRegistry.asObject(rawKeyValue.getValue(), valueType);
    }
  }

  /**
   * Table Iterator implementation for strongly typed tables.
   */
  public class TypedTableIterator implements TableIterator<KEY, TypedKeyValue> {

    private TableIterator<byte[], ? extends KeyValue<byte[], byte[]>>
        rawIterator;
    private final Class<KEY> keyClass;
    private final Class<VALUE> valueClass;

    public TypedTableIterator(
        TableIterator<byte[], ? extends KeyValue<byte[], byte[]>> rawIterator,
        Class<KEY> keyType,
        Class<VALUE> valueType) {
      this.rawIterator = rawIterator;
      keyClass = keyType;
      valueClass = valueType;
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
      byte[] keyBytes = codecRegistry.asRawData(key);
      KeyValue<byte[], byte[]> result = rawIterator.seek(keyBytes);
      if (result == null) {
        return null;
      }
      return new TypedKeyValue(result);
    }

    @Override
    public KEY key() throws IOException {
      byte[] result = rawIterator.key();
      if (result == null) {
        return null;
      }
      return codecRegistry.asObject(result, keyClass);
    }

    @Override
    public TypedKeyValue value() {
      KeyValue keyValue = rawIterator.value();
      if(keyValue != null) {
        return new TypedKeyValue(keyValue, keyClass, valueClass);
      }
      return null;
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
      return new TypedKeyValue(rawIterator.next(), keyType,
          valueType);
    }
  }
}
