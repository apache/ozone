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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.TableCacheMetrics;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

/**
 * Interface for key-value store that stores ozone metadata. Ozone metadata is
 * stored as key value pairs, both key and value are arbitrary byte arrays. Each
 * Table Stores a certain kind of keys and values. This allows a DB to have
 * different kind of tables.
 */
@InterfaceStability.Evolving
public interface Table<KEY, VALUE> {

  /**
   * Puts a key-value pair into the store.
   *
   * @param key metadata key
   * @param value metadata value
   */
  void put(KEY key, VALUE value) throws RocksDatabaseException, CodecException;

  /**
   * Puts a key-value pair into the store as part of a bath operation.
   *
   * @param batch the batch operation
   * @param key metadata key
   * @param value metadata value
   */
  void putWithBatch(BatchOperation batch, KEY key, VALUE value) throws RocksDatabaseException, CodecException;

  /**
   * @return true if the metadata store is empty.
   */
  boolean isEmpty() throws RocksDatabaseException;

  /**
   * Check if a given key exists in Metadata store.
   * (Optimization to save on data deserialization)
   * A lock on the key / bucket needs to be acquired before invoking this API.
   * @param key metadata key
   * @return true if the metadata store contains a key.
   */
  boolean isExist(KEY key) throws RocksDatabaseException, CodecException;

  /**
   * Returns the value mapped to the given key in byte array or returns null
   * if the key is not found.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   */
  VALUE get(KEY key) throws RocksDatabaseException, CodecException;

  /**
   * Skip checking cache and get the value mapped to the given key in byte
   * array or returns null if the key is not found.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   */
  default VALUE getSkipCache(KEY key) throws RocksDatabaseException, CodecException {
    throw new NotImplementedException("getSkipCache is not implemented");
  }

  /**
   * Returns the value mapped to the given key in byte array or returns null
   * if the key is not found.
   *
   * This method is specific to tables implementation. Refer java doc of the
   * implementation for the behavior.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   */
  default VALUE getReadCopy(KEY key) throws RocksDatabaseException, CodecException {
    throw new NotImplementedException("getReadCopy is not implemented");
  }

  /**
   * Returns the value mapped to the given key in byte array or returns null
   * if the key is not found.
   *
   * This method first checks using keyMayExist, if it returns false, we are
   * 100% sure that key does not exist in DB, so it returns null with out
   * calling db.get. If keyMayExist return true, then we use db.get and then
   * return the value. This method will be useful in the cases where the
   * caller is more sure that this key does not exist in DB and keyMayExist
   * will help here.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   */
  VALUE getIfExist(KEY key) throws RocksDatabaseException, CodecException;

  /**
   * Deletes a key from the metadata store.
   *
   * @param key metadata key
   */
  void delete(KEY key) throws RocksDatabaseException, CodecException;

  /**
   * Deletes a key from the metadata store as part of a batch operation.
   *
   * @param batch the batch operation
   * @param key metadata key
   */
  void deleteWithBatch(BatchOperation batch, KEY key) throws CodecException;

  /**
   * Deletes a range of keys from the metadata store.
   *
   * @param beginKey start metadata key
   * @param endKey end metadata key
   */
  void deleteRange(KEY beginKey, KEY endKey) throws RocksDatabaseException, CodecException;

  /** The same as iterator(null, KEY_AND_VALUE). */
  default KeyValueIterator<KEY, VALUE> iterator() throws RocksDatabaseException, CodecException {
    return iterator(null, IteratorType.KEY_AND_VALUE);
  }

  /** The same as iterator(prefix, KEY_AND_VALUE). */
  default KeyValueIterator<KEY, VALUE> iterator(KEY prefix) throws RocksDatabaseException, CodecException {
    return iterator(prefix, IteratorType.KEY_AND_VALUE);
  }

  /**
   * Iterate the elements in this table.
   * <p>
   * Note that using a more restrictive type may improve performance
   * since the unrequired data may not be read from the DB.
   * <p>
   * Note also that, when the prefix is non-empty,
   * using a non-key type may not improve performance
   * since it has to read keys for matching the prefix.
   *
   * @param prefix The prefix of the elements to be iterated.
   * @param type Specify whether key and/or value are required.
   * @return an iterator.
   */
  KeyValueIterator<KEY, VALUE> iterator(KEY prefix, IteratorType type)
      throws RocksDatabaseException, CodecException;

  /**
   * @param prefix The prefix of the elements to be iterated.
   * @return a key-only iterator
   */
  default TableIterator<KEY, KEY> keyIterator(KEY prefix) throws RocksDatabaseException, CodecException {
    final KeyValueIterator<KEY, VALUE> i = iterator(prefix, IteratorType.KEY_ONLY);
    return TableIterator.convert(i, KeyValue::getKey);
  }

  /** The same as keyIterator(null). */
  default TableIterator<KEY, KEY> keyIterator() throws RocksDatabaseException, CodecException {
    return keyIterator(null);
  }

  /**
   * @param prefix The prefix of the elements to be iterated.
   * @return a value-only iterator.
   */
  default TableIterator<KEY, VALUE> valueIterator(KEY prefix) throws RocksDatabaseException, CodecException {
    final KeyValueIterator<KEY, VALUE> i = iterator(prefix, IteratorType.VALUE_ONLY);
    return TableIterator.convert(i, KeyValue::getValue);
  }

  /** The same as valueIterator(null). */
  default TableIterator<KEY, VALUE> valueIterator() throws RocksDatabaseException, CodecException {
    return valueIterator(null);
  }

  /**
   * Returns the Name of this Table.
   * @return - Table Name.
   */
  String getName();

  /**
   * Returns the key count of this Table.  Note the result can be inaccurate.
   * @return Estimated key count of this Table
   */
  long getEstimatedKeyCount() throws RocksDatabaseException;

  /**
   * Add entry to the table cache.
   *
   * If the cacheKey already exists, it will override the entry.
   * @param cacheKey
   * @param cacheValue
   */
  default void addCacheEntry(CacheKey<KEY> cacheKey,
      CacheValue<VALUE> cacheValue) {
    throw new NotImplementedException("addCacheEntry is not implemented");
  }

  /** Add entry to the table cache with a non-null key and a null value. */
  default void addCacheEntry(KEY cacheKey, long epoch) {
    addCacheEntry(new CacheKey<>(cacheKey), CacheValue.get(epoch));
  }

  /** Add entry to the table cache with a non-null key and a non-null value. */
  default void addCacheEntry(KEY cacheKey, VALUE value, long epoch) {
    addCacheEntry(new CacheKey<>(cacheKey),
        CacheValue.get(epoch, value));
  }

  /**
   * Get the cache value from table cache.
   * @param cacheKey
   */
  default CacheValue<VALUE> getCacheValue(CacheKey<KEY> cacheKey) {
    throw new NotImplementedException("getCacheValue is not implemented");
  }

  /**
   * Removes all the entries from the table cache which are matching with
   * epoch provided in the epoch list.
   * @param epochs
   */
  default void cleanupCache(List<Long> epochs) {
    throw new NotImplementedException("cleanupCache is not implemented");
  }

  /**
   * Return cache iterator maintained for this table.
   */
  default Iterator<Map.Entry<CacheKey<KEY>, CacheValue<VALUE>>>
      cacheIterator() {
    throw new NotImplementedException("cacheIterator is not implemented");
  }

  /**
   * Create the metrics datasource that emits table cache metrics.
   */
  default TableCacheMetrics createCacheMetrics() {
    throw new NotImplementedException("getCacheValue is not implemented");
  }

  /**
   * Returns a certain range of key value pairs as a list based on a startKey or count.
   * To prevent race conditions while listing
   * entries, this implementation takes a snapshot and lists the entries from
   * the snapshot. This may, on the other hand, cause the range result slight
   * different with actual data if data is updating concurrently.
   * <p>
   * If the startKey is specified and found in the table, this key and the keys
   * after this key will be included in the result. If the startKey is null
   * all entries will be included as long as other conditions are satisfied.
   * If the given startKey doesn't exist and empty list will be returned.
   * <p>
   * The count argument is to limit number of total entries to return,
   * the value for count must be an integer greater than 0.
   * <p>
   * This method allows to specify a {@link KeyPrefixFilter} to filter keys.
   * Once given, only the entries whose key passes all the filters will be included in the result.
   *
   * @param startKey a start key.
   * @param count max number of entries to return.
   * @param prefix fixed key schema specific prefix
   * @param filter for filtering keys
   * @param isSequential does it require sequential keys?
   * @return a list of entries found in the database or an empty list if the
   * startKey is invalid.
   * @throws IllegalArgumentException if count is less than 0.
   */
  List<KeyValue<KEY, VALUE>> getRangeKVs(KEY startKey,
          int count, KEY prefix, KeyPrefixFilter filter, boolean isSequential)
          throws RocksDatabaseException, CodecException;

  /** The same as getRangeKVs(startKey, count, prefix, filter, false). */
  default List<KeyValue<KEY, VALUE>> getRangeKVs(KEY startKey, int count, KEY prefix, KeyPrefixFilter filter)
      throws RocksDatabaseException, CodecException {
    return getRangeKVs(startKey, count, prefix, filter, false);
  }

  /** The same as getRangeKVs(startKey, count, prefix, null). */
  default List<KeyValue<KEY, VALUE>> getRangeKVs(KEY startKey, int count, KEY prefix)
      throws RocksDatabaseException, CodecException {
    return getRangeKVs(startKey, count, prefix, null);
  }

  /**
   * Deletes all keys with the specified prefix from the metadata store
   * as part of a batch operation.
   * @param batch
   * @param prefix
   */
  void deleteBatchWithPrefix(BatchOperation batch, KEY prefix) throws RocksDatabaseException, CodecException;

  /**
   * Dump all key value pairs with a prefix into an external file.
   * @param externalFile
   * @param prefix
   */
  void dumpToFileWithPrefix(File externalFile, KEY prefix) throws RocksDatabaseException, CodecException;

  /**
   * Load key value pairs from an external file created by
   * dumpToFileWithPrefix.
   * @param externalFile
   */
  void loadFromFile(File externalFile) throws RocksDatabaseException;

  /**
   * Class used to represent the key and value pair of a db entry.
   */
  final class KeyValue<K, V> {
    private final K key;
    private final V value;
    private final int valueByteSize;

    private KeyValue(K key, V value, int valueByteSize) {
      this.key = key;
      this.value = value;
      this.valueByteSize = valueByteSize;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    /** @return the value serialized byte size if it is available; otherwise, return -1. */
    public int getValueByteSize() {
      return value != null ? valueByteSize : -1;
    }

    @Override
    public String toString() {
      return "(key=" + key + ", value=" + value + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof KeyValue)) {
        return false;
      }
      final KeyValue<?, ?> that = (KeyValue<?, ?>) obj;
      return Objects.equals(this.getKey(), that.getKey())
          && Objects.equals(this.getValue(), that.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getKey(), getValue());
    }
  }

  static <K, V> KeyValue<K, V> newKeyValue(K key, V value) {
    return newKeyValue(key, value, -1);
  }

  static <K, V> KeyValue<K, V> newKeyValue(K key, V value, int valueByteSize) {
    return new KeyValue<>(key, value, valueByteSize);
  }

  /** A {@link TableIterator} to iterate {@link KeyValue}s. */
  interface KeyValueIterator<KEY, VALUE>
      extends TableIterator<KEY, KeyValue<KEY, VALUE>> {

  }
}
