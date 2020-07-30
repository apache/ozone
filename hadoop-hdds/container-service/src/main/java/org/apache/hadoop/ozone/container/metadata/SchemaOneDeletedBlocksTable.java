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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.metadata;

import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hdds.utils.db.cache.TableCacheImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * For RocksDB instances written using DB schema version 1, all data is
 * stored in the default column family. This differs from later schema
 * versions, which put deleted blocks in a different column family.
 * As a result, the block IDs used as keys for deleted blocks must be
 * prefixed in schema version 1 so that they can be differentiated from
 * regular blocks. However, these prefixes are not necessary in later schema
 * versions, because the deleted blocks and regular blocks are in different
 * column families.
 * <p>
 * Since clients must operate independently of the underlying schema version,
 * This class is returned returned to clients using a RocksDB schema
 * version 1 instance, allowing them to access keys as if no prefix is
 * required, while it adds the prefix when necessary.
 * This means the client can always omit the deleted prefix when putting and
 * getting keys, regardless of the schema version.
 * <p>
 * Note that this method will only apply prefixes to keys as parameters,
 * never as return types. This means that keys returned through iterators
 * like {@link SchemaOneDeletedBlocksTable#getSequentialRangeKVs},
 * {@link SchemaOneDeletedBlocksTable#getRangeKVs}, and
 * {@link SchemaOneDeletedBlocksTable#iterator} will return keys prefixed
 * with {@link SchemaOneDeletedBlocksTable#DELETED_KEY_PREFIX}.
 */
public class SchemaOneDeletedBlocksTable extends TypedTable<String, NoData> {
  public static final String DELETED_KEY_PREFIX = "#deleted#";

  public SchemaOneDeletedBlocksTable(
          Table<byte[], byte[]> rawTable,
          CodecRegistry codecRegistry, Class<String> keyType,
          Class<NoData> valueType) throws IOException {
    super(rawTable, codecRegistry, keyType, valueType);
  }

  public SchemaOneDeletedBlocksTable(
          Table<byte[], byte[]> rawTable,
          CodecRegistry codecRegistry, Class<String> keyType,
          Class<NoData> valueType,
          TableCacheImpl.CacheCleanupPolicy cleanupPolicy) throws IOException {
    super(rawTable, codecRegistry, keyType, valueType, cleanupPolicy);
  }

  @Override
  public void put(String key, NoData value) throws IOException {
    super.put(prefix(key), value);
  }

  @Override
  public void putWithBatch(BatchOperation batch, String key, NoData value)
          throws IOException {
    super.putWithBatch(batch, prefix(key), value);
  }

  @Override
  public void delete(String key) throws IOException {
    super.delete(prefix(key));
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, String key)
          throws IOException {
    super.deleteWithBatch(batch, prefix(key));
  }

  @Override
  public void addCacheEntry(CacheKey<String> cacheKey,
                            CacheValue<NoData> cacheValue) {
    CacheKey<String> prefixedCacheKey =
            new CacheKey<>(prefix(cacheKey.getCacheKey()));
    super.addCacheEntry(prefixedCacheKey, cacheValue);
  }

  @Override
  public boolean isExist(String key) throws IOException {
    return super.isExist(prefix(key));
  }

  @Override
  public NoData get(String key) throws IOException {
    return super.get(prefix(key));
  }

  @Override
  public NoData getIfExist(String key) throws IOException {
    return super.getIfExist(prefix(key));
  }

  @Override
  public NoData getReadCopy(String key) throws IOException {
    return super.getReadCopy(prefix(key));
  }

  @Override
  public List<TypedKeyValue> getRangeKVs(
          String startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    return super.getRangeKVs(prefix(startKey), count,
            addDeletedFilter(filters));
  }

  @Override
  public List<TypedKeyValue> getSequentialRangeKVs(
          String startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    return super.getSequentialRangeKVs(prefix(startKey), count,
            addDeletedFilter(filters));
  }

  private static String prefix(String key) {
    return key + DELETED_KEY_PREFIX;
  }

  private static MetadataKeyFilters.MetadataKeyFilter[] addDeletedFilter(
          MetadataKeyFilters.MetadataKeyFilter[] currentFilters) {

    List<MetadataKeyFilters.MetadataKeyFilter> newFilters =
            Arrays.asList(currentFilters);
    newFilters.add(MetadataKeyFilters.getDeletedKeyFilter());

    return newFilters.toArray(new MetadataKeyFilters.MetadataKeyFilter[0]);
  }
}
