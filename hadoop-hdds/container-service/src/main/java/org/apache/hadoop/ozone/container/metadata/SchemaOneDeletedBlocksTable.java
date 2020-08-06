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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;

import java.io.IOException;
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
 * This class is returned to clients using {@link DatanodeStoreSchemaOneImpl}
 * instances, allowing them to access keys as if no prefix is
 * required, while it adds the prefix when necessary.
 * This means the client should omit the deleted prefix when putting and
 * getting keys, regardless of the schema version.
 * <p>
 * Note that this class will only apply prefixes to keys as parameters,
 * never as return types. This means that keys returned through iterators
 * like {@link SchemaOneDeletedBlocksTable#getSequentialRangeKVs},
 * {@link SchemaOneDeletedBlocksTable#getRangeKVs}, and
 * {@link SchemaOneDeletedBlocksTable#iterator} will return keys prefixed
 * with {@link SchemaOneDeletedBlocksTable#DELETED_KEY_PREFIX}.
 */
public class SchemaOneDeletedBlocksTable implements Table<String,
        ChunkInfoList> {
  public static final String DELETED_KEY_PREFIX = "#deleted#";

  private final Table<String, ChunkInfoList> table;

  public SchemaOneDeletedBlocksTable(Table<String, ChunkInfoList> table) {
    this.table = table;
  }

  @Override
  public void put(String key, ChunkInfoList value) throws IOException {
    table.put(prefix(key), value);
  }

  @Override
  public void putWithBatch(BatchOperation batch, String key,
                           ChunkInfoList value)
          throws IOException {
    table.putWithBatch(batch, prefix(key), value);
  }

  @Override
  public boolean isEmpty() throws IOException {
    return table.isEmpty();
  }

  @Override
  public void delete(String key) throws IOException {
    table.delete(prefix(key));
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, String key)
          throws IOException {
    table.deleteWithBatch(batch, prefix(key));
  }

  /**
   * Because the actual underlying table in this schema version is the
   * default table where all keys are stored, this method will iterate
   * through all keys in the database.
   */
  @Override
  public TableIterator<String, ? extends KeyValue<String, ChunkInfoList>>
      iterator() {
    return table.iterator();
  }

  @Override
  public String getName() throws IOException {
    return table.getName();
  }

  @Override
  public long getEstimatedKeyCount() throws IOException {
    return table.getEstimatedKeyCount();
  }

  @Override
  public void addCacheEntry(CacheKey<String> cacheKey,
                            CacheValue<ChunkInfoList> cacheValue) {
    CacheKey<String> prefixedCacheKey =
            new CacheKey<>(prefix(cacheKey.getCacheKey()));
    table.addCacheEntry(prefixedCacheKey, cacheValue);
  }

  @Override
  public boolean isExist(String key) throws IOException {
    return table.isExist(prefix(key));
  }

  @Override
  public ChunkInfoList get(String key) throws IOException {
    return table.get(prefix(key));
  }

  @Override
  public ChunkInfoList getIfExist(String key) throws IOException {
    return table.getIfExist(prefix(key));
  }

  @Override
  public ChunkInfoList getReadCopy(String key) throws IOException {
    return table.getReadCopy(prefix(key));
  }

  /**
   * Keys returned in the list by this method will begin with the
   * {@link SchemaOneDeletedBlocksTable#DELETED_KEY_PREFIX}.
   */
  @Override
  public List<? extends KeyValue<String, ChunkInfoList>> getRangeKVs(
          String startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    // Deleted blocks will always have the #deleted# key prefix and nothing
    // else in this schema version. Ignore any user passed prefixes that could
    // collide with this, returning results that are not deleted blocks.
    return table.getRangeKVs(prefix(startKey), count, getDeletedFilter());
  }

  /**
   * Keys returned in the list by this method will begin with the
   * {@link SchemaOneDeletedBlocksTable#DELETED_KEY_PREFIX}.
   */
  @Override
  public List<? extends KeyValue<String, ChunkInfoList>> getSequentialRangeKVs(
          String startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    // Deleted blocks will always have the #deleted# key prefix and nothing
    // else in this schema version. Ignore any user passed prefixes that could
    // collide with this, returning results that are not deleted blocks.
    return table.getSequentialRangeKVs(prefix(startKey), count,
            getDeletedFilter());
  }

  @Override
  public void close() throws Exception {
    table.close();
  }

  private static String prefix(String key) {
    String result = null;
    if (key != null) {
      result = DELETED_KEY_PREFIX + key;
    }

    return result;
  }

  private static MetadataKeyFilters.KeyPrefixFilter getDeletedFilter() {
    return (new MetadataKeyFilters.KeyPrefixFilter())
            .addFilter(DELETED_KEY_PREFIX);
  }
}
