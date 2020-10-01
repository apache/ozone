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
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;

import java.io.IOException;
import java.util.ArrayList;
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
 * required, while it adds or removes the prefix when necessary.
 * This means the client should omit the deleted prefix when putting and
 * getting keys, regardless of the schema version.
 */
public class SchemaOneDeletedBlocksTable extends DatanodeTable<String,
        ChunkInfoList> {
  public static final String DELETED_KEY_PREFIX = "#deleted#";

  public SchemaOneDeletedBlocksTable(Table<String, ChunkInfoList> table) {
    super(table);
  }

  @Override
  public void put(String key, ChunkInfoList value) throws IOException {
    super.put(prefix(key), value);
  }

  @Override
  public void putWithBatch(BatchOperation batch, String key,
                           ChunkInfoList value)
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
  public boolean isExist(String key) throws IOException {
    return super.isExist(prefix(key));
  }

  @Override
  public ChunkInfoList get(String key) throws IOException {
    return super.get(prefix(key));
  }

  @Override
  public ChunkInfoList getIfExist(String key) throws IOException {
    return super.getIfExist(prefix(key));
  }

  @Override
  public ChunkInfoList getReadCopy(String key) throws IOException {
    return super.getReadCopy(prefix(key));
  }

  @Override
  public List<? extends KeyValue<String, ChunkInfoList>> getRangeKVs(
          String startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    // Deleted blocks will always have the #deleted# key prefix and nothing
    // else in this schema version. Ignore any user passed prefixes that could
    // collide with this and return results that are not deleted blocks.
    return unprefix(super.getRangeKVs(prefix(startKey), count,
            getDeletedFilter()));
  }

  @Override
  public List<? extends KeyValue<String, ChunkInfoList>> getSequentialRangeKVs(
          String startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    // Deleted blocks will always have the #deleted# key prefix and nothing
    // else in this schema version. Ignore any user passed prefixes that could
    // collide with this and return results that are not deleted blocks.
    return unprefix(super.getSequentialRangeKVs(prefix(startKey), count,
            getDeletedFilter()));
  }

  private static String prefix(String key) {
    String result = null;
    if (key != null) {
      result = DELETED_KEY_PREFIX + key;
    }

    return result;
  }

  private static String unprefix(String key) {
    String result = null;
    if (key != null && key.startsWith(DELETED_KEY_PREFIX)) {
      result = key.replaceFirst(DELETED_KEY_PREFIX, "");
    }

    return result;
  }

  private static List<KeyValue<String, ChunkInfoList>> unprefix(
      List<? extends KeyValue<String, ChunkInfoList>> kvs) {

    List<KeyValue<String, ChunkInfoList>> processedKVs = new ArrayList<>();
    kvs.forEach(kv -> processedKVs.add(new UnprefixedKeyValue(kv)));

    return processedKVs;
  }

  private static MetadataKeyFilters.KeyPrefixFilter getDeletedFilter() {
    return (new MetadataKeyFilters.KeyPrefixFilter())
            .addFilter(DELETED_KEY_PREFIX);
  }

  /**
   * {@link KeyValue} implementation that removes the deleted key prefix from
   * an existing {@link KeyValue}.
   */
  private static class UnprefixedKeyValue implements KeyValue<String,
        ChunkInfoList> {

    private final KeyValue<String, ChunkInfoList> prefixedKeyValue;

    UnprefixedKeyValue(
            KeyValue<String, ChunkInfoList> prefixedKeyValue) {
      this.prefixedKeyValue = prefixedKeyValue;
    }

    @Override
    public String getKey() throws IOException {
      return unprefix(prefixedKeyValue.getKey());
    }

    @Override
    public ChunkInfoList getValue() throws IOException {
      return prefixedKeyValue.getValue();
    }
  }
}
