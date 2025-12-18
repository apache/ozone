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

package org.apache.hadoop.ozone.container.metadata;

import java.io.File;
import java.util.List;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.IteratorType;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * Wrapper class to represent a table in a datanode RocksDB instance.
 * This class can wrap any existing {@link Table} instance, but will throw
 * {@link UnsupportedOperationException} for {@link Table#iterator}.
 * This is because differing schema versions used in datanode DB layouts may
 * have differing underlying table structures, so iterating a table instance
 * directly, without taking into account key prefixes, may yield unexpected
 * results.
 */
public class DatanodeTable<KEY, VALUE> implements Table<KEY, VALUE> {

  private final Table<KEY, VALUE> table;

  public DatanodeTable(Table<KEY, VALUE> table) {
    this.table = table;
  }

  @Override
  public void put(KEY key, VALUE value) throws RocksDatabaseException, CodecException {
    table.put(key, value);
  }

  @Override
  public void putWithBatch(BatchOperation batch, KEY key, VALUE value) throws RocksDatabaseException, CodecException {
    table.putWithBatch(batch, key, value);
  }

  @Override
  public boolean isEmpty() throws RocksDatabaseException {
    return table.isEmpty();
  }

  @Override
  public void delete(KEY key) throws RocksDatabaseException, CodecException {
    table.delete(key);
  }

  @Override
  public void deleteRange(KEY beginKey, KEY endKey) throws RocksDatabaseException, CodecException {
    table.deleteRange(beginKey, endKey);
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key) throws CodecException, RocksDatabaseException {
    table.deleteWithBatch(batch, key);
  }

  @Override
  public void deleteRangeWithBatch(BatchOperation batch, KEY beginKey, KEY endKey)
      throws CodecException, RocksDatabaseException {
    table.deleteRangeWithBatch(batch, beginKey, endKey);
  }

  @Override
  public final KeyValueIterator<KEY, VALUE> iterator(KEY prefix, IteratorType type) {
    throw new UnsupportedOperationException("Iterating tables directly is not" +
        " supported for datanode containers due to differing schema " +
        "version.");
  }

  @Override
  public String getName() {
    return table.getName();
  }

  @Override
  public long getEstimatedKeyCount() throws RocksDatabaseException {
    return table.getEstimatedKeyCount();
  }

  @Override
  public boolean isExist(KEY key) throws RocksDatabaseException, CodecException {
    return table.isExist(key);
  }

  @Override
  public VALUE get(KEY key) throws RocksDatabaseException, CodecException {
    return table.get(key);
  }

  @Override
  public VALUE getIfExist(KEY key) throws RocksDatabaseException, CodecException {
    return table.getIfExist(key);
  }

  @Override
  public VALUE getReadCopy(KEY key) throws RocksDatabaseException, CodecException {
    return table.getReadCopy(key);
  }

  @Override
  public List<KeyValue<KEY, VALUE>> getRangeKVs(
      KEY startKey, int count, KEY prefix, KeyPrefixFilter filter, boolean isSequential)
      throws RocksDatabaseException, CodecException {
    return table.getRangeKVs(startKey, count, prefix, filter, isSequential);
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, KEY prefix) throws RocksDatabaseException, CodecException {
    table.deleteBatchWithPrefix(batch, prefix);
  }

  @Override
  public void dumpToFileWithPrefix(File externalFile, KEY prefix) throws RocksDatabaseException, CodecException {
    table.dumpToFileWithPrefix(externalFile, prefix);
  }

  @Override
  public void loadFromFile(File externalFile) throws RocksDatabaseException {
    table.loadFromFile(externalFile);
  }
}
