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
import org.apache.hadoop.hdds.utils.db.TableIterator;

import java.io.IOException;
import java.util.List;

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
  public void put(KEY key, VALUE value) throws IOException {
    table.put(key, value);
  }

  @Override
  public void putWithBatch(BatchOperation batch, KEY key,
                           VALUE value) throws IOException {
    table.putWithBatch(batch, key, value);
  }

  @Override
  public boolean isEmpty() throws IOException {
    return table.isEmpty();
  }

  @Override
  public void delete(KEY key) throws IOException {
    table.delete(key);
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key)
          throws IOException {
    table.deleteWithBatch(batch, key);
  }

  @Override
  public final TableIterator<KEY, ? extends KeyValue<KEY, VALUE>> iterator() {
    throw new UnsupportedOperationException("Iterating tables directly is not" +
            " supported for datanode containers due to differing schema " +
            "version.");
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
  public boolean isExist(KEY key) throws IOException {
    return table.isExist(key);
  }

  @Override
  public VALUE get(KEY key) throws IOException {
    return table.get(key);
  }

  @Override
  public VALUE getIfExist(KEY key) throws IOException {
    return table.getIfExist(key);
  }

  @Override
  public VALUE getReadCopy(KEY key) throws IOException {
    return table.getReadCopy(key);
  }

  @Override
  public List<? extends KeyValue<KEY, VALUE>> getRangeKVs(
          KEY startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {
    return table.getRangeKVs(startKey, count, filters);
  }

  @Override
  public List<? extends KeyValue<KEY, VALUE>> getSequentialRangeKVs(
          KEY startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {
    return table.getSequentialRangeKVs(startKey, count, filters);
  }

  @Override
  public void close() throws Exception {
    table.close();
  }
}
