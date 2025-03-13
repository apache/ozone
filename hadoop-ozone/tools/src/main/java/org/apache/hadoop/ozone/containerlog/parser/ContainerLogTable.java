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

package org.apache.hadoop.ozone.containerlog.parser;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

/**
 * Container log table.
 */

public class ContainerLogTable<KEY, VALUE> implements Table<KEY, VALUE> {
  private final Table<KEY, VALUE> table;

  public ContainerLogTable(Table<KEY, VALUE> table) {
    this.table = table;
  }

  @Override
  public void put(KEY key, VALUE value) throws IOException {
    table.put(key, value);
  }

  public Table<KEY, VALUE> getTable() {
    return table;
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
  public void deleteRange(KEY beginKey, KEY endKey) throws IOException {
    table.deleteRange(beginKey, endKey);
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key)
      throws IOException {
    table.deleteWithBatch(batch, key);
  }

  @Override
  public final TableIterator<KEY, ? extends Table.KeyValue<KEY, VALUE>> iterator() throws IOException {
    return new ContainerLogTableIterator<>(this);
  }

  @Override
  public final TableIterator<KEY, ? extends Table.KeyValue<KEY, VALUE>> iterator(
      KEY prefix) {
    throw new UnsupportedOperationException("Iterating tables directly is not" +
        " supported");
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
  public List<? extends Table.KeyValue<KEY, VALUE>> getRangeKVs(
      KEY startKey, int count, KEY prefix,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return table.getRangeKVs(startKey, count, prefix, filters);
  }

  @Override
  public List<? extends Table.KeyValue<KEY, VALUE>> getSequentialRangeKVs(
      KEY startKey, int count, KEY prefix,
      MetadataKeyFilters.MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException {
    return table.getSequentialRangeKVs(startKey, count, prefix, filters);
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, KEY prefix)
      throws IOException {
    table.deleteBatchWithPrefix(batch, prefix);
  }

  @Override
  public void dumpToFileWithPrefix(File externalFile, KEY prefix)
      throws IOException {
    table.dumpToFileWithPrefix(externalFile, prefix);
  }

  @Override
  public void loadFromFile(File externalFile) throws IOException {
    table.loadFromFile(externalFile);
  }

  @Override
  public void close() throws Exception {
    table.close();
  }
}
