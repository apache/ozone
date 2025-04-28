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

package org.apache.hadoop.hdds.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

/**
 * Generic Table Iterator implementation that can be used for unit tests to reduce redundant mocking in tests.
 */
public class MapBackedTableIterator<V> implements TableIterator<String, Table.KeyValue<String, V>> {

  private Iterator<Table.KeyValue<String, V>> itr;
  private final String prefix;
  private final TreeMap<String, V> values;

  public MapBackedTableIterator(TreeMap<String, V> values, String prefix) {
    this.prefix = prefix;
    this.values = values;
    this.seekToFirst();
  }

  @Override
  public void seekToFirst() {
    this.itr = this.values.entrySet().stream()
        .filter(e -> prefix == null || e.getKey().startsWith(prefix))
        .map(e -> Table.newKeyValue(e.getKey(), e.getValue())).iterator();
  }

  @Override
  public void seekToLast() {

  }

  @Override
  public Table.KeyValue<String, V> seek(String s) throws IOException {
    this.itr = this.values.entrySet().stream()
        .filter(e -> prefix == null || e.getKey().startsWith(prefix))
        .filter(e -> e.getKey().compareTo(s) >= 0)
        .map(e -> Table.newKeyValue(e.getKey(), e.getValue())).iterator();
    Map.Entry<String, V> firstEntry = values.ceilingEntry(s);
    return firstEntry == null ? null : Table.newKeyValue(firstEntry.getKey(), firstEntry.getValue());
  }

  @Override
  public void removeFromDB() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean hasNext() {
    return this.itr.hasNext();
  }

  @Override
  public Table.KeyValue<String, V> next() {
    return itr.next();
  }
}
