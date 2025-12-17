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

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * Generic Table Iterator implementation that can be used for unit tests to reduce redundant mocking in tests.
 */
public class MapBackedTableIterator<V> implements Table.KeyValueIterator<String, V> {

  private Iterator<Table.KeyValue<String, V>> itr;
  private final String prefix;
  private final NavigableMap<String, V> values;

  public MapBackedTableIterator(NavigableMap<String, V> values, String prefix) {
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
    this.seek(this.values.lastKey());
  }

  @Override
  public Table.KeyValue<String, V> seek(String s) {
    this.itr = this.values.entrySet().stream()
        .filter(e -> prefix == null || e.getKey().startsWith(prefix))
        .filter(e -> e.getKey().compareTo(s) >= 0)
        .map(e -> Table.newKeyValue(e.getKey(), e.getValue())).iterator();
    Map.Entry<String, V> firstEntry = values.ceilingEntry(s);
    return firstEntry == null ? null : Table.newKeyValue(firstEntry.getKey(), firstEntry.getValue());
  }

  @Override
  public void removeFromDB() {

  }

  @Override
  public void close() {

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
