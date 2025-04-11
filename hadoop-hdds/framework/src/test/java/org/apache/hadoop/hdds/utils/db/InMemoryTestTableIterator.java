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

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Generic Table Iterator implementation that can be used for unit tests to reduce redundant mocking in tests.
 */
public class InMemoryTestTableIterator<K, V> implements Table.KeyValueIterator<K, V> {

  private Iterator<Table.KeyValue<K, V>> itr;
  private final Comparator<byte[]> comparator;
  private final Comparator<K> keyComparator;
  private final Codec<K> keyCodec;
  private final NavigableMap<K, V> values;

  public InMemoryTestTableIterator(Map<K, V> values, K prefix, Codec<K> keyCodec) throws IOException {
    byte[] startKey = prefix == null ? null : keyCodec.toPersistedFormat(prefix);
    byte[] endKey = prefix == null ? null : keyCodec.toPersistedFormat(prefix);
    if (endKey != null) {
      endKey[endKey.length - 1] += 1;
    }
    comparator = new Codec.ByteArrayComparator();
    this.keyComparator = keyCodec.comparator();
    this.keyCodec = keyCodec;
    this.values = values.entrySet().stream()
        .filter(e -> startKey == null || compare(startKey, e.getKey()) >= 0)
        .filter(e -> endKey == null || compare(endKey, e.getKey()) < 0)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v2, TreeMap::new));
    this.seekToFirst();
  }

  private int compare(byte[] b1, K key) {
    try {
      return this.comparator.compare(b1, keyCodec.toPersistedFormat(key));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void seekToFirst() {
    this.itr = this.values.entrySet().stream()
        .map(e -> Table.newKeyValue(e.getKey(), e.getValue())).iterator();
  }

  @Override
  public void seekToLast() {
    try {
      this.seek(this.values.lastKey());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Table.KeyValue<K, V> seek(K s) throws IOException {
    this.itr = this.values.entrySet().stream()
        .filter(e -> keyComparator.compare(e.getKey(), s) >= 0)
        .map(e -> Table.newKeyValue(e.getKey(), e.getValue())).iterator();
    Map.Entry<K, V> firstEntry = values.ceilingEntry(s);
    return firstEntry == null ? null : Table.newKeyValue(firstEntry.getKey(), firstEntry.getValue());
  }

  @Override
  public void removeFromDB() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean hasNext() {
    return this.itr.hasNext();
  }

  @Override
  public Table.KeyValue<K, V> next() {
    return itr.next();
  }
}
