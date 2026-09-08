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

import com.google.common.primitives.UnsignedBytes;
import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;

/**
 * InMemory Table implementation for tests.
 */
public class InMemoryTestTable<KEY, VALUE> implements Table<KEY, VALUE> {
  private final NavigableMap<KEY, VALUE> map;
  private final String name;

  public InMemoryTestTable() {
    this("");
  }

  public InMemoryTestTable(Map<KEY, VALUE> map) {
    this(map, "");
  }

  public InMemoryTestTable(String name) {
    this(Collections.emptyMap(), name);
  }

  public InMemoryTestTable(Map<KEY, VALUE> map, String name) {
    this(new ConcurrentSkipListMap<>(map), name);
  }

  private InMemoryTestTable(NavigableMap<KEY, VALUE> map, String name) {
    this.map = map;
    this.name = name;
  }

  /** Raw {@code byte[]}/{@code byte[]} table with unsigned lexicographic key order. */
  public static InMemoryTestTable<byte[], byte[]> forRawBytes() {
    return new InMemoryTestTable<>(
        new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator()), "raw");
  }

  /** Raw {@code byte[]}/{@code byte[]} table with unsigned lexicographic key order. */
  public static InMemoryTestTable<byte[], byte[]> forRawBytes(String name) {
    return new InMemoryTestTable<>(
        new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator()), name);
  }

  @Override
  public void put(KEY key, VALUE value) {
    map.put(key, value);
  }

  @Override
  public void putWithBatch(BatchOperation batch, KEY key, VALUE value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean isExist(KEY key) {
    return map.containsKey(key);
  }

  @Override
  public VALUE get(KEY key) {
    return map.get(key);
  }

  @Override
  public VALUE getIfExist(KEY key) {
    return map.get(key);
  }

  @Override
  public void delete(KEY key) {
    map.remove(key);
  }

  @Override
  public void deleteWithBatch(BatchOperation batch, KEY key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteRange(KEY beginKey, KEY endKey) {
    map.subMap(beginKey, endKey).clear();
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public KeyValueIterator<KEY, VALUE> iterator(KEY prefix, IteratorType type) {
    if (prefix instanceof byte[]) {
      return new InMemoryKeyValueIterator<>(map, type, (byte[]) prefix);
    }
    NavigableMap<KEY, VALUE> view;
    if (prefix == null) {
      view = map;
    } else if (prefix instanceof String) {
      String endPrefix = (String) prefix + Character.MAX_VALUE;
      view = map.subMap(prefix, true, (KEY) endPrefix, false);
    } else {
      view = map.tailMap(prefix, true);
    }
    return new InMemoryKeyValueIterator<>(view, type, null);
  }

  private static final class InMemoryKeyValueIterator<KEY, VALUE>
      implements KeyValueIterator<KEY, VALUE> {
    private final Iterator<Map.Entry<KEY, VALUE>> entries;
    private final IteratorType type;
    private final byte[] bytePrefix;
    private Map.Entry<KEY, VALUE> lookahead;

    private InMemoryKeyValueIterator(NavigableMap<KEY, VALUE> map, IteratorType type,
        byte[] bytePrefix) {
      if (bytePrefix == null || bytePrefix.length == 0) {
        this.entries = map.entrySet().iterator();
      } else {
        this.entries = map.tailMap((KEY) bytePrefix, true).entrySet().iterator();
      }
      this.type = type;
      this.bytePrefix = bytePrefix;
    }

    private boolean startsWithPrefix(byte[] key) {
      if (bytePrefix == null || bytePrefix.length == 0) {
        return true;
      }
      if (key == null || key.length < bytePrefix.length) {
        return false;
      }
      for (int i = 0; i < bytePrefix.length; i++) {
        if (key[i] != bytePrefix[i]) {
          return false;
        }
      }
      return true;
    }

    private Map.Entry<KEY, VALUE> advance() {
      while (entries.hasNext()) {
        Map.Entry<KEY, VALUE> entry = entries.next();
        if (!(entry.getKey() instanceof byte[])
            || startsWithPrefix((byte[]) entry.getKey())) {
          return entry;
        }
      }
      return null;
    }

    @Override
    public void seekToFirst() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seekToLast() {
      throw new UnsupportedOperationException();
    }

    @Override
    public KeyValue<KEY, VALUE> seek(KEY key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
      if (lookahead != null) {
        return true;
      }
      lookahead = advance();
      return lookahead != null;
    }

    @Override
    public KeyValue<KEY, VALUE> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Map.Entry<KEY, VALUE> entry = lookahead;
      lookahead = null;
      switch (type) {
      case KEY_ONLY:
        return Table.newKeyValue(entry.getKey(), null);
      case VALUE_ONLY:
        return Table.newKeyValue(null, entry.getValue());
      case KEY_AND_VALUE:
      default:
        return Table.newKeyValue(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public void removeFromDB() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long getEstimatedKeyCount() {
    return map.size();
  }

  @Override
  public List<KeyValue<KEY, VALUE>> getRangeKVs(
      KEY startKey, int count, KEY prefix, KeyPrefixFilter filter, boolean isSequential) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteBatchWithPrefix(BatchOperation batch, KEY prefix) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dumpToFileWithPrefix(File externalFile, KEY prefix) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadFromFile(File externalFile) {
    throw new UnsupportedOperationException();
  }

  public NavigableMap<KEY, VALUE> getMap() {
    return map;
  }
}
