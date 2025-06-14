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

import static org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator.Type.KEY_AND_VALUE;
import static org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator.Type.KEY_ONLY;
import static org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator.Type.NEITHER;
import static org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator.Type.VALUE_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;

/**
 * Tests for RocksDBTable Store.
 */
public class TestTypedTable {
  private final List<String> families = Arrays.asList(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
      "First", "Second");

  private RDBStore rdb;
  private final List<UncheckedAutoCloseable> closeables = new ArrayList<>();

  static TableConfig newTableConfig(String name, List<UncheckedAutoCloseable> closeables) {
    final ManagedColumnFamilyOptions option = new ManagedColumnFamilyOptions();
    closeables.add(option::close);
    return new TableConfig(name, option);
  }

  @BeforeEach
  public void setUp(@TempDir File tempDir) throws Exception {
    CodecBuffer.enableLeakDetection();

    final Set<TableConfig> configSet = families.stream()
        .map(name -> newTableConfig(name, closeables))
        .collect(Collectors.toSet());
    final ManagedDBOptions options = TestRDBStore.newManagedDBOptions();
    closeables.add(options::close);
    rdb = TestRDBStore.newRDBStore(tempDir, options, configSet);
  }

  @AfterEach
  public void tearDown() throws Exception {
    rdb.close();
    closeables.forEach(UncheckedAutoCloseable::close);
    closeables.clear();
    CodecBuffer.assertNoLeaks();
  }

  <K, V> TypedTable<K, V> newTypedTable(int index, Codec<K> keyCodec, Codec<V> valueCodec) throws IOException {
    final RDBTable rawTable = rdb.getTable(families.get(index));
    return new TypedTable<>(rawTable, keyCodec, valueCodec, TableCache.CacheType.PARTIAL_CACHE);
  }

  static <V> V put(Map<Long, V> map, long key, LongFunction<V> constructor) {
    return map.put(key, constructor.apply(key));
  }

  static <V> Map<Long, V> newMap(LongFunction<V> constructor) {
    final Map<Long, V> map = new HashMap<>();
    for (long n = 1; n > 0; n <<= 1) {
      put(map, n, constructor);
      put(map, n - 1, constructor);
      put(map, n + 1, constructor);
    }
    put(map, Long.MAX_VALUE, constructor);
    for (int i = 0; i < 1000; i++) {
      final long key = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE) + 1;
      put(map, key, constructor);
    }
    System.out.println("generated " + map.size() + " keys");
    return map;
  }

  @Test
  public void testContainerIDvsLong() throws Exception {
    final Map<Long, ContainerID> keys = newMap(ContainerID::valueOf);

    // Table 1: ContainerID -> String
    // Table 2: Long -> String
    try (TypedTable<ContainerID, String> idTable = newTypedTable(
        1, ContainerID.getCodec(), StringCodec.get());
         TypedTable<Long, String> longTable = newTypedTable(
             2, LongCodec.get(), StringCodec.get())) {

      for (Map.Entry<Long, ContainerID> e : keys.entrySet()) {
        final long n = e.getKey();
        final ContainerID id = e.getValue();
        final String value = id.toString();
        // put the same value to both tables
        idTable.put(id, value);
        longTable.put(n, value);
      }
    }

    // Reopen tables with different key types

    // Table 1: Long -> String
    // Table 2: ContainerID -> String
    try (TypedTable<ContainerID, String> idTable = newTypedTable(
        2, ContainerID.getCodec(), StringCodec.get());
         TypedTable<Long, String> longTable = newTypedTable(
             1, LongCodec.get(), StringCodec.get())) {

      for (Map.Entry<Long, ContainerID> e : keys.entrySet()) {
        final long n = e.getKey();
        final ContainerID id = e.getValue();
        final String expected = id.toString();
        // Read the value using a different key type
        final String idValue = idTable.get(id);
        assertEquals(expected, idValue);
        final String longValue = longTable.get(n);
        assertEquals(expected, longValue);
      }
    }

    // test iterator type
    try (TypedTable<Long, String> longTable = newTypedTable(
        1, LongCodec.get(), StringCodec.get());
         Table.KeyValueIterator<Long, String> neither = longTable.iterator(null, NEITHER);
         Table.KeyValueIterator<Long, String> keyOnly = longTable.iterator(null, KEY_ONLY);
         Table.KeyValueIterator<Long, String> valueOnly = longTable.iterator(null, VALUE_ONLY);
         Table.KeyValueIterator<Long, String> keyAndValue = longTable.iterator(null, KEY_AND_VALUE)) {
      while (keyAndValue.hasNext()) {
        final Table.KeyValue<Long, String> keyValue = keyAndValue.next();
        final Long expectedKey = Objects.requireNonNull(keyValue.getKey());

        final String expectedValue = Objects.requireNonNull(keyValue.getValue());
        assertEquals(keys.get(expectedKey).toString(), expectedValue);

        final int expectedValueSize = keyValue.getValueByteSize();
        assertEquals(expectedValue.length(), expectedValueSize);

        assertKeyValue(expectedKey, null, 0, keyOnly);
        assertKeyValue(null, expectedValue, expectedValueSize, valueOnly);
        assertKeyValue(null, null, 0, neither);
      }

      assertFalse(keyOnly.hasNext());
      assertFalse(valueOnly.hasNext());
      assertFalse(neither.hasNext());
    }
  }

  static <K, V> void assertKeyValue(K expectedKey, V expectedValue, int expectedValueSize,
      Table.KeyValueIterator<K, V> iterator) {
    assertTrue(iterator.hasNext());
    final KeyValue<K, V> computed = iterator.next();
    assertEquals(expectedKey, computed.getKey());
    assertEquals(expectedValue, computed.getValue());
    assertEquals(expectedValueSize, computed.getValueByteSize());
  }
}
