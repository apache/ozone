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

import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_AND_VALUE;
import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_ONLY;
import static org.apache.hadoop.hdds.utils.db.IteratorType.NEITHER;
import static org.apache.hadoop.hdds.utils.db.IteratorType.VALUE_ONLY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
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
      "First", "Second", "Third", "Fourth", "Fifth", "Sixth", "Seventh", "Eighth");

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

  static <V> Map<Long, V> newMap(int numRandom, LongFunction<V> constructor) {
    final Map<Long, V> map = new HashMap<>();
    for (long n = 1; n > 0; n <<= 1) {
      put(map, n, constructor);
      put(map, n - 1, constructor);
      put(map, n + 1, constructor);
    }
    put(map, Long.MAX_VALUE, constructor);
    for (int i = 0; i < numRandom; i++) {
      final long key = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE) + 1;
      put(map, key, constructor);
    }
    System.out.println("generated " + map.size() + " keys");
    return map;
  }

  @Test
  public void testEmptyByteArray() throws Exception {
    final TypedTable<byte[], byte[]> table = newTypedTable(7, ByteArrayCodec.get(), ByteArrayCodec.get());
    final byte[] empty = {};
    final byte[] nonEmpty = "123".getBytes(StandardCharsets.UTF_8);
    runTestSingleKeyValue(empty, empty, table);
    runTestSingleKeyValue(empty, nonEmpty, table);
    runTestSingleKeyValue(nonEmpty, nonEmpty, table);
    runTestSingleKeyValue(nonEmpty, empty, table);
  }

  static <K, V> void runTestSingleKeyValue(K key, V value, TypedTable<K, V> table) throws Exception {
    // The table is supposed to be empty
    try (Table.KeyValueIterator<K, V> i = table.iterator()) {
      assertFalse(i.hasNext());
    }
    assertNull(table.get(key));

    // test put and then get
    table.put(key, value);
    assertEqualsSupportingByteArray(value, table.get(key));

    // test iterator
    try (Table.KeyValueIterator<K, V> i = table.iterator()) {
      assertTrue(i.hasNext());
      final Table.KeyValue<K, V> next = i.next();
      assertEqualsSupportingByteArray(key, next.getKey());
      assertEqualsSupportingByteArray(value, next.getValue());
      assertFalse(i.hasNext());
    }

    // test delete
    table.delete(key);
    assertNull(table.get(key));
  }

  static <T> void assertEqualsSupportingByteArray(T left, T right) {
    if (left instanceof byte[] || right instanceof byte[]) {
      assertArrayEquals((byte[]) left, (byte[]) right);
    } else {
      assertEquals(left, right);
    }
  }

  @Test
  public void testEmptyStringCodecBuffer() throws Exception {
    final StringCodec codec = StringCodec.get();
    assertTrue(codec.supportCodecBuffer());
    runTestEmptyString(codec);
  }

  @Test
  public void testEmptyStringByteArray() throws Exception {
    final Codec<String> codec = CodecTestUtil.newCodecWithoutCodecBuffer(StringCodec.get());
    assertFalse(codec.supportCodecBuffer());
    runTestEmptyString(codec);
  }

  void runTestEmptyString(Codec<String> codec) throws Exception {
    final TypedTable<String, String> table = newTypedTable(8, codec, codec);
    final String empty = "";
    final String nonEmpty = "123";
    runTestSingleKeyValue(empty, empty, table);
    runTestSingleKeyValue(empty, nonEmpty, table);
    runTestSingleKeyValue(nonEmpty, nonEmpty, table);
    runTestSingleKeyValue(nonEmpty, empty, table);
  }

  @Test
  public void testContainerIDvsLong() throws Exception {
    final Map<Long, ContainerID> keys = newMap(1000, ContainerID::valueOf);

    // Table 1: ContainerID -> String
    // Table 2: Long -> String
    final TypedTable<ContainerID, String> idTable = newTypedTable(1, ContainerID.getCodec(), StringCodec.get());
    final TypedTable<Long, String> longTable = newTypedTable(2, LongCodec.get(), StringCodec.get());

    for (Map.Entry<Long, ContainerID> e : keys.entrySet()) {
      final long n = e.getKey();
      final ContainerID id = e.getValue();
      final String value = id.toString();
      // put the same value to both tables
      idTable.put(id, value);
      longTable.put(n, value);
    }

    // Reopen tables with different key types

    // Table 1: Long -> String
    // Table 2: ContainerID -> String
    final TypedTable<ContainerID, String> idTable2 = newTypedTable(2, ContainerID.getCodec(), StringCodec.get());
    final TypedTable<Long, String> longTable2 = newTypedTable(1, LongCodec.get(), StringCodec.get());

    for (Map.Entry<Long, ContainerID> e : keys.entrySet()) {
      final long n = e.getKey();
      final ContainerID id = e.getValue();
      final String expected = id.toString();
      // Read the value using a different key type
      final String idValue = idTable2.get(id);
      assertEquals(expected, idValue);
      final String longValue = longTable2.get(n);
      assertEquals(expected, longValue);
    }
  }

  @Test
  public void testIteratorWithoutPrefixByteArray() throws Exception {
    final Codec<Long> keyCodec = CodecTestUtil.newCodecWithoutCodecBuffer(LongCodec.get());
    assertFalse(keyCodec.supportCodecBuffer());
    runTestIteratorWithoutPrefix(3, keyCodec);
  }

  @Test
  public void testIteratorWithoutPrefixCodecBuffer() throws Exception {
    final LongCodec keyCodec = LongCodec.get();
    assertTrue(keyCodec.supportCodecBuffer());
    runTestIteratorWithoutPrefix(4, keyCodec);
  }

  void runTestIteratorWithoutPrefix(int tableIndex, Codec<Long> keyCodec) throws Exception {
    final Map<Long, ContainerID> keys = newMap(10000, ContainerID::valueOf);
    final TypedTable<Long, ContainerID> table = newTypedTable(tableIndex, keyCodec, ContainerID.getCodec());
    for (Map.Entry<Long, ContainerID> e : keys.entrySet()) {
      table.put(e.getKey(), e.getValue());
    }
    runTestIterators(null, keys, table);
  }

  static <K> void runTestIterators(K prefix, Map<K, ContainerID> map, TypedTable<K, ContainerID> table)
      throws Exception {
    try (Table.KeyValueIterator<K, ContainerID> neither = table.iterator(prefix, NEITHER);
         Table.KeyValueIterator<K, ContainerID> keyOnly = table.iterator(prefix, KEY_ONLY);
         Table.KeyValueIterator<K, ContainerID> valueOnly = table.iterator(prefix, VALUE_ONLY);
         Table.KeyValueIterator<K, ContainerID> keyAndValue = table.iterator(prefix, KEY_AND_VALUE);
         TableIterator<K, K> keyIterator = table.keyIterator(prefix);
         TableIterator<K, ContainerID> valueIterator = table.valueIterator(prefix)) {
      while (keyAndValue.hasNext()) {
        final KeyValue<K, ContainerID> keyValue = keyAndValue.next();
        final K expectedKey = Objects.requireNonNull(keyValue.getKey());
        assertIterator(expectedKey, keyIterator);

        final ContainerID expectedValue = map.remove(expectedKey);
        assertEquals(expectedValue, Objects.requireNonNull(keyValue.getValue()));
        assertIterator(expectedValue, valueIterator);

        final int expectedValueSize = keyValue.getValueByteSize();
        assertEquals(ContainerID.getCodec().toPersistedFormat(expectedValue).length, expectedValueSize);

        assertIterator(expectedKey, null, -1, keyOnly);
        assertIterator(null, expectedValue, expectedValueSize, valueOnly);
        assertIterator(null, null, -1, neither);
      }

      assertFalse(keyIterator.hasNext());
      assertFalse(valueIterator.hasNext());
      assertFalse(keyOnly.hasNext());
      assertFalse(valueOnly.hasNext());
      assertFalse(neither.hasNext());
    }

    assertEquals(0, map.size());
  }

  static <K, V> void assertIterator(V expected, TableIterator<K, V> iterator) {
    assertTrue(iterator.hasNext());
    final V computed = iterator.next();
    assertEquals(expected, computed);
  }

  static <K, V> void assertIterator(K expectedKey, V expectedValue, int expectedValueSize,
      Table.KeyValueIterator<K, V> iterator) {
    assertTrue(iterator.hasNext());
    final KeyValue<K, V> computed = iterator.next();
    assertEquals(expectedKey, computed.getKey());
    assertEquals(expectedValue, computed.getValue());
    assertEquals(expectedValueSize, computed.getValueByteSize());
  }

  @Test
  public void testIteratorWithPrefixCodecBuffer() throws Exception {
    final StringCodec keyCodec = StringCodec.get();
    assertTrue(keyCodec.supportCodecBuffer());
    runTestIteratorWithPrefix(5, keyCodec);
  }

  @Test
  public void testIteratorWithPrefixByteArray() throws Exception {
    final Codec<String> keyCodec = CodecTestUtil.newCodecWithoutCodecBuffer(StringCodec.get());
    assertFalse(keyCodec.supportCodecBuffer());
    runTestIteratorWithPrefix(6, keyCodec);
  }

  void runTestIteratorWithPrefix(int tableIndex, Codec<String> keyCodec) throws Exception {
    final TypedTable<String, ContainerID> table = newTypedTable(tableIndex, keyCodec, ContainerID.getCodec());
    final Map<Long, ContainerID> keys = newMap(10_000, ContainerID::valueOf);
    for (Map.Entry<Long, ContainerID> e : keys.entrySet()) {
      final ContainerID id = e.getValue();
      final String key = id.toString();
      table.put(key, id);
    }

    for (int numDigits = 1; numDigits < 10; numDigits++) {
      runTestIteratorWithPrefix(numDigits, keys, table);
    }
  }

  static void runTestIteratorWithPrefix(int prefixLength, Map<Long, ContainerID> keys,
      TypedTable<String, ContainerID> table) throws Exception {
    final Map<String, Map<String, ContainerID>> prefixMap = new TreeMap<>();
    int shortIdCount = 0;
    for (Map.Entry<Long, ContainerID> e : keys.entrySet()) {
      final ContainerID id = e.getValue();
      final String key = id.toString();
      if (key.length() < prefixLength) {
        shortIdCount++;
      } else {
        final String prefix = key.substring(0, prefixLength);
        prefixMap.computeIfAbsent(prefix, k -> new TreeMap<>()).put(key, id);
      }
    }

    // check size
    final int size = prefixMap.values().stream().map(Map::size).reduce(0, Integer::sum);
    assertEquals(keys.size(), size + shortIdCount);

    for (Map.Entry<String, Map<String, ContainerID>> e : prefixMap.entrySet()) {
      runTestIterators(e.getKey(), e.getValue(), table);
    }
  }
}
