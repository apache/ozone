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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Test persistent map backed by RocksDB.
 */
public class TestRocksDbPersistentMap {
  @TempDir
  private static Path tempDir;
  private static ManagedRocksDB db;
  private static ManagedDBOptions dbOptions;
  private static ManagedColumnFamilyOptions columnFamilyOptions;

  private static AtomicInteger id;

  @BeforeAll
  public static void staticInit() throws RocksDBException {
    dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    columnFamilyOptions = new ManagedColumnFamilyOptions();

    File file = tempDir.resolve("./test-persistent-map").toFile();
    if (!file.mkdirs() && !file.exists()) {
      throw new IllegalArgumentException("Unable to create directory " +
          file);
    }

    String absolutePath = Paths.get(file.toString(), "rocks.db").toFile()
        .getAbsolutePath();

    List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        Collections.singletonList(new ColumnFamilyDescriptor(
            StringUtils.string2Bytes(DEFAULT_COLUMN_FAMILY_NAME),
            columnFamilyOptions));

    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    db = ManagedRocksDB.open(dbOptions, absolutePath, columnFamilyDescriptors,
        columnFamilyHandles);
    id = new AtomicInteger(0);
  }

  @AfterAll
  public static void teardown() throws RocksDBException {
    if (dbOptions != null) {
      dbOptions.close();
    }
    if (columnFamilyOptions != null) {
      columnFamilyOptions.close();
    }
    if (db != null) {
      db.close();
    }
  }

  @Test
  public void testRocksDBPersistentMap() throws IOException, RocksDBException {
    ColumnFamilyHandle columnFamily = null;
    try {
      final CodecRegistry codecRegistry = CodecRegistry.newBuilder().build();
      columnFamily = db.get().createColumnFamily(new ColumnFamilyDescriptor(
          codecRegistry.asRawData("testMap" + id.incrementAndGet()),
          columnFamilyOptions));

      PersistentMap<String, String> persistentMap = new RocksDbPersistentMap<>(
          db,
          columnFamily,
          codecRegistry,
          String.class,
          String.class
      );

      List<String> keys = Arrays.asList("Key1", "Key2", "Key3", "Key1", "Key2");
      List<String> values =
          Arrays.asList("value1", "value2", "Value3", "Value1", "Value2");

      Map<String, String> expectedMap = new HashMap<>();

      for (int i = 0; i < keys.size(); i++) {
        String key = keys.get(i);
        String value = values.get(i);

        persistentMap.put(key, value);
        expectedMap.put(key, value);
      }

      for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
        assertEquals(entry.getValue(), persistentMap.get(entry.getKey()));
      }
    } finally {
      if (columnFamily != null) {
        db.get().dropColumnFamily(columnFamily);
        columnFamily.close();
      }
    }
  }

  /**
   * Test cases for testRocksDBPersistentMapIterator.
   */
  private static Stream<Arguments> rocksDBPersistentMapIteratorCases() {
    return Stream.of(
        Arguments.of(
            Optional.empty(),
            Optional.of("key202"),
            Stream.concat(IntStream.range(0, 100).boxed(),
                    IntStream.range(200, 300).boxed())
                .map(i -> Pair.of(String.format("key%03d", i),
                    String.format("value%03d", i)))
                .collect(Collectors.toList()),
            Stream.concat(IntStream.range(0, 100).boxed(),
                    IntStream.range(200, 202).boxed())
                .map(i -> Pair.of(String.format("key%03d", i),
                    String.format("value%03d", i)))
                .collect(Collectors.toList())),
        Arguments.of(Optional.of("key050"),
            Optional.empty(), Stream.concat(IntStream.range(50, 100).boxed(),
                    IntStream.range(200, 300).boxed())
                .map(i -> Pair.of(String.format("key%03d", i),
                    String.format("value%03d", i)))
                .collect(Collectors.toList()),
            Stream.concat(IntStream.range(50, 100).boxed(),
                    IntStream.range(200, 300).boxed())
                .map(i -> Pair.of(String.format("key%03d", i),
                    String.format("value%03d", i)))
                .collect(Collectors.toList())
            ),
        Arguments.of(Optional.of("key050"),
            Optional.of("key210"),
            Stream.concat(IntStream.range(50, 100).boxed(),
                    IntStream.range(200, 300).boxed())
                .map(i -> Pair.of(String.format("key%03d", i),
                    String.format("value%03d", i)))
                .collect(Collectors.toList()),
            Stream.concat(IntStream.range(50, 100).boxed(),
                    IntStream.range(200, 210).boxed())
                .map(i -> Pair.of(String.format("key%03d", i),
                    String.format("value%03d", i)))
                .collect(Collectors.toList())
        ));
  }

  @ParameterizedTest
  @MethodSource("rocksDBPersistentMapIteratorCases")
  public void testRocksDBPersistentMapIterator(Optional<String> lowerBound,
              Optional<String> upperBound, List<Pair<String, String>> keys,
              List<Pair<String, String>> expectedKeys)
      throws IOException, RocksDBException {
    ColumnFamilyHandle columnFamily = null;
    try {
      final CodecRegistry codecRegistry = CodecRegistry.newBuilder().build();
      columnFamily = db.get().createColumnFamily(new ColumnFamilyDescriptor(
          codecRegistry.asRawData("testMap" + id.incrementAndGet()),
          columnFamilyOptions));

      PersistentMap<String, String> persistentMap = new RocksDbPersistentMap<>(
          db,
          columnFamily,
          codecRegistry,
          String.class,
          String.class
      );

      for (Pair<String, String> stringStringPair : keys) {
        String key = stringStringPair.getKey();
        String value = stringStringPair.getValue();
        persistentMap.put(key, value);
      }
      ClosableIterator<Map.Entry<String, String>> iterator =
          persistentMap.iterator(lowerBound, upperBound);
      int idx = 0;
      while (iterator.hasNext()) {
        Map.Entry<String, String> e = iterator.next();
        assertEquals(Pair.of(e.getKey(), e.getValue()), expectedKeys.get(idx));
        idx += 1;
      }

    } finally {
      if (columnFamily != null) {
        db.get().dropColumnFamily(columnFamily);
        columnFamily.close();
      }
    }
  }
}
