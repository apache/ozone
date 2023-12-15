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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.utils.db;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

/**
 * Tests for RocksDBTable Store.
 */
public class TestTypedRDBTableStore {
  public static final int MAX_DB_UPDATES_SIZE_THRESHOLD = 80;
  private static int count = 0;
  private final List<String> families =
      Arrays.asList(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
          "First", "Second", "Third",
          "Fourth", "Fifth",
          "Sixth", "Seven", "Eighth",
          "Ninth", "Ten");
  private RDBStore rdbStore = null;
  private ManagedDBOptions options = null;
  private CodecRegistry codecRegistry;

  @BeforeEach
  public void setUp(@TempDir File tempDir) throws Exception {
    CodecBuffer.enableLeakDetection();

    options = new ManagedDBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);

    Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    options.setStatistics(statistics);

    Set<TableConfig> configSet = new HashSet<>();
    for (String name : families) {
      TableConfig newConfig = new TableConfig(name,
          new ManagedColumnFamilyOptions());
      configSet.add(newConfig);
    }
    rdbStore = TestRDBStore.newRDBStore(tempDir, options, configSet,
        MAX_DB_UPDATES_SIZE_THRESHOLD);

    codecRegistry = CodecRegistry.newBuilder().build();

  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rdbStore != null) {
      rdbStore.close();
    }
    CodecBuffer.assertNoLeaks();
  }

  @Test
  public void putGetAndEmpty() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "First")) {
      String key =
          RandomStringUtils.random(10);
      String value = RandomStringUtils.random(10);
      testTable.put(key, value);
      Assertions.assertFalse(testTable.isEmpty());
      String readValue = testTable.get(key);
      Assertions.assertEquals(value, readValue);
    }
    try (Table secondTable = rdbStore.getTable("Second")) {
      Assertions.assertTrue(secondTable.isEmpty());
    }
  }

  private Table<String, String> createTypedTable(String name)
      throws IOException {
    return new TypedTable<String, String>(
        rdbStore.getTable(name),
        codecRegistry,
        String.class, String.class);
  }

  @Test
  public void delete() throws Exception {
    List<String> deletedKeys = new LinkedList<>();
    List<String> validKeys = new LinkedList<>();
    String value =
        RandomStringUtils.random(10);
    for (int x = 0; x < 100; x++) {
      deletedKeys.add(
          RandomStringUtils.random(10));
    }

    for (int x = 0; x < 100; x++) {
      validKeys.add(
          RandomStringUtils.random(10));
    }

    // Write all the keys and delete the keys scheduled for delete.
    //Assert we find only expected keys in the Table.
    try (Table<String, String> testTable = createTypedTable(
        "Fourth")) {
      for (int x = 0; x < deletedKeys.size(); x++) {
        testTable.put(deletedKeys.get(x), value);
        testTable.delete(deletedKeys.get(x));
      }

      for (int x = 0; x < validKeys.size(); x++) {
        testTable.put(validKeys.get(x), value);
      }

      for (int x = 0; x < validKeys.size(); x++) {
        Assertions.assertNotNull(testTable.get(validKeys.get(0)));
      }

      for (int x = 0; x < deletedKeys.size(); x++) {
        Assertions.assertNull(testTable.get(deletedKeys.get(0)));
      }
    }
  }

  @Test
  public void batchPut() throws Exception {

    try (Table<String, String> testTable = createTypedTable(
        "Fourth");
        BatchOperation batch = rdbStore.initBatchOperation()) {
      //given
      String key =
          RandomStringUtils.random(10);
      String value =
          RandomStringUtils.random(10);

      //when
      testTable.putWithBatch(batch, key, value);
      rdbStore.commitBatchOperation(batch);

      //then
      Assertions.assertNotNull(testTable.get(key));
    }
  }

  @Test
  public void batchDelete() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "Fourth");
        BatchOperation batch = rdbStore.initBatchOperation()) {

      //given
      String key =
          RandomStringUtils.random(10);
      String value =
          RandomStringUtils.random(10);
      testTable.put(key, value);

      //when
      testTable.deleteWithBatch(batch, key);
      rdbStore.commitBatchOperation(batch);

      //then
      Assertions.assertNull(testTable.get(key));
    }
  }

  private static boolean consume(Table.KeyValue keyValue) {
    count++;
    try {
      Assertions.assertNotNull(keyValue.getKey());
    } catch (IOException ex) {
      Assertions.fail(ex.toString());
    }
    return true;
  }

  @Test
  public void forEachAndIterator() throws Exception {
    final int iterCount = 100;
    try (Table<String, String> testTable = createTypedTable(
        "Sixth")) {
      for (int x = 0; x < iterCount; x++) {
        String key =
            RandomStringUtils.random(10);
        String value =
            RandomStringUtils.random(10);
        testTable.put(key, value);
      }
      int localCount = 0;

      try (TableIterator<String, ? extends KeyValue<String, String>> iter =
          testTable.iterator()) {
        while (iter.hasNext()) {
          Table.KeyValue keyValue = iter.next();
          localCount++;
        }

        Assertions.assertEquals(iterCount, localCount);
        iter.seekToFirst();
        iter.forEachRemaining(TestTypedRDBTableStore::consume);
        Assertions.assertEquals(iterCount, count);

      }
    }
  }

  @Test
  public void testIteratorOnException() throws Exception {
    RDBTable rdbTable = Mockito.mock(RDBTable.class);
    Mockito.when(rdbTable.iterator((CodecBuffer) null))
        .thenThrow(new IOException());
    try (Table<String, String> testTable = new TypedTable<>(rdbTable,
        codecRegistry, String.class, String.class)) {
      Assertions.assertThrows(IOException.class, testTable::iterator);
    }
  }

  @Test
  public void testTypedTableWithCache() throws Exception {
    int iterCount = 10;
    try (Table<String, String> testTable = createTypedTable(
        "Seven")) {

      for (int x = 0; x < iterCount; x++) {
        String key = Integer.toString(x);
        String value = Integer.toString(x);
        testTable.addCacheEntry(new CacheKey<>(key),
            CacheValue.get(x, value));
      }

      // As we have added to cache, so get should return value even if it
      // does not exist in DB.
      for (int x = 0; x < iterCount; x++) {
        Assertions.assertEquals(Integer.toString(1),
            testTable.get(Integer.toString(1)));
      }

    }
  }

  @Test
  public void testTypedTableWithCacheWithFewDeletedOperationType()
      throws Exception {
    int iterCount = 10;
    try (Table<String, String> testTable = createTypedTable(
        "Seven")) {

      for (int x = 0; x < iterCount; x++) {
        String key = Integer.toString(x);
        String value = Integer.toString(x);
        if (x % 2 == 0) {
          testTable.addCacheEntry(new CacheKey<>(key),
              CacheValue.get(x, value));
        } else {
          testTable.addCacheEntry(new CacheKey<>(key),
              CacheValue.get(x));
        }
      }

      // As we have added to cache, so get should return value even if it
      // does not exist in DB.
      for (int x = 0; x < iterCount; x++) {
        if (x % 2 == 0) {
          Assertions.assertEquals(Integer.toString(x),
              testTable.get(Integer.toString(x)));
        } else {
          Assertions.assertNull(testTable.get(Integer.toString(x)));
        }
      }

      ArrayList<Long> epochs = new ArrayList<>();
      for (long i = 0; i <= 5L; i++) {
        epochs.add(i);
      }
      testTable.cleanupCache(epochs);

      GenericTestUtils.waitFor(() ->
          ((TypedTable<String, String>) testTable).getCache().size() == 4,
          100, 5000);


      //Check remaining values
      for (int x = 6; x < iterCount; x++) {
        if (x % 2 == 0) {
          Assertions.assertEquals(Integer.toString(x),
              testTable.get(Integer.toString(x)));
        } else {
          Assertions.assertNull(testTable.get(Integer.toString(x)));
        }
      }


    }
  }

  @Test
  public void testIsExist() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "Eighth")) {
      String key =
          RandomStringUtils.random(10);
      String value = RandomStringUtils.random(10);
      testTable.put(key, value);
      Assertions.assertTrue(testTable.isExist(key));

      String invalidKey = key + RandomStringUtils.random(1);
      Assertions.assertFalse(testTable.isExist(invalidKey));

      testTable.delete(key);
      Assertions.assertFalse(testTable.isExist(key));
    }
  }

  @Test
  public void testGetIfExist() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "Eighth")) {
      String key =
          RandomStringUtils.random(10);
      String value = RandomStringUtils.random(10);
      testTable.put(key, value);
      Assertions.assertNotNull(testTable.getIfExist(key));

      String invalidKey = key + RandomStringUtils.random(1);
      Assertions.assertNull(testTable.getIfExist(invalidKey));

      testTable.delete(key);
      Assertions.assertNull(testTable.getIfExist(key));
    }
  }

  @Test
  public void testIsExistCache() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "Eighth")) {
      String key =
          RandomStringUtils.random(10);
      String value = RandomStringUtils.random(10);
      testTable.addCacheEntry(new CacheKey<>(key),
          CacheValue.get(1L, value));
      Assertions.assertTrue(testTable.isExist(key));

      testTable.addCacheEntry(new CacheKey<>(key),
          CacheValue.get(1L));
      Assertions.assertFalse(testTable.isExist(key));
    }
  }

  @Test
  public void testCountEstimatedRowsInTable() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "Ninth")) {
      // Add a few keys
      final int numKeys = 12345;
      for (int i = 0; i < numKeys; i++) {
        String key =
            RandomStringUtils.random(10);
        String value = RandomStringUtils.random(10);
        testTable.put(key, value);
      }
      long keyCount = testTable.getEstimatedKeyCount();
      // The result should be larger than zero but not exceed(?) numKeys
      Assertions.assertTrue(keyCount > 0 && keyCount <= numKeys);
    }
  }

  @Test
  public void testByteArrayTypedTable() throws Exception {
    try (Table<byte[], byte[]> testTable = new TypedTable<>(
            rdbStore.getTable("Ten"),
            codecRegistry,
            byte[].class, byte[].class)) {
      byte[] key = new byte[] {1, 2, 3};
      byte[] value = new byte[] {4, 5, 6};
      testTable.put(key, value);
      byte[] actualValue = testTable.get(key);
      Assertions.assertArrayEquals(value, testTable.get(key));
      Assertions.assertNotSame(value, actualValue);
      testTable.addCacheEntry(new CacheKey<>(key),
              CacheValue.get(1L, value));
      Assertions.assertSame(value, testTable.get(key));
    }
  }
}
