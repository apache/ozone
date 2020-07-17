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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdds.StringUtils;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

/**
 * Tests for RocksDBTable Store.
 */
public class TestRDBTableStore {
  private static int count = 0;
  private final List<String> families =
      Arrays.asList(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
          "First", "Second", "Third",
          "Fourth", "Fifth",
          "Sixth", "Seventh",
          "Eighth");
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private RDBStore rdbStore = null;
  private DBOptions options = null;

  private static boolean consume(Table.KeyValue keyValue)  {
    count++;
    try {
      Assert.assertNotNull(keyValue.getKey());
    } catch(IOException ex) {
      Assert.fail("Unexpected Exception " + ex.toString());
    }
    return true;
  }

  @Before
  public void setUp() throws Exception {
    options = new DBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);

    Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    options = options.setStatistics(statistics);

    Set<TableConfig> configSet = new HashSet<>();
    for(String name : families) {
      TableConfig newConfig = new TableConfig(name, new ColumnFamilyOptions());
      configSet.add(newConfig);
    }
    rdbStore = new RDBStore(folder.newFolder(), options, configSet);
  }

  @After
  public void tearDown() throws Exception {
    if (rdbStore != null) {
      rdbStore.close();
    }
  }

  @Test
  public void getHandle() throws Exception {
    try (Table testTable = rdbStore.getTable("First")) {
      Assert.assertNotNull(testTable);
      Assert.assertNotNull(((RDBTable) testTable).getHandle());
    }
  }

  @Test
  public void putGetAndEmpty() throws Exception {
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("First")) {
      byte[] key =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
      Assert.assertFalse(testTable.isEmpty());
      byte[] readValue = testTable.get(key);
      Assert.assertArrayEquals(value, readValue);
    }
    try (Table secondTable = rdbStore.getTable("Second")) {
      Assert.assertTrue(secondTable.isEmpty());
    }
  }

  @Test
  public void delete() throws Exception {
    List<byte[]> deletedKeys = new ArrayList<>();
    List<byte[]> validKeys = new ArrayList<>();
    byte[] value =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    for (int x = 0; x < 100; x++) {
      deletedKeys.add(
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8));
    }

    for (int x = 0; x < 100; x++) {
      validKeys.add(
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8));
    }

    // Write all the keys and delete the keys scheduled for delete.
    //Assert we find only expected keys in the Table.
    try (Table testTable = rdbStore.getTable("Fourth")) {
      for (int x = 0; x < deletedKeys.size(); x++) {
        testTable.put(deletedKeys.get(x), value);
        testTable.delete(deletedKeys.get(x));
      }

      for (int x = 0; x < validKeys.size(); x++) {
        testTable.put(validKeys.get(x), value);
      }

      for (int x = 0; x < validKeys.size(); x++) {
        Assert.assertNotNull(testTable.get(validKeys.get(0)));
      }

      for (int x = 0; x < deletedKeys.size(); x++) {
        Assert.assertNull(testTable.get(deletedKeys.get(0)));
      }
    }
  }

  @Test
  public void batchPut() throws Exception {
    try (Table testTable = rdbStore.getTable("Fifth");
        BatchOperation batch = rdbStore.initBatchOperation()) {
      //given
      byte[] key =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      Assert.assertNull(testTable.get(key));

      //when
      testTable.putWithBatch(batch, key, value);
      rdbStore.commitBatchOperation(batch);

      //then
      Assert.assertNotNull(testTable.get(key));
    }
  }

  @Test
  public void batchDelete() throws Exception {
    try (Table testTable = rdbStore.getTable("Fifth");
        BatchOperation batch = rdbStore.initBatchOperation()) {

      //given
      byte[] key =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
      Assert.assertNotNull(testTable.get(key));


      //when
      testTable.deleteWithBatch(batch, key);
      rdbStore.commitBatchOperation(batch);

      //then
      Assert.assertNull(testTable.get(key));
    }
  }

  @Test
  public void forEachAndIterator() throws Exception {
    final int iterCount = 100;
    try (Table testTable = rdbStore.getTable("Sixth")) {
      for (int x = 0; x < iterCount; x++) {
        byte[] key =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        byte[] value =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        testTable.put(key, value);
      }
      int localCount = 0;
      try (TableIterator<byte[], Table.KeyValue> iter = testTable.iterator()) {
        while (iter.hasNext()) {
          Table.KeyValue keyValue = iter.next();
          localCount++;
        }

        Assert.assertEquals(iterCount, localCount);
        iter.seekToFirst();
        iter.forEachRemaining(TestRDBTableStore::consume);
        Assert.assertEquals(iterCount, count);

      }
    }
  }

  @Test
  public void testIsExist() throws Exception {
    DBOptions rocksDBOptions = new DBOptions();
    rocksDBOptions.setCreateIfMissing(true);
    rocksDBOptions.setCreateMissingColumnFamilies(true);

    String tableName = StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY);

    Set<TableConfig> configSet = new HashSet<>();
    TableConfig newConfig = new TableConfig(tableName,
        new ColumnFamilyOptions());
    configSet.add(newConfig);

    File rdbLocation = folder.newFolder();
    RDBStore dbStore = new RDBStore(rdbLocation, rocksDBOptions, configSet);

    byte[] key = RandomStringUtils.random(10, true, false)
        .getBytes(StandardCharsets.UTF_8);
    byte[] value = RandomStringUtils.random(10, true, false)
        .getBytes(StandardCharsets.UTF_8);

    try (Table<byte[], byte[]> testTable = dbStore.getTable(tableName)) {
      testTable.put(key, value);

      // Test if isExist returns true for a key that definitely exists.
      Assert.assertTrue(testTable.isExist(key));

      // Test if isExist returns false for a key that has been deleted.
      testTable.delete(key);
      Assert.assertFalse(testTable.isExist(key));

      byte[] invalidKey =
          RandomStringUtils.random(5).getBytes(StandardCharsets.UTF_8);
      // Test if isExist returns false for a key that is definitely not present.
      Assert.assertFalse(testTable.isExist(invalidKey));

      RDBMetrics rdbMetrics = dbStore.getMetrics();
      Assert.assertEquals(3, rdbMetrics.getNumDBKeyMayExistChecks());
      Assert.assertTrue(rdbMetrics.getNumDBKeyMayExistMisses() == 0);

      // Reinsert key for further testing.
      testTable.put(key, value);
    }

    dbStore.close();
    rocksDBOptions = new DBOptions();
    rocksDBOptions.setCreateIfMissing(true);
    rocksDBOptions.setCreateMissingColumnFamilies(true);
    dbStore = new RDBStore(rdbLocation, rocksDBOptions, configSet);
    try (Table<byte[], byte[]> testTable = dbStore.getTable(tableName)) {
      // Verify isExist works with key not in block cache.
      Assert.assertTrue(testTable.isExist(key));
    } finally {
      dbStore.close();
    }
  }


  @Test
  public void testGetIfExist() throws Exception {
    DBOptions rocksDBOptions = new DBOptions();
    rocksDBOptions.setCreateIfMissing(true);
    rocksDBOptions.setCreateMissingColumnFamilies(true);

    String tableName = StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY);

    Set<TableConfig> configSet = new HashSet<>();
    TableConfig newConfig = new TableConfig(tableName,
        new ColumnFamilyOptions());
    configSet.add(newConfig);

    File rdbLocation = folder.newFolder();
    RDBStore dbStore = new RDBStore(rdbLocation, rocksDBOptions, configSet);

    byte[] key = RandomStringUtils.random(10, true, false)
        .getBytes(StandardCharsets.UTF_8);
    byte[] value = RandomStringUtils.random(10, true, false)
        .getBytes(StandardCharsets.UTF_8);

    try (Table<byte[], byte[]> testTable = dbStore.getTable(tableName)) {
      testTable.put(key, value);

      // Test if isExist returns value for a key that definitely exists.
      Assert.assertNotNull(testTable.getIfExist(key));

      // Test if isExist returns null for a key that has been deleted.
      testTable.delete(key);
      Assert.assertNull(testTable.getIfExist(key));

      byte[] invalidKey =
          RandomStringUtils.random(5).getBytes(StandardCharsets.UTF_8);
      // Test if isExist returns null for a key that is definitely not present.
      Assert.assertNull(testTable.getIfExist(invalidKey));

      RDBMetrics rdbMetrics = dbStore.getMetrics();
      Assert.assertEquals(3, rdbMetrics.getNumDBKeyGetIfExistChecks());


      Assert.assertEquals(0, rdbMetrics.getNumDBKeyGetIfExistMisses());

      Assert.assertEquals(1, rdbMetrics.getNumDBKeyGetIfExistGets());

      // Reinsert key for further testing.
      testTable.put(key, value);
    }

    dbStore.close();
    rocksDBOptions = new DBOptions();
    rocksDBOptions.setCreateIfMissing(true);
    rocksDBOptions.setCreateMissingColumnFamilies(true);
    dbStore = new RDBStore(rdbLocation, rocksDBOptions, configSet);
    try (Table<byte[], byte[]> testTable = dbStore.getTable(tableName)) {
      // Verify getIfExists works with key not in block cache.
      Assert.assertNotNull(testTable.getIfExist(key));
    } finally {
      dbStore.close();
    }
  }


  @Test
  public void testCountEstimatedRowsInTable() throws Exception {
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("Eighth")) {
      // Add a few keys
      final int numKeys = 12345;
      for (int i = 0; i < numKeys; i++) {
        byte[] key =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        byte[] value =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        testTable.put(key, value);
      }
      long keyCount = testTable.getEstimatedKeyCount();
      // The result should be larger than zero but not exceed(?) numKeys
      Assert.assertTrue(keyCount > 0 && keyCount <= numKeys);
    }
  }

  @Test
  public void testIteratorRemoveFromDB() throws Exception {

    // Remove without next removes first entry.
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("Fifth")) {
      writeToTable(testTable, 3);
      TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iterator =
          testTable.iterator();
      iterator.removeFromDB();
      Assert.assertNull(testTable.get("1".getBytes(StandardCharsets.UTF_8)));
      Assert.assertNotNull(testTable.get("2".getBytes(StandardCharsets.UTF_8)));
      Assert.assertNotNull(testTable.get("3".getBytes(StandardCharsets.UTF_8)));
    }

    // Remove after seekToLast removes lastEntry
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("Sixth")) {
      writeToTable(testTable, 3);
      TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iterator =
          testTable.iterator();
      iterator.seekToLast();
      iterator.removeFromDB();
      Assert.assertNotNull(testTable.get("1".getBytes(StandardCharsets.UTF_8)));
      Assert.assertNotNull(testTable.get("2".getBytes(StandardCharsets.UTF_8)));
      Assert.assertNull(testTable.get("3".getBytes(StandardCharsets.UTF_8)));
    }

    // Remove after seek deletes that entry.
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("Sixth")) {
      writeToTable(testTable, 3);
      TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iterator =
          testTable.iterator();
      iterator.seek("3".getBytes(StandardCharsets.UTF_8));
      iterator.removeFromDB();
      Assert.assertNotNull(testTable.get("1".getBytes(StandardCharsets.UTF_8)));
      Assert.assertNotNull(testTable.get("2".getBytes(StandardCharsets.UTF_8)));
      Assert.assertNull(testTable.get("3".getBytes(StandardCharsets.UTF_8)));
    }

    // Remove after next() deletes entry that was returned by next.
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("Sixth")) {
      writeToTable(testTable, 3);
      TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iterator =
          testTable.iterator();
      iterator.seek("2".getBytes(StandardCharsets.UTF_8));
      iterator.next();
      iterator.removeFromDB();
      Assert.assertNotNull(testTable.get("1".getBytes(StandardCharsets.UTF_8)));
      Assert.assertNull(testTable.get("2".getBytes(StandardCharsets.UTF_8)));
      Assert.assertNotNull(testTable.get("3".getBytes(StandardCharsets.UTF_8)));
    }
  }

  private void writeToTable(Table testTable, int num) throws IOException {
    for (int i = 1; i <= num; i++) {
      byte[] key = (i + "").getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
    }
  }
}
