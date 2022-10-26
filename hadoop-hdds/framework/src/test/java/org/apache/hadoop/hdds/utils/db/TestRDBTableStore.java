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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.junit.Assert;
import org.junit.Rule;
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
  private final List<String> prefixedFamilies = Arrays.asList(
      "PrefixFirst",
      "PrefixTwo", "PrefixThree",
      "PrefixFour", "PrefixFifth"
  );
  private static final int PREFIX_LENGTH = 9;
  @Rule
  private RDBStore rdbStore = null;
  private ManagedDBOptions options = null;
  private static byte[][] bytesOf;

  @TempDir
  private File tempDir;

  @BeforeAll
  public static void initConstants() {
    bytesOf = new byte[4][];
    for (int i = 1; i <= 3; i++) {
      bytesOf[i] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
    }
  }

  private static boolean consume(Table.KeyValue keyValue)  {
    count++;
    try {
      Assertions.assertNotNull(keyValue.getKey());
    } catch (IOException ex) {
      Assertions.fail("Unexpected Exception " + ex);
    }
    return true;
  }

  @BeforeEach
  public void setUp() throws Exception {
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
    for (String name : prefixedFamilies) {
      ManagedColumnFamilyOptions cfOptions = new ManagedColumnFamilyOptions();
      cfOptions.useFixedLengthPrefixExtractor(PREFIX_LENGTH);

      TableConfig newConfig = new TableConfig(name, cfOptions);
      configSet.add(newConfig);
    }
    rdbStore = new RDBStore(tempDir, options, configSet);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rdbStore != null) {
      rdbStore.close();
    }
  }

  @Test
  public void getHandle() throws Exception {
    try (Table testTable = rdbStore.getTable("First")) {
      Assertions.assertNotNull(testTable);
      Assertions.assertNotNull(((RDBTable) testTable).getColumnFamily());
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
      Assertions.assertFalse(testTable.isEmpty());
      byte[] readValue = testTable.get(key);
      Assertions.assertArrayEquals(value, readValue);
    }
    try (Table secondTable = rdbStore.getTable("Second")) {
      Assertions.assertTrue(secondTable.isEmpty());
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
        Assertions.assertNotNull(testTable.get(validKeys.get(0)));
      }

      for (int x = 0; x < deletedKeys.size(); x++) {
        Assertions.assertNull(testTable.get(deletedKeys.get(0)));
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
      Assertions.assertNull(testTable.get(key));

      //when
      testTable.putWithBatch(batch, key, value);
      rdbStore.commitBatchOperation(batch);

      //then
      Assertions.assertNotNull(testTable.get(key));
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
      Assertions.assertNotNull(testTable.get(key));


      //when
      testTable.deleteWithBatch(batch, key);
      rdbStore.commitBatchOperation(batch);

      //then
      Assertions.assertNull(testTable.get(key));
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

        Assertions.assertEquals(iterCount, localCount);
        iter.seekToFirst();
        iter.forEachRemaining(TestRDBTableStore::consume);
        Assertions.assertEquals(iterCount, count);

      }
    }
  }

  @Test
  public void testIsExist() throws Exception {
    byte[] key = RandomStringUtils.random(10, true, false)
        .getBytes(StandardCharsets.UTF_8);
    byte[] value = RandomStringUtils.random(10, true, false)
        .getBytes(StandardCharsets.UTF_8);

    final String tableName = families.get(0);
    try (Table<byte[], byte[]> testTable = rdbStore.getTable(tableName)) {
      testTable.put(key, value);

      // Test if isExist returns true for a key that definitely exists.
      Assertions.assertTrue(testTable.isExist(key));

      // Test if isExist returns false for a key that has been deleted.
      testTable.delete(key);
      Assertions.assertFalse(testTable.isExist(key));

      byte[] invalidKey =
          RandomStringUtils.random(5).getBytes(StandardCharsets.UTF_8);
      // Test if isExist returns false for a key that is definitely not present.
      Assertions.assertFalse(testTable.isExist(invalidKey));

      RDBMetrics rdbMetrics = rdbStore.getMetrics();
      Assertions.assertEquals(3, rdbMetrics.getNumDBKeyMayExistChecks());
      Assertions.assertEquals(0, rdbMetrics.getNumDBKeyMayExistMisses());

      // Reinsert key for further testing.
      testTable.put(key, value);
    }

    rdbStore.close();
    setUp();
    try (Table<byte[], byte[]> testTable = rdbStore.getTable(tableName)) {
      // Verify isExist works with key not in block cache.
      Assertions.assertTrue(testTable.isExist(key));
    }
  }


  @Test
  public void testGetIfExist() throws Exception {
    byte[] key = RandomStringUtils.random(10, true, false)
        .getBytes(StandardCharsets.UTF_8);
    byte[] value = RandomStringUtils.random(10, true, false)
        .getBytes(StandardCharsets.UTF_8);

    final String tableName = families.get(0);
    try (Table<byte[], byte[]> testTable = rdbStore.getTable(tableName)) {
      testTable.put(key, value);

      // Test if isExist returns value for a key that definitely exists.
      Assertions.assertNotNull(testTable.getIfExist(key));

      // Test if isExist returns null for a key that has been deleted.
      testTable.delete(key);
      Assertions.assertNull(testTable.getIfExist(key));

      byte[] invalidKey =
          RandomStringUtils.random(5).getBytes(StandardCharsets.UTF_8);
      // Test if isExist returns null for a key that is definitely not present.
      Assertions.assertNull(testTable.getIfExist(invalidKey));

      RDBMetrics rdbMetrics = rdbStore.getMetrics();
      Assertions.assertEquals(3, rdbMetrics.getNumDBKeyGetIfExistChecks());

      Assertions.assertEquals(0, rdbMetrics.getNumDBKeyGetIfExistMisses());

      Assertions.assertEquals(1, rdbMetrics.getNumDBKeyGetIfExistGets());

      // Reinsert key for further testing.
      testTable.put(key, value);
    }

    rdbStore.close();
    setUp();
    try (Table<byte[], byte[]> testTable = rdbStore.getTable(tableName)) {
      // Verify getIfExists works with key not in block cache.
      Assertions.assertNotNull(testTable.getIfExist(key));
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
      Assertions.assertTrue(keyCount > 0 && keyCount <= numKeys);
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
      Assertions.assertNull(testTable.get(bytesOf[1]));
      Assertions.assertNotNull(testTable.get(bytesOf[2]));
      Assertions.assertNotNull(testTable.get(bytesOf[3]));
    }

    // Remove after seekToLast removes lastEntry
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("Sixth")) {
      writeToTable(testTable, 3);
      TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iterator =
          testTable.iterator();
      iterator.seekToLast();
      iterator.removeFromDB();
      Assertions.assertNotNull(testTable.get(bytesOf[1]));
      Assertions.assertNotNull(testTable.get(bytesOf[2]));
      Assertions.assertNull(testTable.get(bytesOf[3]));
    }

    // Remove after seek deletes that entry.
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("Sixth")) {
      writeToTable(testTable, 3);
      TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iterator =
          testTable.iterator();
      iterator.seek(bytesOf[3]);
      iterator.removeFromDB();
      Assertions.assertNotNull(testTable.get(bytesOf[1]));
      Assertions.assertNotNull(testTable.get(bytesOf[2]));
      Assertions.assertNull(testTable.get(bytesOf[3]));
    }

    // Remove after next() deletes entry that was returned by next.
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("Sixth")) {
      writeToTable(testTable, 3);
      TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iterator =
          testTable.iterator();
      iterator.seek(bytesOf[2]);
      iterator.next();
      iterator.removeFromDB();
      Assertions.assertNotNull(testTable.get(bytesOf[1]));
      Assertions.assertNull(testTable.get(bytesOf[2]));
      Assertions.assertNotNull(testTable.get(bytesOf[3]));
    }
  }

  private void writeToTable(Table testTable, int num) throws IOException {
    for (int i = 1; i <= num; i++) {
      byte[] key = bytesOf[i];
      byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
    }
  }

  @Test
  public void testPrefixedIterator() throws Exception {
    int containerCount = 3;
    int blockCount = 5;
    List<String> testPrefixes = generatePrefixes(containerCount);
    List<Map<String, String>> testData = generateKVs(testPrefixes, blockCount);

    try (Table<byte[], byte[]> testTable = rdbStore.getTable("PrefixFirst")) {
      // write data
      populatePrefixedTable(testTable, testData);

      // iterator should seek to right pos in the middle
      byte[] samplePrefix = testPrefixes.get(2).getBytes(
          StandardCharsets.UTF_8);
      try (TableIterator<byte[],
          ? extends Table.KeyValue<byte[], byte[]>> iter = testTable.iterator(
              samplePrefix)) {
        int keyCount = 0;
        while (iter.hasNext()) {
          // iterator should only meet keys with samplePrefix
          Assert.assertTrue(Arrays.equals(
              Arrays.copyOf(iter.next().getKey(), PREFIX_LENGTH),
              samplePrefix));
          keyCount++;
        }

        // iterator should end at right pos
        Assert.assertEquals(blockCount, keyCount);

        // iterator should be able to seekToFirst
        iter.seekToFirst();
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(Arrays.equals(
            Arrays.copyOf(iter.next().getKey(), PREFIX_LENGTH),
            samplePrefix));
      }
    }
  }

  @Test
  public void testPrefixedRangeKVs() throws Exception {
    int containerCount = 3;
    int blockCount = 5;
    List<String> testPrefixes = generatePrefixes(containerCount);
    List<Map<String, String>> testData = generateKVs(testPrefixes, blockCount);

    try (Table<byte[], byte[]> testTable = rdbStore.getTable("PrefixFirst")) {

      // write data
      populatePrefixedTable(testTable, testData);

      byte[] samplePrefix = testPrefixes.get(2).getBytes(
          StandardCharsets.UTF_8);

      // test start at first
      byte[] startKey = samplePrefix;
      List<? extends Table.KeyValue<byte[], byte[]>> rangeKVs = testTable
          .getRangeKVs(startKey, 3, samplePrefix);
      Assert.assertEquals(3, rangeKVs.size());

      // test start with a middle key
      startKey = StringUtils.string2Bytes(
          StringUtils.bytes2String(samplePrefix) + "3");
      rangeKVs = testTable.getRangeKVs(startKey, blockCount, samplePrefix);
      Assert.assertEquals(2, rangeKVs.size());

      // test with a filter
      MetadataKeyFilters.KeyPrefixFilter filter1 = new MetadataKeyFilters
          .KeyPrefixFilter()
          .addFilter(StringUtils.bytes2String(samplePrefix) + "1");
      startKey = StringUtils.string2Bytes(
          StringUtils.bytes2String(samplePrefix));
      rangeKVs = testTable.getRangeKVs(startKey, blockCount,
          samplePrefix, filter1);
      Assert.assertEquals(1, rangeKVs.size());

      // test start with a non-exist key
      startKey = StringUtils.string2Bytes(
          StringUtils.bytes2String(samplePrefix) + 123);
      rangeKVs = testTable.getRangeKVs(startKey, 10, samplePrefix);
      Assert.assertEquals(0, rangeKVs.size());
    }
  }

  @Test
  public void testDumpAndLoadBasic() throws Exception {
    int containerCount = 3;
    int blockCount = 5;
    List<String> testPrefixes = generatePrefixes(containerCount);
    List<Map<String, String>> testData = generateKVs(testPrefixes, blockCount);
    File dumpFile = new File(tempDir, "PrefixTwo.dump");
    byte[] samplePrefix = testPrefixes.get(2).getBytes(StandardCharsets.UTF_8);

    try (Table<byte[], byte[]> testTable1 = rdbStore.getTable("PrefixTwo")) {
      // write data
      populatePrefixedTable(testTable1, testData);

      // dump to external file
      testTable1.dumpToFileWithPrefix(dumpFile, samplePrefix);

      // check dump file exist
      Assert.assertTrue(dumpFile.exists());
      Assert.assertTrue(dumpFile.length() != 0);
    }

    // load dump file into another table
    try (Table<byte[], byte[]> testTable2 = rdbStore.getTable("PrefixThree")) {
      testTable2.loadFromFile(dumpFile);

      // check loaded keys
      try (TableIterator<byte[],
          ? extends Table.KeyValue<byte[], byte[]>> iter = testTable2.iterator(
          samplePrefix)) {
        int keyCount = 0;
        while (iter.hasNext()) {
          // check prefix
          Assert.assertTrue(Arrays.equals(
              Arrays.copyOf(iter.next().getKey(), PREFIX_LENGTH),
              samplePrefix));
          keyCount++;
        }

        // check block count
        Assert.assertEquals(blockCount, keyCount);
      }
    }
  }

  @Test
  public void testDumpAndLoadEmpty() throws Exception {
    int containerCount = 3;
    List<String> testPrefixes = generatePrefixes(containerCount);

    File dumpFile = new File(tempDir, "PrefixFour.dump");
    byte[] samplePrefix = testPrefixes.get(2).getBytes(StandardCharsets.UTF_8);

    try (Table<byte[], byte[]> testTable1 = rdbStore.getTable("PrefixFour")) {
      // no data

      // dump to external file
      testTable1.dumpToFileWithPrefix(dumpFile, samplePrefix);

      // check dump file exist
      Assert.assertTrue(dumpFile.exists());
      // empty dump file
      Assert.assertTrue(dumpFile.length() == 0);
    }

    // load dump file into another table
    try (Table<byte[], byte[]> testTable2 = rdbStore.getTable("PrefixFifth")) {
      testTable2.loadFromFile(dumpFile);

      // check loaded keys
      try (TableIterator<byte[],
          ? extends Table.KeyValue<byte[], byte[]>> iter = testTable2.iterator(
          samplePrefix)) {
        int keyCount = 0;
        while (iter.hasNext()) {
          // check prefix
          Assert.assertTrue(Arrays.equals(
              Arrays.copyOf(iter.next().getKey(), PREFIX_LENGTH),
              samplePrefix));
          keyCount++;
        }

        // check block count
        Assert.assertEquals(0, keyCount);
      }
    }
  }

  private List<String> generatePrefixes(int prefixCount) {
    List<String> prefixes = new ArrayList<>();
    for (int i = 0; i < prefixCount; i++) {
      // use alphabetic chars so we get fixed length prefix when
      // convert to byte[]
      prefixes.add(RandomStringUtils.randomAlphabetic(PREFIX_LENGTH));
    }
    return prefixes;
  }

  private List<Map<String, String>> generateKVs(List<String> prefixes,
      int keyCount) {
    List<Map<String, String>> data = new ArrayList<>();
    for (String prefix : prefixes) {
      Map<String, String> kvs = new HashMap<>();
      for (int i = 0; i < keyCount; i++) {
        String key = prefix + i;
        String val = RandomStringUtils.random(10);
        kvs.put(key, val);
      }
      data.add(kvs);
    }
    return data;
  }

  private void populatePrefixedTable(Table<byte[], byte[]> table,
      List<Map<String, String>> testData) throws IOException {
    for (Map<String, String> segment : testData) {
      for (Map.Entry<String, String> entry : segment.entrySet()) {
        table.put(entry.getKey().getBytes(StandardCharsets.UTF_8),
            entry.getValue().getBytes(StandardCharsets.UTF_8));
      }
    }
  }
}
