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

import static org.apache.hadoop.hdds.StringUtils.bytes2String;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for RocksDBTable Store.
 */
public class TestRDBTableStore {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestRDBTableStore.class);

  public static final int MAX_DB_UPDATES_SIZE_THRESHOLD = 80;
  private static int count = 0;
  private final List<String> families =
      Arrays.asList(bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
          "First", "Second", "Third",
          "Fourth", "Fifth",
          "Sixth", "Seventh",
          "Eighth", "Ninth",
          "Ten");
  private final List<String> prefixedFamilies = Arrays.asList(
      "PrefixFirst",
      "PrefixTwo", "PrefixThree",
      "PrefixFour", "PrefixFifth"
  );
  private static final int PREFIX_LENGTH = 9;
  private RDBStore rdbStore = null;
  private ManagedDBOptions options = null;
  private static byte[][] bytesOf;

  @TempDir
  private File tempDir;

  @BeforeAll
  public static void initConstants() {
    CodecBuffer.enableLeakDetection();
    bytesOf = new byte[4][];
    for (int i = 1; i <= 3; i++) {
      bytesOf[i] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
    }
  }

  private static boolean consume(Table.KeyValue<byte[], byte[]> keyValue)  {
    count++;
    assertNotNull(assertDoesNotThrow(keyValue::getKey));
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
    rdbStore = TestRDBStore.newRDBStore(tempDir, options, configSet,
        MAX_DB_UPDATES_SIZE_THRESHOLD);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rdbStore != null) {
      rdbStore.close();
    }
    if (options != null) {
      options.close();
    }
    CodecTestUtil.gc();
  }

  @Test
  public void getHandle() throws Exception {
    final RDBTable testTable = rdbStore.getTable("First");
    assertNotNull(testTable);
    assertNotNull(testTable.getColumnFamily());
  }

  @Test
  public void putGetAndEmpty() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable("First");
    byte[] key = RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
    byte[] value = RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
    testTable.put(key, value);
    assertFalse(testTable.isEmpty());
    byte[] readValue = testTable.get(key);
    assertArrayEquals(value, readValue);
    Table<byte[], byte[]> secondTable = rdbStore.getTable("Second");
    assertTrue(secondTable.isEmpty());
  }

  @Test
  public void delete() throws Exception {
    List<byte[]> deletedKeys = new ArrayList<>();
    List<byte[]> validKeys = new ArrayList<>();
    byte[] value =
        RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
    for (int x = 0; x < 100; x++) {
      deletedKeys.add(
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8));
    }

    for (int x = 0; x < 100; x++) {
      validKeys.add(
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8));
    }

    // Write all the keys and delete the keys scheduled for delete.
    // Assert we find only expected keys in the Table.
    final Table<byte[], byte[]> testTable = rdbStore.getTable("Fourth");
    for (byte[] bytes : deletedKeys) {
      testTable.put(bytes, value);
      testTable.delete(bytes);
    }

    for (byte[] key : validKeys) {
      testTable.put(key, value);
    }

    for (byte[] validKey : validKeys) {
      assertNotNull(testTable.get(validKey));
    }

    for (byte[] deletedKey : deletedKeys) {
      assertNull(testTable.get(deletedKey));
    }
  }

  @Test
  public void deleteRange() throws Exception {

    // Prepare keys to be written to the test table
    List<byte[]> keys = new ArrayList<>();
    for (int x = 0; x < 100; x++) {
      // Left pad DB keys with zeros
      String k = String.format("%03d", x) + "-" + RandomStringUtils.secure().next(6);
      keys.add(k.getBytes(StandardCharsets.UTF_8));
    }
    // Some random value
    byte[] val = RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);

    final Table<byte[], byte[]> testTable = rdbStore.getTable("Ninth");

    // Write keys to the table
    for (byte[] key : keys) {
      testTable.put(key, val);
    }

    // All keys should exist at this point
    for (byte[] key : keys) {
      assertNotNull(testTable.get(key));
    }

    // Delete a range of keys: [10th, 20th), zero-indexed
    final int deleteRangeBegin = 10, deleteRangeEnd = 20;
    byte[] dRangeBeginKey = keys.get(deleteRangeBegin);
    byte[] dRangeEndKey = keys.get(deleteRangeEnd);

    testTable.deleteRange(dRangeBeginKey, dRangeEndKey);

    // Keys [10th, 20th) should be gone now
    for (int x = deleteRangeBegin; x < deleteRangeEnd; x++) {
      assertNull(testTable.get(keys.get(x)));
    }

    // While the rest of the keys should be untouched
    for (int x = 0; x < deleteRangeBegin; x++) {
      assertNotNull(testTable.get(keys.get(x)));
    }
    for (int x = deleteRangeEnd; x < 100; x++) {
      assertNotNull(testTable.get(keys.get(x)));
    }

    // Delete the rest of the keys
    testTable.deleteRange(keys.get(0), keys.get(100 - 1));

    // Confirm key deletion
    for (int x = 0; x < 100 - 1; x++) {
      assertNull(testTable.get(keys.get(x)));
    }
    // The last key is still there because
    // deleteRange() excludes the endKey by design
    assertNotNull(testTable.get(keys.get(100 - 1)));

    // Delete the last key
    testTable.delete(keys.get(100 - 1));
    assertNull(testTable.get(keys.get(100 - 1)));
  }

  @Test
  public void batchPut() throws Exception {
    final Table<byte[], byte[]> testTable = rdbStore.getTable("Fifth");
    try (BatchOperation batch = rdbStore.initBatchOperation()) {
      //given
      byte[] key =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      assertNull(testTable.get(key));

      //when
      testTable.putWithBatch(batch, key, value);
      rdbStore.commitBatchOperation(batch);

      //then
      assertNotNull(testTable.get(key));
    }
  }

  @Test
  public void batchDelete() throws Exception {
    final Table<byte[], byte[]> testTable = rdbStore.getTable("Fifth");
    try (BatchOperation batch = rdbStore.initBatchOperation()) {

      //given
      byte[] key =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
      assertNotNull(testTable.get(key));


      //when
      testTable.deleteWithBatch(batch, key);
      rdbStore.commitBatchOperation(batch);

      //then
      assertNull(testTable.get(key));
    }
  }

  @Test
  public void putGetTypedTableCodec() throws Exception {
    Table<String, String> testTable = rdbStore.getTable("Ten", StringCodec.get(), StringCodec.get());
    testTable.put("test1", "123");
    assertFalse(testTable.isEmpty());
    assertEquals("123", testTable.get("test1"));
    Table<String, ByteString> testTable2 = rdbStore.getTable("Ten",
        StringCodec.get(), ByteStringCodec.get(), CacheType.NO_CACHE);
    assertEquals("123", testTable2.get("test1").toStringUtf8());
  }

  @Test
  public void forEachAndIterator() throws Exception {
    final int iterCount = 100;
    final Table<byte[], byte[]> testTable = rdbStore.getTable("Sixth");
    for (int x = 0; x < iterCount; x++) {
      byte[] key =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
    }
    int localCount = 0;
    try (Table.KeyValueIterator<byte[], byte[]> iter = testTable.iterator()) {
      while (iter.hasNext()) {
        iter.next();
        localCount++;
      }

      assertEquals(iterCount, localCount);
      iter.seekToFirst();
      iter.forEachRemaining(TestRDBTableStore::consume);
      assertEquals(iterCount, count);

    }
  }

  @Test
  public void testIsExist() throws Exception {
    byte[] key = RandomStringUtils.secure().next(10, true, false)
        .getBytes(StandardCharsets.UTF_8);
    byte[] value = RandomStringUtils.secure().next(10, true, false)
        .getBytes(StandardCharsets.UTF_8);
    final byte[] zeroSizeKey = {(byte) (key[0] + 1)};
    final byte[] zeroSizeValue = {};

    final String tableName = families.get(0);
    Table<byte[], byte[]> testTable = rdbStore.getTable(tableName);
    testTable.put(key, value);

    // Test if isExist returns true for a key that definitely exists.
    assertTrue(testTable.isExist(key));

    // Test if isExist returns false for a key that has been deleted.
    testTable.delete(key);
    assertFalse(testTable.isExist(key));

    // Test a key with zero size value.
    assertNull(testTable.get(zeroSizeKey));
    testTable.put(zeroSizeKey, zeroSizeValue);
    assertEquals(0, testTable.get(zeroSizeKey).length);

    byte[] invalidKey =
        RandomStringUtils.secure().next(5).getBytes(StandardCharsets.UTF_8);
    // Test if isExist returns false for a key that is definitely not present.
    assertFalse(testTable.isExist(invalidKey));

    RDBMetrics rdbMetrics = rdbStore.getMetrics();
    assertEquals(3, rdbMetrics.getNumDBKeyMayExistChecks());
    assertEquals(0, rdbMetrics.getNumDBKeyMayExistMisses());
    assertEquals(2, rdbMetrics.getNumDBKeyGets());

    // Reinsert key for further testing.
    testTable.put(key, value);

    rdbStore.close();
    setUp();
    Table<byte[], byte[]> testTable2 = rdbStore.getTable(tableName);
    // Verify isExist works with key not in block cache.
    assertTrue(testTable2.isExist(key));
    assertEquals(0, testTable2.get(zeroSizeKey).length);
    assertTrue(testTable2.isExist(zeroSizeKey));

    RDBMetrics rdbMetrics2 = rdbStore.getMetrics();
    assertEquals(2, rdbMetrics2.getNumDBKeyMayExistChecks());
    assertEquals(0, rdbMetrics2.getNumDBKeyMayExistMisses());
    assertEquals(2, rdbMetrics2.getNumDBKeyGets());
  }

  @Test
  public void testGetByteBuffer() throws Exception {
    final StringCodec codec = StringCodec.get();
    final String tableName = families.get(0);
    RDBTable testTable = rdbStore.getTable(tableName);
    final TypedTable<String, String> typedTable = new TypedTable<>(
        testTable, StringCodec.get(), StringCodec.get(), CacheType.PARTIAL_CACHE);

    for (int i = 0; i < 20; i++) {
      final int valueSize = TypedTable.BUFFER_SIZE_DEFAULT * i / 4;
      final String key = "key" + i;
      final byte[] keyBytes = codec.toPersistedFormat(key);
      final String value = RandomStringUtils.secure().next(valueSize, true, false);
      final byte[] valueBytes = codec.toPersistedFormat(value);

      testTable.put(keyBytes, valueBytes);
      final byte[] got = testTable.get(keyBytes);
      assertArrayEquals(valueBytes, got);
      assertEquals(value, codec.fromPersistedFormat(got));
      assertEquals(value, typedTable.get(key));
    }
  }

  @Test
  public void testGetIfExist() throws Exception {
    byte[] key = RandomStringUtils.secure().next(10, true, false)
        .getBytes(StandardCharsets.UTF_8);
    byte[] value = RandomStringUtils.secure().next(10, true, false)
        .getBytes(StandardCharsets.UTF_8);

    final String tableName = families.get(0);
    Table<byte[], byte[]> testTable = rdbStore.getTable(tableName);
    testTable.put(key, value);

    // Test if isExist returns value for a key that definitely exists.
    assertNotNull(testTable.getIfExist(key));

    // Test if isExist returns null for a key that has been deleted.
    testTable.delete(key);
    assertNull(testTable.getIfExist(key));

    byte[] invalidKey =
        RandomStringUtils.secure().next(5).getBytes(StandardCharsets.UTF_8);
    // Test if isExist returns null for a key that is definitely not present.
    assertNull(testTable.getIfExist(invalidKey));

    RDBMetrics rdbMetrics = rdbStore.getMetrics();
    assertEquals(3, rdbMetrics.getNumDBKeyGetIfExistChecks());

    assertEquals(0, rdbMetrics.getNumDBKeyGetIfExistMisses());

    assertEquals(0, rdbMetrics.getNumDBKeyGetIfExistGets());

    // Reinsert key for further testing.
    testTable.put(key, value);

    rdbStore.close();
    setUp();
    Table<byte[], byte[]> testTable2 = rdbStore.getTable(tableName);
    // Verify getIfExists works with key not in block cache.
    assertNotNull(testTable2.getIfExist(key));
  }

  @Test
  public void testCountEstimatedRowsInTable() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable("Eighth");
    // Add a few keys
    final int numKeys = 12345;
    for (int i = 0; i < numKeys; i++) {
      byte[] key =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
    }
    long keyCount = testTable.getEstimatedKeyCount();
    // The result should be larger than zero but not exceed(?) numKeys
    assertThat(keyCount).isGreaterThan(0).isLessThanOrEqualTo(numKeys);
  }

  @Test
  public void testIteratorRemoveFromDB() throws Exception {

    // Remove without next removes first entry.
    Table<byte[], byte[]> testTable = rdbStore.getTable("Fifth");
    writeToTable(testTable, 3);
    try (Table.KeyValueIterator<?, ?> iterator =
             testTable.iterator()) {
      iterator.removeFromDB();
    }
    assertNull(testTable.get(bytesOf[1]));
    assertNotNull(testTable.get(bytesOf[2]));
    assertNotNull(testTable.get(bytesOf[3]));

    // Remove after seekToLast removes lastEntry
    Table<byte[], byte[]> testTable2 = rdbStore.getTable("Sixth");
    writeToTable(testTable2, 3);
    try (Table.KeyValueIterator<?, ?> iterator =
             testTable2.iterator()) {
      iterator.seekToLast();
      iterator.removeFromDB();
    }
    assertNotNull(testTable2.get(bytesOf[1]));
    assertNotNull(testTable2.get(bytesOf[2]));
    assertNull(testTable2.get(bytesOf[3]));

    // Remove after seek deletes that entry.
    Table<byte[], byte[]> testTable3 = rdbStore.getTable("Sixth");
    writeToTable(testTable3, 3);
    try (Table.KeyValueIterator<byte[], byte[]> iterator =
             testTable3.iterator()) {
      iterator.seek(bytesOf[3]);
      iterator.removeFromDB();
    }
    assertNotNull(testTable3.get(bytesOf[1]));
    assertNotNull(testTable3.get(bytesOf[2]));
    assertNull(testTable3.get(bytesOf[3]));

    // Remove after next() deletes entry that was returned by next.
    Table<byte[], byte[]> testTable4 = rdbStore.getTable("Sixth");
    writeToTable(testTable4, 3);
    try (Table.KeyValueIterator<byte[], byte[]> iterator =
             testTable4.iterator()) {
      iterator.seek(bytesOf[2]);
      iterator.next();
      iterator.removeFromDB();
    }
    assertNotNull(testTable4.get(bytesOf[1]));
    assertNull(testTable4.get(bytesOf[2]));
    assertNotNull(testTable4.get(bytesOf[3]));
  }

  private void writeToTable(Table<byte[], byte[]> testTable, int num) throws IOException {
    for (int i = 1; i <= num; i++) {
      byte[] key = bytesOf[i];
      byte[] value =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
    }
  }

  @Test
  public void testPrefixedIterator() throws Exception {
    int containerCount = 3;
    int blockCount = 5;
    List<String> testPrefixes = generatePrefixes(containerCount);
    List<Map<String, String>> testData = generateKVs(testPrefixes, blockCount);

    Table<byte[], byte[]> testTable = rdbStore.getTable("PrefixFirst");
    // write data
    populatePrefixedTable(testTable, testData);

    // iterator should seek to right pos in the middle
    byte[] samplePrefix = testPrefixes.get(2).getBytes(
        StandardCharsets.UTF_8);
    try (Table.KeyValueIterator<byte[], byte[]> iter = testTable.iterator(
        samplePrefix)) {
      int keyCount = 0;
      while (iter.hasNext()) {
        // iterator should only meet keys with samplePrefix
        assertArrayEquals(samplePrefix, Arrays.copyOf(iter.next().getKey(), PREFIX_LENGTH));
        keyCount++;
      }

      // iterator should end at right pos
      assertEquals(blockCount, keyCount);

      // iterator should be able to seekToFirst
      iter.seekToFirst();
      assertTrue(iter.hasNext());
      assertArrayEquals(samplePrefix, Arrays.copyOf(iter.next().getKey(), PREFIX_LENGTH));
    }
  }

  @Test
  public void testStringPrefixedIterator() throws Exception {
    final int prefixCount = 3;
    final int keyCount = 5;
    final List<String> prefixes = generatePrefixes(prefixCount);
    final List<Map<String, String>> data = generateKVs(prefixes, keyCount);

    Table<String, String> table = rdbStore.getTable("PrefixFirst", StringCodec.get(), StringCodec.get());
    populateTable(table, data);
    for (String prefix : prefixes) {
      assertIterator(keyCount, prefix, table);
    }

    final String nonExistingPrefix = RandomStringUtils.secure().next(
        PREFIX_LENGTH + 2, false, false);
    assertIterator(0, nonExistingPrefix, table);
  }

  static void assertIterator(int expectedCount, String prefix,
      Table<String, String> table) throws Exception {
    try (Table.KeyValueIterator<String, String> i = table.iterator(prefix)) {
      int keyCount = 0;
      for (; i.hasNext(); keyCount++) {
        Table.KeyValue<String, String> entry = i.next();
        assertEquals(prefix,
            entry.getKey().substring(0, PREFIX_LENGTH));
        assertEquals(entry.getValue().getBytes(StandardCharsets.UTF_8).length,
            entry.getValueByteSize());
      }
      assertEquals(expectedCount, keyCount);

      // test seekToFirst
      i.seekToFirst();
      if (expectedCount > 0) {
        // iterator should be able to seekToFirst
        assertTrue(i.hasNext());
        assertEquals(prefix,
            i.next().getKey().substring(0, PREFIX_LENGTH));
      }
    }
  }

  @Test
  public void testStringPrefixedIteratorCloseDb() throws Exception {
    Table<String, String> testTable = rdbStore.getTable("PrefixFirst", StringCodec.get(), StringCodec.get());
    // iterator should seek to right pos in the middle
    rdbStore.close();
    assertThrows(IOException.class, () -> testTable.iterator("abc"));
  }

  @Test
  public void testPrefixedRangeKVs() throws Exception {
    int containerCount = 3;
    int blockCount = 5;
    List<String> testPrefixes = generatePrefixes(containerCount);
    List<Map<String, String>> testData = generateKVs(testPrefixes, blockCount);

    Table<byte[], byte[]> testTable = rdbStore.getTable("PrefixFirst");

    // write data
    populatePrefixedTable(testTable, testData);

    byte[] samplePrefix = testPrefixes.get(2).getBytes(
        StandardCharsets.UTF_8);

    // test start at first
    byte[] startKey = samplePrefix;
    List<Table.KeyValue<byte[], byte[]>> rangeKVs = testTable
        .getRangeKVs(startKey, 3, samplePrefix);
    assertEquals(3, rangeKVs.size());

    // test start with a middle key
    startKey = StringUtils.string2Bytes(
        bytes2String(samplePrefix) + "3");
    rangeKVs = testTable.getRangeKVs(startKey, blockCount, samplePrefix);
    assertEquals(2, rangeKVs.size());

    // test with a filter
    final KeyPrefixFilter filter1 = KeyPrefixFilter.newFilter(bytes2String(samplePrefix) + "1");
    startKey = StringUtils.string2Bytes(
        bytes2String(samplePrefix));
    rangeKVs = testTable.getRangeKVs(startKey, blockCount,
        samplePrefix, filter1);
    assertEquals(1, rangeKVs.size());

    // test start with a non-exist key
    startKey = StringUtils.string2Bytes(
        bytes2String(samplePrefix) + 123);
    rangeKVs = testTable.getRangeKVs(startKey, 10, samplePrefix);
    assertEquals(0, rangeKVs.size());
  }

  @Test
  public void testDumpAndLoadBasic() throws Exception {
    int containerCount = 3;
    int blockCount = 5;
    List<String> testPrefixes = generatePrefixes(containerCount);
    List<Map<String, String>> testData = generateKVs(testPrefixes, blockCount);
    File dumpFile = new File(tempDir, "PrefixTwo.dump");
    byte[] samplePrefix = testPrefixes.get(2).getBytes(StandardCharsets.UTF_8);

    Table<byte[], byte[]> testTable1 = rdbStore.getTable("PrefixTwo");
    // write data
    populatePrefixedTable(testTable1, testData);

    // dump to external file
    testTable1.dumpToFileWithPrefix(dumpFile, samplePrefix);

    // check dump file exist
    assertTrue(dumpFile.exists());
    assertNotEquals(0, dumpFile.length());

    // load dump file into another table
    Table<byte[], byte[]> testTable2 = rdbStore.getTable("PrefixThree");
    testTable2.loadFromFile(dumpFile);

    // check loaded keys
    try (Table.KeyValueIterator<byte[], byte[]> iter = testTable2.iterator(
        samplePrefix)) {
      int keyCount = 0;
      while (iter.hasNext()) {
        // check prefix
        assertArrayEquals(Arrays.copyOf(iter.next().getKey(), PREFIX_LENGTH), samplePrefix);
        keyCount++;
      }

      // check block count
      assertEquals(blockCount, keyCount);
    }
  }

  @Test
  public void testDumpAndLoadEmpty() throws Exception {
    int containerCount = 3;
    List<String> testPrefixes = generatePrefixes(containerCount);

    File dumpFile = new File(tempDir, "PrefixFour.dump");
    byte[] samplePrefix = testPrefixes.get(2).getBytes(StandardCharsets.UTF_8);

    Table<byte[], byte[]> testTable1 = rdbStore.getTable("PrefixFour");
    // no data

    // dump to external file
    testTable1.dumpToFileWithPrefix(dumpFile, samplePrefix);

    // check dump file exist
    assertTrue(dumpFile.exists());
    // empty dump file
    assertEquals(0, dumpFile.length());

    // load dump file into another table
    Table<byte[], byte[]> testTable2 = rdbStore.getTable("PrefixFifth");
    testTable2.loadFromFile(dumpFile);

    // check loaded keys
    try (Table.KeyValueIterator<byte[], byte[]> iter = testTable2.iterator(
        samplePrefix)) {
      int keyCount = 0;
      while (iter.hasNext()) {
        // check prefix
        assertArrayEquals(Arrays.copyOf(iter.next().getKey(), PREFIX_LENGTH), samplePrefix);
        keyCount++;
      }

      // check block count
      assertEquals(0, keyCount);
    }
  }

  private List<String> generatePrefixes(int prefixCount) {
    List<String> prefixes = new ArrayList<>();
    for (int i = 0; i < prefixCount; i++) {
      // use alphabetic chars so we get fixed length prefix when
      // convert to byte[]
      prefixes.add(RandomStringUtils.secure().nextAlphabetic(PREFIX_LENGTH));
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
        String val = RandomStringUtils.secure().next(10);
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

  private void populateTable(Table<String, String> table,
      List<Map<String, String>> testData) throws IOException {
    for (Map<String, String> segment : testData) {
      for (Map.Entry<String, String> entry : segment.entrySet()) {
        table.put(entry.getKey(), entry.getValue());
        LOG.info("put {}", entry);
      }
    }
  }

  @Test
  public void batchDeleteWithRange() throws Exception {
    final Table<byte[], byte[]> testTable = rdbStore.getTable("Fifth");
    try (BatchOperation batch = rdbStore.initBatchOperation()) {

      //given
      String keyStr = RandomStringUtils.secure().next(10);
      byte[] startKey = ("1-" + keyStr).getBytes(StandardCharsets.UTF_8);
      byte[] keyInRange1 = ("2-" + keyStr).getBytes(StandardCharsets.UTF_8);
      byte[] keyInRange2 = ("3-" + keyStr).getBytes(StandardCharsets.UTF_8);
      byte[] endKey = ("4-" + keyStr).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(startKey, value);
      testTable.put(keyInRange1, value);
      testTable.put(keyInRange2, value);
      testTable.put(endKey, value);
      assertNotNull(testTable.get(startKey));
      assertNotNull(testTable.get(keyInRange1));
      assertNotNull(testTable.get(keyInRange2));
      assertNotNull(testTable.get(endKey));

      //when
      testTable.deleteRangeWithBatch(batch, startKey, endKey);
      rdbStore.commitBatchOperation(batch);

      //then
      assertNull(testTable.get(startKey));
      assertNull(testTable.get(keyInRange1));
      assertNull(testTable.get(keyInRange2));
      assertNotNull(testTable.get(endKey));
    }
  }

  @Test
  public void orderOfBatchOperations() throws Exception {
    final Table<byte[], byte[]> testTable = rdbStore.getTable("Fifth");
    try (BatchOperation batch = rdbStore.initBatchOperation()) {

      //given
      String keyStr = RandomStringUtils.secure().next(10);
      byte[] startKey = ("1-" + keyStr).getBytes(StandardCharsets.UTF_8);
      byte[] keyInRange1 = ("2-" + keyStr).getBytes(StandardCharsets.UTF_8);
      byte[] endKey = ("3-" + keyStr).getBytes(StandardCharsets.UTF_8);
      byte[] value1 = ("value1-" + RandomStringUtils.secure().next(10)).getBytes(StandardCharsets.UTF_8);
      byte[] value2 = ("value2-" + RandomStringUtils.secure().next(10)).getBytes(StandardCharsets.UTF_8);
      byte[] value3 = ("value3-" + RandomStringUtils.secure().next(10)).getBytes(StandardCharsets.UTF_8);

      //when
      testTable.putWithBatch(batch, startKey, value1);
      testTable.putWithBatch(batch, keyInRange1, value1);
      testTable.deleteWithBatch(batch, keyInRange1);
      // ops map key should be <<startKey, endKey>, 1>
      testTable.deleteRangeWithBatch(batch, startKey, endKey);
      testTable.putWithBatch(batch, startKey, value2);
      testTable.putWithBatch(batch, keyInRange1, value2);
      // ops map key is <<startKey, keyInRange1>, 2>.
      testTable.deleteRangeWithBatch(batch, startKey, keyInRange1);
      testTable.putWithBatch(batch, endKey, value1);
      testTable.putWithBatch(batch, endKey, value2);
      // ops map key is <<startKey, endKey>, 3>.
      testTable.deleteRangeWithBatch(batch, startKey, endKey);
      testTable.putWithBatch(batch, startKey, value3);

      rdbStore.commitBatchOperation(batch);

      //then
      assertEquals(bytes2String(value3), bytes2String(testTable.get(startKey)));
      assertNull(testTable.get(keyInRange1));
      assertEquals(bytes2String(value2), bytes2String(testTable.get(endKey)));
    }
  }
}
