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

import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

/**
 * RDBStore Tests.
 */
public class TestRDBStore {

  public static final int MAX_DB_UPDATES_SIZE_THRESHOLD = 80;
  private final List<String> families =
      Arrays.asList(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
          "First", "Second", "Third",
          "Fourth", "Fifth",
          "Sixth");
  private RDBStore rdbStore = null;
  private ManagedDBOptions options;
  private Set<TableConfig> configSet;

  static ManagedDBOptions newManagedDBOptions() {
    final ManagedDBOptions options = new ManagedDBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);

    Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    options.setStatistics(statistics);
    return options;
  }

  static RDBStore newRDBStore(File dbFile, ManagedDBOptions options, Set<TableConfig> families)
      throws IOException {
    return newRDBStore(dbFile, options, families, MAX_DB_UPDATES_SIZE_THRESHOLD);
  }

  public static RDBStore newRDBStore(File dbFile, ManagedDBOptions options,
      Set<TableConfig> families,
      long maxDbUpdatesSizeThreshold)
      throws IOException {
    return new RDBStore(dbFile, options, null, new ManagedWriteOptions(), families,
        false, null, false, null,
        maxDbUpdatesSizeThreshold, true, null, true);
  }

  @BeforeEach
  public void setUp(@TempDir File tempDir) throws Exception {
    CodecBuffer.enableLeakDetection();

    options = newManagedDBOptions();
    configSet = new HashSet<>();
    for (String name : families) {
      TableConfig newConfig = new TableConfig(name,
          new ManagedColumnFamilyOptions());
      configSet.add(newConfig);
    }
    rdbStore = newRDBStore(tempDir, options, configSet,
        MAX_DB_UPDATES_SIZE_THRESHOLD);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rdbStore != null) {
      rdbStore.close();
    }
    CodecBuffer.assertNoLeaks();
  }

  public void insertRandomData(RDBStore dbStore, int familyIndex)
      throws IOException {
    Table<byte[], byte[]> firstTable = dbStore.getTable(families.get(familyIndex));
    assertNotNull(firstTable, "Table cannot be null");
    for (int x = 0; x < 100; x++) {
      byte[] key =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      firstTable.put(key, value);
    }
  }

  @Test
  public void compactDB() throws Exception {
    assertNotNull(rdbStore, "DB Store cannot be null");
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j <= 10; j++) {
        insertRandomData(rdbStore, i);
        rdbStore.flushDB();
      }
    }

    int metaSizeBeforeCompact = rdbStore.getDb().getLiveFilesMetaDataSize();
    rdbStore.compactDB();
    int metaSizeAfterCompact = rdbStore.getDb().getLiveFilesMetaDataSize();

    assertThat(metaSizeAfterCompact).isLessThan(metaSizeBeforeCompact);
    assertEquals(metaSizeAfterCompact, 2);

  }

  @Test
  public void compactTable() throws Exception {
    assertNotNull(rdbStore, "DBStore cannot be null");

    for (int j = 0; j <= 20; j++) {
      insertRandomData(rdbStore, 0);
      rdbStore.flushDB();
    }

    int metaSizeBeforeCompact = rdbStore.getDb().getLiveFilesMetaDataSize();
    rdbStore.compactTable(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY));
    int metaSizeAfterCompact = rdbStore.getDb().getLiveFilesMetaDataSize();

    assertThat(metaSizeAfterCompact).isLessThan(metaSizeBeforeCompact);
    assertThat(metaSizeAfterCompact).isEqualTo(1);
  }

  @Test
  public void close() throws Exception {
    assertNotNull(rdbStore, "DBStore cannot be null");
    // This test does not assert anything if there is any error this test
    // will throw and fail.
    rdbStore.close();
    assertTrue(rdbStore.isClosed());
  }

  @Test
  public void closeUnderlyingDB() throws Exception {
    assertNotNull(rdbStore, "DBStore cannot be null");
    rdbStore.getDb().close();
    assertTrue(rdbStore.isClosed());
  }

  @Test
  public void getEstimatedKeyCount() throws Exception {
    assertNotNull(rdbStore, "DB Store cannot be null");

    // Write 100 keys to the first table.
    insertRandomData(rdbStore, 1);

    // Write 100 keys to the secondTable table.
    insertRandomData(rdbStore, 2);

    // Let us make sure that our estimate is not off by 10%
    assertTrue(rdbStore.getEstimatedKeyCount() > 180
        || rdbStore.getEstimatedKeyCount() < 220);
  }

  @Test
  public void getTable() throws Exception {
    for (String tableName : families) {
      Table<byte[], byte[]> table = rdbStore.getTable(tableName);
      assertNotNull(table, tableName + "is null");
    }
    assertThrows(IOException.class,
        () -> rdbStore.getTable("ATableWithNoName"));
  }

  @Test
  public void listTables() throws Exception {
    final List<Table<?, ?>> tableList = rdbStore.listTables();
    assertNotNull(tableList, "Table list cannot be null");
    Map<String, Table> hashTable = new HashMap<>();

    for (Table t : tableList) {
      hashTable.put(t.getName(), t);
    }

    int count = families.size();
    // Assert that we have all the tables in the list and no more.
    for (String name : families) {
      assertThat(hashTable).containsKey(name);
      count--;
    }
    assertEquals(0, count);
  }

  @Test
  public void testRocksDBCheckpoint() throws Exception {
    assertNotNull(rdbStore, "DB Store cannot be null");

    insertRandomData(rdbStore, 1);
    DBCheckpoint checkpoint =
        rdbStore.getCheckpoint(true);
    assertNotNull(checkpoint);

    RDBStore restoredStoreFromCheckPoint =
        newRDBStore(checkpoint.getCheckpointLocation().toFile(),
            options, configSet, MAX_DB_UPDATES_SIZE_THRESHOLD);

    // Let us make sure that our estimate is not off by 10%
    assertTrue(
        restoredStoreFromCheckPoint.getEstimatedKeyCount() > 90
        || restoredStoreFromCheckPoint.getEstimatedKeyCount() < 110);
    checkpoint.cleanupCheckpoint();
  }

  @Test
  public void testRocksDBCheckpointCleanup() throws Exception {
    assertNotNull(rdbStore, "DB Store cannot be null");

    insertRandomData(rdbStore, 1);
    DBCheckpoint checkpoint =
        rdbStore.getCheckpoint(true);
    assertNotNull(checkpoint);

    assertTrue(Files.exists(
        checkpoint.getCheckpointLocation()));
    checkpoint.cleanupCheckpoint();
    assertFalse(Files.exists(
        checkpoint.getCheckpointLocation()));
  }

  @Test
  public void testGetDBUpdatesSince() throws Exception {
    final Table<byte[], byte[]> firstTable = rdbStore.getTable(families.get(1));
    firstTable.put(
        getBytesUtf16("Key1"),
        getBytesUtf16("Value1"));
    firstTable.put(
        getBytesUtf16("Key2"),
        getBytesUtf16("Value2"));
    assertEquals(2, rdbStore.getDb().getLatestSequenceNumber());

    DBUpdatesWrapper dbUpdatesSince = rdbStore.getUpdatesSince(0);
    assertEquals(2, dbUpdatesSince.getData().size());
  }

  static byte[] getBytesUtf16(String s) {
    return s.getBytes(StandardCharsets.UTF_16);
  }

  @Test
  public void testGetDBUpdatesSinceWithLimitCount() throws Exception {

    final Table<byte[], byte[]> firstTable = rdbStore.getTable(families.get(1));
    firstTable.put(
        getBytesUtf16("Key1"),
        getBytesUtf16("Value1"));
    firstTable.put(
        getBytesUtf16("Key2"),
        getBytesUtf16("Value2"));
    firstTable.put(
        getBytesUtf16("Key3"),
        getBytesUtf16("Value3"));
    firstTable.put(
        getBytesUtf16("Key4"),
        getBytesUtf16("Value4"));
    firstTable.put(
        getBytesUtf16("Key5"),
        getBytesUtf16("Value5"));
    assertEquals(5, rdbStore.getDb().getLatestSequenceNumber());

    DBUpdatesWrapper dbUpdatesSince = rdbStore.getUpdatesSince(0, 5);
    assertEquals(2, dbUpdatesSince.getData().size());
    assertEquals(2, dbUpdatesSince.getCurrentSequenceNumber());
  }

  @Test
  public void testDowngrade() throws Exception {

    // Write data to current DB which has 6 column families at the time of
    // writing this test.
    for (String family : families) {
      final Table<byte[], byte[]> table = rdbStore.getTable(family);
      byte[] key = family.getBytes(StandardCharsets.UTF_8);
      byte[] value = RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8);
      table.put(key, value);
    }
    // Close current DB.
    rdbStore.close();

    // Reopen DB with the last column family removed.
    options = new ManagedDBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);
    configSet = new HashSet<>();
    List<String> familiesMinusOne = families.subList(0, families.size() - 1);
    for (String name : familiesMinusOne) {
      TableConfig newConfig = new TableConfig(name,
          new ManagedColumnFamilyOptions());
      configSet.add(newConfig);
    }
    rdbStore = newRDBStore(rdbStore.getDbLocation(), options, configSet,
        MAX_DB_UPDATES_SIZE_THRESHOLD);
    for (String family : familiesMinusOne) {
      final Table<byte[], byte[]> table = rdbStore.getTable(family);
      assertNotNull(table, family + "is null");
      Object val = table.get(family.getBytes(StandardCharsets.UTF_8));
      assertNotNull(val);
    }

    // Technically the extra column family should also be open, even though
    // we do not use it.
    String extraFamily = families.get(families.size() - 1);
    final Table<byte[], byte[]> table = rdbStore.getTable(extraFamily);
    assertNotNull(table, extraFamily + "is null");
    Object val = table.get(extraFamily.getBytes(StandardCharsets.UTF_8));
    assertNotNull(val);
  }

  @Test
  public void testSstConsistency() throws IOException {
    for (int i = 0; i < 10; i++) {
      insertRandomData(rdbStore, 0);
      insertRandomData(rdbStore, 1);
      insertRandomData(rdbStore, 2);
    }
    DBCheckpoint dbCheckpoint1 = rdbStore.getCheckpoint(true);

    for (int i = 0; i < 10; i++) {
      insertRandomData(rdbStore, 0);
      insertRandomData(rdbStore, 1);
      insertRandomData(rdbStore, 2);
    }
    DBCheckpoint dbCheckpoint2 = rdbStore.getCheckpoint(true);
    compareSstWithSameName(dbCheckpoint1.getCheckpointLocation().toFile(),
        dbCheckpoint2.getCheckpointLocation().toFile());

    for (int i = 0; i < 10; i++) {
      insertRandomData(rdbStore, 0);
      insertRandomData(rdbStore, 1);
      insertRandomData(rdbStore, 2);
    }
    DBCheckpoint dbCheckpoint3 = rdbStore.getCheckpoint(true);
    compareSstWithSameName(dbCheckpoint2.getCheckpointLocation().toFile(),
        dbCheckpoint3.getCheckpointLocation().toFile());
  }

  private void compareSstWithSameName(File checkpoint1, File checkpoint2)
      throws IOException {
    FilenameFilter filter = (dir, name) -> name.endsWith(ROCKSDB_SST_SUFFIX);
    String[] files1 = checkpoint1.list(filter);
    String[] files2 = checkpoint1.list(filter);
    assert files1 != null;
    assert files2 != null;
    // Get all file names in the both checkpoints
    List<String> result = Arrays.asList(files1);
    result.retainAll(Arrays.asList(files2));

    for (String name: result) {
      File fileInCk1 = new File(checkpoint1.getAbsoluteFile(), name);
      File fileInCk2 = new File(checkpoint2.getAbsoluteFile(), name);
      long length1 = fileInCk1.length();
      long length2 = fileInCk2.length();
      assertEquals(length1, length2, name);

      try (InputStream fileStream1 = Files.newInputStream(fileInCk1.toPath());
           InputStream fileStream2 = Files.newInputStream(fileInCk2.toPath())) {
        byte[] content1 = new byte[fileStream1.available()];
        byte[] content2 = new byte[fileStream2.available()];
        fileStream1.read(content1);
        fileStream2.read(content2);
        assertArrayEquals(content1, content2);
      }
    }
  }

  @Test
  public void testIteratorWithSeek() throws Exception {
    final Table<byte[], byte[]> table = rdbStore.getTable(families.get(0));
    // Write keys: a1, a3, a5, b2, b4
    table.put(getBytesUtf16("a1"), getBytesUtf16("val1"));
    table.put(getBytesUtf16("a3"), getBytesUtf16("val3"));
    table.put(getBytesUtf16("a5"), getBytesUtf16("val5"));
    table.put(getBytesUtf16("b2"), getBytesUtf16("val2"));
    table.put(getBytesUtf16("b4"), getBytesUtf16("val4"));

    // Case 1: Seek to existing key, no prefix
    try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iter = table.iterator(null,
        getBytesUtf16("a3"))) {
      assertTrue(iter.hasNext());
      assertArrayEquals(getBytesUtf16("a3"), iter.next().getKey());
      assertTrue(iter.hasNext());
      assertArrayEquals(getBytesUtf16("a5"), iter.next().getKey());
    }

    // Case 2: Seek to non-existent key (should land on next greater), no prefix
    try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iter = table.iterator(null,
        getBytesUtf16("a2"))) {
      assertTrue(iter.hasNext());
      assertArrayEquals(getBytesUtf16("a3"), iter.next().getKey());
    }

    // Case 3: Seek past all keys, no prefix
    try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iter = table.iterator(null,
        getBytesUtf16("z9"))) {
      assertFalse(iter.hasNext());
    }

    // Case 4: Seek with prefix
    try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iter = table.iterator(getBytesUtf16("b"),
        getBytesUtf16("b3"))) {
      assertTrue(iter.hasNext());
      assertArrayEquals(getBytesUtf16("b4"), iter.next().getKey());
      assertFalse(iter.hasNext());
    }

    // Case 5: Seek with prefix to start of prefix
    try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iter = table.iterator(getBytesUtf16("b"),
        getBytesUtf16("b2"))) {
      assertTrue(iter.hasNext());
      assertArrayEquals(getBytesUtf16("b2"), iter.next().getKey());
    }
  }

  @Test
  public void testIteratorSeekEdgeCases() throws Exception {
    final Table<byte[], byte[]> table = rdbStore.getTable(families.get(0));
    // Empty table check
    try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iter = table.iterator(null,
        getBytesUtf16("a1"))) {
      assertFalse(iter.hasNext());
    }

    table.put(getBytesUtf16("a1"), getBytesUtf16("val1"));

    // Seek before first key
    try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iter = table.iterator(null,
        getBytesUtf16("00"))) {
      assertTrue(iter.hasNext());
      assertArrayEquals(getBytesUtf16("a1"), iter.next().getKey());
    }

    // Seek after last key
    try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iter = table.iterator(null,
        getBytesUtf16("b1"))) {
      assertFalse(iter.hasNext());
    }
  }

  @Test
  public void testConcurrentIteratorWithWrites() throws Exception {
    final Table<byte[], byte[]> table = rdbStore.getTable(families.get(1));
    final int keyCount = 5000;
    final CountDownLatch readyLatch = new CountDownLatch(1);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicLong writtenKeys = new AtomicLong(0);

    // Writer thread
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Void> writer = executor.submit(() -> {
      readyLatch.countDown();
      startLatch.await();
      for (int i = 0; i < keyCount; i++) {
        String key = String.format("key-%05d", i);
        table.put(getBytesUtf16(key), getBytesUtf16("value-" + i));
        writtenKeys.incrementAndGet();
        if (i % 100 == 0) {
          Thread.yield(); // Give reader a chance
        }
      }
      return null;
    });

    // Reader thread (using the optimization)
    Future<Void> reader = executor.submit(() -> {
      readyLatch.countDown();
      startLatch.await();
      // Wait for some data to be written
      while (writtenKeys.get() < 100) {
        Thread.sleep(1);
      }

      int seeks = 0;
      for (int i = 0; i < 100; i++) {
        // Randomly seek to a key that should exist (or be close to existing)
        long targetId = (long) (Math.random() * writtenKeys.get());
        String seekKeyStr = String.format("key-%05d", targetId);
        try (TableIterator<byte[], ? extends Table.KeyValue<byte[], byte[]>> iter = table.iterator(null,
            getBytesUtf16(seekKeyStr))) {
          if (iter.hasNext()) {
            assertNotNull(iter.next().getKey());
          }
          seeks++;
        }
      }
      assertTrue(seeks > 0);
      return null;
    });

    readyLatch.await();
    startLatch.countDown();

    writer.get(10, TimeUnit.SECONDS);
    reader.get(10, TimeUnit.SECONDS);

    executor.shutdown();
  }
}
