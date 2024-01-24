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
import java.io.FileInputStream;
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

import org.apache.hadoop.hdds.StringUtils;

import org.apache.commons.lang3.RandomStringUtils;
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

import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RDBStore Tests.
 */
public class TestRDBStore {
  public static RDBStore newRDBStore(File dbFile, ManagedDBOptions options,
      Set<TableConfig> families,
      long maxDbUpdatesSizeThreshold)
      throws IOException {
    return new RDBStore(dbFile, options, null, new ManagedWriteOptions(), families,
        CodecRegistry.newBuilder().build(), false, 1000, null, false,
        maxDbUpdatesSizeThreshold, true, null, "");
  }

  public static final int MAX_DB_UPDATES_SIZE_THRESHOLD = 80;
  private final List<String> families =
      Arrays.asList(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
          "First", "Second", "Third",
          "Fourth", "Fifth",
          "Sixth");
  private RDBStore rdbStore = null;
  private ManagedDBOptions options = null;
  private Set<TableConfig> configSet;

  @BeforeEach
  public void setUp(@TempDir File tempDir) throws Exception {
    CodecBuffer.enableLeakDetection();

    options = new ManagedDBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);

    Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    options.setStatistics(statistics);
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
    try (Table<byte[], byte[]> firstTable = dbStore.getTable(families.
        get(familyIndex))) {
      assertNotNull(firstTable, "Table cannot be null");
      for (int x = 0; x < 100; x++) {
        byte[] key =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        firstTable.put(key, value);
      }
    } catch (Exception e) {
      throw new IOException(e);
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
  public void moveKey() throws Exception {
    byte[] key =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    byte[] value =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);

    try (Table firstTable = rdbStore.getTable(families.get(1))) {
      firstTable.put(key, value);
      try (Table<byte[], byte[]> secondTable = rdbStore
          .getTable(families.get(2))) {
        rdbStore.move(key, firstTable, secondTable);
        byte[] newvalue = secondTable.get(key);
        // Make sure we have value in the second table
        assertNotNull(newvalue);
        //and it is same as what we wrote to the FirstTable
        assertArrayEquals(value, newvalue);
      }
      // After move this key must not exist in the first table.
      assertNull(firstTable.get(key));
    }
  }

  @Test
  public void moveWithValue() throws Exception {
    byte[] key =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    byte[] value =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);

    byte[] nextValue =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    try (Table firstTable = rdbStore.getTable(families.get(1))) {
      firstTable.put(key, value);
      try (Table<byte[], byte[]> secondTable = rdbStore
          .getTable(families.get(2))) {
        rdbStore.move(key, nextValue, firstTable, secondTable);
        byte[] newvalue = secondTable.get(key);
        // Make sure we have value in the second table
        assertNotNull(newvalue);
        //and it is not same as what we wrote to the FirstTable, and equals
        // the new value.
        assertArrayEquals(nextValue, newvalue);
      }
    }

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
      try (Table table = rdbStore.getTable(tableName)) {
        assertNotNull(table, tableName + "is null");
      }
    }
    assertThrows(IOException.class,
        () -> rdbStore.getTable("ATableWithNoName"));
  }

  @Test
  public void listTables() throws Exception {
    List<Table> tableList = rdbStore.listTables();
    assertNotNull(tableList, "Table list cannot be null");
    Map<String, Table> hashTable = new HashMap<>();

    for (Table t : tableList) {
      hashTable.put(t.getName(), t);
    }

    int count = families.size();
    // Assert that we have all the tables in the list and no more.
    for (String name : families) {
      assertTrue(hashTable.containsKey(name));
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

    try (Table firstTable = rdbStore.getTable(families.get(1))) {
      firstTable.put(
          org.apache.commons.codec.binary.StringUtils.getBytesUtf16("Key1"),
          org.apache.commons.codec.binary.StringUtils
              .getBytesUtf16("Value1"));
      firstTable.put(
          org.apache.commons.codec.binary.StringUtils.getBytesUtf16("Key2"),
          org.apache.commons.codec.binary.StringUtils
              .getBytesUtf16("Value2"));
    }
    assertEquals(2, rdbStore.getDb().getLatestSequenceNumber());

    DBUpdatesWrapper dbUpdatesSince = rdbStore.getUpdatesSince(0);
    assertEquals(2, dbUpdatesSince.getData().size());
  }

  @Test
  public void testGetDBUpdatesSinceWithLimitCount() throws Exception {

    try (Table firstTable = rdbStore.getTable(families.get(1))) {
      firstTable.put(
          org.apache.commons.codec.binary.StringUtils.getBytesUtf16("Key1"),
          org.apache.commons.codec.binary.StringUtils
              .getBytesUtf16("Value1"));
      firstTable.put(
          org.apache.commons.codec.binary.StringUtils.getBytesUtf16("Key2"),
          org.apache.commons.codec.binary.StringUtils
              .getBytesUtf16("Value2"));
      firstTable.put(
          org.apache.commons.codec.binary.StringUtils.getBytesUtf16("Key3"),
          org.apache.commons.codec.binary.StringUtils
              .getBytesUtf16("Value3"));
      firstTable.put(
          org.apache.commons.codec.binary.StringUtils.getBytesUtf16("Key4"),
          org.apache.commons.codec.binary.StringUtils
              .getBytesUtf16("Value4"));
      firstTable.put(
          org.apache.commons.codec.binary.StringUtils.getBytesUtf16("Key5"),
          org.apache.commons.codec.binary.StringUtils
              .getBytesUtf16("Value5"));
    }
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
      try (Table table = rdbStore.getTable(family)) {
        byte[] key = family.getBytes(StandardCharsets.UTF_8);
        byte[] value =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        table.put(key, value);
      }
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
      try (Table table = rdbStore.getTable(family)) {
        assertNotNull(table, family + "is null");
        Object val = table.get(family.getBytes(StandardCharsets.UTF_8));
        assertNotNull(val);
      }
    }

    // Technically the extra column family should also be open, even though
    // we do not use it.
    String extraFamily = families.get(families.size() - 1);
    try (Table table = rdbStore.getTable(extraFamily)) {
      assertNotNull(table, extraFamily + "is null");
      Object val = table.get(extraFamily.getBytes(StandardCharsets.UTF_8));
      assertNotNull(val);
    }
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

      try (InputStream fileStream1 = new FileInputStream(fileInCk1);
           InputStream fileStream2 = new FileInputStream(fileInCk2)) {
        byte[] content1 = new byte[fileStream1.available()];
        byte[] content2 = new byte[fileStream2.available()];
        fileStream1.read(content1);
        fileStream2.read(content2);
        assertArrayEquals(content1, content2);
      }
    }
  }
}
