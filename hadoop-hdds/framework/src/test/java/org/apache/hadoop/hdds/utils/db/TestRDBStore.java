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

import javax.management.MBeanServer;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
    rdbStore = new RDBStore(tempDir, options, configSet);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rdbStore != null) {
      rdbStore.close();
    }
  }
  private void insertRandomData(RDBStore dbStore, int familyIndex)
      throws Exception {
    try (Table firstTable = dbStore.getTable(families.get(familyIndex))) {
      Assertions.assertNotNull(firstTable, "Table cannot be null");
      for (int x = 0; x < 100; x++) {
        byte[] key =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        firstTable.put(key, value);
      }
    }
  }

  @Test
  public void compactDB() throws Exception {
    Assertions.assertNotNull(rdbStore, "DB Store cannot be null");
    insertRandomData(rdbStore, 1);
    // This test does not assert anything if there is any error this test
    // will throw and fail.
    rdbStore.compactDB();
  }

  @Test
  public void close() throws Exception {
    Assertions.assertNotNull(rdbStore, "DBStore cannot be null");
    // This test does not assert anything if there is any error this test
    // will throw and fail.
    rdbStore.close();
    Assertions.assertTrue(rdbStore.isClosed());
  }

  @Test
  public void closeUnderlyingDB() throws Exception {
    Assertions.assertNotNull(rdbStore, "DBStore cannot be null");
    rdbStore.getDb().close();
    Assertions.assertTrue(rdbStore.isClosed());
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
        Assertions.assertNotNull(newvalue);
        //and it is same as what we wrote to the FirstTable
        Assertions.assertArrayEquals(value, newvalue);
      }
      // After move this key must not exist in the first table.
      Assertions.assertNull(firstTable.get(key));
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
        Assertions.assertNotNull(newvalue);
        //and it is not same as what we wrote to the FirstTable, and equals
        // the new value.
        Assertions.assertArrayEquals(nextValue, newvalue);
      }
    }

  }

  @Test
  public void getEstimatedKeyCount() throws Exception {
    Assertions.assertNotNull(rdbStore, "DB Store cannot be null");

    // Write 100 keys to the first table.
    insertRandomData(rdbStore, 1);

    // Write 100 keys to the secondTable table.
    insertRandomData(rdbStore, 2);

    // Let us make sure that our estimate is not off by 10%
    Assertions.assertTrue(rdbStore.getEstimatedKeyCount() > 180
        || rdbStore.getEstimatedKeyCount() < 220);
  }

  @Test
  public void getStatMBeanName() throws Exception {

    try (Table firstTable = rdbStore.getTable(families.get(1))) {
      for (int y = 0; y < 100; y++) {
        byte[] key =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        byte[] value =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        firstTable.put(key, value);
      }
    }
    MBeanServer platformMBeanServer =
        ManagementFactory.getPlatformMBeanServer();
    Thread.sleep(2000);

    Object keysWritten = platformMBeanServer
        .getAttribute(rdbStore.getStatMBeanName(), "NUMBER_KEYS_WRITTEN");

    Assertions.assertTrue(((Long) keysWritten) >= 99L);

    Object dbWriteAverage = platformMBeanServer
        .getAttribute(rdbStore.getStatMBeanName(), "DB_WRITE_AVERAGE");
    Assertions.assertTrue((double) dbWriteAverage > 0);
  }

  @Test
  public void getTable() throws Exception {
    for (String tableName : families) {
      try (Table table = rdbStore.getTable(tableName)) {
        Assertions.assertNotNull(table, tableName + "is null");
      }
    }
    Assertions.assertThrows(IOException.class,
        () -> rdbStore.getTable("ATableWithNoName"));
  }

  @Test
  public void listTables() throws Exception {
    List<Table> tableList = rdbStore.listTables();
    Assertions.assertNotNull(tableList, "Table list cannot be null");
    Map<String, Table> hashTable = new HashMap<>();

    for (Table t : tableList) {
      hashTable.put(t.getName(), t);
    }

    int count = families.size();
    // Assert that we have all the tables in the list and no more.
    for (String name : families) {
      Assertions.assertTrue(hashTable.containsKey(name));
      count--;
    }
    Assertions.assertEquals(0, count);
  }

  @Test
  public void testRocksDBCheckpoint() throws Exception {
    Assertions.assertNotNull(rdbStore, "DB Store cannot be null");

    insertRandomData(rdbStore, 1);
    DBCheckpoint checkpoint =
        rdbStore.getCheckpoint(true);
    Assertions.assertNotNull(checkpoint);

    RDBStore restoredStoreFromCheckPoint =
        new RDBStore(checkpoint.getCheckpointLocation().toFile(),
            options, configSet);

    // Let us make sure that our estimate is not off by 10%
    Assertions.assertTrue(
        restoredStoreFromCheckPoint.getEstimatedKeyCount() > 90
        || restoredStoreFromCheckPoint.getEstimatedKeyCount() < 110);
    checkpoint.cleanupCheckpoint();
  }

  @Test
  public void testRocksDBCheckpointCleanup() throws Exception {
    Assertions.assertNotNull(rdbStore, "DB Store cannot be null");

    insertRandomData(rdbStore, 1);
    DBCheckpoint checkpoint =
        rdbStore.getCheckpoint(true);
    Assertions.assertNotNull(checkpoint);

    Assertions.assertTrue(Files.exists(
        checkpoint.getCheckpointLocation()));
    checkpoint.cleanupCheckpoint();
    Assertions.assertFalse(Files.exists(
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
    Assertions.assertEquals(2, rdbStore.getDb().getLatestSequenceNumber());

    DBUpdatesWrapper dbUpdatesSince = rdbStore.getUpdatesSince(0);
    Assertions.assertEquals(2, dbUpdatesSince.getData().size());
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
    }
    Assertions.assertEquals(2, rdbStore.getDb().getLatestSequenceNumber());

    DBUpdatesWrapper dbUpdatesSince = rdbStore.getUpdatesSince(0, 1);
    Assertions.assertEquals(1, dbUpdatesSince.getData().size());
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
    rdbStore = new RDBStore(rdbStore.getDbLocation(), options, configSet);
    for (String family : familiesMinusOne) {
      try (Table table = rdbStore.getTable(family)) {
        Assertions.assertNotNull(table, family + "is null");
        Object val = table.get(family.getBytes(StandardCharsets.UTF_8));
        Assertions.assertNotNull(val);
      }
    }

    // Technically the extra column family should also be open, even though
    // we do not use it.
    String extraFamily = families.get(families.size() - 1);
    try (Table table = rdbStore.getTable(extraFamily)) {
      Assertions.assertNotNull(table, extraFamily + "is null");
      Object val = table.get(extraFamily.getBytes(StandardCharsets.UTF_8));
      Assertions.assertNotNull(val);
    }
  }

}