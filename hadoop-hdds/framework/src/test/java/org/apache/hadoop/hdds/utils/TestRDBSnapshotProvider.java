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
package org.apache.hadoop.hdds.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableConfig;
import org.apache.hadoop.hdds.utils.db.TableIterator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.writeDBCheckpointToStream;
import static org.apache.hadoop.hdds.utils.db.TestRDBStore.newRDBStore;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Common RocksDB's snapshot provider service.
 */
public class TestRDBSnapshotProvider {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestRDBSnapshotProvider.class);

  private final List<String> families =
      Arrays.asList(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
          "First", "Second", "Third");
  public static final int MAX_DB_UPDATES_SIZE_THRESHOLD = 80;

  private RDBStore rdbStore = null;
  private ManagedDBOptions options = null;
  private Set<TableConfig> configSet;
  private RDBSnapshotProvider rdbSnapshotProvider;
  private File testDir;
  private final int numUsedCF = 3;
  private final String leaderId = "leaderNode-1";
  private final AtomicReference<DBCheckpoint> latestCK =
      new AtomicReference<>(null);

  @BeforeEach
  public void init(@TempDir File tempDir) throws Exception {
    CodecBuffer.enableLeakDetection();

    options = getNewDBOptions();
    configSet = new HashSet<>();
    for (String name : families) {
      TableConfig newConfig = new TableConfig(name,
          new ManagedColumnFamilyOptions());
      configSet.add(newConfig);
    }
    testDir = tempDir;
    rdbStore = newRDBStore(tempDir, options, configSet,
        MAX_DB_UPDATES_SIZE_THRESHOLD);
    rdbSnapshotProvider = new RDBSnapshotProvider(testDir, "test.db") {
      @Override
      public void close() {
      }

      @Override
      public void downloadSnapshot(String leaderNodeID, File targetFile)
          throws IOException {
        for (int i = 0; i < 10; i++) {
          insertDataToDB(numUsedCF);
        }
        DBCheckpoint dbCheckpoint = rdbStore.getCheckpoint(true);
        latestCK.set(dbCheckpoint);
        File[] files = dbCheckpoint.getCheckpointLocation().toFile().
            listFiles();
        assertNotNull(files);
        LOG.info("Db files: {}", Arrays.stream(files)
            .map(a -> "".concat(a.getName()).concat(" length: ").
                concat(String.valueOf(a.length())))
            .collect(Collectors.toList()));
        try (OutputStream outputStream = new FileOutputStream(targetFile)) {
          writeDBCheckpointToStream(dbCheckpoint, outputStream,
              HAUtils.getExistingSstFiles(
                  rdbSnapshotProvider.getCandidateDir()), new ArrayList<>());
        }
      }
    };
  }

  @AfterEach
  public void down() throws Exception {
    if (rdbStore != null) {
      rdbStore.close();
    }
    if (testDir.exists()) {
      FileUtil.fullyDelete(testDir);
    }
    CodecBuffer.assertNoLeaks();
  }

  @Test
  public void testDownloadDBSnapshotFromLeader() throws Exception {
    File candidateDir = rdbSnapshotProvider.getCandidateDir();
    assertTrue(candidateDir.exists());

    DBCheckpoint checkpoint;
    int before = HAUtils.getExistingSstFiles(
        rdbSnapshotProvider.getCandidateDir()).size();
    assertEquals(0, before);

    // Get first snapshot
    checkpoint = rdbSnapshotProvider.downloadDBSnapshotFromLeader(leaderId);
    File checkpointDir = checkpoint.getCheckpointLocation().toFile();
    assertEquals(candidateDir, checkpointDir);
    int first = HAUtils.getExistingSstFiles(
        rdbSnapshotProvider.getCandidateDir()).size();

    // Get second snapshot
    checkpoint = rdbSnapshotProvider.downloadDBSnapshotFromLeader(leaderId);
    int second = HAUtils.getExistingSstFiles(
        rdbSnapshotProvider.getCandidateDir()).size();
    assertTrue(second > first, "The second snapshot should" +
        " have more SST files");
    DBCheckpoint latestCheckpoint = latestCK.get();
    compareDB(latestCheckpoint.getCheckpointLocation().toFile(),
        checkpoint.getCheckpointLocation().toFile(), numUsedCF);

    // Get third snapshot
    checkpoint = rdbSnapshotProvider.downloadDBSnapshotFromLeader(leaderId);
    int third = HAUtils.getExistingSstFiles(
        rdbSnapshotProvider.getCandidateDir()).size();
    assertTrue(third > second, "The third snapshot should" +
        " have more SST files");
    compareDB(latestCK.get().getCheckpointLocation().toFile(),
        checkpoint.getCheckpointLocation().toFile(), numUsedCF);

    // Test cleanup candidateDB
    rdbSnapshotProvider.init();
    assertEquals(0, HAUtils.getExistingSstFiles(
        rdbSnapshotProvider.getCandidateDir()).size());
  }

  public void compareDB(File db1, File db2, int columnFamilyUsed)
      throws Exception {
    try (RDBStore rdbStore1 = newRDBStore(db1, getNewDBOptions(),
             configSet, MAX_DB_UPDATES_SIZE_THRESHOLD);
         RDBStore rdbStore2 = newRDBStore(db2, getNewDBOptions(),
             configSet, MAX_DB_UPDATES_SIZE_THRESHOLD)) {
      // all entries should be same from two DB
      for (int i = 0; i < columnFamilyUsed; i++) {
        final String name = families.get(i);
        final Table<byte[], byte[]> table1 = rdbStore1.getTable(name);
        final Table<byte[], byte[]> table2 = rdbStore2.getTable(name);
        try (TableIterator<byte[], ? extends KeyValue<byte[], byte[]>> iterator
                 = table1.iterator()) {
          while (iterator.hasNext()) {
            KeyValue<byte[], byte[]> keyValue = iterator.next();
            byte[] key = keyValue.getKey();
            byte[] value1 = keyValue.getValue();
            byte[] value2 = table2.getIfExist(key);
            assertArrayEquals(value1, value2);
          }
        }
      }
    }
  }

  private void insertDataToDB(int columnFamilyUsed) throws IOException {
    for (int i = 0; i < columnFamilyUsed; i++) {
      insertRandomData(rdbStore, i);
    }
  }

  public ManagedDBOptions getNewDBOptions() {
    ManagedDBOptions managedOptions = new ManagedDBOptions();
    managedOptions.setCreateIfMissing(true);
    managedOptions.setCreateMissingColumnFamilies(true);

    Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    managedOptions.setStatistics(statistics);
    return managedOptions;
  }

  public void insertRandomData(RDBStore dbStore, int familyIndex)
      throws IOException {
    try (Table<byte[], byte[]> firstTable = dbStore.getTable(families.
        get(familyIndex))) {
      Assertions.assertNotNull(firstTable, "Table cannot be null");
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
  public void testCheckLeaderConsistency() throws IOException {
    // Leader initialized to null at startup.
    assertEquals(1, rdbSnapshotProvider.getInitCount());
    File dummyFile = new File(rdbSnapshotProvider.getCandidateDir(),
        "file1.sst");
    Files.write(dummyFile.toPath(),
        "dummyData".getBytes(StandardCharsets.UTF_8));
    assertTrue(dummyFile.exists());

    // Set the leader.
    rdbSnapshotProvider.checkLeaderConsistency("node1");
    assertEquals(2, rdbSnapshotProvider.getInitCount());
    assertFalse(dummyFile.exists());

    // Confirm setting the same leader doesn't reinitialize.
    rdbSnapshotProvider.checkLeaderConsistency("node1");
    assertEquals(2, rdbSnapshotProvider.getInitCount());

    // Confirm setting different leader does reinitialize.
    rdbSnapshotProvider.checkLeaderConsistency("node2");
    assertEquals(3, rdbSnapshotProvider.getInitCount());
  }
}
