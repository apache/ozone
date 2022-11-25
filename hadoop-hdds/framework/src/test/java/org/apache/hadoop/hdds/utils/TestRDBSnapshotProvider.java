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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.TestRDBStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.writeDBCheckpointToStream;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Common RocksDB's snapshot provider service.
 */
public class TestRDBSnapshotProvider extends TestRDBStore {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestRDBSnapshotProvider.class);

  private RDBStore rdbStore = null;
  private String leaderId = "leaderNode-1";
  private RDBSnapshotProvider rdbSnapshotProvider;
  private File testDir;
  private AtomicReference<DBCheckpoint> latestCK = new AtomicReference<>(null);
  private int numUsedCF;

  @BeforeEach
  public void init(@TempDir File tempDir) throws Exception {
    setUp(tempDir);
    testDir = tempDir;
    rdbStore = getRdbStore();
    numUsedCF = 3;
    rdbSnapshotProvider = new RDBSnapshotProvider(testDir, "test.db") {
      @Override
      public void close() throws IOException {
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
                  rdbSnapshotProvider.getCandidateDir()));
        }
      }
    };
  }

  @AfterEach
  public void down() throws Exception {
    tearDown();
    if (testDir.exists()) {
      FileUtil.fullyDelete(testDir);
    }
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
    rdbSnapshotProvider.initialize();
    assertEquals(0, HAUtils.getExistingSstFiles(
        rdbSnapshotProvider.getCandidateDir()).size());
  }

  public void compareDB(File db1, File db2, int columnFamilyUsed)
      throws Exception {
    try (RDBStore rdbStore1 = new RDBStore(db1, getNewDBOptions(),
             getConfigSet());
         RDBStore rdbStore2 = new RDBStore(db2, getNewDBOptions(),
             getConfigSet())) {
      // all entries should be same from two DB
      for (int i = 0; i < columnFamilyUsed; i++) {
        try (TableIterator<byte[], ? extends KeyValue<byte[], byte[]>> iterator
                 = rdbStore1.getTable(getFamilies().get(i)).iterator()) {
          while (iterator.hasNext()) {
            KeyValue<byte[], byte[]> keyValue = iterator.next();
            byte[] key = keyValue.getKey();
            byte[] value1 = keyValue.getValue();
            byte[] value2 = rdbStore2.getTable(getFamilies().get(i))
                .getIfExist(key);
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
}
