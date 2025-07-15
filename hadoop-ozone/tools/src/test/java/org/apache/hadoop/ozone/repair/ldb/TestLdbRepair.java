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

package org.apache.hadoop.ozone.repair.ldb;

import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Tests for RocksDB LDB repair tools.
 */
public class TestLdbRepair {

  private static final String TEST_CF_NAME = "testColumnFamily";
  private static final int NUM_KEYS = 100;
  
  @TempDir
  private Path tempDir;
  private Path dbPath;
  private RDBStore rdbStore;
  private ManagedDBOptions options;
  
  @BeforeEach
  public void setUp() throws Exception {
    CodecBuffer.enableLeakDetection();
    
    dbPath = tempDir.resolve("test.db");
    options = new ManagedDBOptions();
    options.setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    rdbStore = DBStoreBuilder.newBuilder(new OzoneConfiguration())
        .setName(dbPath.toFile().getName())
        .setPath(dbPath.getParent())
        .addTable(TEST_CF_NAME)
        .build();
  }
  
  @AfterEach
  public void tearDown() throws Exception {
    if (rdbStore != null && !rdbStore.isClosed()) {
      rdbStore.close();
    }
    if (options != null) {
      options.close();
    }
    CodecBuffer.assertNoLeaks();
  }

  /**
   * Test manual compaction of RocksDB.
   * This test creates a large number of keys, deletes them, and then triggers compaction.
   * The sizes after each step is compared to ensure that compaction reduces the size of the database.
   */
  @Test
  public void testRocksDBManualCompaction() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable(TEST_CF_NAME);

    // Create many keys
    for (int i = 0; i < NUM_KEYS; i++) {
      String key = "key" + i;
      String value = RandomStringUtils.secure().nextAlphanumeric(100);
      testTable.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }
    rdbStore.flushDB();
    long size1 = calculateSstFileSize(dbPath);
    
    // Delete all keys
    for (int i = 0; i < NUM_KEYS; i++) {
      String key = "key" + i;
      testTable.delete(key.getBytes(StandardCharsets.UTF_8));
    }
    rdbStore.flushDB();
    long size2 = calculateSstFileSize(dbPath);
    rdbStore.close();

    // Trigger compaction of the table
    RocksDBManualCompaction compactionTool = new RocksDBManualCompaction();
    CommandLine cmd = new CommandLine(compactionTool);
    String[] args = {
        "--db", dbPath.toString(),
        "--column-family", TEST_CF_NAME
    };
    int exitCode = withTextFromSystemIn("y")
        .execute(() -> cmd.execute(args));
    assertEquals(0, exitCode, "Compaction command should execute successfully");
    long size3 = calculateSstFileSize(dbPath);
    
    System.out.println("Size after adding keys (size1): " + size1);
    System.out.println("Size after deleting keys (size2): " + size2);
    System.out.println("Size after compaction (size3): " + size3);
    
    // size1 < size2 as deletes increase size due to tombstones
    assertTrue(size1 < size2, 
        String.format("Expected size1 (%d) < size2 (%d), but size1 >= size2", size1, size2));
    
    // size3 < size1 as compaction should reduce size below original
    assertTrue(size3 < size1,
        String.format("Expected size3 (%d) < size1 (%d), but size3 >= size1", size3, size1));
  }

  private long calculateSstFileSize(Path db) throws IOException {
    if (!Files.exists(db)) {
      return 0;
    }
    
    try (Stream<Path> paths = Files.walk(db)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.toString().endsWith(".sst"))
          .mapToLong(path -> {
            try {
              return Files.size(path);
            } catch (IOException e) {
              return 0;
            }
          })
          .sum();
    }
  }
}
