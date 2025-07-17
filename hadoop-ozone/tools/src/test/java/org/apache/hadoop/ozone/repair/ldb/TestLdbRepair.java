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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableConfig;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBlockBasedTableConfig;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.OptionsUtil;
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

    DatabaseOptions optionsBeforeCompaction = readDatabaseOptions();
    // FIXME: After this a new RocksDB OPTIONS was created
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

    DatabaseOptions optionsAfterCompaction = readDatabaseOptions();

    optionsBeforeCompaction.assertEqualOptions(optionsAfterCompaction);
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

  /**
   * Helper method to read the latest RocksDB options from a database and test column family.
   */
  private DatabaseOptions readDatabaseOptions() throws Exception {
    assertNotNull(dbPath);
    ManagedConfigOptions configOptions = new ManagedConfigOptions();
    ManagedDBOptions dbOptions = new ManagedDBOptions();
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = new ArrayList<>();

    try {
      OptionsUtil.loadLatestOptions(configOptions, dbPath.toString(), dbOptions, cfDescList);
      try (ManagedRocksDB db = ManagedRocksDB.open(dbOptions, dbPath.toString(), cfDescList, cfHandleList)) {
        DatabaseOptions.Builder builder = new DatabaseOptions.Builder();
        builder.setMaxBackgroundCompactions(dbOptions.maxBackgroundCompactions())
            .setMaxBackgroundFlushes(dbOptions.maxBackgroundFlushes())
            .setBytesPerSync(dbOptions.bytesPerSync())
            .setMaxLogFileSize(dbOptions.maxLogFileSize())
            .setKeepLogFileNum(dbOptions.keepLogFileNum())
            .setWalTTL(dbOptions.walTtlSeconds())
            .setWalSizeLimit(dbOptions.walSizeLimitMB());

        Optional<ColumnFamilyDescriptor> testCfDesc = cfDescList.stream().filter(cfDesc ->
            TableConfig.toName(cfDesc.getName()).equals(TEST_CF_NAME)).findFirst();
        if (testCfDesc.isPresent()) {
          ColumnFamilyOptions cfOptions = testCfDesc.get().getOptions();
          builder.setWriteBufferSize(cfOptions.writeBufferSize());
          if (cfOptions.tableFormatConfig() instanceof ManagedBlockBasedTableConfig) {
            ManagedBlockBasedTableConfig tableConfig = (ManagedBlockBasedTableConfig) cfOptions.tableFormatConfig();
            builder.setBlockSize(tableConfig.blockSize());
            builder.setPinL0FilterAndIndexBlocksInCache(tableConfig.pinL0FilterAndIndexBlocksInCache());
          }
        }
        return builder.build();
      }
    } finally {
      configOptions.close();
      dbOptions.close();
      IOUtils.closeQuietly(cfHandleList);
    }
  }

  /**
   * Simple data class to hold database options for comparison.
   * This consist of sampled options from the options set in {@link org.apache.hadoop.hdds.utils.db.DBProfile}.
   */
  private static final class DatabaseOptions {
    // DBOptions
    private final int maxBackgroundCompactions;
    private final int maxBackgroundFlushes;
    private final long bytesPerSync;
    private final long maxLogFileSize;
    private final long keepLogFileNum;
    private final long walTTL;
    private final long walSizeLimit;

    // ColumnFamilyOptions
    private final long writeBufferSize;
    // BlockBasedTableConfig
    private final long blockSize;
    private final boolean pinL0FilterAndIndexBlocksInCache;

    private DatabaseOptions(Builder b) {
      this.maxBackgroundCompactions = b.maxBackgroundCompactions;
      this.maxBackgroundFlushes = b.maxBackgroundFlushes;
      this.bytesPerSync = b.bytesPerSync;
      this.maxLogFileSize = b.maxLogFileSize;
      this.keepLogFileNum = b.keepLogFileNum;
      this.walTTL = b.walTTL;
      this.walSizeLimit = b.walSizeLimit;
      this.writeBufferSize = b.writeBufferSize;
      this.blockSize = b.blockSize;
      this.pinL0FilterAndIndexBlocksInCache = b.pinL0FilterAndIndexBlocksInCache;
    }

    public void assertEqualOptions(DatabaseOptions other) {
      assertEquals(this.maxBackgroundCompactions, other.maxBackgroundCompactions);
      assertEquals(this.maxBackgroundFlushes, other.maxBackgroundFlushes);
      assertEquals(this.maxLogFileSize, other.maxLogFileSize);
      assertEquals(this.keepLogFileNum, other.keepLogFileNum);
      assertEquals(this.walTTL, other.walTTL);
      assertEquals(this.walSizeLimit, other.walSizeLimit);


      assertEquals(this.bytesPerSync, other.bytesPerSync);
      assertEquals(this.writeBufferSize, other.writeBufferSize);
      assertEquals(this.blockSize, other.blockSize);
      assertEquals(this.pinL0FilterAndIndexBlocksInCache, other.pinL0FilterAndIndexBlocksInCache);
    }

    private static class Builder {
      // DBOptions
      private int maxBackgroundCompactions;
      private int maxBackgroundFlushes;
      private long bytesPerSync;
      private long maxLogFileSize;
      private long keepLogFileNum;
      private long walTTL;
      private long walSizeLimit;

      // ColumnFamilyOptions
      private long writeBufferSize;
      // BlockBasedTableConfig
      private long blockSize;
      private boolean pinL0FilterAndIndexBlocksInCache;

      Builder() {
      }

      public Builder setMaxBackgroundCompactions(int maxBackgroundCompactions) {
        this.maxBackgroundCompactions = maxBackgroundCompactions;
        return this;
      }

      public Builder setMaxBackgroundFlushes(int maxBackgroundFlushes) {
        this.maxBackgroundFlushes = maxBackgroundFlushes;
        return this;
      }

      public Builder setBytesPerSync(long bytesPerSync) {
        this.bytesPerSync = bytesPerSync;
        return this;
      }

      public Builder setMaxLogFileSize(long maxLogFileSize) {
        this.maxLogFileSize = maxLogFileSize;
        return this;
      }

      public Builder setKeepLogFileNum(long keepLogFileNum) {
        this.keepLogFileNum = keepLogFileNum;
        return this;
      }

      public Builder setWalTTL(long walTTL) {
        this.walTTL = walTTL;
        return this;
      }

      public Builder setWalSizeLimit(long walSizeLimit) {
        this.walSizeLimit = walSizeLimit;
        return this;
      }

      public Builder setWriteBufferSize(long writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
        return this;
      }

      public Builder setBlockSize(long blockSize) {
        this.blockSize = blockSize;
        return this;
      }

      public Builder setPinL0FilterAndIndexBlocksInCache(boolean pinL0FilterAndIndexBlocksInCache) {
        this.pinL0FilterAndIndexBlocksInCache = pinL0FilterAndIndexBlocksInCache;
        return this;
      }

      public DatabaseOptions build() {
        return new DatabaseOptions(this);
      }
    }
  }
}
