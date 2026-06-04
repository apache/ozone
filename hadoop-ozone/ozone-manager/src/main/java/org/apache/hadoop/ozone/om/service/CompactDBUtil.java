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

package org.apache.hadoop.ozone.om.service;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for compacting OM RocksDB column families.
 */
public final class CompactDBUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(CompactDBUtil.class);

  private CompactDBUtil() {
  }

  public static void compactTable(OMMetadataManager omMetadataManager,
                                  String tableName) throws IOException {
    compactTable(omMetadataManager, tableName, 0);
  }

  public static void compactTable(OMMetadataManager omMetadataManager,
      String tableName,
      int bottommostLevelCompaction) throws IOException {
    long startTime = Time.monotonicNow();
    ManagedCompactRangeOptions.BottommostLevelCompaction blcOption =
        getBottommostLevelCompaction(bottommostLevelCompaction);
    LOG.info("Compacting column family: {} with BottommostLevelCompaction={}",
        tableName, blcOption.name());
    try (ManagedCompactRangeOptions options = new ManagedCompactRangeOptions()) {
      options.setBottommostLevelCompaction(blcOption);
      options.setExclusiveManualCompaction(true);
      RocksDatabase rocksDatabase =
          ((RDBStore) omMetadataManager.getStore()).getDb();

      RocksDatabase.ColumnFamily columnFamily =
          rocksDatabase.getColumnFamily(tableName);
      if (columnFamily == null) {
        LOG.error("Unable to trigger compaction for \"{}\". Column family not found ",
            tableName);
        throw new IOException("Column family \"" + tableName + "\" not found.");
      }
      rocksDatabase.compactRange(columnFamily, null, null, options);
      LOG.info("Compaction of column family: {} completed in {} ms",
          tableName, Time.monotonicNow() - startTime);
    }
  }

  public static CompletableFuture<Void> compactTableAsync(OMMetadataManager metadataManager, String tableName) {
    return compactTableAsync(metadataManager, tableName, 0);
  }

  public static CompletableFuture<Void> compactTableAsync(
      OMMetadataManager metadataManager, String tableName, int bottommostLevelCompaction) {
    return CompletableFuture.runAsync(() -> {
      try {
        compactTable(metadataManager, tableName, bottommostLevelCompaction);
      } catch (Exception e) {
        LOG.warn("Failed to compact column family: {}", tableName, e);
        throw new CompletionException("Compaction failed for column family: " + tableName, e);
      }
    });
  }

  /**
   * Converts the given rocksId to a
   * {@link ManagedCompactRangeOptions.BottommostLevelCompaction} enum.
   * Defaults to kSkip if the value is invalid.
   */
  static ManagedCompactRangeOptions.BottommostLevelCompaction getBottommostLevelCompaction(int rocksId) {
    ManagedCompactRangeOptions.BottommostLevelCompaction blc =
        ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(rocksId);
    if (blc == null) {
      LOG.warn("Invalid BottommostLevelCompaction value: {}. Defaulting to kSkip.", rocksId);
      return ManagedCompactRangeOptions.BottommostLevelCompaction.kSkip;
    }
    return blc;
  }
}
