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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.om.OMConfigKeys;
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

  public static void compactTable(OMMetadataManager omMetadataManager, String tableName,
      ManagedCompactRangeOptions.BottommostLevelCompaction compactionType) throws IOException {
    long startTime = Time.monotonicNow();
    try (ManagedCompactRangeOptions options = new ManagedCompactRangeOptions()) {
      options.setBottommostLevelCompaction(compactionType);
      LOG.info("Compacting column family: {} with {} bottommost level compaction",
          tableName, options.bottommostLevelCompaction());
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

  public static CompletableFuture<Void> compactTableAsync(OMMetadataManager metadataManager, String tableName,
      ManagedCompactRangeOptions.BottommostLevelCompaction compactionType) {
    return CompletableFuture.runAsync(() -> {
      try {
        compactTable(metadataManager, tableName, compactionType);
      } catch (Exception e) {
        LOG.warn("Failed to compact column family: {}", tableName, e);
        throw new CompletionException("Compaction failed for column family: " + tableName, e);
      }
    });
  }

  public static ManagedCompactRangeOptions.BottommostLevelCompaction getBottommostLevelCompaction(
      OzoneConfiguration configuration) {
    int compactionType = configuration.getInt(
        OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION,
        OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION_DEFAULT);
    ManagedCompactRangeOptions.BottommostLevelCompaction level =
        ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(compactionType);
    if (level == null) {
      compactionType = OMConfigKeys.OZONE_OM_COMPACTION_SERVICE_BOTTOMMOSTLEVELCOMPACTION_DEFAULT;
      level = ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(compactionType);
      LOG.warn("Invalid bottommost level compaction type. Using default value: {}", level);
    }
    return level;
  }

  /**
   * Converts the given RocksDB id to a
   * {@link ManagedCompactRangeOptions.BottommostLevelCompaction} enum value.
   * Defaults to {@code kSkip} if the id is invalid.
   *
   * @param bottommostLevelCompaction RocksDB id
   *                                  (0=kSkip, 1=kIfHaveCompactionFilter, 2=kForce, 3=kForceOptimized).
   */
  public static ManagedCompactRangeOptions.BottommostLevelCompaction getBottommostLevelCompaction(
      int bottommostLevelCompaction) {
    ManagedCompactRangeOptions.BottommostLevelCompaction level =
        ManagedCompactRangeOptions.BottommostLevelCompaction.fromRocksId(bottommostLevelCompaction);
    if (level == null) {
      LOG.warn("Invalid bottommost level compaction id: {}. Using default: kSkip.", bottommostLevelCompaction);
      return ManagedCompactRangeOptions.BottommostLevelCompaction.kSkip;
    }
    return level;
  }
}
