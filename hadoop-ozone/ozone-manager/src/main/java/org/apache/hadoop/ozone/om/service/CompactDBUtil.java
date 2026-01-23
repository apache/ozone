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
    long startTime = Time.monotonicNow();
    LOG.info("Compacting column family: {}", tableName);
    try (ManagedCompactRangeOptions options = new ManagedCompactRangeOptions()) {
      options.setBottommostLevelCompaction(
          ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
      options.setExclusiveManualCompaction(true);
      RocksDatabase rocksDatabase =
          ((RDBStore) omMetadataManager.getStore()).getDb();

      try {
        RocksDatabase.ColumnFamily columnFamily =
            rocksDatabase.getColumnFamily(tableName);
        rocksDatabase.compactRange(columnFamily, null, null, options);
        LOG.info("Compaction of column family: {} completed in {} ms",
            tableName, Time.monotonicNow() - startTime);
      } catch (NullPointerException ex) {
        LOG.error("Unable to trigger compaction for \"{}\". Column family not found ",
            tableName);
        throw new IOException("Column family \"" + tableName + "\" not found.");
      }
    }
  }
}
