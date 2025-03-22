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

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the background service to compact OM rocksdb tables.
 */
public class CompactionService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(CompactionService.class);

  // Use only a single thread for Compaction.
  private static final int COMPACTOR_THREAD_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final OMMetadataManager omMetadataManager;
  private final AtomicLong numCompactions;
  private final AtomicBoolean suspended;
  private final long compactionThreshold;
  // list of tables that can be compacted
  private static final List<String> COMPACTABLE_TABLES =
      Arrays.asList(DELETED_DIR_TABLE, DELETED_TABLE, DIRECTORY_TABLE, FILE_TABLE, KEY_TABLE);

  public CompactionService(OzoneManager ozoneManager, TimeUnit unit, long interval, long timeout,
      long compactionThreshold) {
    super("CompactionService", interval, unit,
        COMPACTOR_THREAD_POOL_SIZE, timeout,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.omMetadataManager = this.ozoneManager.getMetadataManager();

    this.numCompactions = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
    this.compactionThreshold = compactionThreshold;
  }

  /**
   * Suspend the service (for testing).
   */
  @VisibleForTesting
  public void suspend() {
    suspended.set(true);
  }

  /**
   * Resume the service if suspended (for testing).
   */
  @VisibleForTesting
  public void resume() {
    suspended.set(false);
  }

  /**
   * Returns the number of manual compactions performed.
   *
   * @return long count.
   */
  @VisibleForTesting
  public long getNumCompactions() {
    return numCompactions.get();
  }

  @Override
  public synchronized BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();

    for (String tableName : COMPACTABLE_TABLES) {
      TypedTable table = (TypedTable)omMetadataManager.getTable(tableName);
      if (table.getUncompactedDeletes().get() > compactionThreshold) {
        queue.add(new CompactTask(tableName));
        table.resetUncompactedDeletes();
      }
    }
    return queue;
  }

  private boolean shouldRun() {
    return !suspended.get();
  }

  protected void compactFully(String tableName) throws IOException {
    long startTime = Time.monotonicNow();
    LOG.info("Compacting column family: {}", tableName);
    try (ManagedCompactRangeOptions options = new ManagedCompactRangeOptions()) {
      options.setBottommostLevelCompaction(
          ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
      // Find CF Handler
      RocksDatabase rocksDatabase = ((RDBStore) omMetadataManager.getStore()).getDb();
      RocksDatabase.ColumnFamily columnFamily = rocksDatabase.getColumnFamily(tableName);
      rocksDatabase.compactRange(columnFamily, null, null, options);
      LOG.info("Compaction of column family: {} completed in {} ms",
          tableName, Time.monotonicNow() - startTime);
    }
  }

  private class CompactTask implements BackgroundTask {
    private final String tableName;

    CompactTask(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      // trigger full compaction for the specified table.
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      LOG.debug("Running CompactTask");

      compactFully(tableName);
      numCompactions.incrementAndGet();
      return () -> 1;
    }
  }

}
