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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
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
  // list of tables that can be compacted
  private final List<String> compactableTables;

  public CompactionService(OzoneManager ozoneManager, TimeUnit unit, long interval, long timeout,
                           List<String> tables) {
    super("CompactionService", interval, unit,
        COMPACTOR_THREAD_POOL_SIZE, timeout,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.omMetadataManager = this.ozoneManager.getMetadataManager();

    this.numCompactions = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
    this.compactableTables = validateTables(tables);
  }

  private List<String> validateTables(List<String> tables) {
    if (tables == null || tables.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> validTables = new ArrayList<>();
    Set<String> allTableNames = new HashSet<>(omMetadataManager.listTableNames());
    for (String table : tables) {
      if (allTableNames.contains(table)) {
        validTables.add(table);
      } else {
        LOG.warn("CompactionService: Table \"{}\" not found in OM metadata. Skipping this table.", table);
      }
    }
    if (validTables.isEmpty()) {
      LOG.error("CompactionService: No valid compaction tables found. Failing initialization.");
      throw new IllegalArgumentException("CompactionService: None of the provided tables are valid.");
    }
    return Collections.unmodifiableList(validTables);
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

  @VisibleForTesting
  public List<String> getCompactableTables() {
    return compactableTables;
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
    for (String tableName : compactableTables) {
      queue.add(new CompactTask(tableName));
    }
    return queue;
  }

  private boolean shouldRun() {
    return !suspended.get();
  }

  /**
   * Compact a specific table asynchronously. This method returns immediately
   * with a CompletableFuture that completes when the compaction finishes.
   * This is useful for on-demand compaction requests (e.g., via admin RPC)
   * where the caller doesn't need to wait for completion.
   *
   * @param tableName the name of the table to compact
   * @return CompletableFuture that completes when compaction finishes
   */
  public CompletableFuture<Void> compactTableAsync(String tableName) {
    return CompactDBUtil.compactTableAsync(omMetadataManager, tableName);
  }

  protected void compactFully(String tableName) throws IOException {
    CompactDBUtil.compactTable(omMetadataManager, tableName);
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
