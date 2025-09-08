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
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the background service to delete operation info records
 * according to the retention strategy.
 *
 * NOTE: this is a crude strawman draft approach and needs revised
 */
public class CompletedRequestInfoCleanupService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(CompletedRequestInfoCleanupService.class);

  // Use only a single thread for OpenKeyCleanup. Multiple threads would read
  // from the same table and can send deletion requests for same key multiple
  // times.
  private static final int OPERATION_INFO_DELETING_CORE_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;
  private final AtomicBoolean suspended;

  public CompletedRequestInfoCleanupService(long interval, TimeUnit unit, long timeout,
                                            OzoneManager ozoneManager,
                                            OzoneConfiguration conf) {
    super("CompletedRequestInfoCleanupService", interval, unit,
        OPERATION_INFO_DELETING_CORE_POOL_SIZE, timeout,
        ozoneManager.getThreadNamePrefix());

    this.ozoneManager = ozoneManager;
    this.metadataManager = this.ozoneManager.getMetadataManager();
    this.suspended = new AtomicBoolean(false);
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

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new CompletedRequestInfoCleanupTask());
    return queue;
  }

  // this runs on all OMs
  private boolean shouldRun() {
    return !suspended.get();
  }

  private class CompletedRequestInfoCleanupTask implements BackgroundTask {

    // TODO: number of rows is a crude/flawed criteria for deletion
    // do something better
    private long MAX_KEYS = 20;

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      LOG.debug("Running CompletedRequestInfoCleanupTask");
      long startTime = Time.monotonicNow();
      long estimatedKeyCount;

      // XXX: this is intended to short circuit the need for the
      // lock/table iteration below but unless I'm missing something the
      // row estimate is not very accurate and can produce false
      // positives.  Is there a better way to do this?
      try {
        estimatedKeyCount = metadataManager.countRowsInTable(
            metadataManager.getCompletedRequestInfoTable());

        LOG.debug("cleanupCompletedRequestInfoIfNecessary - estimatedKeyCount={}, maxKeys={}", estimatedKeyCount, MAX_KEYS);
        if (estimatedKeyCount <= MAX_KEYS) {
          LOG.debug("cleanupCompletedRequestInfoIfNecessary - nothing to do");
          return BackgroundTaskResult.EmptyTaskResult.newResult();
        }
      } catch (IOException e) {
        // XXX
        LOG.error("Error while running completed operation consumer " +
            "background task. Will retry at next run.", e);
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      // XXX: do we need a write lock here?

      Table.KeyValue<String, OmCompletedRequestInfo> requestInfoRow;
      try (TableIterator<String, ? extends Table.KeyValue<String, OmCompletedRequestInfo>>
               tableIterator = metadataManager.getCompletedRequestInfoTable().iterator()) {

        tableIterator.seekToFirst();

        // TODO - it seems like we can't trust the key estimate so we
        // need to iterate the whole collection?
        // or could we iterate backwards until we have enough?
        // it feels like there is a better way to do this.

        long actualKeyCount = 0;
        while (tableIterator.hasNext()) {
          requestInfoRow = tableIterator.next();
          actualKeyCount++;
        }

        if (actualKeyCount <= MAX_KEYS) {
          LOG.info("cleanupCompletedRequestInfoIfNecessary - nothing to do");
          return BackgroundTaskResult.EmptyTaskResult.newResult();
        }

        long toDelete = actualKeyCount - MAX_KEYS;
        String firstKeyToDelete = null;
        String lastKeyToDelete = null;
        LOG.debug("cleanupCompletedRequestInfoIfNecessary - actualKeyCount={}, maxKeys={}, toDelete={}", actualKeyCount, MAX_KEYS, toDelete);

        tableIterator.seekToFirst();

        while (tableIterator.hasNext() && toDelete > 0) {
          requestInfoRow = tableIterator.next();

          if (firstKeyToDelete == null) {
            firstKeyToDelete = requestInfoRow.getKey();
          }

          lastKeyToDelete = requestInfoRow.getKey();
          toDelete--;
        }

        LOG.info("cleanupCompletedRequestInfoIfNecessary - firstKeyToDelete={}, lastKeyToDelete={}",
            firstKeyToDelete, lastKeyToDelete);

        // XXX: do we need a lock here?
        if (Objects.equals(firstKeyToDelete, lastKeyToDelete)) {
          metadataManager.getCompletedRequestInfoTable().delete(firstKeyToDelete);
        } else {
          metadataManager.getCompletedRequestInfoTable().deleteRange(firstKeyToDelete, lastKeyToDelete);
        }

      } catch (IOException e) {
        LOG.error("Error while running completed operation consumer " +
            "background task. Will retry at next run.", e);
      }

      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }
}
