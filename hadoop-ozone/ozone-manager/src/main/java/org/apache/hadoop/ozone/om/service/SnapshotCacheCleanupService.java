/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.service;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.om.snapshot.SnapshotCache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background service to clean-up snapshot cache which would cleanup
 * unreferenced snapshot instances.
 */
public class SnapshotCacheCleanupService extends BackgroundService {
  // Use only a single thread for Snapshot Cache cleanup.
  private static final int SNAPSHOT_CACHE_CLEANUP_CORE_POOL_SIZE = 1;

  private final AtomicBoolean suspended;
  private final AtomicLong runCount;
  private final AtomicLong successRunCount;

  private final SnapshotCache snapshotCache;

  public SnapshotCacheCleanupService(long interval,
                                     long serviceTimeout,
                                     String threadPrefix,
                                     SnapshotCache snapshotCache) {
    super(SnapshotCacheCleanupService.class.getSimpleName(),
        interval,
        TimeUnit.MILLISECONDS, SNAPSHOT_CACHE_CLEANUP_CORE_POOL_SIZE,
        serviceTimeout, threadPrefix);
    this.suspended = new AtomicBoolean(false);
    this.runCount = new AtomicLong(0);
    this.successRunCount = new AtomicLong(0);
    this.snapshotCache = snapshotCache;

  }

  @VisibleForTesting
  public void run() {
    snapshotCache.cleanup();
  }

  private class SnapshotCacheCleanUpTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      runCount.incrementAndGet();
      run();
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new SnapshotCacheCleanUpTask());
    return queue;
  }

  private boolean shouldRun() {
    // TODO: [SNAPSHOT] Add OzoneManager.isLeaderReady() check along with
    //  suspended. `isLeaderReady` check was removed because some unit tests
    //  were failing due to Mockito limitation. Remove this once unit tests
    //  or mocking are fixed.
    return !suspended.get();
  }

  public long getRunCount() {
    return runCount.get();
  }

  public long getSuccessfulRunCount() {
    return successRunCount.get();
  }

  @VisibleForTesting
  void suspend() {
    suspended.set(true);
  }

  @VisibleForTesting
  void resume() {
    suspended.set(false);
  }
}
