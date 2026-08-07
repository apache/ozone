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
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ObjectVersionsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReclaimObjectVersionsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background service that reclaims the noncurrent object versions a key has
 * accumulated beyond its bucket's maxVersions, oldest first. Trimming runs
 * here rather than inside the write transaction, so the cost of a version
 * write does not grow with the number of versions the key already has.
 *
 * <p>The versions it selects are handed to OM as a ReclaimObjectVersions
 * request, which moves them to the deletedTable; the blocks themselves are
 * reclaimed by KeyDeletingService, which is where snapshot-awareness lives.
 */
public class VersionCleanupService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(VersionCleanupService.class);

  // Similar to OpenKeyCleanupService, use a single thread.
  private static final int VERSION_CLEANUP_CORE_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final KeyManager keyManager;
  // Dummy client ID to use for response.
  private final ClientId clientId = ClientId.randomId();
  private final int defaultMaxVersions;
  private final int versionLimitPerTask;
  private final AtomicLong submittedVersionCount;
  private final AtomicLong runCount;
  private final AtomicBoolean suspended;

  public VersionCleanupService(long interval, TimeUnit unit, long timeout,
      OzoneManager ozoneManager, ConfigurationSource conf) {
    super("VersionCleanupService", interval, unit,
        VERSION_CLEANUP_CORE_POOL_SIZE, timeout,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.keyManager = ozoneManager.getKeyManager();

    this.defaultMaxVersions = conf.getInt(
        OMConfigKeys.OZONE_OM_VERSIONING_MAX_VERSIONS,
        OMConfigKeys.OZONE_OM_VERSIONING_MAX_VERSIONS_DEFAULT);

    this.versionLimitPerTask = conf.getInt(
        OMConfigKeys.OZONE_OM_VERSION_CLEANUP_LIMIT_PER_TASK,
        OMConfigKeys.OZONE_OM_VERSION_CLEANUP_LIMIT_PER_TASK_DEFAULT);

    this.submittedVersionCount = new AtomicLong(0);
    this.runCount = new AtomicLong(0);
    this.suspended = new AtomicBoolean(false);
  }

  /**
   * Returns the number of times this Background service has run.
   *
   * @return Long, run count.
   */
  @VisibleForTesting
  public long getRunCount() {
    return runCount.get();
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
   * Returns the number of object versions that were submitted for reclamation
   * by this service. A version that is permanently deleted or promoted between
   * being submitted and the request being applied is not reclaimed here.
   *
   * @return long count.
   */
  @VisibleForTesting
  public long getSubmittedVersionCount() {
    return submittedVersionCount.get();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new VersionCleanupTask());
    return queue;
  }

  private boolean shouldRun() {
    return !suspended.get() && ozoneManager.isLeaderReady();
  }

  private class VersionCleanupTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      runCount.incrementAndGet();
      long startTime = Time.monotonicNow();
      List<ObjectVersionsBucket> versionsToReclaim;
      try {
        versionsToReclaim = keyManager.getVersionsToReclaim(defaultMaxVersions,
            versionLimitPerTask);
      } catch (IOException e) {
        LOG.error("Unable to get the object versions to reclaim, retry in "
            + "next interval", e);
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      if (!versionsToReclaim.isEmpty()) {
        int numVersions = versionsToReclaim.stream()
            .mapToInt(ObjectVersionsBucket::getVersionKeysCount)
            .sum();

        submitRequest(createRequest(versionsToReclaim));

        LOG.debug("Number of object versions submitted for reclamation: {}, "
            + "elapsed time: {}ms", numVersions,
            Time.monotonicNow() - startTime);
        submittedVersionCount.addAndGet(numVersions);
        ozoneManager.getDeletionMetrics()
            .incrNumObjectVersionsSentForReclaim(numVersions);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    private OMRequest createRequest(
        List<ObjectVersionsBucket> versionsPerBucket) {
      ReclaimObjectVersionsRequest request =
          ReclaimObjectVersionsRequest.newBuilder()
              .addAllVersionsPerBucket(versionsPerBucket)
              .build();

      return OMRequest.newBuilder()
          .setCmdType(Type.ReclaimObjectVersions)
          .setReclaimObjectVersionsRequest(request)
          .setClientId(clientId.toString())
          .build();
    }

    private void submitRequest(OMRequest omRequest) {
      try {
        OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, clientId, runCount.get());
      } catch (ServiceException e) {
        LOG.error("Object version reclamation request failed. "
            + "Will retry at next run.", e);
      }
    }
  }
}
