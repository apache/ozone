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
import java.time.Duration;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadsExpiredAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the background service to abort incomplete Multipart Upload.
 * Scan the MultipartInfoTable periodically to get MPU keys with
 * creationTimestamp older than a certain threshold, and delete them.
 */
public class MultipartUploadCleanupService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(MultipartUploadCleanupService.class);

  // Similar to OpenKeyCleanupService, use a single thread.
  private static final int MPU_INFO_DELETING_CORE_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final KeyManager keyManager;
  // Dummy client ID to use for response.
  private final ClientId clientId = ClientId.randomId();
  private final Duration expireThreshold;
  private final int mpuPartsLimitPerTask;
  private final AtomicLong submittedMpuInfoCount;
  private final AtomicLong runCount;
  private final AtomicBoolean suspended;

  public MultipartUploadCleanupService(long interval, TimeUnit unit,
        long timeout, OzoneManager ozoneManager, ConfigurationSource conf) {
    super("MultipartUploadCleanupService", interval, unit,
        MPU_INFO_DELETING_CORE_POOL_SIZE, timeout,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.keyManager = ozoneManager.getKeyManager();

    long expireMillis = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_MPU_EXPIRE_THRESHOLD,
        OMConfigKeys.OZONE_OM_MPU_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.expireThreshold = Duration.ofMillis(expireMillis);

    this.mpuPartsLimitPerTask = conf.getInt(
        OMConfigKeys.OZONE_OM_MPU_PARTS_CLEANUP_LIMIT_PER_TASK,
        OMConfigKeys.OZONE_OM_MPU_PARTS_CLEANUP_LIMIT_PER_TASK_DEFAULT);

    this.submittedMpuInfoCount = new AtomicLong(0);
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
   * Returns the number of MPU info that were submitted for deletion by this
   * service. If the MPUInfoTable were completed/aborted
   * from the MPUInfoTable between being submitted for deletion
   * and the actual delete operation, they will not be deleted.
   *
   * @return long count.
   */
  @VisibleForTesting
  public long getSubmittedMpuInfoCount() {
    return submittedMpuInfoCount.get();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new MultipartUploadCleanupTask());
    return queue;
  }

  private boolean shouldRun() {
    return !suspended.get() && ozoneManager.isLeaderReady();
  }

  private class MultipartUploadCleanupTask implements BackgroundTask {

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
      List<ExpiredMultipartUploadsBucket> expiredMultipartUploads = null;
      try {
        expiredMultipartUploads = keyManager.getExpiredMultipartUploads(
            expireThreshold, mpuPartsLimitPerTask);
      } catch (IOException e) {
        LOG.error("Unable to get expired MPU info, retry in next interval", e);
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      if (expiredMultipartUploads != null &&
          !expiredMultipartUploads.isEmpty()) {
        int numExpiredMultipartUploads = expiredMultipartUploads.stream()
            .mapToInt(ExpiredMultipartUploadsBucket::getMultipartUploadsCount)
            .sum();

        OMRequest omRequest = createRequest(expiredMultipartUploads);
        submitRequest(omRequest);

        LOG.debug("Number of expired multipart info submitted for deletion: "
                + "{}, elapsed time: {}ms", numExpiredMultipartUploads,
            Time.monotonicNow() - startTime);
        submittedMpuInfoCount.addAndGet(numExpiredMultipartUploads);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    private OMRequest createRequest(List<ExpiredMultipartUploadsBucket>
                                        expiredMultipartUploadsBuckets) {
      MultipartUploadsExpiredAbortRequest request =
          MultipartUploadsExpiredAbortRequest.newBuilder()
              .addAllExpiredMultipartUploadsPerBucket(
                  expiredMultipartUploadsBuckets)
              .build();

      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.AbortExpiredMultiPartUploads)
          .setMultipartUploadsExpiredAbortRequest(request)
          .setClientId(clientId.toString())
          .build();

      return omRequest;
    }

    private void submitRequest(OMRequest omRequest) {
      try {
        OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, clientId, runCount.get());
      } catch (ServiceException e) {
        LOG.error("Expired multipart info delete request failed. " +
            "Will retry at next run.", e);
      }
    }
  }
}
