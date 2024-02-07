/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.om.ExpiredOpenKeys;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteOpenKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * This is the background service to delete hanging open keys.
 * Scan the metadata of om periodically to get
 * the keys with prefix "#open#" and ask scm to
 * delete metadata accordingly, if scm returns
 * success for keys, then clean up those keys.
 */
public class OpenKeyCleanupService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(OpenKeyCleanupService.class);

  // Use only a single thread for OpenKeyCleanup. Multiple threads would read
  // from the same table and can send deletion requests for same key multiple
  // times.
  private static final int OPEN_KEY_DELETING_CORE_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final KeyManager keyManager;
  // Dummy client ID to use for response, since this is triggered by a
  // service, not the client.
  private final ClientId clientId = ClientId.randomId();
  private final Duration expireThreshold;
  private final int cleanupLimitPerTask;
  private final AtomicLong submittedOpenKeyCount;
  private final AtomicLong runCount;
  private final AtomicBoolean suspended;

  public OpenKeyCleanupService(long interval, TimeUnit unit, long timeout,
                               OzoneManager ozoneManager,
                               ConfigurationSource conf) {
    super("OpenKeyCleanupService", interval, unit,
        OPEN_KEY_DELETING_CORE_POOL_SIZE, timeout,
        ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.keyManager = ozoneManager.getKeyManager();

    long expireMillis = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.expireThreshold = Duration.ofMillis(expireMillis);

    this.cleanupLimitPerTask = conf.getInt(
        OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK,
        OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK_DEFAULT);

    this.submittedOpenKeyCount = new AtomicLong(0);
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
   * Returns the number of open keys that were submitted for deletion by this
   * service. If these keys were committed from the open key table between
   * being submitted for deletion and the actual delete operation, they will
   * not be deleted.
   *
   * @return long count.
   */
  @VisibleForTesting
  public long getSubmittedOpenKeyCount() {
    return submittedOpenKeyCount.get();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new OpenKeyCleanupTask(BucketLayout.DEFAULT));
    queue.add(new OpenKeyCleanupTask(BucketLayout.FILE_SYSTEM_OPTIMIZED));
    return queue;
  }

  private boolean shouldRun() {
    return !suspended.get() && ozoneManager.isLeaderReady();
  }

  private boolean isRatisEnabled() {
    return ozoneManager.isRatisEnabled();
  }

  private class OpenKeyCleanupTask implements BackgroundTask {

    private final BucketLayout bucketLayout;

    OpenKeyCleanupTask(BucketLayout bucketLayout) {
      this.bucketLayout = bucketLayout;
    }

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
      final ExpiredOpenKeys expiredOpenKeys;
      try {
        expiredOpenKeys = keyManager.getExpiredOpenKeys(expireThreshold,
            cleanupLimitPerTask, bucketLayout);
      } catch (IOException e) {
        LOG.error("Unable to get hanging open keys, retry in next interval", e);
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      final Collection<OpenKeyBucket.Builder> openKeyBuckets
          = expiredOpenKeys.getOpenKeyBuckets();
      final int numOpenKeys = openKeyBuckets.stream()
          .mapToInt(OpenKeyBucket.Builder::getKeysCount)
          .sum();
      if (!openKeyBuckets.isEmpty()) {
        // delete non-hsync'ed keys
        final OMRequest omRequest = createDeleteOpenKeysRequest(
            openKeyBuckets.stream());
        final OMResponse response = submitRequest(omRequest);
        if (response != null && response.getSuccess()) {
          ozoneManager.getMetrics().incNumOpenKeysCleaned(numOpenKeys);
        }
      }

      final List<CommitKeyRequest.Builder> hsyncKeys
          = expiredOpenKeys.getHsyncKeys();
      final int numHsyncKeys = hsyncKeys.size();
      if (!hsyncKeys.isEmpty()) {
        // commit hsync'ed keys
        hsyncKeys.forEach(b -> {
          final OMResponse response = submitRequest(createCommitKeyRequest(b));
          if (response != null && response.getSuccess()) {
            ozoneManager.getMetrics().incNumOpenKeysHSyncCleaned();
          }
        });
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Number of expired open keys submitted for deletion: {},"
                + " for commit: {}, elapsed time: {}ms",
            numOpenKeys, numHsyncKeys, Time.monotonicNow() - startTime);
      }
      final int numKeys = numOpenKeys + numHsyncKeys;
      submittedOpenKeyCount.addAndGet(numKeys);
      return () -> numKeys;
    }

    private OMRequest createCommitKeyRequest(
        CommitKeyRequest.Builder request) {
      return OMRequest.newBuilder()
          .setCmdType(Type.CommitKey)
          .setCommitKeyRequest(request)
          .setClientId(clientId.toString())
          .build();
    }

    private OMRequest createDeleteOpenKeysRequest(
        Stream<OpenKeyBucket.Builder> openKeyBuckets) {
      final DeleteOpenKeysRequest.Builder request
          = DeleteOpenKeysRequest.newBuilder()
          .setBucketLayout(bucketLayout.toProto());
      openKeyBuckets.forEach(request::addOpenKeysPerBucket);

      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.DeleteOpenKeys)
          .setDeleteOpenKeysRequest(request)
          .setClientId(clientId.toString())
          .build();

      return omRequest;
    }

    private OMResponse submitRequest(OMRequest omRequest) {
      try {
        if (isRatisEnabled()) {
          OzoneManagerRatisServer server = ozoneManager.getOmRatisServer();

          RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
              .setClientId(clientId)
              .setServerId(server.getRaftPeerId())
              .setGroupId(server.getRaftGroupId())
              .setCallId(runCount.get())
              .setMessage(Message.valueOf(
                  OMRatisHelper.convertRequestToByteString(omRequest)))
              .setType(RaftClientRequest.writeRequestType())
              .build();

          return server.submitRequest(omRequest, raftClientRequest);
        } else {
          return ozoneManager.getOmServerProtocol().submitRequest(
              null, omRequest);
        }
      } catch (ServiceException e) {
        LOG.error("Open key " + omRequest.getCmdType()
            + " request failed. Will retry at next run.", e);
      }
      return null;
    }
  }
}
