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
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupRevokedSTSTokensRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background service that periodically scans the revoked STS token table and submits OM requests to
 * remove entries have been present past the cleanup threshold.
 */
public class RevokedSTSTokenCleanupService extends BackgroundService {
  private static final Logger LOG = LoggerFactory.getLogger(RevokedSTSTokenCleanupService.class);

  // Use a single thread
  private static final int REVOKED_STS_TOKEN_CLEANER_CORE_POOL_SIZE = 1;
  private static final Clock CLOCK = Clock.system(ZoneOffset.UTC);
  private static final long CLEANUP_THRESHOLD = 12 * 60 * 60 * 1000L; // 12 hours in milliseconds

  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;
  private final AtomicBoolean suspended;
  private final AtomicLong runCount;
  private final AtomicLong submittedDeletedEntryCount;
  // Dummy client ID to use for response, since this is triggered by a
  // service, not the client.
  private final ClientId clientId = ClientId.randomId();

  /**
   * Creates a Revoked STS Token cleanup service.
   *
   * @param interval        the interval between successive runs
   * @param unit            the time unit for {@code interval}
   * @param serviceTimeout  timeout for a single run
   * @param ozoneManager    the OzoneManager instance
   */
  public RevokedSTSTokenCleanupService(long interval, TimeUnit unit, long serviceTimeout, OzoneManager ozoneManager) {
    super(
        "RevokedSTSTokenCleanupService", interval, unit, REVOKED_STS_TOKEN_CLEANER_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager.getThreadNamePrefix());
    this.ozoneManager = ozoneManager;
    this.metadataManager = ozoneManager.getMetadataManager();
    this.suspended = new AtomicBoolean(false);
    this.runCount = new AtomicLong(0);
    this.submittedDeletedEntryCount = new AtomicLong(0);
  }

  /**
   * Returns the number of times this Background service has run.
   * @return Long, run count.
   */
  @VisibleForTesting
  public long getRunCount() {
    return runCount.get();
  }

  /**
   * Returns the number of entries this Background service has submitted for deletion.
   * @return Long, submitted for deletion entry count.
   */
  @VisibleForTesting
  public long getSubmittedDeletedEntryCount() {
    return submittedDeletedEntryCount.get();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    final BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new RevokedSTSTokenCleanupTask());
    return queue;
  }

  private boolean shouldRun() {
    return !suspended.get() && ozoneManager.isLeaderReady();
  }

  private class RevokedSTSTokenCleanupTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      final long startTime = Time.monotonicNow();
      runCount.incrementAndGet();
      final Table<String, Long> revokedStsTokenTable = metadataManager.getS3RevokedStsTokenTable();

      // Collect entries that have existed for over 12 hours during the scan
      final List<String> sessionTokensToCleanup = new ArrayList<>();

      // Iterate over all entries in the revoked STS token table and remove
      // those whose initialCreationTimeMillis is more than 12 hours
      try (Table.KeyValueIterator<String, Long> iterator = revokedStsTokenTable.iterator()) {
        iterator.seekToFirst();
        while (iterator.hasNext()) {
          final Table.KeyValue<String, Long> entry = iterator.next();
          final String sessionToken = entry.getKey();
          final Long initialCreationTimeMillis = entry.getValue();

          if (shouldCleanup(initialCreationTimeMillis)) {
            sessionTokensToCleanup.add(sessionToken);
          }
        }
      } catch (IOException e) {
        // IO exceptions while iterating should be logged and retried next run.
        LOG.error("Failure while scanning s3RevokedStsTokenTable.  It will be retried in the next interval", e);
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      final long deletedInRun;
      if (!sessionTokensToCleanup.isEmpty()) {
        LOG.info("Found {} revoked STS token entries to clean up.", sessionTokensToCleanup.size());
        final boolean success = submitCleanupRequest(sessionTokensToCleanup);
        if (success) {
          deletedInRun = sessionTokensToCleanup.size();
        } else {
          deletedInRun = 0;
          LOG.warn(
              "RevokedSTSTokenCleanupService failed to submit cleanup request. Expired entries will be retried in " +
              "the next run.");
        }
      } else {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      if (deletedInRun > 0) {
        submittedDeletedEntryCount.addAndGet(deletedInRun);
      }

      final long elapsed = Time.monotonicNow() - startTime;
      LOG.info(
          "RevokedSTSTokenCleanupService run completed. deletedEntriesInRun={}, totalDeletedEntries={}, " +
          "elapsedTimeMs={}", deletedInRun, submittedDeletedEntryCount.get(), elapsed);

      final long resultCount = deletedInRun;
      return () -> (int) resultCount;
    }

    /**
     * Returns true if the given STS session token has been in the table past the cleanup threshold.
     */
    private boolean shouldCleanup(long initialCreationTimeMillis) {
      final long now = CLOCK.millis();

      if (now - initialCreationTimeMillis > CLEANUP_THRESHOLD) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Revoked STS token entry created at {} is older than 12 hours, will clean up. Current time: {}",
              initialCreationTimeMillis, now);
        }
        return true;
      }
      return false;
    }

    /**
     * Builds and submits an OMRequest to delete the provided revoked STS token(s).
     */
    private boolean submitCleanupRequest(List<String> sessionTokens) {
      final CleanupRevokedSTSTokensRequest request = CleanupRevokedSTSTokensRequest.newBuilder()
          .addAllSessionToken(sessionTokens)
          .build();

      final OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.CleanupRevokedSTSTokens)
          .setCleanupRevokedSTSTokensRequest(request)
          .setClientId(clientId.toString())
          .setVersion(ClientVersion.CURRENT_VERSION)
          .build();

      try {
        final OMResponse omResponse = OzoneManagerRatisUtils.submitRequest(
            ozoneManager, omRequest, clientId, runCount.get());
        return omResponse != null && omResponse.getSuccess();
      } catch (ServiceException e) {
        LOG.error("Revoked STS token cleanup request failed. Will retry at next run.", e);
        return false;
      }
    }
  }
}


