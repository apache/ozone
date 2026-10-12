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
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.S3STSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteRevokedSTSTokensRequest;
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
  // Keep revocation entries until max STS token lifetime after the cutoff was captured.
  private static final long CLEANUP_THRESHOLD = TimeUnit.SECONDS.toMillis(S3STSUtils.MAX_DURATION_SECONDS); // 12 hours

  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;
  private final AtomicBoolean suspended;
  private final AtomicLong runCount;
  private final AtomicLong submittedDeletedEntryCount;
  private final AtomicLong callIdCount;
  // Dummy client ID to use for response, since this is triggered by a
  // service, not the client.
  private final ClientId clientId = ClientId.randomId();
  private final int ratisByteLimit;

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
    this.callIdCount = new AtomicLong(0);
    int limit = (int) ozoneManager.getConfiguration().getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT, StorageUnit.BYTES);
    // Always go to 90% of max limit for request as other header(s) will be added
    this.ratisByteLimit = (int) (limit * 0.9);
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

  private final class RevokedSTSTokenCleanupTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }

      final long startTime = Time.monotonicNow();
      runCount.incrementAndGet();
      final Table<String, Long> revokedStsTokenTable = metadataManager.getS3RevokedStsTokenTable();

      long deletedInRun = 0;
      final List<String> batch = new ArrayList<>();

      try (Table.KeyValueIterator<String, Long> iterator = revokedStsTokenTable.iterator()) {
        iterator.seekToFirst();
        while (iterator.hasNext()) {
          final Table.KeyValue<String, Long> entry = iterator.next();
          final String originalAccessKeyId = entry.getKey();
          final Long revocationTimeMillis = entry.getValue();

          if (shouldCleanup(revocationTimeMillis)) {
            // Calculate the size this originalAccessKeyId would add to the protobuf message.
            // Make a copy of the batch to do the size check
            final List<String> batchCopyWithCandidate = new ArrayList<>(batch);
            batchCopyWithCandidate.add(originalAccessKeyId);
            int batchWithCandidateSize = getBatchSerializedSize(batchCopyWithCandidate);

            // If adding this originalAccessKeyId would exceed the limit, submit the current batch
            if (batchWithCandidateSize > ratisByteLimit) {
              if (!batch.isEmpty()) {
                if (submitCleanupRequest(batch)) {
                  deletedInRun += batch.size();
                } else {
                  LOG.warn("Failed to submit batch of {} revoked tokens.", batch.size());
                }
                batch.clear();

                // Re-calculate the size of the candidate key alone in an empty batch
                // to check if it exceeds the limit by itself.
                final List<String> singleCandidateBatch = new ArrayList<>();
                singleCandidateBatch.add(originalAccessKeyId);
                batchWithCandidateSize = getBatchSerializedSize(singleCandidateBatch);
              }

              // Check if the single key exceeds the limit (either strictly single or after flush)
              if (batchWithCandidateSize > ratisByteLimit) {
                LOG.error(
                    "Single originalAccessKeyId entry size ({}) would exceed the ratisByteLimit ({}). " +
                    "revocationTimeMillis: {}", batchWithCandidateSize, ratisByteLimit, revocationTimeMillis);
                continue;
              }
            }
            batch.add(originalAccessKeyId);
          }
        }
      } catch (IOException e) {
        LOG.error("Failure while scanning s3RevokedStsTokenTable.  It will be retried in the next interval", e);
        if (deletedInRun == 0) {
          return BackgroundTaskResult.EmptyTaskResult.newResult();
        }
      }

      // Submit any remaining tokens
      if (!batch.isEmpty()) {
        if (submitCleanupRequest(batch)) {
          deletedInRun += batch.size();
        } else {
          LOG.warn("Failed to submit final batch of {} revoked tokens.", batch.size());
        }
      }

      // Update stats
      if (deletedInRun > 0) {
        submittedDeletedEntryCount.addAndGet(deletedInRun);
        LOG.info("Found and removed {} revoked STS token entries.", deletedInRun);
      }

      final long elapsed = Time.monotonicNow() - startTime;
      LOG.info("RevokedSTSTokenCleanupService run completed. deletedEntriesInRun={}, totalDeletedEntries={}, " +
          "callIdCount={}, elapsedTimeMs={}", deletedInRun, submittedDeletedEntryCount.get(), callIdCount.get(),
          elapsed);

      final long resultCount = deletedInRun;
      return () -> (int) resultCount;
    }

    /**
     * Returns true if the revocation cutoff is older than the cleanup threshold.
     */
    private boolean shouldCleanup(long revocationTimeMillis) {
      final long now = CLOCK.millis();

      if (now - revocationTimeMillis > CLEANUP_THRESHOLD) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Revoked STS token cutoff at {} is older than {} ms, will clean up. Current time: {}",
              revocationTimeMillis, CLEANUP_THRESHOLD, now);
        }
        return true;
      }
      return false;
    }

    /**
     * Builds and submits an OMRequest to delete the provided originalAccessKeyId revocation entries.
     */
    private boolean submitCleanupRequest(List<String> originalAccessKeyIds) {
      final DeleteRevokedSTSTokensRequest request = DeleteRevokedSTSTokensRequest.newBuilder()
          .addAllOriginalAccessKeyId(originalAccessKeyIds)
          .build();

      final OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.DeleteRevokedSTSTokens)
          .setDeleteRevokedSTSTokensRequest(request)
          .setClientId(clientId.toString())
          .setVersion(ClientVersion.CURRENT_VERSION)
          .build();

      try {
        final OMResponse omResponse = OzoneManagerRatisUtils.submitRequest(
            ozoneManager, omRequest, clientId, callIdCount.incrementAndGet());
        return omResponse != null && omResponse.getSuccess();
      } catch (ServiceException e) {
        LOG.error("Revoked STS token cleanup request failed. Will retry at next run.", e);
        return false;
      }
    }

    private int getBatchSerializedSize(List<String> originalAccessKeyIdBatch) {
      final DeleteRevokedSTSTokensRequest request = DeleteRevokedSTSTokensRequest.newBuilder()
          .addAllOriginalAccessKeyId(originalAccessKeyIdBatch)
          .build();

      return request.getSerializedSize();
    }
  }
}


