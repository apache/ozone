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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartAbortInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3ExpiredMultipartUploadsAbortResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles requests to move both MPU open keys from the open key/file table and
 * MPU part keys to delete table. Modifies the open key/file table cache only,
 * and no underlying databases.
 * The delete table cache does not need to be modified since it is not used
 * for client response validation.
 */
public class S3ExpiredMultipartUploadsAbortRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3ExpiredMultipartUploadsAbortRequest.class);

  public S3ExpiredMultipartUploadsAbortRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumExpiredMPUAbortRequests();

    OzoneManagerProtocolProtos.MultipartUploadsExpiredAbortRequest
        multipartUploadsExpiredAbortRequest = getOmRequest()
        .getMultipartUploadsExpiredAbortRequest();

    List<ExpiredMultipartUploadsBucket> submittedExpiredMPUsPerBucket =
        multipartUploadsExpiredAbortRequest
            .getExpiredMultipartUploadsPerBucketList();

    long numSubmittedMPUs = 0;
    for (ExpiredMultipartUploadsBucket mpuByBucket:
        submittedExpiredMPUsPerBucket) {
      numSubmittedMPUs += mpuByBucket.getMultipartUploadsCount();
    }

    LOG.debug("{} expired multi-uploads submitted for deletion.",
        numSubmittedMPUs);
    omMetrics.incNumExpiredMPUSubmittedForAbort(numSubmittedMPUs);

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());

    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;
    Map<OmBucketInfo, List<OmMultipartAbortInfo>>
        abortedMultipartUploads = new HashMap<>();

    try {
      for (ExpiredMultipartUploadsBucket mpuByBucket:
          submittedExpiredMPUsPerBucket) {
        // For each bucket where the MPU will be aborted from,
        // get its bucket lock and update the cache accordingly.
        updateTableCache(ozoneManager, trxnLogIndex, mpuByBucket,
            abortedMultipartUploads);
      }

      omClientResponse = new S3ExpiredMultipartUploadsAbortResponse(
          omResponse.build(), abortedMultipartUploads
      );

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse =
          new S3ExpiredMultipartUploadsAbortResponse(createErrorOMResponse(
              omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Only successfully aborted MPUs are included in the audit.
    auditAbortedMPUs(ozoneManager, abortedMultipartUploads);

    processResults(omMetrics, numSubmittedMPUs,
        abortedMultipartUploads.size(),
        multipartUploadsExpiredAbortRequest, result);

    return omClientResponse;

  }

  private void auditAbortedMPUs(OzoneManager ozoneManager,
      Map<OmBucketInfo, List<OmMultipartAbortInfo>> abortedMultipartUploads) {
    for (Map.Entry<OmBucketInfo, List<OmMultipartAbortInfo>> entry :
        abortedMultipartUploads.entrySet()) {
      KeyArgs.Builder keyArgsAuditBuilder = KeyArgs.newBuilder()
          .setVolumeName(entry.getKey().getVolumeName())
          .setBucketName(entry.getKey().getBucketName());

      for (OmMultipartAbortInfo abortInfo: entry.getValue()) {
        // See RpcClient#abortMultipartUpload
        KeyArgs keyArgsForAudit = keyArgsAuditBuilder
            .setKeyName(abortInfo.getMultipartKey())
            .setMultipartUploadID(abortInfo.getOmMultipartKeyInfo()
                .getUploadID())
            .build();
        Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgsForAudit);
        auditMap.put(OzoneConsts.UPLOAD_ID, abortInfo.getOmMultipartKeyInfo()
            .getUploadID());
        markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
            OMAction.ABORT_EXPIRED_MULTIPART_UPLOAD, auditMap,
            null, getOmRequest().getUserInfo()));
      }
    }
  }

  private void processResults(OMMetrics omMetrics,
      long numSubmittedExpiredMPUs, long numAbortedMPUs,
      OzoneManagerProtocolProtos.MultipartUploadsExpiredAbortRequest request,
      Result result) {

    switch (result) {
    case SUCCESS:
      LOG.debug("Aborted {} expired MPUs out of {} submitted MPus.",
            numAbortedMPUs, numSubmittedExpiredMPUs);
      break;
    case FAILURE:
      omMetrics.incNumExpiredMpuAbortRequestFails();
      LOG.error("Failure occurred while trying to abort {} submitted " +
          "expired MPUs.", numAbortedMPUs);
      break;
    default:
      LOG.error("Unrecognized result for " +
          "MultipartUploadsExpiredAbortRequest: {}", request);
    }

  }

  private void updateTableCache(OzoneManager ozoneManager,
        long trxnLogIndex, ExpiredMultipartUploadsBucket mpusPerBucket,
        Map<OmBucketInfo, List<OmMultipartAbortInfo>> abortedMultipartUploads)
      throws IOException {

    boolean acquiredLock = false;
    String volumeName = mpusPerBucket.getVolumeName();
    String bucketName = mpusPerBucket.getBucketName();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OmBucketInfo omBucketInfo = null;
    BucketLayout bucketLayout = null;
    try {
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);

      if (omBucketInfo == null) {
        LOG.warn("Volume: {}, Bucket: {} does not exist, skipping deletion.",
            volumeName, bucketName);
        return;
      }

      // Do not use getBucketLayout since the expired MPUs request might
      // contains MPUs from all kind of buckets
      bucketLayout = omBucketInfo.getBucketLayout();

      for (ExpiredMultipartUploadInfo expiredMPU:
          mpusPerBucket.getMultipartUploadsList()) {
        String expiredMPUKeyName = expiredMPU.getName();

        // If the MPU key is no longer present in the table, MPU
        // might have been completed / aborted, and should not be
        // aborted.
        OmMultipartKeyInfo omMultipartKeyInfo =
            omMetadataManager.getMultipartInfoTable().get(expiredMPUKeyName);

        if (omMultipartKeyInfo != null) {
          if (trxnLogIndex < omMultipartKeyInfo.getUpdateID()) {
            LOG.warn("Transaction log index {} is smaller than " +
                    "the current updateID {} of MPU key {}, skipping deletion.",
                trxnLogIndex, omMultipartKeyInfo.getUpdateID(),
                expiredMPUKeyName);
            continue;
          }

          // Set the UpdateID to current transactionLogIndex
          omMultipartKeyInfo = omMultipartKeyInfo.toBuilder()
              .setUpdateID(trxnLogIndex)
              .build();

          // Parse the multipart upload components (e.g. volume, bucket, key)
          // from the multipartInfoTable db key

          OmMultipartUpload multipartUpload;
          try {
            multipartUpload =
                OmMultipartUpload.from(expiredMPUKeyName);
          } catch (IllegalArgumentException e) {
            LOG.warn("Aborting expired MPU failed: MPU key: " +
                expiredMPUKeyName + " has invalid structure, " +
                "skipping this MPU.");
            continue;
          }

          String multipartOpenKey;
          try {
            multipartOpenKey =
                OMMultipartUploadUtils
                    .getMultipartOpenKey(multipartUpload.getVolumeName(),
                        multipartUpload.getBucketName(),
                        multipartUpload.getKeyName(),
                        multipartUpload.getUploadId(), omMetadataManager,
                        bucketLayout);
          } catch (OMException ome) {
            LOG.warn("Aborting expired MPU Failed: volume: " +
                multipartUpload.getVolumeName() + ", bucket: " +
                multipartUpload.getBucketName() + ", key: " +
                multipartUpload.getKeyName() + ". Cannot parse the open key" +
                "for this MPU, skipping this MPU.");
            continue;
          }

          // When abort uploaded key, we need to subtract the PartKey length
          // from the volume usedBytes.
          long quotaReleased = 0;
          int keyFactor = omMultipartKeyInfo.getReplicationConfig()
              .getRequiredNodes();
          for (PartKeyInfo iterPartKeyInfo : omMultipartKeyInfo.
              getPartKeyInfoMap()) {
            quotaReleased +=
                iterPartKeyInfo.getPartKeyInfo().getDataSize() * keyFactor;
          }
          omBucketInfo.incrUsedBytes(-quotaReleased);

          OmMultipartAbortInfo omMultipartAbortInfo =
              new OmMultipartAbortInfo.Builder()
                  .setMultipartKey(expiredMPUKeyName)
                  .setMultipartOpenKey(multipartOpenKey)
                  .setMultipartKeyInfo(omMultipartKeyInfo)
                  .setBucketLayout(omBucketInfo.getBucketLayout())
                  .build();

          abortedMultipartUploads.computeIfAbsent(omBucketInfo,
              k -> new ArrayList<>()).add(omMultipartAbortInfo);

          // Update cache of openKeyTable and multipartInfo table.
          // No need to add the cache entries to delete table, as the entries
          // in delete table are not used by any read/write operations.

          // Unlike normal MPU abort request where the MPU open keys needs
          // to exist. For OpenKeyCleanupService run prior to
          // HDDS-9017, these MPU open keys might already be deleted,
          // causing "orphan" MPU keys (MPU entry exist in
          // multipartInfoTable, but not in openKeyTable).
          // We can skip this existence check and just delete the
          // multipartInfoTable. The existence check can be re-added
          // once there are no "orphan" keys
          if (omMetadataManager.getOpenKeyTable(bucketLayout)
              .isExist(multipartOpenKey)) {
            omMetadataManager.getOpenKeyTable(bucketLayout)
                .addCacheEntry(new CacheKey<>(multipartOpenKey),
                    CacheValue.get(trxnLogIndex));
          }
          omMetadataManager.getMultipartInfoTable()
              .addCacheEntry(new CacheKey<>(expiredMPUKeyName),
                  CacheValue.get(trxnLogIndex));

          long numParts = omMultipartKeyInfo.getPartKeyInfoMap().size();
          ozoneManager.getMetrics().incNumExpiredMPUAborted();
          ozoneManager.getMetrics().incNumExpiredMPUPartsAborted(numParts);
          LOG.debug("Expired MPU {} aborted containing {} parts.",
              expiredMPUKeyName, numParts);
        } else {
          LOG.debug("MPU key {} was not aborted, as it was not " +
              "found in the multipart info table", expiredMPUKeyName);
        }
      }
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
    }

  }
}
