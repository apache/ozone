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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMOpenKeysDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles requests to move open keys from the open key table to the delete
 * table. Modifies the open key table cache only, and no underlying databases.
 * The delete table cache does not need to be modified since it is not used
 * for client response validation.
 */
public class OMOpenKeysDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMOpenKeysDeleteRequest.class);

  private static final AuditLogger AUDIT = new AuditLogger(AuditLoggerType.OMSYSTEMLOGGER);
  private static final String AUDIT_PARAM_NUM_OPEN_KEYS = "numOpenKeysDeleted";
  private static final String AUDIT_PARAM_OPEN_KEYS = "openKeysDeleted";

  public OMOpenKeysDeleteRequest(OMRequest omRequest,
                                 BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumOpenKeyDeleteRequests();

    OzoneManagerProtocolProtos.DeleteOpenKeysRequest deleteOpenKeysRequest =
            getOmRequest().getDeleteOpenKeysRequest();

    List<OpenKeyBucket> submittedOpenKeyBuckets =
            deleteOpenKeysRequest.getOpenKeysPerBucketList();

    long numSubmittedOpenKeys = 0;
    for (OpenKeyBucket keyBucket: submittedOpenKeyBuckets) {
      numSubmittedOpenKeys += keyBucket.getKeysCount();
    }

    LOG.debug("{} open keys submitted for deletion.", numSubmittedOpenKeys);
    omMetrics.incNumOpenKeysSubmittedForDeletion(numSubmittedOpenKeys);

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(getOmRequest());

    Exception exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;
    // Map containing a pair of BucketId and delete key info.
    Map<String, Pair<Long, OmKeyInfo>> deletedOpenKeys = new HashMap<>();
    Map<String, String> auditParams = new LinkedHashMap<>();
    try {
      for (OpenKeyBucket openKeyBucket: submittedOpenKeyBuckets) {
        // For each bucket where keys will be deleted from,
        // get its bucket lock and update the cache accordingly.
        updateOpenKeyTableCache(ozoneManager, trxnLogIndex,
            openKeyBucket, deletedOpenKeys);
      }

      omClientResponse = new OMOpenKeysDeleteResponse(omResponse.build(),
          deletedOpenKeys, getBucketLayout());

      result = Result.SUCCESS;

      List<String> deletedOpenKeysLight = new ArrayList<>(deletedOpenKeys.size());
      for (Pair<Long, OmKeyInfo> key : deletedOpenKeys.values()) {
        OmKeyInfo keyInfo = key.getRight();
        OzoneManagerProtocolProtos.KeyArgs keyArgs = OzoneManagerProtocolProtos.KeyArgs.newBuilder()
            .setVolumeName(keyInfo.getVolumeName())
            .setBucketName(keyInfo.getBucketName())
            .setKeyName(keyInfo.getKeyName())
            .build();
        deletedOpenKeysLight.add(buildLightKeyArgsAuditMap(keyArgs).toString());
      }
      auditParams.put(AUDIT_PARAM_NUM_OPEN_KEYS, String.valueOf(deletedOpenKeys.size()));
      auditParams.put(AUDIT_PARAM_OPEN_KEYS, deletedOpenKeysLight.toString());
      AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(OMSystemAction.OPEN_KEY_CLEANUP, auditParams));
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.OPEN_KEY_CLEANUP, auditParams, ex));
      exception = ex;
      omClientResponse =
          new OMOpenKeysDeleteResponse(createErrorOMResponse(omResponse,
              exception), getBucketLayout());
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    processResults(omMetrics, numSubmittedOpenKeys, deletedOpenKeys.size(),
        deleteOpenKeysRequest, result);

    return omClientResponse;
  }

  private void processResults(OMMetrics omMetrics, long numSubmittedOpenKeys,
      long numDeletedOpenKeys,
      OzoneManagerProtocolProtos.DeleteOpenKeysRequest request, Result result) {

    switch (result) {
    case SUCCESS:
      LOG.debug("Deleted {} open keys out of {} submitted keys.",
          numDeletedOpenKeys, numSubmittedOpenKeys);
      break;
    case FAILURE:
      omMetrics.incNumOpenKeyDeleteRequestFails();
      LOG.error("Failure occurred while trying to delete {} submitted open " +
              "keys.", numSubmittedOpenKeys);
      break;
    default:
      LOG.error("Unrecognized result for OMOpenKeysDeleteRequest: {}",
          request);
    }
  }

  protected void updateOpenKeyTableCache(OzoneManager ozoneManager,
      long trxnLogIndex, OpenKeyBucket keysPerBucket,
      Map<String, Pair<Long, OmKeyInfo>> deletedOpenKeys) throws IOException {

    boolean acquiredLock = false;
    String volumeName = keysPerBucket.getVolumeName();
    String bucketName = keysPerBucket.getBucketName();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();
      OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
      long bucketId = omBucketInfo == null ? 0L : omBucketInfo.getObjectID();
      for (OpenKey key: keysPerBucket.getKeysList()) {
        String fullKeyName = key.getName();

        // If an open key is no longer present in the table, it was committed
        // and should not be deleted.
        OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(getBucketLayout())
                .get(fullKeyName);
        if (omKeyInfo != null) {
          if (trxnLogIndex < omKeyInfo.getUpdateID()) {
            LOG.warn("Transaction log index {} is smaller than " +
                "the current updateID {} of key {}, skipping deletion.",
                trxnLogIndex, omKeyInfo.getUpdateID(), fullKeyName);
            continue;
          }

          // Set the UpdateID to current transactionLogIndex
          omKeyInfo = omKeyInfo.toBuilder()
              .setUpdateID(trxnLogIndex)
              .build();
          deletedOpenKeys.put(fullKeyName, Pair.of(bucketId, omKeyInfo));

          // Update openKeyTable cache.
          omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
              new CacheKey<>(fullKeyName),
              CacheValue.get(trxnLogIndex));

          ozoneManager.getMetrics().incNumOpenKeysDeleted();
          LOG.debug("Open key {} deleted.", fullKeyName);

          // No need to add cache entries to delete table. As delete table will
          // be used by DeleteKeyService only, not used for any client response
          // validation, so we don't need to add to cache.
        } else {
          LOG.debug("Key {} was not deleted, as it was not " +
                  "found in the open key table.", fullKeyName);
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
