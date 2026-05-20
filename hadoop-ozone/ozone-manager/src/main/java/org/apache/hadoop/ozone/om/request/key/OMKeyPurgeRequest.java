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

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.validatePreviousSnapshotId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketNameInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketPurgeKeysSize;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles purging of keys from OM DB.
 */
public class OMKeyPurgeRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyPurgeRequest.class);

  private static final AuditLogger AUDIT = new AuditLogger(AuditLoggerType.OMSYSTEMLOGGER);
  private static final String AUDIT_PARAM_KEYS_DELETED = "keysDeleted";
  private static final String AUDIT_PARAM_RENAMED_KEYS_PURGED = "renamedKeysPurged";
  private static final String AUDIT_PARAMS_DELETED_KEYS_LIST = "deletedKeysList";
  private static final String AUDIT_PARAMS_RENAMED_KEYS_LIST = "renamedKeysList";
  private static final String AUDIT_PARAM_SNAPSHOT_ID = "snapshotId";

  public OMKeyPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    PurgeKeysRequest purgeKeysRequest = getOmRequest().getPurgeKeysRequest();
    List<DeletedKeys> bucketDeletedKeysList = purgeKeysRequest.getDeletedKeysList();
    List<SnapshotMoveKeyInfos> keysToUpdateList = purgeKeysRequest.getKeysToUpdateList();
    String fromSnapshot = purgeKeysRequest.hasSnapshotTableKey() ? purgeKeysRequest.getSnapshotTableKey() : null;
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    final SnapshotInfo fromSnapshotInfo;
    try {
      fromSnapshotInfo = fromSnapshot != null ? SnapshotUtils.getSnapshotInfo(ozoneManager,
          fromSnapshot) : null;
      // Checking if this request is an old request or new one.
      if (purgeKeysRequest.hasExpectedPreviousSnapshotID()) {
        // Validating previous snapshot since while purging deletes, a snapshot create request could make this purge
        // key request invalid on AOS since the deletedKey would be in the newly created snapshot. This would add an
        // redundant tombstone entry in the deletedTable. It is better to skip the transaction.
        UUID expectedPreviousSnapshotId = purgeKeysRequest.getExpectedPreviousSnapshotID().hasUuid()
            ? fromProtobuf(purgeKeysRequest.getExpectedPreviousSnapshotID().getUuid()) : null;
        validatePreviousSnapshotId(fromSnapshotInfo, omMetadataManager.getSnapshotChainManager(),
            expectedPreviousSnapshotId);
      }
    } catch (IOException e) {
      LOG.error("Error occurred while performing OmKeyPurge. ", e);
      if (LOG.isDebugEnabled()) {
        AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.KEY_DELETION, null, e));
      }
      return new OMKeyPurgeResponse(createErrorOMResponse(omResponse, e));
    }

    List<String> keysToBePurgedList = new ArrayList<>();

    int numKeysDeleted = 0;
    List<String> renamedKeysToBePurged = new ArrayList<>(purgeKeysRequest.getRenamedKeysList());
    for (DeletedKeys bucketWithDeleteKeys : bucketDeletedKeysList) {
      List<String> keysList = bucketWithDeleteKeys.getKeysList();
      keysToBePurgedList.addAll(keysList);
      numKeysDeleted = numKeysDeleted + keysList.size();
    }
    DeletingServiceMetrics deletingServiceMetrics = ozoneManager.getDeletionMetrics();
    deletingServiceMetrics.incrNumKeysPurged(numKeysDeleted);
    deletingServiceMetrics.incrNumRenameEntriesPurged(renamedKeysToBePurged.size());

    if (keysToBePurgedList.isEmpty() && renamedKeysToBePurged.isEmpty()) {
      OMException oe = new OMException("No keys found to be purged or renamed in the request.",
          OMException.ResultCodes.KEY_DELETION_ERROR);
      AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.KEY_DELETION, null, oe));
      return new OMKeyPurgeResponse(createErrorOMResponse(omResponse, oe));
    }

    // Setting transaction info for snapshot, this is to prevent duplicate purge requests to OM from background
    // services.
    try {
      TransactionInfo transactionInfo = TransactionInfo.valueOf(context.getTermIndex());
      if (fromSnapshotInfo != null) {
        fromSnapshotInfo.setLastTransactionInfo(transactionInfo.toByteString());
        omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(fromSnapshotInfo.getTableKey()),
            CacheValue.get(context.getIndex(), fromSnapshotInfo));
      } else {
        // Update the deletingServiceMetrics with the transaction index to indicate the
        // last purge transaction when running for AOS
        deletingServiceMetrics.setLastAOSTransactionInfo(transactionInfo);
      }
      List<OmBucketInfo> bucketInfoList = updateBucketSize(purgeKeysRequest.getBucketPurgeKeysSizeList(),
          omMetadataManager);

      if (LOG.isDebugEnabled()) {
        Map<String, String> auditParams = new LinkedHashMap<>();
        if (fromSnapshotInfo != null) {
          auditParams.put(AUDIT_PARAM_SNAPSHOT_ID, fromSnapshotInfo.getSnapshotId().toString());
        }
        auditParams.put(AUDIT_PARAM_KEYS_DELETED, String.valueOf(numKeysDeleted));
        auditParams.put(AUDIT_PARAM_RENAMED_KEYS_PURGED, String.valueOf(renamedKeysToBePurged.size()));
        if (!keysToBePurgedList.isEmpty()) {
          auditParams.put(AUDIT_PARAMS_DELETED_KEYS_LIST, String.join(",", keysToBePurgedList));
        }
        if (!renamedKeysToBePurged.isEmpty()) {
          auditParams.put(AUDIT_PARAMS_RENAMED_KEYS_LIST, String.join(",", renamedKeysToBePurged));
        }
        AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(OMSystemAction.KEY_DELETION, auditParams));
      }
      return new OMKeyPurgeResponse(omResponse.build(), keysToBePurgedList, renamedKeysToBePurged, fromSnapshotInfo,
          keysToUpdateList, bucketInfoList);
    } catch (IOException e) {
      AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.KEY_DELETION, null, e));
      return new OMKeyPurgeResponse(createErrorOMResponse(omResponse, e));
    }
  }

  private List<OmBucketInfo> updateBucketSize(List<BucketPurgeKeysSize> bucketPurgeKeysSizeList,
      OMMetadataManager omMetadataManager) throws OMException {
    Map<String, Map<String, List<BucketPurgeKeysSize>>> bucketPurgeKeysSizes = new HashMap<>();
    List<String[]> bucketKeyList = new ArrayList<>();
    for (BucketPurgeKeysSize bucketPurgeKey : bucketPurgeKeysSizeList) {
      String volumeName = bucketPurgeKey.getBucketNameInfo().getVolumeName();
      String bucketName = bucketPurgeKey.getBucketNameInfo().getBucketName();
      bucketPurgeKeysSizes.computeIfAbsent(volumeName, k -> new HashMap<>())
          .computeIfAbsent(bucketName, k -> {
            bucketKeyList.add(new String[]{volumeName, bucketName});
            return new ArrayList<>();
          }).add(bucketPurgeKey);
    }
    mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLocks(BUCKET_LOCK, bucketKeyList));
    boolean acquiredLock = getOmLockDetails().isLockAcquired();
    if (!acquiredLock) {
      throw new OMException("Failed to acquire bucket lock for purging keys.",
          OMException.ResultCodes.KEY_DELETION_ERROR);
    }
    List<OmBucketInfo> bucketInfoList = new ArrayList<>();
    try {
      for (Map.Entry<String, Map<String, List<BucketPurgeKeysSize>>> volEntry : bucketPurgeKeysSizes.entrySet()) {
        String volumeName = volEntry.getKey();
        for (Map.Entry<String, List<BucketPurgeKeysSize>> bucketEntry : volEntry.getValue().entrySet()) {
          String bucketName = bucketEntry.getKey();
          OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
          // Check null if bucket has been deleted.
          if (omBucketInfo != null) {
            boolean bucketUpdated = false;
            for (BucketPurgeKeysSize bucketPurgeKeysSize : bucketEntry.getValue()) {
              BucketNameInfo bucketNameInfo = bucketPurgeKeysSize.getBucketNameInfo();
              if (bucketNameInfo.getBucketId() == omBucketInfo.getObjectID()) {
                omBucketInfo.purgeSnapshotUsedBytes(bucketPurgeKeysSize.getPurgedBytes());
                omBucketInfo.purgeSnapshotUsedNamespace(bucketPurgeKeysSize.getPurgedNamespace());
                bucketUpdated = true;
              }
            }
            if (bucketUpdated) {
              bucketInfoList.add(omBucketInfo.copyObject());
            }
          }
        }
      }
      return bucketInfoList;
    } finally {
      mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLocks(BUCKET_LOCK, bucketKeyList));
    }
  }
}
