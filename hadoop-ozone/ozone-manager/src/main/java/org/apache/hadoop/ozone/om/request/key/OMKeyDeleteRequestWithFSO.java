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

import static org.apache.hadoop.ozone.OzoneConsts.DELETED_HSYNC_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteKey request - prefix layout.
 */
public class OMKeyDeleteRequestWithFSO extends OMKeyDeleteRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyDeleteRequestWithFSO.class);

  public OMKeyDeleteRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    DeleteKeyRequest deleteKeyRequest = getOmRequest().getDeleteKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        deleteKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildLightKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    boolean recursive = keyArgs.getRecursive();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();
    OMPerformanceMetrics perfMetrics = ozoneManager.getPerfMetrics();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    Exception exception = null;
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    Result result = null;
    OmBucketInfo omBucketInfo = null;
    long startNanos = Time.monotonicNowNanos();
    try {
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
          omMetadataManager, volumeName, bucketName, keyName, 0,
          ozoneManager.getDefaultReplicationConfig());

      if (keyStatus == null) {
        throw new OMException("Key not found. Key:" + keyName, KEY_NOT_FOUND);
      }

      OmKeyInfo omKeyInfo = keyStatus.getKeyInfo();
      // New key format for the fileTable & dirTable.
      // For example, the user given key path is '/a/b/c/d/e/file1', then in DB
      // keyName field stores only the leaf node name, which is 'file1'.
      String fileName = OzoneFSUtils.getFileName(keyName);
      omKeyInfo.setKeyName(fileName);

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo = omKeyInfo.toBuilder()
          .setUpdateID(trxnLogIndex)
          .build();

      final long volumeId = omMetadataManager.getVolumeId(volumeName);
      final long bucketId = omMetadataManager.getBucketId(volumeName,
              bucketName);
      String ozonePathKey = omMetadataManager.getOzonePathKey(volumeId,
              bucketId, omKeyInfo.getParentObjectID(),
              omKeyInfo.getFileName());
      OmKeyInfo deletedOpenKeyInfo = null;

      if (keyStatus.isDirectory()) {
        // Check if there are any sub path exists under the user requested path
        if (!recursive &&
            OMFileRequest.hasChildren(omKeyInfo, omMetadataManager)) {
          throw new OMException("Directory is not empty. Key:" + keyName,
                  DIRECTORY_NOT_EMPTY);
        }

        // Update dir cache.
        omMetadataManager.getDirectoryTable().addCacheEntry(
                new CacheKey<>(ozonePathKey),
                CacheValue.get(trxnLogIndex));
      } else {
        // Update table cache.
        omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
                new CacheKey<>(ozonePathKey),
                CacheValue.get(trxnLogIndex));
      }

      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);

      long quotaReleased = sumBlockLengths(omKeyInfo);
      // Empty entries won't be added to deleted table so this key shouldn't get added to snapshotUsed space.
      boolean isKeyNonEmpty = !OmKeyInfo.isKeyEmpty(omKeyInfo);
      omBucketInfo.decrUsedBytes(quotaReleased, isKeyNonEmpty);
      omBucketInfo.decrUsedNamespace(1L, isKeyNonEmpty);

      // If omKeyInfo has hsync metadata, delete its corresponding open key as well
      String dbOpenKey = null;
      String hsyncClientId = omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
      if (hsyncClientId != null) {
        Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
        long parentId = omKeyInfo.getParentObjectID();
        dbOpenKey = omMetadataManager.getOpenFileName(volumeId, bucketId, parentId, fileName, hsyncClientId);
        OmKeyInfo openKeyInfo = openKeyTable.get(dbOpenKey);
        if (openKeyInfo != null) {
          openKeyInfo = openKeyInfo.withMetadataMutations(
              metadata -> metadata.put(DELETED_HSYNC_KEY, "true"));
          openKeyTable.addCacheEntry(dbOpenKey, openKeyInfo, trxnLogIndex);
          deletedOpenKeyInfo = openKeyInfo;
        } else {
          LOG.warn("Potentially inconsistent DB state: open key not found with dbOpenKey '{}'", dbOpenKey);
        }
      }

      if (keyStatus.isFile()) {
        auditMap.put(OzoneConsts.DATA_SIZE, String.valueOf(omKeyInfo.getDataSize()));
        auditMap.put(OzoneConsts.REPLICATION_CONFIG, omKeyInfo.getReplicationConfig().toString());
      }

      omClientResponse = new OMKeyDeleteResponseWithFSO(omResponse
          .setDeleteKeyResponse(DeleteKeyResponse.newBuilder()).build(),
          keyName, omKeyInfo,
          omBucketInfo.copyObject(), keyStatus.isDirectory(), volumeId, deletedOpenKeyInfo);

      result = Result.SUCCESS;
      long endNanosDeleteKeySuccessLatencyNs = Time.monotonicNowNanos();
      perfMetrics.setDeleteKeySuccessLatencyNs(endNanosDeleteKeySuccessLatencyNs - startNanos);
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyDeleteResponseWithFSO(
          createErrorOMResponse(omResponse, exception), getBucketLayout());
      long endNanosDeleteKeyFailureLatencyNs = Time.monotonicNowNanos();
      perfMetrics.setDeleteKeyFailureLatencyNs(endNanosDeleteKeyFailureLatencyNs - startNanos);
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Performing audit logging outside of the lock.
    markForAudit(auditLogger, buildAuditMessage(OMAction.DELETE_KEY, auditMap,
        exception, userInfo));


    switch (result) {
    case SUCCESS:
      omMetrics.decNumKeys();
      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key:{}.",
          volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyDeleteRequest: {}",
          deleteKeyRequest);
    }

    return omClientResponse;
  }

  @Override
  protected OzoneManagerProtocolProtos.KeyArgs resolveBucketAndCheckAcls(
      OzoneManager ozoneManager,
      OzoneManagerProtocolProtos.KeyArgs.Builder newKeyArgs)
      throws IOException {
    return captureLatencyNs(
        ozoneManager.getPerfMetrics().getDeleteKeyResolveBucketAndAclCheckLatencyNs(),
        () -> resolveBucketAndCheckKeyAclsWithFSO(newKeyArgs.build(),
            ozoneManager, IAccessAuthorizer.ACLType.DELETE));
  }
}
