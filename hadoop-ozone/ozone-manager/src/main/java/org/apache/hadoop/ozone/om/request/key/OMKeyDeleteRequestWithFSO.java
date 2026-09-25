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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    DeleteKeyRequest deleteKeyRequest = getOmRequest().getDeleteKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        deleteKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildLightKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();
    OMPerformanceMetrics perfMetrics = ozoneManager.getPerfMetrics();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    Exception exception = null;
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    Result result = null;
    long startNanos = Time.monotonicNowNanos();
    try {
      PreparedKeyDelete prepared =
          prepareKeyDelete(ozoneManager, keyArgs, auditMap, trxnLogIndex);

      // Phase 2 (bucket write lock): re-check that the key still exists, then apply the cache
      // mutations and publish the quota copy. The lock is held only for this mutation tail, so
      // bucket read-lock holders still observe each transaction atomically (all mutations or none),
      // but are no longer blocked for the Phase 1 path walk and emptiness scan.
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      omClientResponse = applyKeyDelete(omMetadataManager, keyArgs, prepared,
          trxnLogIndex, omResponse);

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

    auditAndLogResult(ozoneManager, deleteKeyRequest, auditMap, exception, result);

    return omClientResponse;
  }

  /**
   * Phase 1 (no bucket lock): resolves the key, checks directory emptiness and prepares the quota
   * delta and the hsync open-key copy. All of this reads committed state only. The OM apply path is
   * single-threaded (OzoneManagerStateMachine uses a single-thread executor), so no other transaction
   * can change what these reads observe before Phase 2 mutates; the double-buffer flush/cleanup
   * threads only materialize already-committed epochs and never alter a key's visible value.
   * <p>
   * Keeping these reads out of the bucket write lock is the point of HDDS-16289: it stops the lone
   * apply thread from gating readers of a hot bucket (getBucketInfo/getFileStatus/lookupKey). Delete
   * has two costly reads here, not one: getOMKeyInfoIfExists walks the path segment by segment, and
   * hasChildren scans the whole dirTable and fileTable cache before seeking RocksDB, so on a
   * non-recursive directory delete it dominates the hold. If OM ever applies transactions in parallel
   * per bucket/key, these reads must be re-validated under the lock in {@link #applyKeyDelete}.
   * <p>
   * Also fills in the data-size and replication audit parameters for a file delete, which are read
   * off the resolved key.
   */
  private PreparedKeyDelete prepareKeyDelete(OzoneManager ozoneManager,
      OzoneManagerProtocolProtos.KeyArgs keyArgs, Map<String, String> auditMap, long trxnLogIndex)
      throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    // Validate bucket and volume exists or not.
    validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

    OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
        omMetadataManager, volumeName, bucketName, keyName, 0,
        ozoneManager.getDefaultReplicationConfig());

    if (keyStatus == null) {
      throw new OMException("Key not found. Key:" + keyName, KEY_NOT_FOUND);
    }

    OmKeyInfo omKeyInfo = keyStatus.getKeyInfo();
    validateIfMatchETag(keyArgs, omKeyInfo);
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

    if (keyStatus.isDirectory() && !keyArgs.getRecursive()
        && OMFileRequest.hasChildren(omKeyInfo, omMetadataManager)) {
      // Check if there are any sub path exists under the user requested path
      throw new OMException("Directory is not empty. Key:" + keyName,
              DIRECTORY_NOT_EMPTY);
    }

    // If omKeyInfo has hsync metadata, delete its corresponding open key as well. Only the cache
    // entry is published under the lock in Phase 2; reading and rewriting the copy is done here.
    OmKeyInfo deletedOpenKeyInfo = null;
    String dbOpenKey = null;
    String hsyncClientId = omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
    if (hsyncClientId != null) {
      long parentId = omKeyInfo.getParentObjectID();
      dbOpenKey = omMetadataManager.getOpenFileName(volumeId, bucketId, parentId, fileName, hsyncClientId);
      OmKeyInfo openKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenKey);
      if (openKeyInfo != null) {
        deletedOpenKeyInfo = openKeyInfo.withMetadataMutations(
            metadata -> metadata.put(DELETED_HSYNC_KEY, "true"));
      } else {
        LOG.warn("Potentially inconsistent DB state: open key not found with dbOpenKey '{}'", dbOpenKey);
      }
    }

    if (keyStatus.isFile()) {
      auditMap.put(OzoneConsts.DATA_SIZE, String.valueOf(omKeyInfo.getDataSize()));
      auditMap.put(OzoneConsts.REPLICATION_CONFIG, omKeyInfo.getReplicationConfig().toString());
    }

    return new PreparedKeyDelete(keyStatus.isDirectory(), omKeyInfo, volumeId, ozonePathKey,
        sumBlockLengths(omKeyInfo), dbOpenKey, deletedOpenKeyInfo);
  }

  /**
   * Phase 2 (under the bucket write lock): re-checks the key resolved in Phase 1, then tombstones it
   * in the directory or file table, marks any hsync open key deleted, applies the bucket quota
   * release and publishes the bucket copy.
   */
  private OMClientResponse applyKeyDelete(OMMetadataManager omMetadataManager,
      OzoneManagerProtocolProtos.KeyArgs keyArgs, PreparedKeyDelete prepared, long trxnLogIndex,
      OMResponse.Builder omResponse) throws IOException {
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    // Cheap O(1) re-check of the key resolved in Phase 1. Under serial apply this always holds; it
    // is a tripwire that fails safe, the same way the Phase 1 existence check does, if that
    // invariant is ever broken by a concurrent writer.
    final boolean keyStillExists = prepared.isDirectory
        ? omMetadataManager.getDirectoryTable().get(prepared.ozonePathKey) != null
        : omMetadataManager.getKeyTable(getBucketLayout()).get(prepared.ozonePathKey) != null;
    if (!keyStillExists) {
      throw new OMException("Key not found. Key:" + keyName, KEY_NOT_FOUND);
    }

    if (prepared.isDirectory) {
      // Update dir cache.
      omMetadataManager.getDirectoryTable().addCacheEntry(
              new CacheKey<>(prepared.ozonePathKey),
              CacheValue.get(trxnLogIndex));
    } else {
      // Update table cache.
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
              new CacheKey<>(prepared.ozonePathKey),
              CacheValue.get(trxnLogIndex));
    }

    OmBucketInfo omBucketInfo =
        getBucketInfoForUpdate(omMetadataManager, volumeName, bucketName);

    // Empty entries won't be added to deleted table so this key shouldn't get added to snapshotUsed space.
    boolean isKeyNonEmpty = !OmKeyInfo.isKeyEmpty(prepared.omKeyInfo);
    omBucketInfo.decrUsedBytes(prepared.quotaReleased, isKeyNonEmpty);
    omBucketInfo.decrUsedNamespace(1L, isKeyNonEmpty || prepared.isDirectory);

    if (prepared.deletedOpenKeyInfo != null) {
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          prepared.dbOpenKey, prepared.deletedOpenKeyInfo, trxnLogIndex);
    }

    omMetadataManager.getBucketTable().addCacheEntry(
        omMetadataManager.getBucketKey(volumeName, bucketName), omBucketInfo, trxnLogIndex);

    return new OMKeyDeleteResponseWithFSO(omResponse
        .setDeleteKeyResponse(DeleteKeyResponse.newBuilder()).build(),
        keyName, prepared.omKeyInfo,
        omBucketInfo.copyObject(), prepared.isDirectory, prepared.volumeId,
        prepared.deletedOpenKeyInfo);
  }

  /**
   * Phase 1 output of {@link #prepareKeyDelete}, consumed by {@link #applyKeyDelete} under the bucket
   * write lock: the resolved key and its path key, the bytes to release and the hsync open key to
   * mark deleted.
   */
  private static final class PreparedKeyDelete {
    private final boolean isDirectory;
    private final OmKeyInfo omKeyInfo;
    private final long volumeId;
    private final String ozonePathKey;
    private final long quotaReleased;
    private final String dbOpenKey;
    private final OmKeyInfo deletedOpenKeyInfo;

    PreparedKeyDelete(boolean isDirectory, OmKeyInfo omKeyInfo, long volumeId, String ozonePathKey,
        long quotaReleased, String dbOpenKey, OmKeyInfo deletedOpenKeyInfo) {
      this.isDirectory = isDirectory;
      this.omKeyInfo = omKeyInfo;
      this.volumeId = volumeId;
      this.ozonePathKey = ozonePathKey;
      this.quotaReleased = quotaReleased;
      this.dbOpenKey = dbOpenKey;
      this.deletedOpenKeyInfo = deletedOpenKeyInfo;
    }
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

  /**
   * Emits the audit log and the result log outside the bucket lock.
   */
  private void auditAndLogResult(OzoneManager ozoneManager, DeleteKeyRequest deleteKeyRequest,
      Map<String, String> auditMap, Exception exception, Result result) {
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.DELETE_KEY, auditMap,
        exception, getOmRequest().getUserInfo()));

    OzoneManagerProtocolProtos.KeyArgs keyArgs = deleteKeyRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    OMMetrics omMetrics = ozoneManager.getMetrics();

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
  }
}
