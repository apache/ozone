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
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
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
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteMarkerResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyVersionDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteKey request.
 */
public class OMKeyDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyDeleteRequest.class);

  public OMKeyDeleteRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    DeleteKeyRequest deleteKeyRequest = super.preExecute(ozoneManager)
        .getDeleteKeyRequest();
    Objects.requireNonNull(deleteKeyRequest, "deleteKeyRequest == null");

    OzoneManagerProtocolProtos.KeyArgs keyArgs = deleteKeyRequest.getKeyArgs();
    OmKeyArgs.validateAddressedVersion(keyArgs);

    String keyPath = keyArgs.getKeyName();

    OmUtils.verifyKeyNameWithSnapshotReservedWordForDeletion(keyPath);
    keyPath = normalizeKeyPath(ozoneManager.getEnableFileSystemPaths(), keyPath, getBucketLayout());

    OzoneManagerProtocolProtos.KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now()).setKeyName(keyPath)
            // A delete without a versionId on a versioned bucket creates a
            // delete marker, and a marker is a version, so it needs an id
            // proposed here on the OM that received the request. Unused on an
            // unversioned bucket, and by a delete that addresses a version.
            .setProposedVersionId(
                ozoneManager.getVersionIdAllocator().propose());

    KeyArgs resolvedArgs = resolveBucketAndCheckAcls(ozoneManager, newKeyArgs);
    return getOmRequest().toBuilder()
        .setDeleteKeyRequest(deleteKeyRequest.toBuilder()
            .setKeyArgs(resolvedArgs))
        .setUserInfo(getUserIfNotExists(ozoneManager)).build();
  }

  protected KeyArgs resolveBucketAndCheckAcls(OzoneManager ozoneManager,
      KeyArgs.Builder newKeyArgs) throws IOException {
    return captureLatencyNs(
          ozoneManager.getPerfMetrics().getDeleteKeyResolveBucketAndAclCheckLatencyNs(),
          () -> resolveBucketAndCheckKeyAcls(newKeyArgs.build(), ozoneManager, ACLType.DELETE));
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    DeleteKeyRequest deleteKeyRequest = getOmRequest().getDeleteKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs = deleteKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildLightKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();
    OMPerformanceMetrics perfMetrics = ozoneManager.getPerfMetrics();
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    Exception exception = null;
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    Result result = null;
    // whether the request removed a key that was visible to plain reads; a
    // delete marker supersedes the current version without removing anything
    boolean visibleKeyRemoved = false;
    // whether the request took the delete marker path, and so may have left
    // versionedKeyTable entries in the table cache for this transaction
    boolean insertingDeleteMarker = false;
    // whether the request took the permanent version delete path, and so may
    // have left versionedKeyTable entries in the table cache
    boolean deletingVersion = false;
    long startNanos = Time.monotonicNowNanos();
    try {
      String objectKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      OmKeyInfo omKeyInfo =
          omMetadataManager.getKeyTable(getBucketLayout()).get(objectKey);

      OmBucketInfo omBucketInfo =
          getBucketInfo(omMetadataManager, volumeName, bucketName);

      if (keyArgs.hasVersionId() || keyArgs.getNullVersion()) {
        // DELETE ?versionId= permanently removes one version. It is the only
        // delete that destroys data on a versioned bucket.
        // Reported as not-found rather than unsupported: S3 answers a delete
        // naming a version that does not exist with a plain success, which the
        // gateway reaches by swallowing KEY_NOT_FOUND. NOT_SUPPORTED_OPERATION
        // would surface an error AWS never returns here.
        if (!omBucketInfo.hasEverBeenVersioned()) {
          throw new OMException("Bucket " + bucketName
              + " does not have S3 versioning enabled, so it holds no version "
              + "the request could name", KEY_NOT_FOUND);
        }
        deletingVersion = true;
        omClientResponse = deleteVersion(omMetadataManager, omBucketInfo,
            omKeyInfo, keyArgs, volumeName, bucketName, keyName, trxnLogIndex,
            omResponse);
        // Noncurrent versions are invisible to plain reads, so removing one
        // does not change the visible key count. Deleting the current one
        // with nothing left to promote takes the key away entirely, and the
        // record that was there is what stops being visible.
        visibleKeyRemoved = omKeyInfo != null && !omKeyInfo.isDeleteMarker()
            && omMetadataManager.getKeyTable(getBucketLayout())
                .get(objectKey) == null;
      } else if (omBucketInfo.hasEverBeenVersioned()) {
        // A delete without a versionId removes no data: a delete marker
        // becomes the current version and the version it supersedes moves to
        // the versionedKeyTable. Like S3, the marker is inserted even when the
        // key does not exist. While versioning is suspended the marker takes
        // the key's null version slot instead of creating a version.
        insertingDeleteMarker = true;
        omClientResponse = insertDeleteMarker(ozoneManager, omMetadataManager,
            omBucketInfo, omKeyInfo, objectKey, keyArgs, trxnLogIndex,
            omResponse);
        // The key stays in the keyTable as a marker, so nothing is released;
        // only superseding a visible object is one fewer visible key.
        visibleKeyRemoved = omKeyInfo != null && !omKeyInfo.isDeleteMarker();
      } else {
        if (omKeyInfo == null) {
          throw new OMException("Key not found", KEY_NOT_FOUND);
        }

        validateIfMatchETag(keyArgs, omKeyInfo);

        // Set the UpdateID to current transactionLogIndex
        omKeyInfo = omKeyInfo.toBuilder()
            .setUpdateID(trxnLogIndex)
            .build();

        // Update table cache. Put a tombstone entry
        omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
            new CacheKey<>(
                omMetadataManager.getOzoneKey(volumeName, bucketName, keyName)),
            CacheValue.get(trxnLogIndex));

        long quotaReleased = sumBlockLengths(omKeyInfo);
        // Empty entries won't be added to deleted table so this key shouldn't get added to snapshotUsed space.
        boolean isKeyNonEmpty = !OmKeyInfo.isKeyEmpty(omKeyInfo);
        omBucketInfo.decrUsedBytes(quotaReleased, isKeyNonEmpty);
        omBucketInfo.decrUsedNamespace(1L, isKeyNonEmpty);
        OmKeyInfo deletedOpenKeyInfo = null;

        // If omKeyInfo has hsync metadata, delete its corresponding open key as well
        String dbOpenKey = null;
        String hsyncClientId = omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
        if (hsyncClientId != null) {
          Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
          dbOpenKey = omMetadataManager.getOpenKey(volumeName, bucketName, keyName, hsyncClientId);
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

        omClientResponse = new OMKeyDeleteResponse(
            omResponse.setDeleteKeyResponse(DeleteKeyResponse.newBuilder())
                .build(), omKeyInfo,
            omBucketInfo.copyObject(), deletedOpenKeyInfo);
        if (omKeyInfo.isFile()) {
          auditMap.put(OzoneConsts.DATA_SIZE, String.valueOf(omKeyInfo.getDataSize()));
          auditMap.put(OzoneConsts.REPLICATION_CONFIG, omKeyInfo.getReplicationConfig().toString());
        }
        visibleKeyRemoved = true;
      }

      result = Result.SUCCESS;
      long endNanosDeleteKeySuccessLatencyNs = Time.monotonicNowNanos();
      perfMetrics.setDeleteKeySuccessLatencyNs(endNanosDeleteKeySuccessLatencyNs - startNanos);
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      // The failure response has to declare the same tables as the successful
      // one: the double buffer cleans up the table cache from the response's
      // CleanupTableInfo, so a delete marker request that failed after
      // touching the versionedKeyTable cache would otherwise leave an entry
      // behind that is in no DB and is never cleaned up.
      OMResponse errorResponse = createErrorOMResponse(omResponse, exception);
      if (insertingDeleteMarker) {
        omClientResponse =
            new OMKeyDeleteMarkerResponse(errorResponse, getBucketLayout());
      } else if (deletingVersion) {
        omClientResponse =
            new OMKeyVersionDeleteResponse(errorResponse, getBucketLayout());
      } else {
        omClientResponse =
            new OMKeyDeleteResponse(errorResponse, getBucketLayout());
      }
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
    markForAudit(auditLogger,
        buildAuditMessage(OMAction.DELETE_KEY, auditMap, exception, userInfo));

    switch (result) {
    case SUCCESS:
      if (visibleKeyRemoved) {
        omMetrics.decNumKeys();
      }
      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key:{}.", volumeName,
          bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyDeleteRequest: {}",
          deleteKeyRequest);
    }

    return omClientResponse;
  }

  /**
   * Permanently removes the addressed version: the only delete that destroys
   * data on a versioned bucket. Removing the current version hands its keyTable
   * place over to the next-newest version of the key, or removes the key
   * outright when none survives.
   *
   * @param currentVersion the key's current version, or null if it has none
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OMClientResponse deleteVersion(OMMetadataManager omMetadataManager,
      OmBucketInfo omBucketInfo, OmKeyInfo currentVersion,
      OzoneManagerProtocolProtos.KeyArgs keyArgs, String volumeName,
      String bucketName, String keyName, long trxnLogIndex,
      OMResponse.Builder omResponse) throws IOException {

    boolean nullVersion = keyArgs.getNullVersion();
    boolean deletingCurrent = currentVersion != null && (nullVersion
        ? currentVersion.isNullVersionRecord()
        : Long.valueOf(keyArgs.getVersionId()).equals(
            currentVersion.getVersionId()));

    String objectKey =
        omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);

    // The dbKey the version is deleted from, in the keyTable when it is the
    // current version and in the versionedKeyTable otherwise. Everything that
    // can fail runs before the first cache entry is added, so that a failed
    // request leaves no versionedKeyTable cache entry behind.
    String deletedVersionKey;
    OmKeyInfo version;
    if (deletingCurrent) {
      deletedVersionKey = objectKey;
      version = currentVersion;
    } else if (nullVersion) {
      Pair<String, OmKeyInfo> nullSlot = getNoncurrentNullVersion(
          omMetadataManager, volumeName, bucketName, keyName);
      deletedVersionKey = nullSlot == null ? null : nullSlot.getKey();
      version = nullSlot == null ? null : nullSlot.getValue();
    } else {
      deletedVersionKey = omMetadataManager.getVersionedOzoneKey(
          volumeName, bucketName, keyName, keyArgs.getVersionId());
      version =
          omMetadataManager.getVersionedKeyTable().get(deletedVersionKey);
    }
    if (version == null) {
      throw new OMException("Version not found for key " + keyName,
          KEY_NOT_FOUND);
    }

    // Removing the current version leaves the key without one, so the newest
    // noncurrent version is promoted to keep the invariant that keyTable holds
    // the current version of every key that still has one. The record moves
    // unchanged: promotion is positional, the version keeps its identity.
    String promotedKey = null;
    OmKeyInfo promoted = null;
    if (deletingCurrent) {
      Pair<String, OmKeyInfo> newest = getNewestNoncurrentVersion(
          omMetadataManager, volumeName, bucketName, keyName);
      if (newest != null) {
        promotedKey = newest.getKey();
        promoted = newest.getValue();
      }
    }

    version = version.toBuilder().setUpdateID(trxnLogIndex).build();

    if (deletingCurrent) {
      if (promoted != null) {
        omMetadataManager.getKeyTable(getBucketLayout())
            .addCacheEntry(objectKey, promoted, trxnLogIndex);
        omMetadataManager.getVersionedKeyTable().addCacheEntry(
            new CacheKey<>(promotedKey), CacheValue.get(trxnLogIndex));
      } else {
        // no version survives, so the key disappears entirely
        omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
            new CacheKey<>(objectKey), CacheValue.get(trxnLogIndex));
      }
    } else {
      omMetadataManager.getVersionedKeyTable().addCacheEntry(
          new CacheKey<>(deletedVersionKey), CacheValue.get(trxnLogIndex));
    }

    // A delete marker holds no blocks, so it releases namespace but no space.
    long quotaReleased = sumBlockLengths(version);
    boolean isVersionNonEmpty = !OmKeyInfo.isKeyEmpty(version);
    omBucketInfo.decrUsedBytes(quotaReleased, isVersionNonEmpty);
    omBucketInfo.decrUsedNamespace(1L, isVersionNonEmpty);

    return new OMKeyVersionDeleteResponse(
        omResponse.setDeleteKeyResponse(DeleteKeyResponse.newBuilder()).build(),
        version, deletedVersionKey, deletingCurrent,
        promotedKey, promoted, omBucketInfo.copyObject());
  }

  /**
   * Supersedes the key's current version with a delete marker: a record with
   * no data blocks that makes plain reads of the key return KEY_NOT_FOUND
   * while every existing version stays readable by versionId. The superseded
   * current version becomes a noncurrent version; a record written before
   * versioning was enabled carries no versionId and becomes the key's null
   * version. When the key does not exist the marker is still inserted, as S3
   * does.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OMClientResponse insertDeleteMarker(OzoneManager ozoneManager,
      OMMetadataManager omMetadataManager, OmBucketInfo omBucketInfo,
      OmKeyInfo currentVersion, String objectKey,
      OzoneManagerProtocolProtos.KeyArgs keyArgs, long trxnLogIndex,
      OMResponse.Builder omResponse) throws IOException {

    DeleteMarkerInsertion inserted = insertDeleteMarker(ozoneManager,
        omMetadataManager, omBucketInfo, currentVersion, objectKey,
        keyArgs.getKeyName(), keyArgs.getProposedVersionId(),
        keyArgs.getModificationTime(), trxnLogIndex);

    return new OMKeyDeleteMarkerResponse(
        omResponse.setDeleteKeyResponse(DeleteKeyResponse.newBuilder()).build(),
        inserted.getDeleteMarker(), inserted.getObjectKey(),
        inserted.getDemotedVersionKey(), inserted.getDemotedVersion(),
        omBucketInfo.copyObject())
        .withReplacedNullVersion(inserted.getReplacedNullVersionKey(),
            inserted.getKeysToDelete());
  }

  /**
   * Validates key delete requests.
   * We do not want to allow older clients to delete keys in buckets which use
   * non LEGACY layouts.
   *
   * @param req - the request to validate
   * @param ctx - the validation context
   * @return the validated request
   * @throws OMException if the request is invalid
   */
  @RequestFeatureValidator(
      conditions = ValidationCondition.OLDER_CLIENT_REQUESTS,
      processingPhase = RequestProcessingPhase.PRE_PROCESS,
      requestType = Type.DeleteKey
  )
  public static OMRequest blockDeleteKeyWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getDeleteKeyRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getDeleteKeyRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }
}
