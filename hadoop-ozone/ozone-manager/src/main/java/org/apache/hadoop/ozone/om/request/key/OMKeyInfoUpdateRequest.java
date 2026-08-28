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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyInfoUpdateResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyInfoUpdateResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

/**
 * Base class for write requests that update an existing key in place without
 * changing its data, such as S3 object tagging and object metadata updates.
 *
 * <p>Subclasses supply the request-type specifics (which sub-request carries
 * the {@link KeyArgs}, how the key is mutated, metrics and audit action). The
 * object store layout is handled here; FSO subclasses override
 * {@link #resolveTarget} to use {@link #resolveFsoTarget}.
 */
public abstract class OMKeyInfoUpdateRequest extends OMKeyRequest {

  protected OMKeyInfoUpdateRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  /**
   * @return the {@link KeyArgs} carried by this request type.
   */
  protected abstract KeyArgs getKeyArgs(OMRequest omRequest);

  /**
   * Rebuilds the request with the normalized and resolved key args.
   *
   * @param preExecutedRequest result of {@code super.preExecute}
   * @param resolvedKeyArgs    normalized key args with the bucket resolved
   */
  protected abstract OMRequest buildUpdatedRequest(OMRequest preExecutedRequest, KeyArgs resolvedKeyArgs)
      throws IOException;

  /**
   * Applies the request-type specific update to the existing key. The update ID
   * is set by the caller.
   */
  protected abstract OmKeyInfo.Builder applyUpdate(OmKeyInfo existingKeyInfo, KeyArgs keyArgs);

  /**
   * Sets the request-type specific (empty) success sub-response.
   */
  protected abstract void setSuccessResponse(OMResponse.Builder omResponse);

  protected abstract OMAction getAuditAction();

  protected abstract void incRequestMetric(OMMetrics omMetrics);

  protected abstract void incFailureMetric(OMMetrics omMetrics);

  /**
   * @return the request type name, used in log and error messages.
   */
  protected abstract String getOperationName();

  protected abstract Logger getLogger();

  /**
   * Whether {@link #preExecute} stamps a fresh modification time on the request.
   * S3 last modified time only changes when the object content changes, so this
   * is false unless the operation is defined to update it.
   */
  protected boolean updatesModificationTime() {
    return false;
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest preExecutedRequest = super.preExecute(ozoneManager);
    KeyArgs keyArgs = getKeyArgs(preExecutedRequest);

    String keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
        keyArgs.getKeyName(), getBucketLayout());

    KeyArgs.Builder newKeyArgs = keyArgs.toBuilder()
        .setKeyName(keyPath);
    if (updatesModificationTime()) {
      newKeyArgs.setModificationTime(Time.now());
    }

    KeyArgs resolvedArgs = resolveBucketAndCheckKeyAcls(newKeyArgs.build(),
        ozoneManager, ACLType.WRITE);
    return buildUpdatedRequest(preExecutedRequest, resolvedArgs);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    KeyArgs keyArgs = getKeyArgs(getOmRequest());
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    incRequestMetric(omMetrics);

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    Result result = null;
    try {
      mergeOmLockDetails(
          omMetadataManager.getLock()
              .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName)
      );
      acquiredLock = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      KeyUpdateTarget target = resolveTarget(ozoneManager, omMetadataManager,
          volumeName, bucketName, keyName);

      OmKeyInfo omKeyInfo = applyUpdate(target.getKeyInfo(), keyArgs)
          .setUpdateID(trxnLogIndex)
          .build();

      // Update table cache
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(target.getDbKey()),
          CacheValue.get(trxnLogIndex, omKeyInfo)
      );

      setSuccessResponse(omResponse);
      omClientResponse = target.createResponse(omResponse.build(), omKeyInfo);

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = createErrorResponse(omResponse, exception);
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        getAuditAction(), auditMap, exception, getOmRequest().getUserInfo()
    ));

    switch (result) {
    case SUCCESS:
      getLogger().debug("{} success. Volume:{}, Bucket:{}, Key:{}.", getOperationName(),
          volumeName, bucketName, keyName);
      break;
    case FAILURE:
      incFailureMetric(omMetrics);
      if (OMClientRequestUtils.shouldLogClientRequestFailure(exception)) {
        getLogger().error("{} failed. Volume:{}, Bucket:{}, Key:{}.", getOperationName(),
            volumeName, bucketName, keyName, exception);
      }
      break;
    default:
      getLogger().error("Unrecognized Result for {}: {}", getOperationName(), getOmRequest());
    }

    return omClientResponse;
  }

  /**
   * Resolves the existing key targeted by this update. Object store layout by
   * default; FSO subclasses override this to call {@link #resolveFsoTarget}.
   */
  protected KeyUpdateTarget resolveTarget(OzoneManager ozoneManager, OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName) throws IOException {
    String dbOzoneKey =
        omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
    if (omKeyInfo == null) {
      throw new OMException("Key not found", KEY_NOT_FOUND);
    }
    return new KeyUpdateTarget(omKeyInfo, dbOzoneKey);
  }

  /**
   * Resolves the existing file targeted by this update in an FSO bucket.
   * Directories are rejected: these updates apply to objects only.
   */
  protected final KeyUpdateTarget resolveFsoTarget(OzoneManager ozoneManager, OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName) throws IOException {
    OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
        omMetadataManager, volumeName, bucketName, keyName, 0,
        ozoneManager.getDefaultReplicationConfig());

    if (keyStatus == null) {
      throw new OMException("Key not found. Key: " + keyName, KEY_NOT_FOUND);
    }

    if (keyStatus.isDirectory()) {
      throw new OMException(getOperationName() + " is not currently supported for FSO directory",
          NOT_SUPPORTED_OPERATION);
    }

    OmKeyInfo omKeyInfo = keyStatus.getKeyInfo();
    // Reverting back the full path to key name
    // Eg: a/b/c/d/e/file1 -> file1
    omKeyInfo.setKeyName(OzoneFSUtils.getFileName(keyName));
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    final String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());

    return new KeyUpdateTarget(omKeyInfo, dbKey, volumeId, bucketId);
  }

  private OMClientResponse createErrorResponse(OMResponse.Builder omResponse, IOException exception) {
    OMResponse errorResponse = createErrorOMResponse(omResponse, exception);
    return getBucketLayout().isFileSystemOptimized()
        ? new OMKeyInfoUpdateResponseWithFSO(errorResponse, getBucketLayout())
        : new OMKeyInfoUpdateResponse(errorResponse, getBucketLayout());
  }

  /**
   * The existing key targeted by an update, resolved according to the bucket
   * layout.
   */
  protected static final class KeyUpdateTarget {

    private final OmKeyInfo keyInfo;
    private final String dbKey;
    private final long volumeId;
    private final long bucketId;
    private final boolean fileSystemOptimized;

    KeyUpdateTarget(OmKeyInfo keyInfo, String dbKey) {
      this(keyInfo, dbKey, 0L, 0L, false);
    }

    KeyUpdateTarget(OmKeyInfo keyInfo, String dbKey, long volumeId, long bucketId) {
      this(keyInfo, dbKey, volumeId, bucketId, true);
    }

    private KeyUpdateTarget(OmKeyInfo keyInfo, String dbKey, long volumeId, long bucketId,
        boolean fileSystemOptimized) {
      this.keyInfo = keyInfo;
      this.dbKey = dbKey;
      this.volumeId = volumeId;
      this.bucketId = bucketId;
      this.fileSystemOptimized = fileSystemOptimized;
    }

    OmKeyInfo getKeyInfo() {
      return keyInfo;
    }

    String getDbKey() {
      return dbKey;
    }

    OMClientResponse createResponse(OMResponse omResponse, OmKeyInfo updatedKeyInfo) {
      return fileSystemOptimized
          ? new OMKeyInfoUpdateResponseWithFSO(omResponse, updatedKeyInfo, volumeId, bucketId)
          : new OMKeyInfoUpdateResponse(omResponse, updatedKeyInfo);
    }
  }
}
