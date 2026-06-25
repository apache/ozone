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
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator;
import org.apache.hadoop.ozone.om.request.validation.ValidationCondition;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles rename key request.
 */
public class OMKeyRenameRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyRenameRequest.class);

  public OMKeyRenameRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    RenameKeyRequest renameKeyRequest = super.preExecute(ozoneManager)
        .getRenameKeyRequest();
    Objects.requireNonNull(renameKeyRequest, "renameKeyRequest == null");

    KeyArgs renameKeyArgs = renameKeyRequest.getKeyArgs();

    if (ozoneManager.getConfig().isKeyNameCharacterCheckEnabled()) {
      OmUtils.validateKeyName(renameKeyRequest.getToKeyName());
    }

    String srcKey = extractSrcKey(renameKeyArgs);
    String dstKey = extractDstKey(renameKeyRequest);

    // Set modification time & srcKeyName.
    KeyArgs.Builder newKeyArgs = renameKeyArgs.toBuilder()
        .setModificationTime(Time.now()).setKeyName(srcKey);

    KeyArgs resolvedArgs = resolveBucketAndCheckAcls(newKeyArgs.build(),
        ozoneManager, srcKey, dstKey);

    return getOmRequest().toBuilder()
        .setRenameKeyRequest(renameKeyRequest.toBuilder().setToKeyName(dstKey)
            .setKeyArgs(resolvedArgs))
        .setUserInfo(getUserIfNotExists(ozoneManager)).build();

  }

  protected KeyArgs resolveBucketAndCheckAcls(KeyArgs keyArgs,
      OzoneManager ozoneManager, String fromKeyName, String toKeyName)
      throws IOException {
    KeyArgs resolvedArgs = resolveBucketLink(ozoneManager, keyArgs);
    // check Acl
    String volumeName = resolvedArgs.getVolumeName();
    String bucketName = resolvedArgs.getBucketName();

    checkKeyAcls(ozoneManager, volumeName, bucketName, fromKeyName,
        IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY);
    checkKeyAcls(ozoneManager, volumeName, bucketName, toKeyName,
        IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);
    return resolvedArgs;
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    RenameKeyRequest renameKeyRequest = getOmRequest().getRenameKeyRequest();
    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        renameKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildAuditMap(keyArgs, renameKeyRequest);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String fromKeyName = keyArgs.getKeyName();
    String toKeyName = renameKeyRequest.getToKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyRenames();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    Exception exception = null;
    OmKeyInfo fromKeyValue = null;
    String toKey = null, fromKey = null;
    Result result = null;
    try {
      if (toKeyName.isEmpty() || fromKeyName.isEmpty()) {
        throw new OMException("Key name is empty",
            OMException.ResultCodes.INVALID_KEY_NAME);
      }
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      // Check if toKey exists
      fromKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          fromKeyName);
      toKey = omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName);
      OmKeyInfo toKeyValue =
          omMetadataManager.getKeyTable(getBucketLayout()).get(toKey);

      if (toKeyValue != null) {
        throw new OMException("Key already exists " + toKeyName,
              OMException.ResultCodes.KEY_ALREADY_EXISTS);
      }

      // fromKeyName should exist
      fromKeyValue =
          omMetadataManager.getKeyTable(getBucketLayout()).get(fromKey);
      if (fromKeyValue == null) {
          // TODO: Add support for renaming open key
        throw new OMException("Key not found " + fromKey, KEY_NOT_FOUND);
      }

      fromKeyValue = fromKeyValue.toBuilder()
          .setUpdateID(trxnLogIndex)
          .build();

      fromKeyValue.setKeyName(toKeyName);

      //Set modification time
      fromKeyValue.setModificationTime(keyArgs.getModificationTime());

      // Add to cache.
      // fromKey should be deleted, toKey should be added with newly updated
      // omKeyInfo.
      Table<String, OmKeyInfo> keyTable =
          omMetadataManager.getKeyTable(getBucketLayout());

      keyTable.addCacheEntry(new CacheKey<>(fromKey),
          CacheValue.get(trxnLogIndex));

      keyTable.addCacheEntry(new CacheKey<>(toKey),
          CacheValue.get(trxnLogIndex, fromKeyValue));

      omClientResponse = new OMKeyRenameResponse(omResponse
          .setRenameKeyResponse(RenameKeyResponse.newBuilder()).build(),
          fromKeyName, toKeyName, fromKeyValue, getBucketLayout());

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyRenameResponse(createErrorOMResponse(
          omResponse, exception), getBucketLayout());
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    markForAudit(auditLogger, buildAuditMessage(OMAction.RENAME_KEY, auditMap,
        exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("Rename Key is successfully completed for volume:{} bucket:{}" +
              " fromKey:{} toKey:{}. ", volumeName, bucketName, fromKeyName,
          toKeyName);
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumKeyRenameFails();
      LOG.error("Rename key failed for volume:{} bucket:{} fromKey:{} " +
              "toKey:{}. Exception: {}.", volumeName, bucketName,
          fromKeyName, toKeyName, exception.getMessage());
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyRenameRequest: {}",
          renameKeyRequest);
    }
    return omClientResponse;
  }

  private Map<String, String> buildAuditMap(
      KeyArgs keyArgs, RenameKeyRequest renameKeyRequest) {
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    auditMap.remove(OzoneConsts.KEY);
    auditMap.put(OzoneConsts.SRC_KEY, keyArgs.getKeyName());
    auditMap.put(OzoneConsts.DST_KEY, renameKeyRequest.getToKeyName());
    return auditMap;
  }


  /**
   * Validates rename key requests.
   * We do not want to allow older clients to rename keys in buckets which use
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
      requestType = Type.RenameKey
  )
  public static OMRequest blockRenameKeyWithBucketLayoutFromOldClient(
      OMRequest req, ValidationContext ctx) throws IOException {
    if (req.getRenameKeyRequest().hasKeyArgs()) {
      KeyArgs keyArgs = req.getRenameKeyRequest().getKeyArgs();

      if (keyArgs.hasVolumeName() && keyArgs.hasBucketName()) {
        BucketLayout bucketLayout = ctx.getBucketLayout(
            keyArgs.getVolumeName(), keyArgs.getBucketName());
        bucketLayout.validateSupportedOperation();
      }
    }
    return req;
  }

  /**
   * Returns the source key name.
   *
   * @param keyArgs
   * @return source key name
   * @throws OMException
   */
  protected String extractSrcKey(KeyArgs keyArgs) throws OMException {
    return keyArgs.getKeyName();
  }

  /**
   * Returns the destination key name.
   *
   * @param renameKeyRequest
   * @return destination key name
   * @throws OMException
   */
  protected String extractDstKey(RenameKeyRequest renameKeyRequest)
      throws OMException {
    return renameKeyRequest.getToKeyName();
  }
}
