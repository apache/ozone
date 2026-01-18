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

import static org.apache.hadoop.ozone.OmUtils.normalizeKey;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.RENAME_OPEN_FILE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.OMClientRequestUtils.validateKeyName;

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
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles rename key request - prefix layout.
 */
public class OMKeyRenameRequestWithFSO extends OMKeyRenameRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMKeyRenameRequestWithFSO.class);

  public OMKeyRenameRequestWithFSO(OMRequest omRequest,
                                   BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest omRequest = commonKeyRenamePreExecute(ozoneManager);
    final KeyArgs keyArgs = omRequest.getRenameKeyRequest().getKeyArgs();
    validateKeyName(keyArgs.getKeyName());
    return omRequest;
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    RenameKeyRequest renameKeyRequest = getOmRequest().getRenameKeyRequest();
    KeyArgs keyArgs = renameKeyRequest.getKeyArgs();
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
    OmKeyInfo fromKeyValue;
    Result result;
    try {
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      // Check if fromKey exists
      OzoneFileStatus fromKeyFileStatus = OMFileRequest.getOMKeyInfoIfExists(
          omMetadataManager, volumeName, bucketName, fromKeyName, 0,
          ozoneManager.getDefaultReplicationConfig());

      // case-1) fromKeyName should exist, otw throws exception
      if (fromKeyFileStatus == null) {
        // TODO: Add support for renaming open key
        throw new OMException("Key not found " + fromKeyName, KEY_NOT_FOUND);
      }

      if (fromKeyFileStatus.getKeyInfo().isHsync()) {
        throw new OMException("Open file cannot be renamed since it is " +
            "hsync'ed: volumeName=" + volumeName + ", bucketName=" +
            bucketName + ", key=" + fromKeyName, RENAME_OPEN_FILE);
      }

      // source existed
      fromKeyValue = fromKeyFileStatus.getKeyInfo();
      boolean isRenameDirectory = fromKeyFileStatus.isDirectory();

      // case-2) Cannot rename a directory to its own subdirectory
      OMFileRequest.verifyToDirIsASubDirOfFromDirectory(fromKeyName,
              toKeyName, fromKeyFileStatus.isDirectory());

      OzoneFileStatus toKeyFileStatus = OMFileRequest.getOMKeyInfoIfExists(
          omMetadataManager, volumeName, bucketName, toKeyName, 0,
          ozoneManager.getDefaultReplicationConfig());

      // Check if toKey exists.
      if (toKeyFileStatus != null) {
        // Destination exists and following are different cases:
        OmKeyInfo toKeyValue = toKeyFileStatus.getKeyInfo();

        if (fromKeyValue.getKeyName().equals(toKeyValue.getKeyName())) {
          // case-3) If src == destin then check source and destin of same type
          // (a) If dst is a file then return true.
          // (b) Otherwise throws exception.
          // TODO: Discuss do we need to throw exception for file as well.
          if (toKeyFileStatus.isFile()) {
            result = Result.SUCCESS;
          } else {
            throw new OMException("Key already exists " + toKeyName,
                    OMException.ResultCodes.KEY_ALREADY_EXISTS);
          }
        } else if (toKeyFileStatus.isDirectory()) {
          // case-4) If dst is a directory then rename source as sub-path of it
          // For example: rename /source to /dst will lead to /dst/source
          String fromFileName = OzoneFSUtils.getFileName(fromKeyName);
          String newToKeyName = OzoneFSUtils.appendFileNameToKeyPath(toKeyName,
                  fromFileName);
          OzoneFileStatus newToOzoneFileStatus =
              OMFileRequest.getOMKeyInfoIfExists(omMetadataManager,
                  volumeName, bucketName, newToKeyName, 0,
                  ozoneManager.getDefaultReplicationConfig());

          if (newToOzoneFileStatus != null) {
            // case-5) If new destin '/dst/source' exists then throws exception
            throw new OMException(String.format(
                    "Failed to rename %s to %s, file already exists or not " +
                            "empty!", fromKeyName, newToKeyName),
                    OMException.ResultCodes.KEY_ALREADY_EXISTS);
          }

          omClientResponse = renameKey(toKeyValue, newToKeyName, fromKeyValue,
              fromKeyName, isRenameDirectory, keyArgs.getModificationTime(),
              ozoneManager, omResponse, trxnLogIndex);
          result = Result.SUCCESS;
        } else {
          // case-6) If destination is a file type and if exists then throws
          // key already exists exception.
          throw new OMException("Failed to rename, key already exists "
                  + toKeyName, OMException.ResultCodes.KEY_ALREADY_EXISTS);
        }
      } else {
        // Destination doesn't exist and the cases are:
        // case-7) Check whether dst parent dir exists or not. If parent
        // doesn't exist then throw exception, otw the source can be renamed to
        // destination path.
        OmKeyInfo toKeyParent = OMFileRequest.getKeyParentDir(volumeName,
                bucketName, toKeyName, ozoneManager, omMetadataManager);

        omClientResponse = renameKey(toKeyParent, toKeyName, fromKeyValue,
            fromKeyName, isRenameDirectory, keyArgs.getModificationTime(),
            ozoneManager, omResponse, trxnLogIndex);

        result = Result.SUCCESS;
      }
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyRenameResponseWithFSO(createErrorOMResponse(
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
                      " fromKey:{} toKey:{}. ", volumeName, bucketName,
              fromKeyName, toKeyName);
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

  @Override
  protected KeyArgs resolveBucketAndCheckAcls(KeyArgs keyArgs,
      OzoneManager ozoneManager, String fromKeyName, String toKeyName)
      throws IOException {
    KeyArgs resolvedArgs = resolveBucketLink(ozoneManager, keyArgs);
    // check Acl
    String volumeName = resolvedArgs.getVolumeName();
    String bucketName = resolvedArgs.getBucketName();
    // check Acls to see if user has access to perform delete operation on
    // old key and create operation on new key

    // check Acl fromKeyName
    checkACLsWithFSO(ozoneManager, volumeName, bucketName, fromKeyName,
        IAccessAuthorizer.ACLType.DELETE);

    // check Acl toKeyName
    if (toKeyName.isEmpty()) {
      // if the toKeyName is empty we are checking the ACLs of the bucket
      checkBucketAcls(ozoneManager, volumeName, bucketName, toKeyName,
          IAccessAuthorizer.ACLType.CREATE);
    } else {
      checkKeyAcls(ozoneManager, volumeName, bucketName, toKeyName,
          IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);
    }

    return resolvedArgs;
  }

  @SuppressWarnings("parameternumber")
  private OMClientResponse renameKey(OmKeyInfo toKeyParent, String toKeyName,
      OmKeyInfo fromKeyValue, String fromKeyName, boolean isRenameDirectory,
      long modificationTime, OzoneManager ozoneManager,
      OMResponse.Builder omResponse, long trxnLogIndex) throws IOException {
    final OMMetadataManager ommm = ozoneManager.getMetadataManager();
    final long volumeId = ommm.getVolumeId(fromKeyValue.getVolumeName());
    final long bucketId = ommm.getBucketId(fromKeyValue.getVolumeName(),
            fromKeyValue.getBucketName());
    OmBucketInfo omBucketInfo = null;
    final String dbFromKey = ommm.getOzonePathKey(volumeId, bucketId,
            fromKeyValue.getParentObjectID(), fromKeyValue.getFileName());
    String toKeyFileName;
    if (toKeyName.isEmpty()) {
      // if toKeyName is empty we use the source key name.
      toKeyFileName = OzoneFSUtils.getFileName(fromKeyName);
    } else {
      toKeyFileName = OzoneFSUtils.getFileName(toKeyName);
    }
    OmKeyInfo fromKeyParent = null;
    OMMetadataManager metadataMgr = ozoneManager.getMetadataManager();
    Table<String, OmDirectoryInfo> dirTable = metadataMgr.getDirectoryTable();
    String bucketKey = metadataMgr.getBucketKey(
        fromKeyValue.getVolumeName(), fromKeyValue.getBucketName());

    OmKeyInfo.Builder fromKeyBuilder = fromKeyValue.toBuilder()
        .setUpdateID(trxnLogIndex);
    // Set toFileName
    fromKeyBuilder.setKeyName(toKeyFileName);
    // Set toKeyObjectId
    if (toKeyParent != null) {
      fromKeyBuilder.setParentObjectID(toKeyParent.getObjectID());
    } else {
      omBucketInfo = metadataMgr.getBucketTable().get(bucketKey);
      fromKeyBuilder.setParentObjectID(omBucketInfo.getObjectID());
    }
    fromKeyValue = fromKeyBuilder.build();

    // Set modification time
    omBucketInfo = setModificationTime(ommm, omBucketInfo, toKeyParent, volumeId, bucketId,
        modificationTime, dirTable, trxnLogIndex);
    fromKeyParent = OMFileRequest.getKeyParentDir(fromKeyValue.getVolumeName(),
        fromKeyValue.getBucketName(), fromKeyName, ozoneManager, metadataMgr);
    if (fromKeyParent == null && omBucketInfo == null) {
      // Get omBucketInfo only when needed to reduce unnecessary DB IO
      omBucketInfo = metadataMgr.getBucketTable().get(bucketKey);
    }
    omBucketInfo = setModificationTime(ommm, omBucketInfo, fromKeyParent, volumeId,
        bucketId, modificationTime, dirTable, trxnLogIndex);

    // destination dbKeyName
    String dbToKey = ommm.getOzonePathKey(volumeId, bucketId,
            fromKeyValue.getParentObjectID(), toKeyFileName);

    // Add to cache.
    // dbFromKey should be deleted, dbToKey should be added with newly updated
    // omKeyInfo.
    // Add from_key and to_key details into cache.
    if (isRenameDirectory) {
      dirTable.addCacheEntry(new CacheKey<>(dbFromKey),
              CacheValue.get(trxnLogIndex));

      dirTable.addCacheEntry(new CacheKey<>(dbToKey),
          CacheValue.get(trxnLogIndex,
              OMFileRequest.getDirectoryInfo(fromKeyValue)));
    } else {
      Table<String, OmKeyInfo> keyTable =
          metadataMgr.getKeyTable(getBucketLayout());

      keyTable.addCacheEntry(new CacheKey<>(dbFromKey),
              CacheValue.get(trxnLogIndex));

      keyTable.addCacheEntry(new CacheKey<>(dbToKey),
              CacheValue.get(trxnLogIndex, fromKeyValue));
    }

    OMClientResponse omClientResponse = new OMKeyRenameResponseWithFSO(
        omResponse.setRenameKeyResponse(RenameKeyResponse.newBuilder()).build(),
        dbFromKey, dbToKey, fromKeyParent, toKeyParent, fromKeyValue,
        omBucketInfo, isRenameDirectory, getBucketLayout());
    return omClientResponse;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private OmBucketInfo setModificationTime(OMMetadataManager omMetadataManager,
      OmBucketInfo bucketInfo, OmKeyInfo keyParent,
      long volumeId, long bucketId, long modificationTime,
      Table<String, OmDirectoryInfo> dirTable, long trxnLogIndex)
      throws OMException {
    // For Filesystem. rename will change renamed file parent directory
    // modification time but not change its modification time.
    if (keyParent != null) {
      keyParent.setModificationTime(modificationTime);
      String dbToKeyParent = omMetadataManager.getOzonePathKey(volumeId,
          bucketId, keyParent.getParentObjectID(), keyParent.getFileName());
      dirTable.addCacheEntry(new CacheKey<>(dbToKeyParent),
          CacheValue.get(trxnLogIndex,
              OMFileRequest.getDirectoryInfo(keyParent)));
      return bucketInfo;
    }
    // For FSO a bucket is root of the filesystem, so rename an
    // object at the root of a bucket need change bucket's modificationTime
    if (bucketInfo == null) {
      throw new OMException("Bucket not found",
          OMException.ResultCodes.BUCKET_NOT_FOUND);
    }
    OmBucketInfo newBucketInfo = bucketInfo.toBuilder()
        .setModificationTime(modificationTime)
        .build();
    String bucketKey = omMetadataManager.getBucketKey(
        newBucketInfo.getVolumeName(), newBucketInfo.getBucketName());
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(bucketKey),
        CacheValue.get(trxnLogIndex, newBucketInfo));

    return newBucketInfo;
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
   * Returns the normalized and validated destination key name. It is handling
   * the case when the toKeyName is empty (if we are renaming a key to bucket
   * level, e.g. source is /vol1/buck1/dir1/key1 and dest is /vol1/buck1).
   *
   * @param request
   * @return {@code String}
   * @throws OMException
   */
  @Override
  protected String extractDstKey(RenameKeyRequest request) throws OMException {
    String normalizedDstKey = normalizeKey(request.getToKeyName(), false);
    return normalizedDstKey.isEmpty() ?
        normalizedDstKey :
        isValidKeyPath(normalizedDstKey);
  }

  /**
   * Returns the validated and normalized source key name.
   *
   * @param keyArgs
   * @return {@code String}
   * @throws OMException
   */
  @Override
  protected String extractSrcKey(KeyArgs keyArgs) throws OMException {
    return validateAndNormalizeKey(keyArgs.getKeyName());
  }
}
