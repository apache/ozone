/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.key;

import com.google.common.base.Optional;
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
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RenameKeyResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles rename key request - prefix layout.
 */
public class OMKeyRenameRequestWithFSO extends OMKeyRenameRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMKeyRenameRequestWithFSO.class);

  public OMKeyRenameRequestWithFSO(OMRequest omRequest,
                                   BucketLayout bucketLayout)
      throws OMException {
    super(omRequest, bucketLayout);
    OMClientRequestUtils.checkFSOClientRequestPreconditions(getBucketLayout());
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

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
    IOException exception = null;
    OmKeyInfo fromKeyValue;
    Result result;
    try {
      if (toKeyName.length() == 0 || fromKeyName.length() == 0) {
        throw new OMException("Key name is empty",
                OMException.ResultCodes.INVALID_KEY_NAME);
      }

      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      // check Acls to see if user has access to perform delete operation on
      // old key and create operation on new key

      // check Acl fromKeyName
      checkACLsWithFSO(ozoneManager, volumeName, bucketName, fromKeyName,
          IAccessAuthorizer.ACLType.DELETE);

      // check Acl toKeyName
      checkKeyAcls(ozoneManager, volumeName, bucketName, toKeyName,
              IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      // Check if fromKey exists
      OzoneFileStatus fromKeyFileStatus =
              OMFileRequest.getOMKeyInfoIfExists(omMetadataManager, volumeName,
                      bucketName, fromKeyName, 0);
      // case-1) fromKeyName should exist, otw throws exception
      if (fromKeyFileStatus == null) {
        // TODO: Add support for renaming open key
        throw new OMException("Key not found " + fromKeyName, KEY_NOT_FOUND);
      }

      // source existed
      fromKeyValue = fromKeyFileStatus.getKeyInfo();
      boolean isRenameDirectory = fromKeyFileStatus.isDirectory();

      // case-2) Cannot rename a directory to its own subdirectory
      OMFileRequest.verifyToDirIsASubDirOfFromDirectory(fromKeyName,
              toKeyName, fromKeyFileStatus.isDirectory());

      OzoneFileStatus toKeyFileStatus =
              OMFileRequest.getOMKeyInfoIfExists(omMetadataManager,
                      volumeName, bucketName, toKeyName, 0);

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
                          volumeName, bucketName, newToKeyName, 0);

          if (newToOzoneFileStatus != null) {
            // case-5) If new destin '/dst/source' exists then throws exception
            throw new OMException(String.format(
                    "Failed to rename %s to %s, file already exists or not " +
                            "empty!", fromKeyName, newToKeyName),
                    OMException.ResultCodes.KEY_ALREADY_EXISTS);
          }

          omClientResponse = renameKey(toKeyValue.getObjectID(), trxnLogIndex,
                  fromKeyValue, isRenameDirectory, newToKeyName,
                  keyArgs.getModificationTime(), omResponse, ozoneManager);
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
        long toKeyParentId = OMFileRequest.getToKeyNameParentId(volumeName,
                bucketName, toKeyName, fromKeyName, omMetadataManager);

        omClientResponse = renameKey(toKeyParentId, trxnLogIndex,
                fromKeyValue, isRenameDirectory, toKeyName,
                keyArgs.getModificationTime(), omResponse, ozoneManager);

        result = Result.SUCCESS;
      }
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyRenameResponseWithFSO(createErrorOMResponse(
              omResponse, exception), getBucketLayout());
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
              omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
                bucketName);
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.RENAME_KEY, auditMap,
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
                      "toKey:{}. Key: {} not found.", volumeName, bucketName,
              fromKeyName, toKeyName, fromKeyName);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyRenameRequest: {}",
              renameKeyRequest);
    }
    return omClientResponse;
  }

  @SuppressWarnings("parameternumber")
  private OMClientResponse renameKey(long toKeyParentId,
      long trxnLogIndex, OmKeyInfo fromKeyValue, boolean isRenameDirectory,
      String toKeyName, long modificationTime, OMResponse.Builder omResponse,
      OzoneManager ozoneManager) {

    String dbFromKey = fromKeyValue.getPath();
    String toKeyFileName = OzoneFSUtils.getFileName(toKeyName);

    fromKeyValue.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
    // Set toFileName
    fromKeyValue.setKeyName(toKeyFileName);
    fromKeyValue.setFileName(toKeyFileName);
    // Set toKeyObjectId
    fromKeyValue.setParentObjectID(toKeyParentId);
    //Set modification time
    fromKeyValue.setModificationTime(modificationTime);

    // destination dbKeyName
    String dbToKey = fromKeyValue.getPath();

    // Add to cache.
    // dbFromKey should be deleted, dbToKey should be added with newly updated
    // omKeyInfo.
    // Add from_key and to_key details into cache.
    OMMetadataManager metadataMgr = ozoneManager.getMetadataManager();
    if (isRenameDirectory) {
      Table<String, OmDirectoryInfo> dirTable = metadataMgr.getDirectoryTable();
      dirTable.addCacheEntry(new CacheKey<>(dbFromKey),
              new CacheValue<>(Optional.absent(), trxnLogIndex));

      dirTable.addCacheEntry(new CacheKey<>(dbToKey),
              new CacheValue<>(Optional.of(OMFileRequest.
                              getDirectoryInfo(fromKeyValue)), trxnLogIndex));
    } else {
      Table<String, OmKeyInfo> keyTable =
          metadataMgr.getKeyTable(getBucketLayout());

      keyTable.addCacheEntry(new CacheKey<>(dbFromKey),
              new CacheValue<>(Optional.absent(), trxnLogIndex));

      keyTable.addCacheEntry(new CacheKey<>(dbToKey),
              new CacheValue<>(Optional.of(fromKeyValue), trxnLogIndex));
    }

    OMClientResponse omClientResponse = new OMKeyRenameResponseWithFSO(
        omResponse.setRenameKeyResponse(RenameKeyResponse.newBuilder()).build(),
        dbFromKey, dbToKey, fromKeyValue, isRenameDirectory,
        getBucketLayout());
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
}
