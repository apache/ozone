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

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RenameKeyResponse;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles rename key request.
 */
public class OMKeyRenameRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyRenameRequest.class);

  public OMKeyRenameRequest(OMRequest omRequest) {
    super(omRequest);
  }

  /**
   * Stores the result of request execution for Rename Requests.
   */
  private enum Result {
    SUCCESS,
    DELETE_FROM_KEY_ONLY,
    REPLAY,
    FAILURE,
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    RenameKeyRequest renameKeyRequest = getOmRequest().getRenameKeyRequest();
    Preconditions.checkNotNull(renameKeyRequest);

    // Set modification time.
    KeyArgs.Builder newKeyArgs = renameKeyRequest.getKeyArgs().toBuilder()
            .setModificationTime(Time.now());

    return getOmRequest().toBuilder()
        .setRenameKeyRequest(renameKeyRequest.toBuilder()
            .setKeyArgs(newKeyArgs)).setUserInfo(getUserInfo()).build();

  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    RenameKeyRequest renameKeyRequest = getOmRequest().getRenameKeyRequest();
    OzoneManagerProtocolProtos.KeyArgs renameKeyArgs =
        renameKeyRequest.getKeyArgs();

    String volumeName = renameKeyArgs.getVolumeName();
    String bucketName = renameKeyArgs.getBucketName();
    String fromKeyName = renameKeyArgs.getKeyName();
    String toKeyName = renameKeyRequest.getToKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyRenames();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap =
        buildAuditMap(renameKeyArgs, renameKeyRequest);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    OmKeyInfo fromKeyValue = null;
    String toKey = null, fromKey = null;
    Result result = null;
    try {
      if (toKeyName.length() == 0 || fromKeyName.length() == 0) {
        throw new OMException("Key name is empty",
            OMException.ResultCodes.INVALID_KEY_NAME);
      }
      // check Acls to see if user has access to perform delete operation on
      // old key and create operation on new key
      checkKeyAcls(ozoneManager, volumeName, bucketName, fromKeyName,
          IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY);
      checkKeyAcls(ozoneManager, volumeName, bucketName, toKeyName,
          IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      // fromKeyName should exist
      OMFileRequest.OzKeyInfo fromOzKeyInfo =
              OMFileRequest.getOmKeyInfoFromDB(volumeName,
                      bucketName,
                      fromKeyName,
                      omMetadataManager);
      fromKeyValue = fromOzKeyInfo.getOmKeyInfo();
      if (fromKeyValue == null) {
        // TODO: Add support for renaming open key
        throw new OMException("Key not found " + fromKeyName, KEY_NOT_FOUND);
      }
      // identify fromKey is a directory or a file
      boolean isDir = fromOzKeyInfo.isDir();

      // Check if toKey exists
      OMFileRequest.OzKeyInfo toOzKeyInfo =
              OMFileRequest.getOmKeyInfoFromDB(volumeName,
                      bucketName,
                      toKeyName, omMetadataManager);
      OmKeyInfo toKeyValue = toOzKeyInfo.getOmKeyInfo();
      if (toKeyValue != null) {
        // Check if this transaction is a replay of ratis logs.
        if (isReplay(ozoneManager, toKeyValue, trxnLogIndex)) {

          // Check if fromKey is still in the DB and created before this
          // replay.
          // For example, lets say we have the following sequence of
          // transactions.
          //     Trxn 1 : Create Key1
          //     Trnx 2 : Rename Key1 to Key2 -> Deletes Key1 and Creates Key2
          // Now if these transactions are replayed:
          //     Replay Trxn 1 : Creates Key1 again as Key1 does not exist in DB
          //     Replay Trxn 2 : Key2 is not created as it exists in DB and the
          //                     request would be deemed a replay. But Key1
          //                     is still in the DB and needs to be deleted.
          if (fromKeyValue != null) {
            // Check if this replay transaction was after the fromKey was
            // created. If so, we have to delete the fromKey.
            if (ozoneManager.isRatisEnabled() &&
                trxnLogIndex > fromKeyValue.getUpdateID()) {
              // Add to cache. Only fromKey should be deleted. ToKey already
              // exists in DB as this transaction is a replay.
              result = Result.DELETE_FROM_KEY_ONLY;
              String dbFromKeyname =
                      omMetadataManager.getOzoneLeafNodeKey(
                              fromKeyValue.getParentObjectID(),
                              fromKeyValue.getLeafNodeName());
              if(isDir){
                Table<String, OmDirectoryInfo> dirTable = omMetadataManager
                        .getDirectoryTable();
                dirTable.addCacheEntry(new CacheKey<>(dbFromKeyname),
                        new CacheValue<>(Optional.absent(), trxnLogIndex));
              } else {
                Table<String, OmKeyInfo> keyTable = omMetadataManager
                        .getKeyTable();
                keyTable.addCacheEntry(new CacheKey<>(dbFromKeyname),
                        new CacheValue<>(Optional.absent(), trxnLogIndex));
              }

              omClientResponse = new OMKeyRenameResponse(omResponse
                  .setRenameKeyResponse(RenameKeyResponse.newBuilder()).build(),
                  fromKeyName, fromKeyValue, isDir);
            }
          }

          if (result == null) {
            result = Result.REPLAY;
            // If toKey exists and fromKey does not, then no further action is
            // required. Return a dummy OMClientResponse.
            omClientResponse = new OMKeyRenameResponse(createReplayOMResponse(
                omResponse));
          }
        } else {

          if (fromKeyValue != null) {
            if (fromKeyValue.getKeyName().equals(toKeyValue.getKeyName())) {
              // if dst exists and source and destination are same,
              // check both the src and dst are of same type
              if (toOzKeyInfo.isDir() && fromOzKeyInfo.isDir()) {
                result = Result.SUCCESS;
              } else {
                // toKeyName should not exist
                throw new OMException("Key already exists " + toKeyName,
                        OMException.ResultCodes.KEY_ALREADY_EXISTS);
              }
            } else if (toOzKeyInfo.isDir()) {
              // If dst is a directory, rename source as subpath of it.
              // for example rename /source to /dst will lead to /dst/source
              String fromFileName = OzoneFSUtils.getFileName(fromKeyName);
              String newToKeyName = OzoneFSUtils.appendKeyName(toKeyName,
                      fromFileName);
              OMFileRequest.OzKeyInfo newToOzKeyInfo=
                      OMFileRequest.getOmKeyInfoFromDB(volumeName,
                              bucketName,
                              newToKeyName, omMetadataManager);
              OmKeyInfo newToKeyValue = newToOzKeyInfo.getOmKeyInfo();
              if(newToKeyValue != null) {
                // If dst exists and not a directory not empty
                throw new OMException(String.format(
                        "Failed to rename %s to %s," +
                                " file already exists or not empty!",
                        fromKeyName, newToKeyName),
                        OMException.ResultCodes.KEY_ALREADY_EXISTS);
              }
              omClientResponse = renameKey(trxnLogIndex,
                      fromKeyValue, isDir,
                      newToKeyName, toOzKeyInfo.getOmKeyInfo().getObjectID(),
                      renameKeyArgs.getModificationTime(),
                      omResponse, ozoneManager,
                      omMetadataManager);
                result = Result.SUCCESS;
            } else {
              // toKeyName should not exist
              throw new OMException("Key already exists " + toKeyName,
                      OMException.ResultCodes.KEY_ALREADY_EXISTS);
            }
          }
        }
      } else {

        // This transaction is not a replay and destination doesn't exits.

        // Cannot rename a directory to its own subdirectory
        OMFileRequest.verifyToDirIsASubDirOfFromDirectory(fromKeyName,
                toKeyName, isDir);

        // Destination doesn't exist, check whether dst parent dir exists or not
        // if the parent exists, the source can still be renamed to dst path
        OMFileRequest.verifyToKeynameParentDirExists(volumeName, bucketName,
                toKeyName, fromKeyName, omMetadataManager);

        omClientResponse = renameKey(trxnLogIndex,
                fromKeyValue, isDir,
                toKeyName, toOzKeyInfo.getLastKnownParentId(),
                renameKeyArgs.getModificationTime(),
                omResponse, ozoneManager,
                omMetadataManager);
        result = Result.SUCCESS;
      }
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyRenameResponse(createErrorOMResponse(
          omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
            omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    if (result == Result.SUCCESS || result == Result.FAILURE) {
      auditLog(auditLogger, buildAuditMessage(OMAction.RENAME_KEY, auditMap,
          exception, getOmRequest().getUserInfo()));
    }

    switch (result) {
    case SUCCESS:
      LOG.debug("Rename Key is successfully completed for volume:{} bucket:{}" +
              " fromKey:{} toKey:{}. ", volumeName, bucketName, fromKeyName,
          toKeyName);
      break;
    case DELETE_FROM_KEY_ONLY:
      LOG.debug("Replayed transaction {}: {}. Renamed Key {} already exists. " +
              "Deleting old key {}.", trxnLogIndex, renameKeyRequest, toKey,
          fromKey);
      break;
    case REPLAY:
      LOG.debug("Replayed Transaction {} ignored. Request: {}", trxnLogIndex,
          renameKeyRequest);
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

  @NotNull
  private OMClientResponse renameKey(long trxnLogIndex,
                                     OmKeyInfo fromKeyValue,
                                     boolean isDir,
                                     String toKeyName,
                                     long toLastKnownParentId,
                                     long modificationTime,
                                     OMResponse.Builder omResponse,
                                     OzoneManager ozoneManager,
                                     OMMetadataManager omMetadataManager) {

    // fromKey should be deleted, toKey should be added with newly updated
    // omKeyInfo.
    String dbFromKeyname =
            omMetadataManager.getOzoneLeafNodeKey(
                    fromKeyValue.getParentObjectID(),
                    fromKeyValue.getLeafNodeName());
    String dbToKeyname =
            omMetadataManager.getOzoneLeafNodeKey(
                    toLastKnownParentId,
                    OzoneFSUtils.getFileName(toKeyName));

    // Do rename by updating fromKeyValue object to toKeyName details.
    fromKeyValue.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
    fromKeyValue.setKeyName(toKeyName);
    fromKeyValue.setLeafNodeName(OzoneFSUtils.getFileName(toKeyName));
    fromKeyValue.setParentObjectID(toLastKnownParentId);
    //Set modification time
    fromKeyValue.setModificationTime(modificationTime);

    // Add from_key and to_key details into cache.
    if (isDir) {
      Table<String, OmDirectoryInfo> dirTable =
              omMetadataManager.getDirectoryTable();
      dirTable.addCacheEntry(new CacheKey<>(dbFromKeyname),
              new CacheValue<>(Optional.absent(), trxnLogIndex));

      dirTable.addCacheEntry(new CacheKey<>(dbToKeyname),
              new CacheValue<>(
                      Optional.of(OMFileRequest.
                              getDirectoryInfo(fromKeyValue)),
                      trxnLogIndex));
    } else {
      Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable();
      keyTable.addCacheEntry(new CacheKey<>(dbFromKeyname),
              new CacheValue<>(Optional.absent(), trxnLogIndex));

      keyTable.addCacheEntry(new CacheKey<>(dbToKeyname),
              new CacheValue<>(Optional.of(fromKeyValue), trxnLogIndex));
    }
    OMClientResponse omClientResponse = new OMKeyRenameResponse(omResponse
        .setRenameKeyResponse(RenameKeyResponse.newBuilder()).build(),
            dbFromKeyname, dbToKeyname, fromKeyValue, isDir);
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
