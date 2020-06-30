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
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.*;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles rename key request.
 */
public class OMKeysRenameRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeysRenameRequest.class);

  public OMKeysRenameRequest(OMRequest omRequest) {
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

    RenameKeysRequest renameKeysRequest = getOmRequest().getRenameKeysRequest();
    Preconditions.checkNotNull(renameKeysRequest);

    return getOmRequest().toBuilder()
        .setRenameKeysRequest(renameKeysRequest.toBuilder())
        .setUserInfo(getUserInfo()).build();

  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    RenameKeysRequest renameKeysRequest = getOmRequest().getRenameKeysRequest();
    OMClientResponse omClientResponse = null;
    Set<String> unRenamedKeys = new HashSet<>();
    Set<String> renamedKeys = new HashSet<>();

    for (RenameKeyRequest renameKeyRequest : renameKeysRequest
        .getRenameKeyRequestList()) {
      OzoneManagerProtocolProtos.KeyArgs renameKeyArgs =
          renameKeyRequest.getKeyArgs();
      unRenamedKeys.add(renameKeyArgs.getKeyName());
    }

    for (RenameKeyRequest renameKeyRequest : renameKeysRequest
        .getRenameKeyRequestList()) {
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

        // Check if toKey exists
        fromKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
            fromKeyName);
        toKey =
            omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName);
        OmKeyInfo toKeyValue = omMetadataManager.getKeyTable().get(toKey);

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
            //     Replay Trxn 1 : Creates Key1 again it does not exist in DB
            //     Replay Trxn 2 : Key2 is not created as it exists in DB and
            //                     the request would be deemed a replay. But
            //                     Key1 is still in the DB and needs to be
            //                     deleted.
            fromKeyValue = omMetadataManager.getKeyTable().get(fromKey);
            if (fromKeyValue != null) {
              // Check if this replay transaction was after the fromKey was
              // created. If so, we have to delete the fromKey.
              if (ozoneManager.isRatisEnabled() &&
                  trxnLogIndex > fromKeyValue.getUpdateID()) {
                // Add to cache. Only fromKey should be deleted. ToKey already
                // exists in DB as this transaction is a replay.
                result = Result.DELETE_FROM_KEY_ONLY;
                Table<String, OmKeyInfo> keyTable = omMetadataManager
                    .getKeyTable();
                keyTable.addCacheEntry(new CacheKey<>(fromKey),
                    new CacheValue<>(Optional.absent(), trxnLogIndex));

                omClientResponse = new OMKeyRenameResponse(omResponse
                    .setRenameKeyResponse(RenameKeyResponse.newBuilder())
                    .build(), fromKeyName, fromKeyValue);
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
            // This transaction is not a replay. toKeyName should not exist
            throw new OMException("Key already exists " + toKeyName,
                OMException.ResultCodes.KEY_ALREADY_EXISTS);
          }
        } else {

          // This transaction is not a replay.

          // fromKeyName should exist
          fromKeyValue = omMetadataManager.getKeyTable().get(fromKey);
          if (fromKeyValue == null) {
            // TODO: Add support for renaming open key
            throw new OMException("Key not found " + fromKey, KEY_NOT_FOUND);
          }

          fromKeyValue.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

          fromKeyValue.setKeyName(toKeyName);
          //Set modification time
          fromKeyValue.setModificationTime(renameKeyArgs.getModificationTime());

          // Add to cache.
          // fromKey should be deleted, toKey should be added with newly updated
          // omKeyInfo.
          Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable();

          keyTable.addCacheEntry(new CacheKey<>(fromKey),
              new CacheValue<>(Optional.absent(), trxnLogIndex));

          keyTable.addCacheEntry(new CacheKey<>(toKey),
              new CacheValue<>(Optional.of(fromKeyValue), trxnLogIndex));

          omClientResponse = new OMKeyRenameResponse(omResponse
              .setRenameKeyResponse(RenameKeyResponse.newBuilder()).build(),
              fromKeyName, toKeyName, fromKeyValue);

          renamedKeys.add(fromKeyName);
          unRenamedKeys.remove(fromKeyName);
          result = Result.SUCCESS;
        }
      } catch (IOException ex) {
        result = Result.FAILURE;
        exception = ex;
        String deleteMessage = String.format(
            "The keys that has been renamed form Batch: {%s}.",
            StringUtils.join(renamedKeys, ","));
        String unDeleteMessage = String.format(
            "The keys that hasn't been renamed form Batch: {%s}.",
            StringUtils.join(unRenamedKeys, ","));
        omClientResponse = new OMKeyRenameResponse(
            createErrorOMResponse(omResponse, exception));
//        omClientResponse = new OMKeyRenameResponse(
//            createOperationKeysErrorOMResponse(omResponse, exception,
//                unRenamedKeys));
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
        LOG.debug("Rename Key is successfully completed for volume:{} bucket:{}"
                + " fromKey:{} toKey:{}. ", volumeName, bucketName, fromKeyName,
            toKeyName);
        break;
      case DELETE_FROM_KEY_ONLY:
        LOG.debug("Replayed transaction {}: {}. Renamed Key {} already exists. "
                + "Deleting old key {}.", trxnLogIndex, renameKeyRequest, toKey,
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
}
