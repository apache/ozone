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
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
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

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    RenameKeyRequest renameKeyRequest = getOmRequest().getRenameKeyRequest();
    Preconditions.checkNotNull(renameKeyRequest);

    // Verify key name
    final boolean checkKeyNameEnabled = ozoneManager.getConfiguration()
         .getBoolean(OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_KEY,
                 OMConfigKeys.OZONE_OM_KEYNAME_CHARACTER_CHECK_ENABLED_DEFAULT);
    if(checkKeyNameEnabled){
      OmUtils.validateKeyName(renameKeyRequest.getToKeyName());
    }

    KeyArgs renameKeyArgs = renameKeyRequest.getKeyArgs();

    // Set modification time.
    KeyArgs.Builder newKeyArgs = renameKeyArgs.toBuilder()
            .setModificationTime(Time.now());

    return getOmRequest().toBuilder()
        .setRenameKeyRequest(renameKeyRequest.toBuilder()
            .setKeyArgs(newKeyArgs))
        .setUserInfo(getUserIfNotExists(ozoneManager)).build();

  }


  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

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
    IOException exception = null;
    OmKeyInfo fromKeyValue = null;
    String toKey = null, fromKey = null;
    Result result = null;
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
      toKey = omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName);
      OmKeyInfo toKeyValue = omMetadataManager.getKeyTable().get(toKey);

      if (toKeyValue != null) {
        throw new OMException("Key already exists " + toKeyName,
              OMException.ResultCodes.KEY_ALREADY_EXISTS);
      }

      // fromKeyName should exist
      fromKeyValue = omMetadataManager.getKeyTable().get(fromKey);
      if (fromKeyValue == null) {
          // TODO: Add support for renaming open key
        throw new OMException("Key not found " + fromKey, KEY_NOT_FOUND);
      }

      fromKeyValue.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      fromKeyValue.setKeyName(toKeyName);

      //Set modification time
      fromKeyValue.setModificationTime(keyArgs.getModificationTime());

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

      result = Result.SUCCESS;
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

    auditLog(auditLogger, buildAuditMessage(OMAction.RENAME_KEY, auditMap,
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
              "toKey:{}. Key: {} not found.", volumeName, bucketName,
          fromKeyName, toKeyName, fromKeyName);
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
}
