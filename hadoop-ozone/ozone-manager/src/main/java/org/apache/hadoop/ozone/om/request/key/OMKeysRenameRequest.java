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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmRenameKeyInfo;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysRenameResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_RENAME;
import static org.apache.hadoop.ozone.OzoneConsts.BUCKET;
import static org.apache.hadoop.ozone.OzoneConsts.RENAMED_KEYS_MAP;
import static org.apache.hadoop.ozone.OzoneConsts.UNRENAMED_KEYS_MAP;
import static org.apache.hadoop.ozone.OzoneConsts.VOLUME;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles rename keys request.
 */
public class OMKeysRenameRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeysRenameRequest.class);

  public OMKeysRenameRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    RenameKeysRequest renameKeys = getOmRequest().getRenameKeysRequest();
    Preconditions.checkNotNull(renameKeys);

    List<RenameKeyRequest> renameKeyList = new ArrayList<>();
    for (RenameKeyRequest renameKey : renameKeys.getRenameKeyRequestList()) {
      // Set modification time in preExecute.
      KeyArgs.Builder newKeyArgs = renameKey.getKeyArgs().toBuilder()
          .setModificationTime(Time.now());
      renameKey.toBuilder().setKeyArgs(newKeyArgs);
      renameKeyList.add(renameKey);
    }
    RenameKeysRequest renameKeysRequest = RenameKeysRequest
        .newBuilder().addAllRenameKeyRequest(renameKeyList).build();
    return getOmRequest().toBuilder().setRenameKeysRequest(renameKeysRequest)
        .setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    RenameKeysRequest renameKeysRequest = getOmRequest().getRenameKeysRequest();
    OMClientResponse omClientResponse = null;
    // fromKeyName -> toKeyName
    List<RenameKeyArgs> unRenamedKeys = new ArrayList<>();

    List<OmRenameKeyInfo> renameKeyInfoList = new ArrayList<>();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyRenames();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();


    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;
    OmKeyInfo fromKeyValue = null;

    Result result = null;
    Map<String, String> auditMap = null;
    RenameKeyRequest renameRequest = null;
    String volumeName = null;
    String bucketName = null;
    String fromKeyName = null;
    String toKeyName = null;
    boolean acquiredLock = false;
    boolean renameStatus = true;
    try {
      for (RenameKeyRequest renameKeyRequest : renameKeysRequest
          .getRenameKeyRequestList()) {
        OzoneManagerProtocolProtos.KeyArgs keyArgs =
            renameKeyRequest.getKeyArgs();
        auditMap = buildAuditMap(volumeName, bucketName, renameKeyInfoList,
            unRenamedKeys);
        keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
        volumeName = keyArgs.getVolumeName();
        bucketName = keyArgs.getBucketName();
        fromKeyName = keyArgs.getKeyName();
        toKeyName = renameKeyRequest.getToKeyName();
        renameRequest = renameKeyRequest;

        RenameKeyArgs renameKeyArgs = RenameKeyArgs.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName)
            .setFromKeyName(fromKeyName).setToKeyName(toKeyName).build();

        if (toKeyName.length() == 0 || fromKeyName.length() == 0) {
          renameStatus = false;
          unRenamedKeys.add(renameKeyArgs);
          LOG.error("Key name is empty fromKeyName {} toKeyName {}",
              fromKeyName, toKeyName);
          continue;
        }

        try {
          // check Acls to see if user has access to perform delete operation
          // on old key and create operation on new key
          checkKeyAcls(ozoneManager, volumeName, bucketName, fromKeyName,
              IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY);
          checkKeyAcls(ozoneManager, volumeName, bucketName, toKeyName,
              IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);
        } catch (Exception ex) {
          renameStatus = false;
          unRenamedKeys.add(renameKeyArgs);
          LOG.error("Acl check failed for fromKeyName {} toKeyName {}",
              fromKeyName, toKeyName, ex);
          continue;
        }

        // Check if toKey exists
        String fromKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
            fromKeyName);
        String toKey =
            omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName);
        OmKeyInfo toKeyValue = omMetadataManager.getKeyTable().get(toKey);

        if (toKeyValue != null) {

          renameStatus = false;
          unRenamedKeys.add(renameKeyArgs);
          LOG.error("Received a request name of new key {} already exists",
              toKeyName);
        }

        // fromKeyName should exist
        fromKeyValue = omMetadataManager.getKeyTable().get(fromKey);
        if (fromKeyValue == null) {
          renameStatus = false;
          unRenamedKeys.add(renameKeyArgs);
          LOG.error("Received a request to rename a Key does not exist {}",
              fromKey);
          continue;
        }

        fromKeyValue.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

        fromKeyValue.setKeyName(toKeyName);

        //Set modification time
        fromKeyValue.setModificationTime(keyArgs.getModificationTime());

        acquiredLock =
            omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
                volumeName, bucketName);
        // Add to cache.
        // fromKey should be deleted, toKey should be added with newly updated
        // omKeyInfo.
        Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable();
        keyTable.addCacheEntry(new CacheKey<>(fromKey),
            new CacheValue<>(Optional.absent(), trxnLogIndex));
        keyTable.addCacheEntry(new CacheKey<>(toKey),
            new CacheValue<>(Optional.of(fromKeyValue), trxnLogIndex));
        renameKeyInfoList
            .add(new OmRenameKeyInfo(fromKeyName, fromKeyValue));

        if (acquiredLock) {
          omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
              bucketName);
          acquiredLock = false;
        }
      }
      acquiredLock =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);
      omClientResponse = new OMKeysRenameResponse(omResponse
          .setRenameKeysResponse(RenameKeysResponse.newBuilder()
              .setStatus(renameStatus).addAllUnRenamedKeys(unRenamedKeys))
          .setStatus(renameStatus ? OK : PARTIAL_RENAME)
          .setSuccess(renameStatus).build(),
          renameKeyInfoList);

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      createErrorOMResponse(omResponse, ex);

      omResponse.setRenameKeysResponse(RenameKeysResponse.newBuilder()
          .setStatus(renameStatus).addAllUnRenamedKeys(unRenamedKeys).build());
      omClientResponse = new OMKeysRenameResponse(omResponse.build());

    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    auditLog(auditLogger, buildAuditMessage(OMAction.RENAME_KEYS, auditMap,
        exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("Rename Keys is successfully completed for auditMap:{}.",
          auditMap.toString());
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumKeyRenameFails();
      LOG.error("Rename keys failed for auditMap:{}.", auditMap.toString());
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyRenameRequest: {}",
          renameRequest);
    }

    return omClientResponse;
  }

  /**
   * Build audit map for RenameKeys request.
   *
   * @param volumeName
   * @param bucketName
   * @param renameKeys
   * @param unRenameKeys
   * @return
   */
  private Map<String, String> buildAuditMap(String volumeName,
                                            String bucketName,
                                            List<OmRenameKeyInfo> renameKeys,
                                            List<RenameKeyArgs> unRenameKeys) {
    Map<String, String> renameKeysMap = new HashMap<>();
    Map<String, String> unRenameKeysMap = new HashMap<>();
    Map<String, String> auditMap = new HashMap<>();

    for (OmRenameKeyInfo keyInfo : renameKeys) {
      renameKeysMap.put(keyInfo.getFromKeyName(),
          keyInfo.getNewKeyInfo().getKeyName());
    }
    for (RenameKeyArgs keyArgs : unRenameKeys) {
      unRenameKeysMap.put(keyArgs.getFromKeyName(), keyArgs.getToKeyName());
    }

    auditMap.put(VOLUME, volumeName);
    auditMap.put(BUCKET, bucketName);
    auditMap.put(RENAMED_KEYS_MAP, renameKeysMap.toString());
    auditMap.put(UNRENAMED_KEYS_MAP, unRenameKeysMap.toString());
    return auditMap;
  }
}
