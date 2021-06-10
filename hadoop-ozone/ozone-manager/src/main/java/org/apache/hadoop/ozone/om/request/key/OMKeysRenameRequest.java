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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.OmRenameKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysRenameResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysMap;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_RENAME;
import static org.apache.hadoop.ozone.OzoneConsts.RENAMED_KEYS_MAP;
import static org.apache.hadoop.ozone.OzoneConsts.UNRENAMED_KEYS_MAP;
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
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    RenameKeysRequest renameKeysRequest = getOmRequest().getRenameKeysRequest();
    RenameKeysArgs renameKeysArgs = renameKeysRequest.getRenameKeysArgs();
    String volumeName = renameKeysArgs.getVolumeName();
    String bucketName = renameKeysArgs.getBucketName();
    OMClientResponse omClientResponse = null;

    List<RenameKeysMap> unRenamedKeys = new ArrayList<>();

    // fromKeyName -> toKeyName
    Map<String, String> renamedKeys = new HashMap<>();

    Map<String, OmKeyInfo> fromKeyAndToKeyInfo = new HashMap<>();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyRenames();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;
    OmKeyInfo fromKeyValue = null;
    Result result = null;
    Map<String, String> auditMap = new LinkedHashMap<>();
    String fromKeyName = null;
    String toKeyName = null;
    boolean acquiredLock = false;
    boolean renameStatus = true;

    try {
      ResolvedBucket bucket = ozoneManager.resolveBucketLink(
          Pair.of(volumeName, bucketName), this);
      bucket.audit(auditMap);
      volumeName = bucket.realVolume();
      bucketName = bucket.realBucket();
      acquiredLock =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      String volumeOwner = getVolumeOwner(omMetadataManager, volumeName);
      for (RenameKeysMap renameKey : renameKeysArgs.getRenameKeysMapList()) {

        fromKeyName = renameKey.getFromKeyName();
        toKeyName = renameKey.getToKeyName();
        RenameKeysMap.Builder unRenameKey = RenameKeysMap.newBuilder();

        if (toKeyName.length() == 0 || fromKeyName.length() == 0) {
          renameStatus = false;
          unRenamedKeys.add(
              unRenameKey.setFromKeyName(fromKeyName).setToKeyName(toKeyName)
                  .build());
          LOG.error("Key name is empty fromKeyName {} toKeyName {}",
              fromKeyName, toKeyName);
          continue;
        }

        try {
          // check Acls to see if user has access to perform delete operation
          // on old key and create operation on new key
          checkKeyAcls(ozoneManager, volumeName, bucketName, fromKeyName,
              IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY,
              volumeOwner);
          checkKeyAcls(ozoneManager, volumeName, bucketName, toKeyName,
              IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY,
              volumeOwner);
        } catch (Exception ex) {
          renameStatus = false;
          unRenamedKeys.add(
              unRenameKey.setFromKeyName(fromKeyName).setToKeyName(toKeyName)
                  .build());
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
          unRenamedKeys.add(
              unRenameKey.setFromKeyName(fromKeyName).setToKeyName(toKeyName)
                  .build());
          LOG.error("Received a request name of new key {} already exists",
              toKeyName);
        }

        // fromKeyName should exist
        fromKeyValue = omMetadataManager.getKeyTable().get(fromKey);
        if (fromKeyValue == null) {
          renameStatus = false;
          unRenamedKeys.add(
              unRenameKey.setFromKeyName(fromKeyName).setToKeyName(toKeyName)
                  .build());
          LOG.error("Received a request to rename a Key does not exist {}",
              fromKey);
          continue;
        }

        fromKeyValue.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

        fromKeyValue.setKeyName(toKeyName);

        //Set modification time
        fromKeyValue.setModificationTime(Time.now());

        // Add to cache.
        // fromKey should be deleted, toKey should be added with newly updated
        // omKeyInfo.
        Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable();
        keyTable.addCacheEntry(new CacheKey<>(fromKey),
            new CacheValue<>(Optional.absent(), trxnLogIndex));
        keyTable.addCacheEntry(new CacheKey<>(toKey),
            new CacheValue<>(Optional.of(fromKeyValue), trxnLogIndex));
        renamedKeys.put(fromKeyName, toKeyName);
        fromKeyAndToKeyInfo.put(fromKeyName, fromKeyValue);
      }

      OmRenameKeys newOmRenameKeys =
          new OmRenameKeys(volumeName, bucketName, null, fromKeyAndToKeyInfo);
      omClientResponse = new OMKeysRenameResponse(omResponse
          .setRenameKeysResponse(RenameKeysResponse.newBuilder()
              .setStatus(renameStatus)
              .addAllUnRenamedKeys(unRenamedKeys))
          .setStatus(renameStatus ? OK : PARTIAL_RENAME)
          .setSuccess(renameStatus).build(),
          newOmRenameKeys);

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

    auditMap = buildAuditMap(auditMap, renamedKeys, unRenamedKeys);
    auditLog(auditLogger, buildAuditMessage(OMAction.RENAME_KEYS, auditMap,
        exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      LOG.debug("Rename Keys is successfully completed for auditMap:{}.",
          auditMap);
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumKeyRenameFails();
      LOG.error("Rename keys failed for auditMap:{}.", auditMap);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeysRenameRequest: {}",
          renameKeysRequest);
    }

    return omClientResponse;
  }

  /**
   * Build audit map for RenameKeys request.
   *
   * @param auditMap
   * @param renamedKeys
   * @param unRenameKeys
   * @return
   */
  private Map<String, String> buildAuditMap(Map<String, String> auditMap,
                                            Map<String, String> renamedKeys,
                                            List<RenameKeysMap> unRenameKeys) {
    Map<String, String> unRenameKeysMap = new HashMap<>();
    for (RenameKeysMap renameKeysMap : unRenameKeys) {
      unRenameKeysMap.put(renameKeysMap.getFromKeyName(),
          renameKeysMap.getToKeyName());
    }
    auditMap.put(RENAMED_KEYS_MAP, renamedKeys.toString());
    auditMap.put(UNRENAMED_KEYS_MAP, unRenameKeysMap.toString());
    return auditMap;
  }
}
