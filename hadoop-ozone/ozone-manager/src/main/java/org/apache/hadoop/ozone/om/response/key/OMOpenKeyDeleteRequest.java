/*
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
package org.apache.hadoop.ozone.om.response.key;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeysPerBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

public class OMOpenKeyDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMOpenKeyDeleteRequest.class);

  public OMOpenKeyDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    OzoneManagerProtocolProtos.DeleteOpenKeysRequest deleteOpenKeysRequest =
            getOmRequest().getDeleteOpenKeysRequest();

    List<OpenKeysPerBucket> expiredOpenKeys =
            deleteOpenKeysRequest.getOpenKeysToDeleteList();

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(getOmRequest());

    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;
    List<OmKeyInfo> deletedKeys = new ArrayList<>();

    try {
      for (OpenKeysPerBucket openKeysPerBucket: expiredOpenKeys) {
        // For each bucket where keys will be deleted from,
        // get its bucket lock and update the cache accordingly.
        deletedKeys.addAll(updateCache(ozoneManager, trxnLogIndex,
            openKeysPerBucket));
      }

      omClientResponse = new OMOpenKeyDeleteResponse(omResponse.build(),
          deletedKeys, ozoneManager.isRatisEnabled());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse =
          new OMKeyDeleteResponse(createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
              omDoubleBufferHelper);
    }

    writeMetrics(ozoneManager.getMetrics(), deletedKeys, result);
    writeAuditLog(ozoneManager.getAuditLogger(), deletedKeys, exception);

    return omClientResponse;
  }

  private void writeMetrics(OMMetrics omMetrics,
      List<OmKeyInfo> deletedKeys, Result result) {

    // TODO: Add Metrics
//    switch (result) {
//    case SUCCESS:
//      omMetrics.incNumOpenKeyDeletes(deletedKeys.size());
//      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
//              bucketName, keyName);
//      break;
//    case FAILURE:
//      omMetrics.incNumOpenKeyCleanupFails();
//      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key:{}.",
//              volumeName, bucketName, keyName, exception);
//      break;
//    default:
//      LOG.error("Unrecognized Result for OMOpenKeyDeleteRequest: {}",
//              openKeyDeleteRequest);
//    }
  }

  private void writeAuditLog(AuditLogger auditLogger,
      List<OmKeyInfo> deletedKeys, Exception exception) {
    // TODO: Audit logging to track operations.
//    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
//
//    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
//
//    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_OPEN_KEY, auditMap,
//            exception, userInfo));
  }

  private List<OmKeyInfo> updateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OpenKeysPerBucket expiredKeysInBucket)
      throws IOException {

    List<OmKeyInfo> deletedKeys = new ArrayList<>();

    boolean acquiredLock = false;
    String volumeName = expiredKeysInBucket.getVolumeName();
    String bucketName = expiredKeysInBucket.getBucketName();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);

      for (OpenKey key: expiredKeysInBucket.getKeysList()) {
        String fullKeyName = omMetadataManager.getOpenKey(volumeName,
                bucketName, key.getName(), key.getId());

        // If an open key is no longer present in the table, it was committed
        // and should not be deleted.
        OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable().get(fullKeyName);
        if (omKeyInfo != null) {
          // Set the UpdateID to current transactionLogIndex
          omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());
          deletedKeys.add(omKeyInfo);

          // Update table cache.
          omMetadataManager.getOpenKeyTable().addCacheEntry(
                  new CacheKey<>(fullKeyName),
                  new CacheValue<>(Optional.absent(), trxnLogIndex));

          // No need to add cache entries to delete table. As delete table will
          // be used by DeleteKeyService only, not used for any client response
          // validation, so we don't need to add to cache.
        }
      }
    } finally {
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
                bucketName);
      }
    }

    return deletedKeys;
  }
}
