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

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles purging of keys from OM DB.
 */
public class OMKeyPurgeRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyPurgeRequest.class);

  public OMKeyPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    PurgeKeysRequest purgeKeysRequest = getOmRequest().getPurgeKeysRequest();
    List<DeletedKeys> bucketDeletedKeysList = purgeKeysRequest
        .getDeletedKeysList();
    List<String> keysToBePurgedList = new ArrayList<>();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    boolean success = true;
    IOException exception = null;

    // Filter the keys that have updateID > transactionLogIndex. This is done so
    // that in case this transaction is a replay, we do not purge keys
    // created after the original purge request.
    // PurgeKeys request has keys belonging to same bucket grouped together.
    // We get each bucket lock and check the above condition.
    for (DeletedKeys bucketWithDeleteKeys : bucketDeletedKeysList) {
      boolean acquiredLock = false;
      String volumeName = bucketWithDeleteKeys.getVolumeName();
      String bucketName = bucketWithDeleteKeys.getBucketName();
      ArrayList<String> keysNotPurged = new ArrayList<>();
      Result result = null;
      try {
        acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
            volumeName, bucketName);
        for (String deletedKey : bucketWithDeleteKeys.getKeysList()) {
          RepeatedOmKeyInfo repeatedOmKeyInfo =
              omMetadataManager.getDeletedTable().get(deletedKey);
          // Update cache of trashTable.
          omMetadataManager.getTrashTable().addCacheEntry(
              new CacheKey<>(deletedKey),
              new CacheValue<>(Optional.absent(), trxnLogIndex));
          boolean purgeKey = true;
          if (repeatedOmKeyInfo != null) {
            for (OmKeyInfo omKeyInfo : repeatedOmKeyInfo.getOmKeyInfoList()) {
              // Discard those keys whose updateID is > transactionLogIndex.
              // This could happen when the PurgeRequest is replayed.
              if (isReplay(ozoneManager, omKeyInfo,
                  trxnLogIndex)) {
                purgeKey = false;
                result = Result.REPLAY;
                break;
              }
              // TODO: If a deletedKey has any one OmKeyInfo which was
              //  deleted after the original PurgeRequest (updateID >
              //  trxnLogIndex), we avoid purging that whole key in the
              //  replay request. Instead of discarding the whole key, we can
              //  identify the OmKeyInfo's which have updateID <
              //  trxnLogIndex and purge only those OMKeyInfo's from the
              //  deletedKey in DeletedTable.
            }
            if (purgeKey) {
              keysToBePurgedList.add(deletedKey);
            } else {
              keysNotPurged.add(deletedKey);
            }
          }
        }
      } catch (IOException ex) {
        success = false;
        exception = ex;
        break;
      } finally {
        if (acquiredLock) {
          omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
              bucketName);
        }
      }

      if (result == Result.REPLAY) {
        LOG.debug("Replayed Transaction {}. Request: {}", trxnLogIndex,
            purgeKeysRequest);
        if (!keysNotPurged.isEmpty()) {
          StringBuilder notPurgeList = new StringBuilder();
          for (String key : keysNotPurged) {
            notPurgeList.append(", ").append(key);
          }
          LOG.debug("Following keys from Volume:{}, Bucket:{} will not be" +
              " purged: {}", notPurgeList.toString().substring(2));
        }
      }
    }

    if (success) {
      if (LOG.isDebugEnabled()) {
        if (keysToBePurgedList.isEmpty()) {
          LOG.debug("No keys will be purged as part of KeyPurgeRequest: {}",
              purgeKeysRequest);
        } else {
          LOG.debug("Following keys will be purged as part of " +
              "KeyPurgeRequest: {} - {}", purgeKeysRequest,
              String.join(",", keysToBePurgedList));
        }
      }
      omClientResponse = new OMKeyPurgeResponse(omResponse.build(),
          keysToBePurgedList);
    } else {
      omClientResponse = new OMKeyPurgeResponse(createErrorOMResponse(
          omResponse, exception));
    }

    addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
        omDoubleBufferHelper);
    return omClientResponse;
  }
}
