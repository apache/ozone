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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMReplayException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.key.OMTrashRecoverResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RecoverTrashRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RecoverTrashResponse;


import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.RECOVERED_KEY_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .RECOVERED_KEY_NOT_FOUND;

/**
 * Handles RecoverTrash request.
 */
public class OMTrashRecoverRequest extends OMKeyRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMTrashRecoverRequest.class);

  public OMTrashRecoverRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) {
    RecoverTrashRequest recoverTrashRequest = getOmRequest()
        .getRecoverTrashRequest();
    Preconditions.checkNotNull(recoverTrashRequest);

    long modificationTime = Time.now();

    return getOmRequest().toBuilder()
        .setRecoverTrashRequest(
            recoverTrashRequest.toBuilder()
                .setModificationTime(modificationTime))
        .setUserInfo(getUserInfo()).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    RecoverTrashRequest recoverTrashRequest = getOmRequest()
        .getRecoverTrashRequest();
    Preconditions.checkNotNull(recoverTrashRequest);

    String volumeName = recoverTrashRequest.getVolumeName();
    String bucketName = recoverTrashRequest.getBucketName();
    String keyName = recoverTrashRequest.getKeyName();
    String destinationBucket = recoverTrashRequest.getDestinationBucket();

    /** TODO: HDDS-2818. New Metrics for Trash Key Recover and Fails.
     *  OMMetrics omMetrics = ozoneManager.getMetrics();
     */

    OMResponse.Builder omResponse = OmResponseUtil
        .getOMResponseBuilder(getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquireLock = false;
    OMClientResponse omClientResponse = null;
    //TODO: HDDS-2818. New Metrics for Trash Key Recover and Fails.
    Result result = null;
    try {
      // Check acl for the destination bucket.
      checkBucketAcls(ozoneManager, volumeName, destinationBucket, keyName,
          IAccessAuthorizer.ACLType.WRITE);

      acquireLock = omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, destinationBucket);

      // Validate original vol/buc, destinationBucket exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      validateBucketAndVolume(omMetadataManager, volumeName, destinationBucket);

      // TODO: HDDS-2425. recovering trash in non-existing bucket.

      String trashTableKey = omMetadataManager
          .getOzoneKey(volumeName, bucketName, keyName);
      RepeatedOmKeyInfo trashRepeatedKeyInfo =
          omMetadataManager.getTrashTable().get(trashTableKey);
      OmKeyInfo trashKeyInfo = null;
      if (trashRepeatedKeyInfo != null) {
        int lastKeyIndex = trashRepeatedKeyInfo.getOmKeyInfoList().size() - 1;
        trashKeyInfo = trashRepeatedKeyInfo
            .getOmKeyInfoList().get(lastKeyIndex);
        // update modificationTime after recovering.
        trashKeyInfo.setModificationTime(
            recoverTrashRequest.getModificationTime());

        // Check this transaction is replayed or not.
        if (isReplay(ozoneManager, trashKeyInfo, transactionLogIndex)) {
          throw new OMReplayException();
        }

        // Set the updateID to current transactionLogIndex.
        trashKeyInfo.setUpdateID(transactionLogIndex,
            ozoneManager.isRatisEnabled());

        // Update cache of keyTable,
        if (omMetadataManager.getKeyTable().get(trashTableKey) != null) {
          throw new OMException(
              "The bucket has key of same name as recovered key",
              RECOVERED_KEY_ALREADY_EXISTS);
        } else {
          omMetadataManager.getKeyTable().addCacheEntry(
              new CacheKey<>(trashTableKey),
              new CacheValue<>(Optional.of(trashKeyInfo), transactionLogIndex));
        }

        // Update cache of trashTable.
        trashRepeatedKeyInfo.getOmKeyInfoList().remove(lastKeyIndex);
        omMetadataManager.getTrashTable().addCacheEntry(
            new CacheKey<>(trashTableKey),
            new CacheValue<>(Optional.of(trashRepeatedKeyInfo),
                transactionLogIndex));

        // Update cache of deletedTable.
        omMetadataManager.getDeletedTable().addCacheEntry(
            new CacheKey<>(trashTableKey),
            new CacheValue<>(Optional.of(trashRepeatedKeyInfo),
                transactionLogIndex));

        omResponse.setSuccess(true);

      } else {
        /* key we want to recover not exist */
        throw new OMException("Recovered key is not in trash table",
            RECOVERED_KEY_NOT_FOUND);
      }

      result = Result.SUCCESS;
      omClientResponse = new OMTrashRecoverResponse(trashRepeatedKeyInfo,
          trashKeyInfo,
          omResponse.setRecoverTrashResponse(
              RecoverTrashResponse.newBuilder().setResponse(true))
              .build());

    } catch (OMException | OMReplayException ex) {
      LOG.error("Fail for recovering trash.", ex);
      if (ex instanceof OMReplayException) {
        omClientResponse = new OMTrashRecoverResponse(null, null,
            createReplayOMResponse(omResponse));
        result = Result.REPLAY;
      } else {
        omClientResponse = new OMTrashRecoverResponse(null, null,
            createErrorOMResponse(omResponse, ex));
        result = Result.FAILURE;
      }
    } catch (IOException ex) {
      LOG.error("Fail for recovering trash.", ex);
      omClientResponse = new OMTrashRecoverResponse(null, null,
          createErrorOMResponse(omResponse, ex));
      result = Result.FAILURE;
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquireLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            destinationBucket);
      }
    }

    return omClientResponse;
  }

}
