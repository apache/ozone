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
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysInBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
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
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    PurgeKeysRequest purgeKeysRequest = getOmRequest().getPurgeKeysRequest();
    List<DeleteKeysInBucket> bucketDeleteKeysList = purgeKeysRequest
        .getDeleteKeysInBucketsList();
    List<String> keysToBePurgedList = new ArrayList<>();

    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setCmdType(Type.PurgeKeys)
        .setPurgeKeysResponse(PurgeKeysResponse.newBuilder().build())
        .setStatus(Status.OK)
        .setSuccess(true);
    OMClientResponse omClientResponse = null;
    boolean success = true;
    IOException exception = null;

    // Filter the keys that objectID > transactionLogIndex. This is done so
    // that in case this transaction is a replay, we do not purge keys
    // created after the original purge request.
    // PurgeKeys request has keys belonging to same bucket grouped together.
    // We get each bucket lock and check the above condition.
    for (DeleteKeysInBucket bucketWithDeleteKeys : bucketDeleteKeysList) {
      boolean acquiredLock = false;
      String volumeName = bucketWithDeleteKeys.getVolumeName();
      String bucketName = bucketWithDeleteKeys.getBucketName();
      try {
        acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
            volumeName, bucketName);
        for (String deletedKey : bucketWithDeleteKeys.getKeysList()) {
          RepeatedOmKeyInfo repeatedOmKeyInfo =
              omMetadataManager.getDeletedTable().get(deletedKey);
          boolean purgeKey = true;
          if (repeatedOmKeyInfo != null) {
            for (OmKeyInfo omKeyInfo : repeatedOmKeyInfo.getOmKeyInfoList()) {
              if (transactionLogIndex < omKeyInfo.getUpdateID()) {
                purgeKey = false;
                break;
              }
            }
            if (purgeKey) {
              keysToBePurgedList.add(deletedKey);
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
    }

    if (success) {
      omClientResponse = new OMKeyPurgeResponse(omResponse.build(),
          keysToBePurgedList);
    } else {
      omClientResponse = new OMKeyPurgeResponse(createErrorOMResponse(
          omResponse, exception));
    }

    omClientResponse.setFlushFuture(ozoneManagerDoubleBufferHelper.add(
        omClientResponse, transactionLogIndex));
    return omClientResponse;
  }
}
