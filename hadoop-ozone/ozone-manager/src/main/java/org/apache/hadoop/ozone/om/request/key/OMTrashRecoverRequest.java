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

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.response.key.OMTrashRecoverResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

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

    return getOmRequest().toBuilder().build();
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

    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setCmdType(Type.RecoverTrash).setStatus(Status.OK)
        .setSuccess(true);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquireLock = false;
    OMClientResponse omClientResponse = null;
    try {

      // check Acl
      checkKeyAcls(ozoneManager, volumeName, destinationBucket, keyName,
          IAccessAuthorizer.ACLType.WRITE, OzoneObj.ResourceType.KEY);

      acquireLock = omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, destinationBucket);

      // Validate.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      validateBucketAndVolume(omMetadataManager, volumeName, destinationBucket);


      /** TODO: HDDS-2425. HDDS-2426.
       *  Update cache.
       *    omMetadataManager.getKeyTable().addCacheEntry(
       *    new CacheKey<>(),
       *    new CacheValue<>()
       *    );
       *
       *  Execute recovering trash in non-existing bucket.
       *  Execute recovering trash in existing bucket.
       *    omClientResponse = new OMTrashRecoverResponse(omKeyInfo,
       *    omResponse.setRecoverTrashResponse(
       *    RecoverTrashResponse.newBuilder())
       *    .build());
       */
      omClientResponse = null;

    } catch (IOException ex) {
      LOG.error("Fail for recovering trash.", ex);
      omClientResponse = new OMTrashRecoverResponse(null,
          createErrorOMResponse(omResponse, ex));
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
