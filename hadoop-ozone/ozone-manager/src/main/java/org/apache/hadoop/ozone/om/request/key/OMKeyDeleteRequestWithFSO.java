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
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles DeleteKey request - prefix layout.
 */
public class OMKeyDeleteRequestWithFSO extends OMKeyDeleteRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyDeleteRequestWithFSO.class);

  public OMKeyDeleteRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    DeleteKeyRequest deleteKeyRequest = getOmRequest().getDeleteKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        deleteKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    boolean recursive = keyArgs.getRecursive();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    Result result = null;
    OmBucketInfo omBucketInfo = null;
    try {
      keyArgs = resolveBucketLink(ozoneManager, keyArgs, auditMap);
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();

      checkACLs(ozoneManager, volumeName, bucketName, keyName,
          IAccessAuthorizer.ACLType.DELETE);

      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);

      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      OzoneFileStatus keyStatus =
              OMFileRequest.getOMKeyInfoIfExists(omMetadataManager, volumeName,
                      bucketName, keyName, 0);

      if (keyStatus == null) {
        throw new OMException("Key not found. Key:" + keyName, KEY_NOT_FOUND);
      }

      OmKeyInfo omKeyInfo = keyStatus.getKeyInfo();
      // New key format for the fileTable & dirTable.
      // For example, the user given key path is '/a/b/c/d/e/file1', then in DB
      // keyName field stores only the leaf node name, which is 'file1'.
      String fileName = OzoneFSUtils.getFileName(keyName);
      omKeyInfo.setKeyName(fileName);

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      String ozonePathKey = omMetadataManager.getOzonePathKey(
              omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());

      if (keyStatus.isDirectory()) {
        // Check if there are any sub path exists under the user requested path
        if (!recursive && OMFileRequest.hasChildren(omKeyInfo,
                omMetadataManager)) {
          throw new OMException("Directory is not empty. Key:" + keyName,
                  DIRECTORY_NOT_EMPTY);
        }

        // Update dir cache.
        omMetadataManager.getDirectoryTable().addCacheEntry(
                new CacheKey<>(ozonePathKey),
                new CacheValue<>(Optional.absent(), trxnLogIndex));
      } else {
        // Update table cache.
        omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
                new CacheKey<>(ozonePathKey),
                new CacheValue<>(Optional.absent(), trxnLogIndex));
      }

      omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);

      // TODO: HDDS-4565: consider all the sub-paths if the path is a dir.
      long quotaReleased = sumBlockLengths(omKeyInfo);
      omBucketInfo.incrUsedBytes(-quotaReleased);
      omBucketInfo.incrUsedNamespace(-1L);

      // No need to add cache entries to delete table. As delete table will
      // be used by DeleteKeyService only, not used for any client response
      // validation, so we don't need to add to cache.
      // TODO: Revisit if we need it later.

      omClientResponse = new OMKeyDeleteResponseWithFSO(omResponse
          .setDeleteKeyResponse(DeleteKeyResponse.newBuilder()).build(),
          keyName, omKeyInfo, ozoneManager.isRatisEnabled(),
          omBucketInfo.copyObject(), keyStatus.isDirectory());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyDeleteResponseWithFSO(
          createErrorOMResponse(omResponse, exception), getBucketLayout());
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
            omDoubleBufferHelper);
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_KEY, auditMap,
        exception, userInfo));


    switch (result) {
    case SUCCESS:
      omMetrics.decNumKeys();
      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key:{}.",
          volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyDeleteRequest: {}",
          deleteKeyRequest);
    }

    return omClientResponse;
  }
}
