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

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.key.OMKeySetTimesResponseWithFSO;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handle set times request for bucket for prefix layout.
 */
public class OMKeySetTimesRequestWithFSO extends OMKeySetTimesRequest {

  @Override
  public OzoneManagerProtocolProtos.OMRequest preExecute(
      OzoneManager ozoneManager) throws IOException {
    return super.preExecute(ozoneManager);
  }

  public OMKeySetTimesRequestWithFSO(
      OzoneManagerProtocolProtos.OMRequest omReq, BucketLayout bucketLayout) {
    super(omReq, bucketLayout);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex) {
    OmKeyInfo omKeyInfo = null;

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    Exception exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    String volume = null;
    String bucket = null;
    String key = null;
    boolean operationResult = false;
    Result result = null;
    try {
      volume = getVolumeName();
      bucket = getBucketName();
      key = getKeyName();

      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, bucket, key);
      }
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volume, bucket));
      lockAcquired = getOmLockDetails().isLockAcquired();
      OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
          omMetadataManager, volume, bucket, key, 0,
          ozoneManager.getDefaultReplicationConfig());
      if (keyStatus == null) {
        throw new OMException("Key not found. Key:" + key, KEY_NOT_FOUND);
      }
      omKeyInfo = keyStatus.getKeyInfo();
      // setting Key name back to Ozone Key before updating cache value.
      omKeyInfo.setKeyName(OzoneFSUtils.getFileName(key));
      final long volumeId = omMetadataManager.getVolumeId(volume);
      final long bucketId = omMetadataManager.getBucketId(volume, bucket);
      final String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
          omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());
      boolean isDirectory = keyStatus.isDirectory();
      operationResult = true;
      apply(omKeyInfo);
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // update cache.
      if (isDirectory) {
        Table<String, OmDirectoryInfo> dirTable =
            omMetadataManager.getDirectoryTable();
        dirTable.addCacheEntry(new CacheKey<>(dbKey),
            CacheValue.get(trxnLogIndex,
                OMFileRequest.getDirectoryInfo(omKeyInfo)));
      } else {
        omMetadataManager.getKeyTable(getBucketLayout())
            .addCacheEntry(new CacheKey<>(dbKey),
                CacheValue.get(trxnLogIndex, omKeyInfo));
      }
      omClientResponse = onSuccess(omResponse, omKeyInfo, operationResult,
          isDirectory, volumeId, bucketId);
      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = onFailure(omResponse, exception);
    } finally {
      if (lockAcquired) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volume, bucket));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    Map<String, String> auditMap = new LinkedHashMap<>();
    onComplete(result, exception, ozoneManager.getAuditLogger(), auditMap);

    return omClientResponse;
  }

  @Override
  protected OzoneManagerProtocolProtos.OMResponse.Builder onInit() {
    return OmResponseUtil.getOMResponseBuilder(getOmRequest());
  }

  private OMClientResponse onSuccess(OMResponse.Builder omResponse,
      OmKeyInfo omKeyInfo, boolean operationResult, boolean isDir,
      long volumeId, long bucketId) {
    omResponse.setSuccess(operationResult);
    omResponse.setSetTimesResponse(
        OzoneManagerProtocolProtos.SetTimesResponse.newBuilder());
    return new OMKeySetTimesResponseWithFSO(omResponse.build(), omKeyInfo,
        isDir, getBucketLayout(), volumeId, bucketId);
  }

  @Override
  protected OMClientResponse onFailure(OMResponse.Builder omResponse,
      Exception exception) {
    return new OMKeySetTimesResponseWithFSO(createErrorOMResponse(
        omResponse, exception), getBucketLayout());
  }
}
