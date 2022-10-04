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
package org.apache.hadoop.ozone.om.request.key.acl;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.ObjectParser;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.acl.OMKeyAclResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;


/**
 * Handles key ACL requests - prefix layout.
 */
public abstract class OMKeyAclRequestWithFSO extends OMKeyAclRequest {

  public OMKeyAclRequestWithFSO(OzoneManagerProtocolProtos.OMRequest omReq,
                                BucketLayout bucketLayout) {
    super(omReq);
    setBucketLayout(bucketLayout);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    OmKeyInfo omKeyInfo = null;

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse = onInit();
    OMClientResponse omClientResponse = null;
    IOException exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    String volume = null;
    String bucket = null;
    String key = null;
    boolean operationResult = false;
    Result result = null;
    try {
      ObjectParser objectParser = new ObjectParser(getPath(),
          OzoneManagerProtocolProtos.OzoneObj.ObjectType.KEY);

      volume = objectParser.getVolume();
      bucket = objectParser.getBucket();
      key = objectParser.getKey();

      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, bucket, key);
      }
      lockAcquired = omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volume, bucket);
      OzoneFileStatus keyStatus = OMFileRequest
          .getOMKeyInfoIfExists(omMetadataManager, volume, bucket, key, 0);
      if (keyStatus == null) {
        throw new OMException("Key not found. Key:" + key, KEY_NOT_FOUND);
      }
      omKeyInfo = keyStatus.getKeyInfo();
      final long volumeId = omMetadataManager.getVolumeId(volume);
      final long bucketId = omMetadataManager.getBucketId(volume, bucket);
      final String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
              omKeyInfo.getParentObjectID(), omKeyInfo.getFileName());
      boolean isDirectory = keyStatus.isDirectory();
      operationResult = apply(omKeyInfo, trxnLogIndex);
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Update the modification time when updating ACLs of Key.
      long modificationTime = omKeyInfo.getModificationTime();
      if (getOmRequest().getAddAclRequest().hasObj() && operationResult) {
        modificationTime =
            getOmRequest().getAddAclRequest().getModificationTime();
      } else if (getOmRequest().getSetAclRequest().hasObj()) {
        modificationTime =
            getOmRequest().getSetAclRequest().getModificationTime();
      } else if (getOmRequest().getRemoveAclRequest().hasObj()
          && operationResult) {
        modificationTime =
            getOmRequest().getRemoveAclRequest().getModificationTime();
      }
      omKeyInfo.setModificationTime(modificationTime);

      // update cache.
      if (isDirectory) {
        Table<String, OmDirectoryInfo> dirTable =
            omMetadataManager.getDirectoryTable();
        dirTable.addCacheEntry(new CacheKey<>(dbKey),
            new CacheValue<>(Optional.of(OMFileRequest.
                getDirectoryInfo(omKeyInfo)), trxnLogIndex));
      } else {
        omMetadataManager.getKeyTable(getBucketLayout())
            .addCacheEntry(new CacheKey<>(dbKey),
                new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));
      }
      omClientResponse = onSuccess(omResponse, omKeyInfo, operationResult,
          isDirectory, volumeId, bucketId);
      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = onFailure(omResponse, ex);
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (lockAcquired) {
        omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volume, bucket);
      }
    }

    OzoneObj obj = getObject();
    Map<String, String> auditMap = obj.toAuditMap();
    onComplete(result, operationResult, exception, trxnLogIndex,
        ozoneManager.getAuditLogger(), auditMap);

    return omClientResponse;
  }

  /**
   * Get the om client response on failure case with lock.
   *
   * @param omResp
   * @param exception
   * @return OMClientResponse
   */
  @Override
  OMClientResponse onFailure(
      OzoneManagerProtocolProtos.OMResponse.Builder omResp,
      IOException exception) {
    return new OMKeyAclResponseWithFSO(
        createErrorOMResponse(omResp, exception), getBucketLayout());
  }

  abstract OMClientResponse onSuccess(
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      OmKeyInfo omKeyInfo, boolean operationResult, boolean isDirectory,
      long volumeId, long bucketId);

}
