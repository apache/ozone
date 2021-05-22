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

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.ObjectParser;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.acl.OMKeyAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Base class for Bucket acl request.
 */
public abstract class OMKeyAclRequest extends OMClientRequest {


  public OMKeyAclRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    OmKeyInfo omKeyInfo = null;

    OMResponse.Builder omResponse = onInit();
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
          ObjectType.KEY);

      volume = objectParser.getVolume();
      bucket = objectParser.getBucket();
      key = objectParser.getKey();

      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, bucket, key);
      }
      lockAcquired =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume,
              bucket);

      String dbKey = omMetadataManager.getOzoneKey(volume, bucket, key);
      omKeyInfo = omMetadataManager.getKeyTable().get(dbKey);

      if (omKeyInfo == null) {
        throw new OMException(OMException.ResultCodes.KEY_NOT_FOUND);
      }

      operationResult = apply(omKeyInfo, trxnLogIndex);
      omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

      // Update the modification time when updating ACLs of Key.
      long modificationTime = omKeyInfo.getModificationTime();
      if (getOmRequest().getAddAclRequest().hasObj() && operationResult) {
        modificationTime = getOmRequest().getAddAclRequest()
            .getModificationTime();
      } else if (getOmRequest().getSetAclRequest().hasObj() && operationResult){
        modificationTime = getOmRequest().getSetAclRequest()
            .getModificationTime();
      } else if (getOmRequest().getRemoveAclRequest().hasObj()
          && operationResult) {
        modificationTime = getOmRequest().getRemoveAclRequest()
            .getModificationTime();
      }
      omKeyInfo.setModificationTime(modificationTime);

      // update cache.
      omMetadataManager.getKeyTable().addCacheEntry(
          new CacheKey<>(dbKey),
          new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));

      omClientResponse = onSuccess(omResponse, omKeyInfo, operationResult);
      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = onFailure(omResponse, ex);
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (lockAcquired) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume,
            bucket);
      }
    }

    OzoneObj obj = getObject();
    Map<String, String> auditMap = obj.toAuditMap();
    onComplete(result, operationResult, exception, trxnLogIndex,
        ozoneManager.getAuditLogger(), auditMap);

    return omClientResponse;
  }

  /**
   * Get the path name from the request.
   * @return path name
   */
  abstract String getPath();

  /**
   * Get Key object Info from the request.
   * @return OzoneObjInfo
   */
  abstract OzoneObj getObject();

  // TODO: Finer grain metrics can be moved to these callbacks. They can also
  // be abstracted into separate interfaces in future.
  /**
   * Get the initial om response builder with lock.
   * @return om response builder.
   */
  abstract OMResponse.Builder onInit();

  /**
   * Get the om client response on success case with lock.
   * @param omResponse
   * @param omKeyInfo
   * @param operationResult
   * @return OMClientResponse
   */
  abstract OMClientResponse onSuccess(
      OMResponse.Builder omResponse, OmKeyInfo omKeyInfo,
      boolean operationResult);

  /**
   * Get the om client response on failure case with lock.
   * @param omResponse
   * @param exception
   * @return OMClientResponse
   */
  OMClientResponse onFailure(OMResponse.Builder omResponse,
      IOException exception) {
    return new OMKeyAclResponse(createErrorOMResponse(omResponse, exception));
  }

  /**
   * Completion hook for final processing before return without lock.
   * Usually used for logging without lock and metric update.
   * @param operationResult
   * @param exception
   */
  abstract void onComplete(Result result, boolean operationResult,
      IOException exception, long trxnLogIndex, AuditLogger auditLogger,
      Map<String, String> auditMap);

  /**
   * Apply the acl operation, if successfully completed returns true,
   * else false.
   * @param omKeyInfo
   * @param trxnLogIndex
   */
  abstract boolean apply(OmKeyInfo omKeyInfo, long trxnLogIndex);
}

