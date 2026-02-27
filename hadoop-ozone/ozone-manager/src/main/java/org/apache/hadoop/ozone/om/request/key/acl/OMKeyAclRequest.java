/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.key.acl;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.ObjectParser;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.acl.OMKeyAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Bucket acl request.
 */
public abstract class OMKeyAclRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory
      .getLogger(OMKeyAclRequest.class);

  private BucketLayout bucketLayout = BucketLayout.DEFAULT;

  public OMKeyAclRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    OmKeyInfo omKeyInfo = null;

    OMResponse.Builder omResponse = onInit();
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
      ObjectParser objectParser = new ObjectParser(getPath(),
          ObjectType.KEY);
      ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
          Pair.of(objectParser.getVolume(), objectParser.getBucket()));
      volume = resolvedBucket.realVolume();
      bucket = resolvedBucket.realBucket();
      key = objectParser.getKey();

      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, bucket, key);
      }
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume,
              bucket));
      lockAcquired = getOmLockDetails().isLockAcquired();

      String dbKey = omMetadataManager.getOzoneKey(volume, bucket, key);
      omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .get(dbKey);

      if (omKeyInfo == null) {
        throw new OMException(OMException.ResultCodes.KEY_NOT_FOUND);
      }

      OmKeyInfo.Builder builder = omKeyInfo.toBuilder();
      operationResult = apply(builder, trxnLogIndex);

      // Update the modification time when updating ACLs of Key.
      long modificationTime = omKeyInfo.getModificationTime();
      if (getOmRequest().getAddAclRequest().hasObj() && operationResult) {
        modificationTime = getOmRequest().getAddAclRequest()
            .getModificationTime();
      } else if (getOmRequest().getSetAclRequest().hasObj()
          && operationResult) {
        modificationTime = getOmRequest().getSetAclRequest()
            .getModificationTime();
      } else if (getOmRequest().getRemoveAclRequest().hasObj()
          && operationResult) {
        modificationTime = getOmRequest().getRemoveAclRequest()
            .getModificationTime();
      }

      omKeyInfo = builder
          .setModificationTime(modificationTime)
          .setUpdateID(trxnLogIndex)
          .build();

      // update cache.
      omMetadataManager.getKeyTable(getBucketLayout())
          .addCacheEntry(new CacheKey<>(dbKey),
              CacheValue.get(trxnLogIndex, omKeyInfo));

      omClientResponse = onSuccess(omResponse, omKeyInfo, operationResult);
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

  public void initializeBucketLayout(OzoneManager ozoneManager) {
    OmBucketInfo buckInfo;
    try {
      ObjectParser objectParser = new ObjectParser(getPath(),
          OzoneManagerProtocolProtos.OzoneObj.ObjectType.KEY);

      String volume = objectParser.getVolume();
      String bucket = objectParser.getBucket();

      String buckKey =
          ozoneManager.getMetadataManager().getBucketKey(volume, bucket);

      try {
        buckInfo =
            ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
        if (buckInfo == null) {
          LOG.error("Bucket not found: {}/{} ", volume, bucket);
          // defaulting to BucketLayout.DEFAULT
          return;
        }
        bucketLayout = buckInfo.getBucketLayout();
      } catch (IOException e) {
        LOG.error("Failed to get bucket for the key: " + buckKey, e);
      }
    } catch (OMException ome) {
      LOG.error("Invalid Path: " + getPath(), ome);
      // Handle exception
      // defaulting to BucketLayout.DEFAULT
      return;
    }
  }

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
      Exception exception) {
    return new OMKeyAclResponse(createErrorOMResponse(omResponse, exception),
        getBucketLayout());
  }

  /**
   * Completion hook for final processing before return without lock.
   * Usually used for logging without lock and metric update.
   * @param operationResult
   * @param exception
   */
  abstract void onComplete(Result result, boolean operationResult,
      Exception exception, long trxnLogIndex, AuditLogger auditLogger,
      Map<String, String> auditMap);

  /**
   * Apply the acl operation, if successfully completed returns true,
   * else false.
   * @param builder
   * @param trxnLogIndex
   */
  abstract boolean apply(OmKeyInfo.Builder builder, long trxnLogIndex);

  public void setBucketLayout(BucketLayout bucketLayout) {
    this.bucketLayout = bucketLayout;
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }
}

