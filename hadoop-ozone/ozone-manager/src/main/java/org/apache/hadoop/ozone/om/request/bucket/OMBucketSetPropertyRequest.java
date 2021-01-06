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

package org.apache.hadoop.ozone.om.request.bucket;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.om.request.OMClientRequest;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketSetPropertyResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyResponse;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handle SetBucketProperty Request.
 */
public class OMBucketSetPropertyRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketSetPropertyRequest.class);

  public OMBucketSetPropertyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    SetBucketPropertyRequest setBucketPropertyRequest =
        getOmRequest().getSetBucketPropertyRequest();
    Preconditions.checkNotNull(setBucketPropertyRequest);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketUpdates();

    BucketArgs bucketArgs = setBucketPropertyRequest.getBucketArgs();
    OmBucketArgs omBucketArgs = OmBucketArgs.getFromProtobuf(bucketArgs);

    String volumeName = bucketArgs.getVolumeName();
    String bucketName = bucketArgs.getBucketName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OmBucketInfo omBucketInfo = null;

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    IOException exception = null;
    boolean acquiredBucketLock = false, success = true;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, null);
      }

      // acquire lock.
      acquiredBucketLock =  omMetadataManager.getLock().acquireWriteLock(
          BUCKET_LOCK, volumeName, bucketName);

      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo dbBucketInfo =
          omMetadataManager.getBucketTable().get(bucketKey);
      //Check if bucket exist
      if (dbBucketInfo == null) {
        LOG.debug("bucket: {} not found ", bucketName);
        throw new OMException("Bucket doesn't exist",
            OMException.ResultCodes.BUCKET_NOT_FOUND);
      }

      OmBucketInfo.Builder bucketInfoBuilder = OmBucketInfo.newBuilder();
      bucketInfoBuilder.setVolumeName(dbBucketInfo.getVolumeName())
          .setBucketName(dbBucketInfo.getBucketName())
          .setObjectID(dbBucketInfo.getObjectID())
          .setUpdateID(transactionLogIndex);
      bucketInfoBuilder.addAllMetadata(KeyValueUtil
          .getFromProtobuf(bucketArgs.getMetadataList()));

      //Check StorageType to update
      StorageType storageType = omBucketArgs.getStorageType();
      if (storageType != null) {
        bucketInfoBuilder.setStorageType(storageType);
        LOG.debug("Updating bucket storage type for bucket: {} in volume: {}",
            bucketName, volumeName);
      } else {
        bucketInfoBuilder.setStorageType(dbBucketInfo.getStorageType());
      }

      //Check Versioning to update
      Boolean versioning = omBucketArgs.getIsVersionEnabled();
      if (versioning != null) {
        bucketInfoBuilder.setIsVersionEnabled(versioning);
        LOG.debug("Updating bucket versioning for bucket: {} in volume: {}",
            bucketName, volumeName);
      } else {
        bucketInfoBuilder
            .setIsVersionEnabled(dbBucketInfo.getIsVersionEnabled());
      }

      //Check quotaInBytes and quotaInNamespace to update
      String volumeKey = omMetadataManager.getVolumeKey(volumeName);
      OmVolumeArgs omVolumeArgs = omMetadataManager.getVolumeTable()
          .get(volumeKey);
      if (checkQuotaBytesValid(omMetadataManager, omVolumeArgs, omBucketArgs,
          volumeKey)) {
        bucketInfoBuilder.setQuotaInBytes(omBucketArgs.getQuotaInBytes());
      } else {
        bucketInfoBuilder.setQuotaInBytes(dbBucketInfo.getQuotaInBytes());
      }
      if (checkQuotaNamespaceValid(omVolumeArgs, omBucketArgs)) {
        bucketInfoBuilder.setQuotaInNamespace(
            omBucketArgs.getQuotaInNamespace());
      } else {
        bucketInfoBuilder.setQuotaInNamespace(
            dbBucketInfo.getQuotaInNamespace());
      }

      bucketInfoBuilder.setCreationTime(dbBucketInfo.getCreationTime());

      // Set acls from dbBucketInfo if it has any.
      if (dbBucketInfo.getAcls() != null) {
        bucketInfoBuilder.setAcls(dbBucketInfo.getAcls());
      }

      // Set the objectID to dbBucketInfo objectID, if present
      if (dbBucketInfo.getObjectID() != 0) {
        bucketInfoBuilder.setObjectID(dbBucketInfo.getObjectID());
      }

      // Set the updateID to current transaction log index
      bucketInfoBuilder.setUpdateID(transactionLogIndex);

      omBucketInfo = bucketInfoBuilder.build();

      // Update table cache.
      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          new CacheValue<>(Optional.of(omBucketInfo), transactionLogIndex));

      omResponse.setSetBucketPropertyResponse(
          SetBucketPropertyResponse.newBuilder().build());
      omClientResponse = new OMBucketSetPropertyResponse(
          omResponse.build(), omBucketInfo);
    } catch (IOException ex) {
      success = false;
      exception = ex;
      omClientResponse = new OMBucketSetPropertyResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredBucketLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.UPDATE_BUCKET,
        omBucketArgs.toAuditMap(), exception, userInfo));

    // return response.
    if (success) {
      LOG.debug("Setting bucket property for bucket:{} in volume:{}",
          bucketName, volumeName);
      return omClientResponse;
    } else {
      LOG.error("Setting bucket property failed for bucket:{} in volume:{}",
          bucketName, volumeName, exception);
      omMetrics.incNumBucketUpdateFails();
      return omClientResponse;
    }
  }

  public boolean checkQuotaBytesValid(OMMetadataManager metadataManager,
      OmVolumeArgs omVolumeArgs, OmBucketArgs omBucketArgs, String volumeKey)
      throws IOException {
    long quotaInBytes = omBucketArgs.getQuotaInBytes();

    if (quotaInBytes == OzoneConsts.QUOTA_RESET &&
        omVolumeArgs.getQuotaInBytes() != OzoneConsts.QUOTA_RESET) {
      throw new OMException("Can not clear bucket spaceQuota because" +
          " volume spaceQuota is not cleared.",
          OMException.ResultCodes.QUOTA_ERROR);
    }

    if (quotaInBytes == 0) {
      return false;
    }

    long totalBucketQuota = 0;
    long volumeQuotaInBytes = omVolumeArgs.getQuotaInBytes();

    if (quotaInBytes > OzoneConsts.QUOTA_RESET) {
      totalBucketQuota = quotaInBytes;
    }
    List<OmBucketInfo> bucketList = metadataManager.listBuckets(
        omVolumeArgs.getVolume(), null, null, Integer.MAX_VALUE);
    for(OmBucketInfo bucketInfo : bucketList) {
      long nextQuotaInBytes = bucketInfo.getQuotaInBytes();
      if(nextQuotaInBytes > OzoneConsts.QUOTA_RESET &&
          !omBucketArgs.getBucketName().equals(bucketInfo.getBucketName())) {
        totalBucketQuota += nextQuotaInBytes;
      }
    }

    if(volumeQuotaInBytes < totalBucketQuota &&
        volumeQuotaInBytes != OzoneConsts.QUOTA_RESET) {
      throw new IllegalArgumentException("Total buckets quota in this volume " +
          "should not be greater than volume quota : the total space quota is" +
          " set to:" + totalBucketQuota + ". But the volume space quota is:" +
          volumeQuotaInBytes);
    }
    return true;
  }

  public boolean checkQuotaNamespaceValid(OmVolumeArgs omVolumeArgs,
      OmBucketArgs omBucketArgs) {
    long quotaInNamespace = omBucketArgs.getQuotaInNamespace();

    if ((quotaInNamespace <= 0
         && quotaInNamespace != OzoneConsts.QUOTA_RESET)) {
      return false;
    }
    return true;
  }
}
