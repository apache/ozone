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

package org.apache.hadoop.ozone.om.request.volume;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeSetQuotaResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles set Quota request for volume.
 */
public class OMVolumeSetQuotaRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeSetQuotaRequest.class);

  public OMVolumeSetQuotaRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    super.preExecute(ozoneManager);

    long modificationTime = Time.now();
    SetVolumePropertyRequest.Builder setPropertyRequestBuilde = getOmRequest()
        .getSetVolumePropertyRequest().toBuilder()
        .setModificationTime(modificationTime);

    SetVolumePropertyRequest setVolumePropertyRequest =
        getOmRequest().getSetVolumePropertyRequest();
    String volume = setVolumePropertyRequest.getVolumeName();

    // ACL check during preExecute
    if (ozoneManager.getAclsEnabled()) {
      try {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE, volume,
            null, null);
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        Map<String, String> auditMap = new LinkedHashMap<>();
        auditMap.put(OzoneConsts.VOLUME, volume);
        auditMap.put(OzoneConsts.QUOTA_IN_BYTES,
            String.valueOf(setVolumePropertyRequest.getQuotaInBytes()));
        markForAudit(ozoneManager.getAuditLogger(),
            buildAuditMessage(OMAction.SET_QUOTA, auditMap, ex,
                getOmRequest().getUserInfo()));
        throw ex;
      }
    }

    return getOmRequest().toBuilder()
        .setSetVolumePropertyRequest(setPropertyRequestBuilde)
        .setUserInfo(getUserIfNotExists(ozoneManager))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    SetVolumePropertyRequest setVolumePropertyRequest =
        getOmRequest().getSetVolumePropertyRequest();

    Preconditions.checkNotNull(setVolumePropertyRequest);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    if (!setVolumePropertyRequest.hasQuotaInBytes()
        && !setVolumePropertyRequest.hasQuotaInNamespace()) {
      omResponse.setStatus(OzoneManagerProtocolProtos.Status.INVALID_REQUEST)
          .setSuccess(false);
      return new OMVolumeSetQuotaResponse(omResponse.build());
    }

    String volume = setVolumePropertyRequest.getVolumeName();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeUpdates();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    Map<String, String> auditMap = buildVolumeAuditMap(volume);
    auditMap.put(OzoneConsts.QUOTA_IN_BYTES,
        String.valueOf(setVolumePropertyRequest.getQuotaInBytes()));

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    Exception exception = null;
    boolean acquireVolumeLock = false;
    OMClientResponse omClientResponse = null;
    try {
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volume));
      acquireVolumeLock = getOmLockDetails().isLockAcquired();

      OmVolumeArgs omVolumeArgs = getVolumeInfo(omMetadataManager, volume);
      if (checkQuotaBytesValid(omMetadataManager,
          setVolumePropertyRequest.getQuotaInBytes(), volume)) {
        omVolumeArgs.setQuotaInBytes(
            setVolumePropertyRequest.getQuotaInBytes());
      } else {
        omVolumeArgs.setQuotaInBytes(omVolumeArgs.getQuotaInBytes());
      }
      if (checkQuotaNamespaceValid(omMetadataManager,
          setVolumePropertyRequest.getQuotaInNamespace(), volume)) {
        omVolumeArgs.setQuotaInNamespace(
            setVolumePropertyRequest.getQuotaInNamespace());
      } else {
        omVolumeArgs.setQuotaInNamespace(omVolumeArgs.getQuotaInNamespace());
      }

      omVolumeArgs.setUpdateID(transactionLogIndex);
      omVolumeArgs.setModificationTime(
          setVolumePropertyRequest.getModificationTime());

      // update cache.
      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getVolumeKey(volume)),
          CacheValue.get(transactionLogIndex, omVolumeArgs));

      omResponse.setSetVolumePropertyResponse(
          SetVolumePropertyResponse.newBuilder().build());
      omClientResponse = new OMVolumeSetQuotaResponse(omResponse.build(),
          omVolumeArgs);
    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMVolumeSetQuotaResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquireVolumeLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(VOLUME_LOCK, volume));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Performing audit logging outside of the lock.
    markForAudit(auditLogger, buildAuditMessage(OMAction.SET_QUOTA, auditMap,
        exception, userInfo));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("Changing volume quota is successfully completed for volume: " +
          "{} quota:{}", volume, setVolumePropertyRequest.getQuotaInBytes());
    } else {
      omMetrics.incNumVolumeUpdateFails();
      LOG.error("Changing volume quota failed for volume:{} quota:{}", volume,
          setVolumePropertyRequest.getQuotaInBytes(), exception);
    }
    return omClientResponse;
  }

  public boolean checkQuotaBytesValid(OMMetadataManager metadataManager,
      long volumeQuotaInBytes, String volumeName) throws IOException {
    long totalBucketQuota = 0;

    if (volumeQuotaInBytes < OzoneConsts.QUOTA_RESET
        || volumeQuotaInBytes == 0) {
      return false;
    }
    
    // if volume quota is for reset, no need further check
    if (volumeQuotaInBytes == OzoneConsts.QUOTA_RESET) {
      return true;
    }

    boolean isBucketQuotaSet = true;
    List<OmBucketInfo> bucketList = metadataManager.listBuckets(
        volumeName, null, null, Integer.MAX_VALUE, false);
    for (OmBucketInfo bucketInfo : bucketList) {
      if (bucketInfo.isLink()) {
        continue;
      }
      long nextQuotaInBytes = bucketInfo.getQuotaInBytes();
      if (nextQuotaInBytes > OzoneConsts.QUOTA_RESET) {
        totalBucketQuota += nextQuotaInBytes;
      } else {
        isBucketQuotaSet = false;
        break;
      }
    }
    
    if (!isBucketQuotaSet) {
      throw new OMException("Can not set volume space quota on volume " +
          "as some of buckets in this volume have no quota set.",
          OMException.ResultCodes.QUOTA_ERROR);
    }
    
    if (volumeQuotaInBytes < totalBucketQuota &&
        volumeQuotaInBytes != OzoneConsts.QUOTA_RESET) {
      throw new OMException("Total buckets quota in this volume " +
          "should not be greater than volume quota : the total space quota is" +
          ":" + totalBucketQuota + ". But the volume space quota is:" +
          volumeQuotaInBytes, OMException.ResultCodes.QUOTA_EXCEEDED);
    }
    return true;
  }

  public boolean checkQuotaNamespaceValid(OMMetadataManager metadataManager,
      long quotaInNamespace, String volumeName) throws IOException {
    if (quotaInNamespace < OzoneConsts.QUOTA_RESET || quotaInNamespace == 0) {
      return false;
    }

    // if volume quota is for reset, no need further check
    if (quotaInNamespace == OzoneConsts.QUOTA_RESET) {
      return true;
    }

    List<OmBucketInfo> bucketList = metadataManager.listBuckets(
        volumeName, null, null, Integer.MAX_VALUE, false);
    if (bucketList.size() > quotaInNamespace) {
      throw new OMException("Total number of buckets " + bucketList.size() +
          " in this volume should not be greater than volume namespace quota "
          + quotaInNamespace, OMException.ResultCodes.QUOTA_EXCEEDED);
    }
    return true;
  }

}


