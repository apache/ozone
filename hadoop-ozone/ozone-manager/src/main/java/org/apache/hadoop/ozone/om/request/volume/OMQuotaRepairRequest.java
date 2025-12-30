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

import static org.apache.hadoop.ozone.OzoneConsts.OLD_QUOTA_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.QUOTA_RESET;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMQuotaRepairResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle OMQuotaRepairRequest Request.
 */
public class OMQuotaRepairRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMQuotaRepairRequest.class);

  public OMQuotaRepairRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    UserGroupInformation ugi = createUGIForApi();
    if (ozoneManager.getAclsEnabled() && !ozoneManager.isAdmin(ugi)) {
      throw new OMException("Access denied for user " + ugi + ". Admin privilege is required for quota repair.",
          OMException.ResultCodes.ACCESS_DENIED);
    }
    return super.preExecute(ozoneManager);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    OzoneManagerProtocolProtos.QuotaRepairRequest quotaRepairRequest =
        getOmRequest().getQuotaRepairRequest();
    Objects.requireNonNull(quotaRepairRequest, "quotaRepairRequest == null");

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    Map<Pair<String, String>, OmBucketInfo> bucketMap = new HashMap<>();
    OMClientResponse omClientResponse = null;
    try {
      for (int i = 0; i < quotaRepairRequest.getBucketCountCount(); ++i) {
        OzoneManagerProtocolProtos.BucketQuotaCount bucketCountInfo = quotaRepairRequest.getBucketCount(i);
        updateBucketInfo(omMetadataManager, bucketCountInfo, transactionLogIndex, bucketMap);
      }
      Map<String, OmVolumeArgs> volUpdateMap;
      if (quotaRepairRequest.getSupportVolumeOldQuota()) {
        volUpdateMap = updateOldVolumeQuotaSupport(omMetadataManager, transactionLogIndex);
      } else {
        volUpdateMap = Collections.emptyMap();
      }
      omResponse.setQuotaRepairResponse(
          OzoneManagerProtocolProtos.QuotaRepairResponse.newBuilder().build());
      omClientResponse = new OMQuotaRepairResponse(omResponse.build(), volUpdateMap, bucketMap);
    } catch (IOException ex) {
      LOG.error("failed to update repair count", ex);
      omClientResponse = new OMQuotaRepairResponse(createErrorOMResponse(omResponse, ex));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    return omClientResponse;
  }
  
  private void updateBucketInfo(
      OMMetadataManager omMetadataManager, OzoneManagerProtocolProtos.BucketQuotaCount bucketCountInfo,
      long transactionLogIndex, Map<Pair<String, String>, OmBucketInfo> bucketMap) throws IOException {
    // acquire lock.
    mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
        BUCKET_LOCK, bucketCountInfo.getVolName(), bucketCountInfo.getBucketName()));
    boolean acquiredBucketLock = getOmLockDetails().isLockAcquired();
    try {
      String bucketKey = omMetadataManager.getBucketKey(bucketCountInfo.getVolName(),
          bucketCountInfo.getBucketName());
      OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
      if (null == bucketInfo) {
        // bucket might be deleted when running repair count parallel
        return;
      }
      bucketInfo.incrUsedBytes(bucketCountInfo.getDiffUsedBytes());
      bucketInfo.incrUsedNamespace(bucketCountInfo.getDiffUsedNamespace());
      if (bucketCountInfo.getSupportOldQuota()) {
        OmBucketInfo.Builder builder = bucketInfo.toBuilder();
        if (bucketInfo.getQuotaInBytes() == OLD_QUOTA_DEFAULT) {
          builder.setQuotaInBytes(QUOTA_RESET);
        }
        if (bucketInfo.getQuotaInNamespace() == OLD_QUOTA_DEFAULT) {
          builder.setQuotaInNamespace(QUOTA_RESET);
        }
        bucketInfo = builder.build();
      }

      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey), CacheValue.get(transactionLogIndex, bucketInfo));
      bucketMap.put(Pair.of(bucketCountInfo.getVolName(), bucketCountInfo.getBucketName()), bucketInfo);
    } finally {
      if (acquiredBucketLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, bucketCountInfo.getVolName(), bucketCountInfo.getBucketName()));
      }
    }
  }

  private Map<String, OmVolumeArgs> updateOldVolumeQuotaSupport(
      OMMetadataManager metadataManager, long transactionLogIndex) throws IOException {
    LOG.info("Starting volume quota support update");
    Map<String, OmVolumeArgs> volUpdateMap = new HashMap<>();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
             iterator = metadataManager.getVolumeTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmVolumeArgs> entry = iterator.next();
        OmVolumeArgs omVolumeArgs = entry.getValue();
        if (!(omVolumeArgs.getQuotaInBytes() == OLD_QUOTA_DEFAULT
            || omVolumeArgs.getQuotaInNamespace() == OLD_QUOTA_DEFAULT)) {
          continue;
        }
        mergeOmLockDetails(metadataManager.getLock().acquireWriteLock(
            VOLUME_LOCK, omVolumeArgs.getVolume()));
        boolean acquiredVolumeLock = getOmLockDetails().isLockAcquired();
        try {
          OmVolumeArgs.Builder builder = omVolumeArgs.toBuilder();
          boolean isQuotaReset = false;
          if (omVolumeArgs.getQuotaInBytes() == OLD_QUOTA_DEFAULT) {
            builder.setQuotaInBytes(QUOTA_RESET);
            isQuotaReset = true;
          }
          if (omVolumeArgs.getQuotaInNamespace() == OLD_QUOTA_DEFAULT) {
            builder.setQuotaInNamespace(QUOTA_RESET);
            isQuotaReset = true;
          }
          if (isQuotaReset) {
            OmVolumeArgs updated = builder.build();
            metadataManager.getVolumeTable().addCacheEntry(
                new CacheKey<>(entry.getKey()), CacheValue.get(transactionLogIndex, updated));
            volUpdateMap.put(entry.getKey(), updated);
          }
        } finally {
          if (acquiredVolumeLock) {
            mergeOmLockDetails(metadataManager.getLock().releaseWriteLock(VOLUME_LOCK, omVolumeArgs.getVolume()));
          }
        }
      }
    }
    LOG.info("Completed volume quota support update for volume count {}", volUpdateMap.size());
    return volUpdateMap;
  }
}
