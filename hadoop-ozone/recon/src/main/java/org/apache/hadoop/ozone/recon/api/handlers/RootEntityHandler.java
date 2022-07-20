/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.api.handlers;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for handling root entity type.
 */
public class RootEntityHandler extends EntityHandler {
  public RootEntityHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM, String path) {
    super(reconNamespaceSummaryManager, omMetadataManager,
          reconSCM, null, path);
  }

  @Override
  public NamespaceSummaryResponse getSummaryResponse()
          throws IOException {
    NamespaceSummaryResponse namespaceSummaryResponse =
            new NamespaceSummaryResponse(EntityType.ROOT);
    List<OmVolumeArgs> volumes = listVolumes();
    namespaceSummaryResponse.setNumVolume(volumes.size());
    List<OmBucketInfo> allBuckets = listBucketsUnderVolume(null);
    namespaceSummaryResponse.setNumBucket(allBuckets.size());
    int totalNumDir = 0;
    long totalNumKey = 0L;
    for (OmBucketInfo bucket : allBuckets) {
      long bucketObjectId = bucket.getObjectID();
      totalNumDir += getTotalDirCount(bucketObjectId);
      totalNumKey += getTotalKeyCount(bucketObjectId);
    }

    namespaceSummaryResponse.setNumTotalDir(totalNumDir);
    namespaceSummaryResponse.setNumTotalKey(totalNumKey);

    return namespaceSummaryResponse;
  }

  @Override
  public DUResponse getDuResponse(
          boolean listFile, boolean withReplica)
          throws IOException {
    DUResponse duResponse = new DUResponse();
    duResponse.setPath(getNormalizedPath());
    ReconOMMetadataManager omMetadataManager = getOmMetadataManager();
    List<OmVolumeArgs> volumes = listVolumes();
    duResponse.setCount(volumes.size());

    List<DUResponse.DiskUsage> volumeDuData = new ArrayList<>();
    long totalDataSize = 0L;
    long totalDataSizeWithReplica = 0L;
    for (OmVolumeArgs volume: volumes) {
      String volumeName = volume.getVolume();
      String subpath = omMetadataManager.getVolumeKey(volumeName);
      DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
      long dataSize = 0;
      diskUsage.setSubpath(subpath);
      BucketHandler bucketHandler;
      long volumeDU = 0;
      // iterate all buckets per volume to get total data size
      for (OmBucketInfo bucket: listBucketsUnderVolume(volumeName)) {
        long bucketObjectID = bucket.getObjectID();
        dataSize += getTotalSize(bucketObjectID);
        // count replicas
        // TODO: to be dropped or optimized in the future
        if (withReplica) {
          bucketHandler =
            BucketHandler.getBucketHandler(
              getReconNamespaceSummaryManager(),
              getOmMetadataManager(), getReconSCM(), bucket);
          volumeDU += bucketHandler.calculateDUUnderObject(bucketObjectID);
        }
      }
      totalDataSize += dataSize;

      // count replicas
      // TODO: to be dropped or optimized in the future
      if (withReplica) {
        totalDataSizeWithReplica += volumeDU;
        diskUsage.setSizeWithReplica(volumeDU);
      }
      diskUsage.setSize(dataSize);
      volumeDuData.add(diskUsage);
    }
    if (withReplica) {
      duResponse.setSizeWithReplica(totalDataSizeWithReplica);
    }
    duResponse.setSize(totalDataSize);
    duResponse.setDuData(volumeDuData);

    return duResponse;
  }

  @Override
  public QuotaUsageResponse getQuotaResponse()
          throws IOException {
    QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
    List<OmVolumeArgs> volumes = listVolumes();
    List<OmBucketInfo> buckets = listBucketsUnderVolume(null);
    long quotaInBytes = 0L;
    long quotaUsedInBytes = 0L;

    for (OmVolumeArgs volume: volumes) {
      final long quota = volume.getQuotaInBytes();
      assert (quota >= -1L);
      if (quota == -1L) {
        // If one volume has unlimited quota, the "root" quota is unlimited.
        quotaInBytes = -1L;
        break;
      }
      quotaInBytes += quota;
    }
    for (OmBucketInfo bucket: buckets) {
      long bucketObjectId = bucket.getObjectID();
      quotaUsedInBytes += getTotalSize(bucketObjectId);
    }

    quotaUsageResponse.setQuota(quotaInBytes);
    quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
    return quotaUsageResponse;
  }

  @Override
  public FileSizeDistributionResponse getDistResponse()
          throws IOException {
    FileSizeDistributionResponse distResponse =
        new FileSizeDistributionResponse();
    List<OmBucketInfo> allBuckets = listBucketsUnderVolume(null);
    int[] fileSizeDist = new int[ReconConstants.NUM_OF_BINS];

    // accumulate file size distribution arrays from all buckets
    for (OmBucketInfo bucket : allBuckets) {
      long bucketObjectId = bucket.getObjectID();
      int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
      // add on each bin
      for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
        fileSizeDist[i] += bucketFileSizeDist[i];
      }
    }
    distResponse.setFileSizeDist(fileSizeDist);
    return distResponse;
  }

}
