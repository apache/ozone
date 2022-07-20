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
 * Class for handling volume entity type.
 */
public class VolumeEntityHandler extends EntityHandler {
  public VolumeEntityHandler(
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
            new NamespaceSummaryResponse(EntityType.VOLUME);
    String[] names = getNames();
    List<OmBucketInfo> buckets = listBucketsUnderVolume(names[0]);
    namespaceSummaryResponse.setNumBucket(buckets.size());
    int totalDir = 0;
    long totalKey = 0L;

    // iterate all buckets to collect the total object count.
    for (OmBucketInfo bucket : buckets) {
      long bucketObjectId = bucket.getObjectID();
      totalDir += getTotalDirCount(bucketObjectId);
      totalKey += getTotalKeyCount(bucketObjectId);
    }

    namespaceSummaryResponse.setNumTotalDir(totalDir);
    namespaceSummaryResponse.setNumTotalKey(totalKey);

    return namespaceSummaryResponse;
  }

  @Override
  public DUResponse getDuResponse(
          boolean listFile, boolean withReplica)
          throws IOException {
    DUResponse duResponse = new DUResponse();
    duResponse.setPath(getNormalizedPath());
    String[] names = getNames();
    String volName = names[0];
    List<OmBucketInfo> buckets = listBucketsUnderVolume(volName);
    duResponse.setCount(buckets.size());

    // List of DiskUsage data for all buckets
    List<DUResponse.DiskUsage> bucketDuData = new ArrayList<>();
    long volDataSize = 0L;
    long volDataSizeWithReplica = 0L;
    for (OmBucketInfo bucket: buckets) {
      String bucketName = bucket.getBucketName();
      long bucketObjectID = bucket.getObjectID();
      String subpath = getOmMetadataManager().getBucketKey(volName, bucketName);
      DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
      diskUsage.setSubpath(subpath);
      long dataSize = getTotalSize(bucketObjectID);
      volDataSize += dataSize;
      if (withReplica) {
        BucketHandler bucketHandler =
              BucketHandler.getBucketHandler(
                  getReconNamespaceSummaryManager(),
                  getOmMetadataManager(), getReconSCM(), bucket);
        long bucketDU = bucketHandler
              .calculateDUUnderObject(bucketObjectID);
        diskUsage.setSizeWithReplica(bucketDU);
        volDataSizeWithReplica += bucketDU;
      }
      diskUsage.setSize(dataSize);
      bucketDuData.add(diskUsage);
    }
    if (withReplica) {
      duResponse.setSizeWithReplica(volDataSizeWithReplica);
    }
    duResponse.setSize(volDataSize);
    duResponse.setDuData(bucketDuData);
    return duResponse;
  }

  @Override
  public QuotaUsageResponse getQuotaResponse()
          throws IOException {
    QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
    String[] names = getNames();
    List<OmBucketInfo> buckets = listBucketsUnderVolume(names[0]);
    String volKey = getOmMetadataManager().getVolumeKey(names[0]);
    OmVolumeArgs volumeArgs =
            getOmMetadataManager().getVolumeTable().getSkipCache(volKey);
    long quotaInBytes = volumeArgs.getQuotaInBytes();
    long quotaUsedInBytes = 0L;

    // Get the total data size used by all buckets
    for (OmBucketInfo bucketInfo: buckets) {
      long bucketObjectId = bucketInfo.getObjectID();
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
    String[] names = getNames();
    List<OmBucketInfo> buckets = listBucketsUnderVolume(names[0]);
    int[] volumeFileSizeDist = new int[ReconConstants.NUM_OF_BINS];

    // accumulate file size distribution arrays from all buckets under volume
    for (OmBucketInfo bucket : buckets) {
      long bucketObjectId = bucket.getObjectID();
      int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
      // add on each bin
      for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
        volumeFileSizeDist[i] += bucketFileSizeDist[i];
      }
    }
    distResponse.setFileSizeDist(volumeFileSizeDist);
    return distResponse;
  }

}
