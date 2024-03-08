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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.CountStats;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.ObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import java.io.IOException;
import java.util.ArrayList;

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

    List<OmVolumeArgs> volumes = getOmMetadataManager().listVolumes();
    List<OmBucketInfo> allBuckets = getOmMetadataManager().
        listBucketsUnderVolume(null);
    int totalNumDir = 0;
    long totalNumKey = 0L;
    for (OmBucketInfo bucket : allBuckets) {
      long bucketObjectId = bucket.getObjectID();
      totalNumDir += getTotalDirCount(bucketObjectId);
      totalNumKey += getTotalKeyCount(bucketObjectId);
    }
    CountStats countStats = new CountStats(
        volumes.size(), allBuckets.size(), totalNumDir, totalNumKey);

    return NamespaceSummaryResponse.newBuilder()
        .setEntityType(EntityType.ROOT)
        .setCountStats(countStats)
        .setObjectDBInfo(getPrefixObjDbInfo())
        .setStatus(ResponseStatus.OK)
        .build();
  }

  private ObjectDBInfo getPrefixObjDbInfo()
      throws IOException {
    OmPrefixInfo omPrefixInfo = getOmMetadataManager().getPrefixTable()
        .getSkipCache(OzoneConsts.OM_KEY_PREFIX);
    if (null == omPrefixInfo) {
      return new ObjectDBInfo();
    }
    return new ObjectDBInfo(omPrefixInfo);
  }

  @Override
  public DUResponse getDuResponse(
      boolean listFile, boolean withReplica, boolean sortSubPaths)
          throws IOException {
    DUResponse duResponse = new DUResponse();
    duResponse.setPath(getNormalizedPath());
    ReconOMMetadataManager omMetadataManager = getOmMetadataManager();
    List<OmVolumeArgs> volumes = getOmMetadataManager().listVolumes();
    duResponse.setCount(volumes.size());

    // Map to hold total size for each volume
    Map<String, Long> volumeSizes = new HashMap<>();
    for (OmVolumeArgs volume : volumes) {
      long totalVolumeSize = 0;
      List<OmBucketInfo> buckets =
          omMetadataManager.listBucketsUnderVolume(volume.getVolume());
      for (OmBucketInfo bucket : buckets) {
        long bucketObjectID = bucket.getObjectID();
        totalVolumeSize += getTotalSize(bucketObjectID);
      }
      volumeSizes.put(volume.getVolume(), totalVolumeSize);
    }

    // Apply sorting based on sortSubPaths flag
    SortedResult sortedResult = getSortedResult(volumes, volumeSizes, sortSubPaths);
    List<DUResponse.DiskUsage> volumeDuData = new ArrayList<>();
    long totalDataSize = 0L;
    long totalDataSizeWithReplica = 0L;

    for (OmVolumeArgs volume : sortedResult.sortedVolumes) {
      String volumeName = volume.getVolume();
      String subpath = omMetadataManager.getVolumeKey(volumeName);
      DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
      long dataSize = 0;
      diskUsage.setSubpath(subpath);
      BucketHandler bucketHandler;
      long volumeDU = 0;
      // iterate all buckets per volume to get total data size
      for (OmBucketInfo bucket: getOmMetadataManager().
          listBucketsUnderVolume(volumeName)) {
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
    SCMNodeStat stats = getReconSCM().getScmNodeManager().getStats();
    long quotaInBytes = stats.getCapacity().get();
    long quotaUsedInBytes = getDuResponse(true, true, false).getSizeWithReplica();
    quotaUsageResponse.setQuota(quotaInBytes);
    quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
    return quotaUsageResponse;
  }

  @Override
  public FileSizeDistributionResponse getDistResponse()
          throws IOException {
    FileSizeDistributionResponse distResponse =
        new FileSizeDistributionResponse();
    List<OmBucketInfo> allBuckets = getOmMetadataManager()
        .listBucketsUnderVolume(null);
    int[] fileSizeDist = new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];

    // accumulate file size distribution arrays from all buckets
    for (OmBucketInfo bucket : allBuckets) {
      long bucketObjectId = bucket.getObjectID();
      int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
      // add on each bin
      for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
        fileSizeDist[i] += bucketFileSizeDist[i];
      }
    }
    distResponse.setFileSizeDist(fileSizeDist);
    return distResponse;
  }

  private SortedResult getSortedResult(List<OmVolumeArgs> volumes,
                                       Map<String, Long> volumeSizes,
                                       boolean sortSubPaths) {
    if (sortSubPaths) {
      // Sort volumes based on the total size in descending order
      List<OmVolumeArgs> sortedVolumes = volumes.stream()
          .sorted((v1, v2) -> volumeSizes.get(v2.getVolume())
              .compareTo(volumeSizes.get(v1.getVolume())))
          .collect(Collectors.toList());
      return new SortedResult(sortedVolumes, volumeSizes);
    } else {
      // Return volumes as is without sorting
      return new SortedResult(new ArrayList<>(volumes), volumeSizes);
    }
  }

  private static class SortedResult {
    public final List<OmVolumeArgs> sortedVolumes;
    public final Map<String, Long> volumeSizes;

    public SortedResult(List<OmVolumeArgs> sortedVolumes, Map<String, Long> volumeSizes) {
      this.sortedVolumes = sortedVolumes;
      this.volumeSizes = volumeSizes;
    }
  }

}
