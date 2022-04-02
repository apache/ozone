package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.*;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RootEntityHandler extends EntityHandler {
  public RootEntityHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM) {
    super(reconNamespaceSummaryManager, omMetadataManager, reconSCM, null);
  }

  @Override
  public NamespaceSummaryResponse getSummaryResponse(String[] names) throws
      IOException {
    NamespaceSummaryResponse namespaceSummaryResponse = new NamespaceSummaryResponse(
        EntityType.ROOT);
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
  public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                  boolean withReplica) throws IOException {
    DUResponse duResponse = new DUResponse();
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
      // iterate all buckets per volume to get total data size
      for (OmBucketInfo bucket: listBucketsUnderVolume(volumeName)) {
        long bucketObjectID = bucket.getObjectID();
        dataSize += getTotalSize(bucketObjectID);
      }
      totalDataSize += dataSize;

      // count replicas
      // TODO: to be dropped or optimized in the future
      if (withReplica) {
        long volumeDU = calculateDUForVolume(volumeName);
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
  public QuotaUsageResponse getQuotaResponse(String[] names) throws IOException {
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
  public FileSizeDistributionResponse getDistResponse(String[] names) throws IOException {
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
