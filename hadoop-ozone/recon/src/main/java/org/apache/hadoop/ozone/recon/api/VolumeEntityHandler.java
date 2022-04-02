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

public class VolumeEntityHandler extends EntityHandler {
  public VolumeEntityHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM) {
    super(reconNamespaceSummaryManager, omMetadataManager, reconSCM, null);
  }

  @Override
  public NamespaceSummaryResponse getSummaryResponse(String[] names) throws
      IOException {
      NamespaceSummaryResponse namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.VOLUME);
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
  public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                  boolean withReplica) throws IOException {
      DUResponse duResponse = new DUResponse();
      String volName = names[0];
      List<OmBucketInfo> buckets = listBucketsUnderVolume(volName);
      duResponse.setCount(buckets.size());

      // List of DiskUsage data for all buckets
      List<DUResponse.DiskUsage> bucketDuData = new ArrayList<>();
      long volDataSize = 0L;
      long volDataSizeWithReplica = 0L;
      for (OmBucketInfo bucket: buckets) {
        BucketHandler bucketHandler = BucketHandler.getBucketHandler(bucket, reconNamespaceSummaryManager,
            omMetadataManager, reconSCM);
        String bucketName = bucket.getBucketName();
        long bucketObjectID = bucket.getObjectID();
        String subpath = omMetadataManager.getBucketKey(volName, bucketName);
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(subpath);
        long dataSize = getTotalSize(bucketObjectID);
        volDataSize += dataSize;
        if (withReplica) {
          long bucketDU = bucketHandler.calculateDUUnderObject(bucketObjectID);
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
  public QuotaUsageResponse getQuotaResponse(String[] names) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      List<OmBucketInfo> buckets = listBucketsUnderVolume(names[0]);
      String volKey = omMetadataManager.getVolumeKey(names[0]);
      OmVolumeArgs volumeArgs =
              omMetadataManager.getVolumeTable().getSkipCache(volKey);
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
  public FileSizeDistributionResponse getDistResponse(String[] names) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
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
