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
import java.util.Set;

public class BucketEntityHandler extends EntityHandler {
  public BucketEntityHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler) {
    super(reconNamespaceSummaryManager, omMetadataManager, reconSCM, bucketHandler);
  }

  @Override
  public NamespaceSummaryResponse getSummaryResponse(String[] names) throws
      IOException {
      NamespaceSummaryResponse namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.BUCKET);
      assert (names.length == 2);
      long bucketObjectId = bucketHandler.getBucketObjectId(names);
      namespaceSummaryResponse.setNumTotalDir(getTotalDirCount(bucketObjectId));
      namespaceSummaryResponse.setNumTotalKey(getTotalKeyCount(bucketObjectId));

      return namespaceSummaryResponse;
  }

  @Override
  public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                  boolean withReplica) throws IOException {
      DUResponse duResponse = new DUResponse();
      long bucketObjectId = bucketHandler.getBucketObjectId(names);
      NSSummary bucketNSSummary =
              reconNamespaceSummaryManager.getNSSummary(bucketObjectId);
      // empty bucket, because it's not a parent of any directory or key
      if (bucketNSSummary == null) {
        if (withReplica) {
          duResponse.setSizeWithReplica(0L);
        }
        return duResponse;
      }

      // get object IDs for all its subdirectories
      Set<Long> bucketSubdirs = bucketNSSummary.getChildDir();
      duResponse.setKeySize(bucketNSSummary.getSizeOfFiles());
      List<DUResponse.DiskUsage> dirDUData = new ArrayList<>();
      long bucketDataSize = duResponse.getKeySize();
      long bucketDataSizeWithReplica = 0L;
      for (long subdirObjectId: bucketSubdirs) {
        NSSummary subdirNSSummary = reconNamespaceSummaryManager
                .getNSSummary(subdirObjectId);

        // get directory's name and generate the next-level subpath.
        String dirName = subdirNSSummary.getDirName();
        String subpath = BucketHandler.buildSubpath(path, dirName);
        // we need to reformat the subpath in the response in a
        // format with leading slash and without trailing slash
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(subpath);
        long dataSize = getTotalSize(subdirObjectId);
        bucketDataSize += dataSize;

        if (withReplica) {
          long dirDU = bucketHandler.calculateDUUnderObject(subdirObjectId);
          diskUsage.setSizeWithReplica(dirDU);
          bucketDataSizeWithReplica += dirDU;
        }
        diskUsage.setSize(dataSize);
        dirDUData.add(diskUsage);
      }
      // Either listFile or withReplica is enabled, we need the directKeys info
      if (listFile || withReplica) {
        bucketDataSizeWithReplica += bucketHandler.handleDirectKeys(bucketObjectId,
                withReplica, listFile, dirDUData, path);
      }
      if (withReplica) {
        duResponse.setSizeWithReplica(bucketDataSizeWithReplica);
      }
      duResponse.setCount(dirDUData.size());
      duResponse.setSize(bucketDataSize);
      duResponse.setDuData(dirDUData);
      return duResponse;
  }

  @Override
  public QuotaUsageResponse getQuotaResponse(String[] names) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      String bucketKey = omMetadataManager.getBucketKey(names[0], names[1]);
      OmBucketInfo bucketInfo = omMetadataManager
              .getBucketTable().getSkipCache(bucketKey);
      long bucketObjectId = bucketInfo.getObjectID();
      long quotaInBytes = bucketInfo.getQuotaInBytes();
      long quotaUsedInBytes = getTotalSize(bucketObjectId);
      quotaUsageResponse.setQuota(quotaInBytes);
      quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
      return quotaUsageResponse;
  }

  @Override
  public FileSizeDistributionResponse getDistResponse(String[] names) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
      long bucketObjectId = bucketHandler.getBucketObjectId(names);
      int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
      distResponse.setFileSizeDist(bucketFileSizeDist);
      return distResponse;
  }

}
