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

public class DirectoryEntityHandler extends EntityHandler {
  public DirectoryEntityHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler) {
    super(reconNamespaceSummaryManager, omMetadataManager, reconSCM, bucketHandler);
  }

  @Override
  public NamespaceSummaryResponse getSummaryResponse(String[] names) throws
      IOException {
      // path should exist so we don't need any extra verification/null check
      long dirObjectId = bucketHandler.getDirObjectId(names);
      NamespaceSummaryResponse namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.DIRECTORY);
      namespaceSummaryResponse.setNumTotalDir(getTotalDirCount(dirObjectId));
      namespaceSummaryResponse.setNumTotalKey(getTotalKeyCount(dirObjectId));

      return namespaceSummaryResponse;
  }

  @Override
  public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                  boolean withReplica) throws IOException {
      DUResponse duResponse = new DUResponse();
      long dirObjectId = bucketHandler.getDirObjectId(names);
      NSSummary dirNSSummary =
              reconNamespaceSummaryManager.getNSSummary(dirObjectId);
      // Empty directory
      if (dirNSSummary == null) {
        if (withReplica) {
          duResponse.setSizeWithReplica(0L);
        }
        return duResponse;
      }

      Set<Long> subdirs = dirNSSummary.getChildDir();

      duResponse.setKeySize(dirNSSummary.getSizeOfFiles());
      long dirDataSize = duResponse.getKeySize();
      long dirDataSizeWithReplica = 0L;
      List<DUResponse.DiskUsage> subdirDUData = new ArrayList<>();
      // iterate all subdirectories to get disk usage data
      for (long subdirObjectId: subdirs) {
        NSSummary subdirNSSummary =
                reconNamespaceSummaryManager.getNSSummary(subdirObjectId);
        String subdirName = subdirNSSummary.getDirName();
        // build the path for subdirectory
        String subpath = BucketHandler.buildSubpath(path, subdirName);
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        // reformat the response
        diskUsage.setSubpath(subpath);
        long dataSize = getTotalSize(subdirObjectId);
        dirDataSize += dataSize;

        if (withReplica) {
          long subdirDU = bucketHandler.calculateDUUnderObject(subdirObjectId);
          diskUsage.setSizeWithReplica(subdirDU);
          dirDataSizeWithReplica += subdirDU;
        }

        diskUsage.setSize(dataSize);
        subdirDUData.add(diskUsage);
      }

      // handle direct keys under directory
      if (listFile || withReplica) {
        dirDataSizeWithReplica += bucketHandler.handleDirectKeys(dirObjectId, withReplica,
                listFile, subdirDUData, path);
      }

      if (withReplica) {
        duResponse.setSizeWithReplica(dirDataSizeWithReplica);
      }
      duResponse.setCount(subdirDUData.size());
      duResponse.setSize(dirDataSize);
      duResponse.setDuData(subdirDUData);

      return duResponse;
  }

  @Override
  public QuotaUsageResponse getQuotaResponse(String[] names) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      quotaUsageResponse.setResponseCode(
              ResponseStatus.TYPE_NOT_APPLICABLE);
      return quotaUsageResponse;
  }

  @Override
  public FileSizeDistributionResponse getDistResponse(String[] names) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
      long dirObjectId = bucketHandler.getDirObjectId(names);
      int[] dirFileSizeDist = getTotalFileSizeDist(dirObjectId);
      distResponse.setFileSizeDist(dirFileSizeDist);
      return distResponse;
  }

}
