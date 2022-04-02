package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.*;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KeyEntityHandler extends EntityHandler {
  public KeyEntityHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler) {
    super(reconNamespaceSummaryManager, omMetadataManager, reconSCM, bucketHandler);
  }

  @Override
  public NamespaceSummaryResponse getSummaryResponse(String[] names) throws
      IOException {
      NamespaceSummaryResponse namespaceSummaryResponse = new NamespaceSummaryResponse(EntityType.KEY);

      return namespaceSummaryResponse;
  }

  @Override
  public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                  boolean withReplica) throws IOException {
      DUResponse duResponse = new DUResponse();
      // DU for key doesn't have subpaths
      duResponse.setCount(0);
      // The object ID for the directory that the key is directly in
      long parentObjectId = bucketHandler.getDirObjectId(names, names.length - 1);
      String fileName = names[names.length - 1];
      String ozoneKey =
              omMetadataManager.getOzonePathKey(parentObjectId, fileName);
      OmKeyInfo keyInfo =
              omMetadataManager.getFileTable().getSkipCache(ozoneKey);
      duResponse.setSize(keyInfo.getDataSize());
      if (withReplica) {
        long keySizeWithReplica = bucketHandler.getKeySizeWithReplication(keyInfo);
        duResponse.setSizeWithReplica(keySizeWithReplica);
      }
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
      // key itself doesn't have file size distribution
      distResponse.setStatus(ResponseStatus.TYPE_NOT_APPLICABLE);
      return distResponse;
  }

}
