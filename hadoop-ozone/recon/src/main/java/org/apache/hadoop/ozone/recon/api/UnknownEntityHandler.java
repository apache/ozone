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

public class UnknownEntityHandler extends EntityHandler {
  public UnknownEntityHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM) {
    super(reconNamespaceSummaryManager, omMetadataManager, reconSCM, null);
  }

  @Override
  public NamespaceSummaryResponse getSummaryResponse(String[] names) throws
      IOException {
      NamespaceSummaryResponse namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.UNKNOWN);
      namespaceSummaryResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);

      return namespaceSummaryResponse;
  }

  @Override
  public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                  boolean withReplica) throws IOException {
      DUResponse duResponse = new DUResponse();
      duResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);

      return duResponse;
  }

  @Override
  public QuotaUsageResponse getQuotaResponse(String[] names) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      quotaUsageResponse.setResponseCode(ResponseStatus.PATH_NOT_FOUND);

      return quotaUsageResponse;
  }

  @Override
  public FileSizeDistributionResponse getDistResponse(String[] names) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
      distResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);
      return distResponse;
  }

}
