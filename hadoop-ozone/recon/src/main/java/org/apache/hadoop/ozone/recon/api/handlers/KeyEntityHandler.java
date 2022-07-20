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
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;

/**
 * Class for handling key entity type.
 */
public class KeyEntityHandler extends EntityHandler {
  public KeyEntityHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      BucketHandler bucketHandler, String path) {
    super(reconNamespaceSummaryManager, omMetadataManager,
          reconSCM, bucketHandler, path);
  }

  @Override
  public NamespaceSummaryResponse getSummaryResponse()
          throws IOException {
    NamespaceSummaryResponse namespaceSummaryResponse =
            new NamespaceSummaryResponse(EntityType.KEY);

    return namespaceSummaryResponse;
  }

  @Override
  public DUResponse getDuResponse(
          boolean listFile, boolean withReplica)
          throws IOException {
    DUResponse duResponse = new DUResponse();
    duResponse.setPath(getNormalizedPath());
    // DU for key doesn't have subpaths
    duResponse.setCount(0);
    OmKeyInfo keyInfo = getBucketHandler().getKeyInfo(getNames());

    duResponse.setSize(keyInfo.getDataSize());
    if (withReplica) {
      long keySizeWithReplica = getBucketHandler()
              .getKeySizeWithReplication(keyInfo);
      duResponse.setSizeWithReplica(keySizeWithReplica);
    }
    return duResponse;
  }

  @Override
  public QuotaUsageResponse getQuotaResponse()
          throws IOException {
    QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
    quotaUsageResponse.setResponseCode(
            ResponseStatus.TYPE_NOT_APPLICABLE);
    return quotaUsageResponse;
  }

  @Override
  public FileSizeDistributionResponse getDistResponse()
          throws IOException {
    FileSizeDistributionResponse distResponse =
            new FileSizeDistributionResponse();
    // key itself doesn't have file size distribution
    distResponse.setStatus(ResponseStatus.TYPE_NOT_APPLICABLE);
    return distResponse;
  }

}
