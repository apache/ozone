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
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;

import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Class for handling directory entity type.
 */
public class DirectoryEntityHandler extends EntityHandler {

  public DirectoryEntityHandler(
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
    // path should exist so we don't need any extra verification/null check
    long dirObjectId = getBucketHandler().getDirObjectId(getNames());
    NamespaceSummaryResponse namespaceSummaryResponse =
            new NamespaceSummaryResponse(EntityType.DIRECTORY);
    namespaceSummaryResponse
        .setNumTotalDir(getTotalDirCount(dirObjectId));
    namespaceSummaryResponse.setNumTotalKey(getTotalKeyCount(dirObjectId));

    return namespaceSummaryResponse;
  }

  @Override
  public DUResponse getDuResponse(
          boolean listFile, boolean withReplica)
          throws IOException {
    DUResponse duResponse = new DUResponse();
    duResponse.setPath(getNormalizedPath());
    long dirObjectId = getBucketHandler().getDirObjectId(getNames());
    NSSummary dirNSSummary =
            getReconNamespaceSummaryManager().getNSSummary(dirObjectId);
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
              getReconNamespaceSummaryManager().getNSSummary(subdirObjectId);
      String subdirName = subdirNSSummary.getDirName();
      // build the path for subdirectory
      String subpath = BucketHandler
              .buildSubpath(getNormalizedPath(), subdirName);
      DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
      // reformat the response
      diskUsage.setSubpath(subpath);
      long dataSize = getTotalSize(subdirObjectId);
      dirDataSize += dataSize;

      if (withReplica) {
        long subdirDU = getBucketHandler()
                .calculateDUUnderObject(subdirObjectId);
        diskUsage.setSizeWithReplica(subdirDU);
        dirDataSizeWithReplica += subdirDU;
      }

      diskUsage.setSize(dataSize);
      subdirDUData.add(diskUsage);
    }

    // handle direct keys under directory
    if (listFile || withReplica) {
      dirDataSizeWithReplica += getBucketHandler()
              .handleDirectKeys(dirObjectId, withReplica,
                  listFile, subdirDUData, getNormalizedPath());
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
    long dirObjectId = getBucketHandler().getDirObjectId(getNames());
    int[] dirFileSizeDist = getTotalFileSizeDist(dirObjectId);
    distResponse.setFileSizeDist(dirFileSizeDist);
    return distResponse;
  }

}
