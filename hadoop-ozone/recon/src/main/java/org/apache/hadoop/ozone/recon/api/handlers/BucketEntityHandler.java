/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.handlers;

import static org.apache.hadoop.ozone.recon.ReconConstants.DISK_USAGE_TOP_RECORDS_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconUtils.sortDiskUsageDescendingWithLimit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.recon.api.types.BucketObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.CountStats;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

/**
 * Class for handling bucket entity type.
 */
public class BucketEntityHandler extends EntityHandler {
  public BucketEntityHandler(
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

    String[] names = getNames();
    assert (names.length == 2);
    long bucketObjectId = getBucketHandler().getBucketObjectId(names);

    CountStats countStats = new CountStats(
        -1, -1,
        getTotalDirCount(bucketObjectId), getTotalKeyCount(bucketObjectId));
    return NamespaceSummaryResponse.newBuilder()
        .setEntityType(EntityType.BUCKET)
        .setCountStats(countStats)
        .setObjectDBInfo(getBucketObjDbInfo(names))
        .setStatus(ResponseStatus.OK)
        .build();
  }

  private BucketObjectDBInfo getBucketObjDbInfo(String[] names)
      throws IOException {
    String volName = names[0];
    String bucketName = names[1];
    String bucketKey = getOmMetadataManager().
        getBucketKey(volName, bucketName);
    if (null == bucketKey) {
      return new BucketObjectDBInfo();
    }
    OmBucketInfo omBucketInfo = getOmMetadataManager()
        .getBucketTable().getSkipCache(bucketKey);
    if (null == omBucketInfo) {
      return new BucketObjectDBInfo();
    }
    return new BucketObjectDBInfo(omBucketInfo);
  }

  @Override
  public DUResponse getDuResponse(
      boolean listFile, boolean withReplica, boolean sortSubpaths)
          throws IOException {
    DUResponse duResponse = new DUResponse();
    duResponse.setPath(getNormalizedPath());
    long bucketObjectId = getBucketHandler().getBucketObjectId(getNames());
    NSSummary bucketNSSummary =
            getReconNamespaceSummaryManager().getNSSummary(bucketObjectId);
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
    long bucketDataSize = bucketNSSummary.getSizeOfFiles();
    if (withReplica) {
      duResponse.setSizeWithReplica(bucketNSSummary.getReplicatedSizeOfFiles());
    }
    for (long subdirObjectId: bucketSubdirs) {
      NSSummary subdirNSSummary = getReconNamespaceSummaryManager()
              .getNSSummary(subdirObjectId);

      // get directory's name and generate the next-level subpath.
      String dirName = subdirNSSummary.getDirName();
      String subpath = BucketHandler.buildSubpath(getNormalizedPath(), dirName);
      // we need to reformat the subpath in the response in a
      // format with leading slash and without trailing slash
      DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
      diskUsage.setSubpath(subpath);

      if (withReplica) {
        diskUsage.setSizeWithReplica(subdirNSSummary.getReplicatedSizeOfFiles());
      }
      diskUsage.setSize(subdirNSSummary.getSizeOfFiles());
      dirDUData.add(diskUsage);
    }
    if (listFile || withReplica) {
      getBucketHandler().handleDirectKeys(bucketObjectId, withReplica,
          listFile, dirDUData, getNormalizedPath());
    }
    duResponse.setCount(dirDUData.size());
    duResponse.setSize(bucketDataSize);

    if (sortSubpaths) {
      // Parallel sort directory/files DU data in descending order of size and returns the top N elements.
      dirDUData = sortDiskUsageDescendingWithLimit(dirDUData,
          DISK_USAGE_TOP_RECORDS_LIMIT);
    }

    duResponse.setDuData(dirDUData);

    return duResponse;
  }

  @Override
  public QuotaUsageResponse getQuotaResponse()
          throws IOException {
    QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
    String[] names = getNames();
    String bucketKey = getOmMetadataManager().getBucketKey(names[0], names[1]);
    OmBucketInfo bucketInfo = getOmMetadataManager()
            .getBucketTable().getSkipCache(bucketKey);
    long bucketObjectId = bucketInfo.getObjectID();
    long quotaInBytes = bucketInfo.getQuotaInBytes();
    long quotaUsedInBytes = getTotalSize(bucketObjectId);
    quotaUsageResponse.setQuota(quotaInBytes);
    quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
    return quotaUsageResponse;
  }

  @Override
  public FileSizeDistributionResponse getDistResponse()
          throws IOException {
    FileSizeDistributionResponse distResponse =
            new FileSizeDistributionResponse();
    long bucketObjectId = getBucketHandler().getBucketObjectId(getNames());
    int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
    distResponse.setFileSizeDist(bucketFileSizeDist);
    return distResponse;
  }

}
