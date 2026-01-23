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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.recon.api.types.CountStats;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.ObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

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
    CountStats countStats = new CountStats(
        -1, -1,
        getTotalDirCount(dirObjectId), getTotalKeyCount(dirObjectId));
    return NamespaceSummaryResponse.newBuilder()
        .setEntityType(EntityType.DIRECTORY)
        .setCountStats(countStats)
        .setObjectDBInfo(getDirectoryObjDbInfo(getNames()))
        .setStatus(ResponseStatus.OK)
        .build();
  }

  private ObjectDBInfo getDirectoryObjDbInfo(String[] names)
      throws IOException {
    OmDirectoryInfo omDirectoryInfo = getBucketHandler().getDirInfo(names);
    if (null == omDirectoryInfo) {
      return new ObjectDBInfo();
    }
    return new ObjectDBInfo(omDirectoryInfo);
  }

  @Override
  public DUResponse getDuResponse(
      boolean listFile, boolean withReplica, boolean sortSubPaths)
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
    if (withReplica) {
      duResponse.setSizeWithReplica(dirNSSummary.getReplicatedSizeOfFiles());
    }
    List<DUResponse.DiskUsage> subdirDUData = new ArrayList<>();
    // iterate all subdirectories to get disk usage data
    for (long subdirObjectId: subdirs) {
      NSSummary subdirNSSummary =
              getReconNamespaceSummaryManager().getNSSummary(subdirObjectId);
      // for the subdirName we need the subdir filename, not the key name
      // Eg. /vol/bucket1/dir1/dir2,
      // key name is /dir1/dir2
      // we need to get dir2
      Path subdirPath = Paths.get(subdirNSSummary.getDirName());
      Path subdirFileName = subdirPath.getFileName();
      String subdirName;
      // Checking for null to get rid of a findbugs error and
      // then throwing the NPException to avoid swallowing it.
      // Error: Possible null pointer dereference in
      // ...DirectoryEntityHandler.getDuResponse(boolean, boolean) due to
      // return value of called method Dereferenced at DirectoryEntityHandler
      if (subdirFileName != null) {
        subdirName = subdirFileName.toString();
      } else {
        throw new NullPointerException("Subdirectory file name is null.");
      }
      // build the path for subdirectory
      String subpath = BucketHandler
              .buildSubpath(getNormalizedPath(), subdirName);
      DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
      // reformat the response
      diskUsage.setSubpath(subpath);
      if (withReplica) {
        diskUsage.setSizeWithReplica(subdirNSSummary.getReplicatedSizeOfFiles());
      }

      diskUsage.setSize(subdirNSSummary.getSizeOfFiles());
      subdirDUData.add(diskUsage);
    }
    if (listFile || withReplica) {
      getBucketHandler().handleDirectKeys(dirObjectId, withReplica,
              listFile, subdirDUData, getNormalizedPath());
    }

    duResponse.setCount(subdirDUData.size());
    duResponse.setSize(dirDataSize);

    if (sortSubPaths) {
      // Parallel sort subdirDUData in descending order of size and returns the top N elements.
      subdirDUData = sortDiskUsageDescendingWithLimit(subdirDUData,
          DISK_USAGE_TOP_RECORDS_LIMIT);
    }

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
