/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Enum class for namespace type.
 */
public enum EntityType {
  ROOT {
  },
  VOLUME {
  },
  BUCKET {
    @Override
    public NamespaceSummaryResponse getSummaryResponse(String[] names, EntityUtils entityUtils) throws IOException {
      NamespaceSummaryResponse namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.BUCKET);
      assert (names.length == 2);
      long bucketObjectId = entityUtils.getBucketObjectId(names);
      namespaceSummaryResponse.setNumTotalDir(entityUtils.getTotalDirCount(bucketObjectId));
      namespaceSummaryResponse.setNumTotalKey(entityUtils.getTotalKeyCount(bucketObjectId));

      return namespaceSummaryResponse;
    }

    @Override
    public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                    boolean withReplica, EntityUtils entityUtils) throws IOException {
      DUResponse duResponse = new DUResponse();
      long bucketObjectId = entityUtils.getBucketObjectId(names);
      ReconNamespaceSummaryManager reconNamespaceSummaryManager =
              entityUtils.getReconNamespaceSummaryManager();
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
        String subpath = EntityUtils.buildSubpath(path, dirName);
        // we need to reformat the subpath in the response in a
        // format with leading slash and without trailing slash
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(subpath);
        long dataSize = entityUtils.getTotalSize(subdirObjectId);
        bucketDataSize += dataSize;

        if (withReplica) {
          long dirDU = entityUtils.calculateDUUnderObject(subdirObjectId);
          diskUsage.setSizeWithReplica(dirDU);
          bucketDataSizeWithReplica += dirDU;
        }
        diskUsage.setSize(dataSize);
        dirDUData.add(diskUsage);
      }
      // Either listFile or withReplica is enabled, we need the directKeys info
      if (listFile || withReplica) {
        bucketDataSizeWithReplica += entityUtils.handleDirectKeys(bucketObjectId,
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
    public QuotaUsageResponse getQuotaResponse(String[] names, EntityUtils entityUtils) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      ReconOMMetadataManager omMetadataManager = entityUtils.getOmMetadataManager();
      String bucketKey = omMetadataManager.getBucketKey(names[0], names[1]);
      OmBucketInfo bucketInfo = omMetadataManager
              .getBucketTable().getSkipCache(bucketKey);
      long bucketObjectId = bucketInfo.getObjectID();
      long quotaInBytes = bucketInfo.getQuotaInBytes();
      long quotaUsedInBytes = entityUtils.getTotalSize(bucketObjectId);
      quotaUsageResponse.setQuota(quotaInBytes);
      quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
      return quotaUsageResponse;
    }

    @Override
    public FileSizeDistributionResponse getDistResponse(String[] names, EntityUtils entityUtils) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
      long bucketObjectId = entityUtils.getBucketObjectId(names);
      int[] bucketFileSizeDist = entityUtils.getTotalFileSizeDist(bucketObjectId);
      distResponse.setFileSizeDist(bucketFileSizeDist);
      return distResponse;
    }
  },
  DIRECTORY {
    @Override
    public NamespaceSummaryResponse getSummaryResponse(String[] names, EntityUtils entityUtils) throws IOException {
      // path should exist so we don't need any extra verification/null check
      long dirObjectId = entityUtils.getDirObjectId(names);
      NamespaceSummaryResponse namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.DIRECTORY);
      namespaceSummaryResponse.setNumTotalDir(entityUtils.getTotalDirCount(dirObjectId));
      namespaceSummaryResponse.setNumTotalKey(entityUtils.getTotalKeyCount(dirObjectId));

      return namespaceSummaryResponse;
    }

    @Override
    public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                    boolean withReplica, EntityUtils entityUtils) throws IOException {
      DUResponse duResponse = new DUResponse();
      long dirObjectId = entityUtils.getDirObjectId(names);
      ReconNamespaceSummaryManager reconNamespaceSummaryManager =
              entityUtils.getReconNamespaceSummaryManager();
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
        String subpath = EntityUtils.buildSubpath(path, subdirName);
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        // reformat the response
        diskUsage.setSubpath(subpath);
        long dataSize = entityUtils.getTotalSize(subdirObjectId);
        dirDataSize += dataSize;

        if (withReplica) {
          long subdirDU = entityUtils.calculateDUUnderObject(subdirObjectId);
          diskUsage.setSizeWithReplica(subdirDU);
          dirDataSizeWithReplica += subdirDU;
        }

        diskUsage.setSize(dataSize);
        subdirDUData.add(diskUsage);
      }

      // handle direct keys under directory
      if (listFile || withReplica) {
        dirDataSizeWithReplica += entityUtils.handleDirectKeys(dirObjectId, withReplica,
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
    public QuotaUsageResponse getQuotaResponse(String[] names, EntityUtils entityUtils) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      quotaUsageResponse.setResponseCode(
              ResponseStatus.TYPE_NOT_APPLICABLE);
      return quotaUsageResponse;
    }

    @Override
    public FileSizeDistributionResponse getDistResponse(String[] names, EntityUtils entityUtils) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
      long dirObjectId = entityUtils.getDirObjectId(names);
      int[] dirFileSizeDist = entityUtils.getTotalFileSizeDist(dirObjectId);
      distResponse.setFileSizeDist(dirFileSizeDist);
      return distResponse;
    }
  },
  KEY {
    @Override
    public NamespaceSummaryResponse getSummaryResponse(String[] names, EntityUtils entityUtils) throws IOException {
      NamespaceSummaryResponse namespaceSummaryResponse = new NamespaceSummaryResponse(EntityType.KEY);

      return namespaceSummaryResponse;
    }

    @Override
    public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                    boolean withReplica, EntityUtils entityUtils) throws IOException {
      DUResponse duResponse = new DUResponse();
      // DU for key doesn't have subpaths
      duResponse.setCount(0);
      // The object ID for the directory that the key is directly in
      long parentObjectId = entityUtils.getDirObjectId(names, names.length - 1);
      String fileName = names[names.length - 1];
      ReconOMMetadataManager omMetadataManager = entityUtils.getOmMetadataManager();
      String ozoneKey =
              omMetadataManager.getOzonePathKey(parentObjectId, fileName);
      OmKeyInfo keyInfo =
              omMetadataManager.getFileTable().getSkipCache(ozoneKey);
      duResponse.setSize(keyInfo.getDataSize());
      if (withReplica) {
        long keySizeWithReplica = entityUtils.getKeySizeWithReplication(keyInfo);
        duResponse.setSizeWithReplica(keySizeWithReplica);
      }
      return duResponse;
    }

    @Override
    public QuotaUsageResponse getQuotaResponse(String[] names, EntityUtils entityUtils) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      quotaUsageResponse.setResponseCode(
              ResponseStatus.TYPE_NOT_APPLICABLE);
      return quotaUsageResponse;
    }

    @Override
    public FileSizeDistributionResponse getDistResponse(String[] names, EntityUtils entityUtils) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
      // key itself doesn't have file size distribution
      distResponse.setStatus(ResponseStatus.TYPE_NOT_APPLICABLE);
      return distResponse;
    }
  },
  UNKNOWN { // if path is invalid
    @Override
    public NamespaceSummaryResponse getSummaryResponse(String[] names, EntityUtils entityUtils) throws IOException {
      NamespaceSummaryResponse namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.UNKNOWN);
      namespaceSummaryResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);

      return namespaceSummaryResponse;
    }

    @Override
    public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                    boolean withReplica, EntityUtils entityUtils) throws IOException {
      DUResponse duResponse = new DUResponse();
      duResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);

      return duResponse;
    }

    @Override
    public QuotaUsageResponse getQuotaResponse(String[] names, EntityUtils entityUtils) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      quotaUsageResponse.setResponseCode(ResponseStatus.PATH_NOT_FOUND);

      return quotaUsageResponse;
    }

    @Override
    public FileSizeDistributionResponse getDistResponse(String[] names, EntityUtils entityUtils) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
      distResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);
      return distResponse;
    }
  };

  abstract public NamespaceSummaryResponse getSummaryResponse(String[] names, EntityUtils entityUtils) throws IOException;
  abstract public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                           boolean withReplica, EntityUtils entityUtils) throws IOException;
  abstract public QuotaUsageResponse getQuotaResponse(String[] names, EntityUtils entityUtils) throws IOException;
  abstract public FileSizeDistributionResponse getDistResponse(String[] names, EntityUtils entityUtils) throws IOException;

}
