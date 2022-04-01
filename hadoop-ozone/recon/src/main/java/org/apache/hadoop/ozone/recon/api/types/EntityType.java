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
    @Override
    public NamespaceSummaryResponse getSummaryResponse(String[] names, EntityUtils entityUtils) throws IOException {
      NamespaceSummaryResponse namespaceSummaryResponse = new NamespaceSummaryResponse(EntityType.ROOT);
      List<OmVolumeArgs> volumes = entityUtils.listVolumes();
      namespaceSummaryResponse.setNumVolume(volumes.size());
      List<OmBucketInfo> allBuckets = entityUtils.listBucketsUnderVolume(null);
      namespaceSummaryResponse.setNumBucket(allBuckets.size());
      int totalNumDir = 0;
      long totalNumKey = 0L;
      for (OmBucketInfo bucket : allBuckets) {
        long bucketObjectId = bucket.getObjectID();
        totalNumDir += entityUtils.getTotalDirCount(bucketObjectId);
        totalNumKey += entityUtils.getTotalKeyCount(bucketObjectId);
      }

      namespaceSummaryResponse.setNumTotalDir(totalNumDir);
      namespaceSummaryResponse.setNumTotalKey(totalNumKey);

      return namespaceSummaryResponse;
    }

    @Override
    public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                    boolean withReplica, EntityUtils entityUtils) throws IOException {
      DUResponse duResponse = new DUResponse();
      ReconOMMetadataManager omMetadataManager = entityUtils.getOmMetadataManager();
      List<OmVolumeArgs> volumes = entityUtils.listVolumes();
      duResponse.setCount(volumes.size());

      List<DUResponse.DiskUsage> volumeDuData = new ArrayList<>();
      long totalDataSize = 0L;
      long totalDataSizeWithReplica = 0L;
      for (OmVolumeArgs volume: volumes) {
        String volumeName = volume.getVolume();
        String subpath = omMetadataManager.getVolumeKey(volumeName);
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        long dataSize = 0;
        diskUsage.setSubpath(subpath);
        // iterate all buckets per volume to get total data size
        for (OmBucketInfo bucket: entityUtils.listBucketsUnderVolume(volumeName)) {
          long bucketObjectID = bucket.getObjectID();
          dataSize += entityUtils.getTotalSize(bucketObjectID);
        }
        totalDataSize += dataSize;

        // count replicas
        // TODO: to be dropped or optimized in the future
        if (withReplica) {
          long volumeDU = entityUtils.calculateDUForVolume(volumeName);
          totalDataSizeWithReplica += volumeDU;
          diskUsage.setSizeWithReplica(volumeDU);
        }
        diskUsage.setSize(dataSize);
        volumeDuData.add(diskUsage);
      }
      if (withReplica) {
        duResponse.setSizeWithReplica(totalDataSizeWithReplica);
      }
      duResponse.setSize(totalDataSize);
      duResponse.setDuData(volumeDuData);

      return duResponse;
    }

    @Override
    public QuotaUsageResponse getQuotaResponse(String[] names, EntityUtils entityUtils) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      List<OmVolumeArgs> volumes = entityUtils.listVolumes();
      List<OmBucketInfo> buckets = entityUtils.listBucketsUnderVolume(null);
      long quotaInBytes = 0L;
      long quotaUsedInBytes = 0L;

      for (OmVolumeArgs volume: volumes) {
        final long quota = volume.getQuotaInBytes();
        assert (quota >= -1L);
        if (quota == -1L) {
          // If one volume has unlimited quota, the "root" quota is unlimited.
          quotaInBytes = -1L;
          break;
        }
        quotaInBytes += quota;
      }
      for (OmBucketInfo bucket: buckets) {
        long bucketObjectId = bucket.getObjectID();
        quotaUsedInBytes += entityUtils.getTotalSize(bucketObjectId);
      }

      quotaUsageResponse.setQuota(quotaInBytes);
      quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
      return quotaUsageResponse;
    }

    @Override
    public FileSizeDistributionResponse getDistResponse(String[] names, EntityUtils entityUtils) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
      List<OmBucketInfo> allBuckets = entityUtils.listBucketsUnderVolume(null);
      int[] fileSizeDist = new int[ReconConstants.NUM_OF_BINS];

      // accumulate file size distribution arrays from all buckets
      for (OmBucketInfo bucket : allBuckets) {
        long bucketObjectId = bucket.getObjectID();
        int[] bucketFileSizeDist = entityUtils.getTotalFileSizeDist(bucketObjectId);
        // add on each bin
        for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
          fileSizeDist[i] += bucketFileSizeDist[i];
        }
      }
      distResponse.setFileSizeDist(fileSizeDist);
      return distResponse;
    }
  },
  VOLUME {
    @Override
    public NamespaceSummaryResponse getSummaryResponse(String[] names, EntityUtils entityUtils) throws IOException {
      NamespaceSummaryResponse namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.VOLUME);
      List<OmBucketInfo> buckets = entityUtils.listBucketsUnderVolume(names[0]);
      namespaceSummaryResponse.setNumBucket(buckets.size());
      int totalDir = 0;
      long totalKey = 0L;

      // iterate all buckets to collect the total object count.
      for (OmBucketInfo bucket : buckets) {
        long bucketObjectId = bucket.getObjectID();
        totalDir += entityUtils.getTotalDirCount(bucketObjectId);
        totalKey += entityUtils.getTotalKeyCount(bucketObjectId);
      }

      namespaceSummaryResponse.setNumTotalDir(totalDir);
      namespaceSummaryResponse.setNumTotalKey(totalKey);

      return namespaceSummaryResponse;
    }

    @Override
    public DUResponse getDuResponse(String path, String[] names, boolean listFile,
                                    boolean withReplica, EntityUtils entityUtils) throws IOException {
      DUResponse duResponse = new DUResponse();
      ReconOMMetadataManager omMetadataManager = entityUtils.getOmMetadataManager();
      String volName = names[0];
      List<OmBucketInfo> buckets = entityUtils.listBucketsUnderVolume(volName);
      duResponse.setCount(buckets.size());

      // List of DiskUsage data for all buckets
      List<DUResponse.DiskUsage> bucketDuData = new ArrayList<>();
      long volDataSize = 0L;
      long volDataSizeWithReplica = 0L;
      for (OmBucketInfo bucket: buckets) {
        String bucketName = bucket.getBucketName();
        long bucketObjectID = bucket.getObjectID();
        String subpath = omMetadataManager.getBucketKey(volName, bucketName);
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(subpath);
        long dataSize = entityUtils.getTotalSize(bucketObjectID);
        volDataSize += dataSize;
        if (withReplica) {
          long bucketDU = entityUtils.calculateDUUnderObject(bucketObjectID);
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
    public QuotaUsageResponse getQuotaResponse(String[] names, EntityUtils entityUtils) throws IOException {
      QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
      ReconOMMetadataManager omMetadataManager = entityUtils.getOmMetadataManager();
      List<OmBucketInfo> buckets = entityUtils.listBucketsUnderVolume(names[0]);
      String volKey = omMetadataManager.getVolumeKey(names[0]);
      OmVolumeArgs volumeArgs =
              omMetadataManager.getVolumeTable().getSkipCache(volKey);
      long quotaInBytes = volumeArgs.getQuotaInBytes();
      long quotaUsedInBytes = 0L;

      // Get the total data size used by all buckets
      for (OmBucketInfo bucketInfo: buckets) {
        long bucketObjectId = bucketInfo.getObjectID();
        quotaUsedInBytes += entityUtils.getTotalSize(bucketObjectId);
      }
      quotaUsageResponse.setQuota(quotaInBytes);
      quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
      return quotaUsageResponse;
    }

    @Override
    public FileSizeDistributionResponse getDistResponse(String[] names, EntityUtils entityUtils) throws IOException {
      FileSizeDistributionResponse distResponse =
              new FileSizeDistributionResponse();
      List<OmBucketInfo> buckets = entityUtils.listBucketsUnderVolume(names[0]);
      int[] volumeFileSizeDist = new int[ReconConstants.NUM_OF_BINS];

      // accumulate file size distribution arrays from all buckets under volume
      for (OmBucketInfo bucket : buckets) {
        long bucketObjectId = bucket.getObjectID();
        int[] bucketFileSizeDist = entityUtils.getTotalFileSizeDist(bucketObjectId);
        // add on each bin
        for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
          volumeFileSizeDist[i] += bucketFileSizeDist[i];
        }
      }
      distResponse.setFileSizeDist(volumeFileSizeDist);
      return distResponse;
    }
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
