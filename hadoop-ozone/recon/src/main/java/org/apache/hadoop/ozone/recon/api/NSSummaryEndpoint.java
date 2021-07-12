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

package org.apache.hadoop.ozone.recon.api;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.BasicResponse;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * REST APIs for namespace metadata summary.
 */
@Path("/nssummary")
@Produces(MediaType.APPLICATION_JSON)
public class NSSummaryEndpoint {
  @Inject
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  /**
   * This endpoint will return the entity type and aggregate count of objects.
   * @param path in a format with leading slash, and without trailing slash
   * @return HTTP response with basic info: entity type, num of objects
   * @throws IOException IOE
   */
  @GET
  @Path("/basic")
  public Response getBasicInfo (
          @QueryParam("path") String path) throws IOException {

    String[] names = parseRequestPath(path);
    EntityType type = getEntityType(names);
    BasicResponse basicResponse = null;
    switch (type) {
    case VOLUME: {
      basicResponse = new BasicResponse(EntityType.VOLUME);
      List<OmBucketInfo> buckets = omMetadataManager.listBuckets(names[0],
              null, null, Integer.MAX_VALUE);
      basicResponse.setTotalBucket(buckets.size());
      int totalDir = 0;
      int totalKey = 0;
      for (OmBucketInfo bucket : buckets) {
        long bucketObjectId = bucket.getObjectID();
        totalDir += getTotalDirCount(bucketObjectId);
        totalKey += getTotalKeyCount(bucketObjectId);
      }
      basicResponse.setTotalDir(totalDir);
      basicResponse.setTotalKey(totalKey);
      break;
    }
    case BUCKET: {
      basicResponse = new BasicResponse(EntityType.BUCKET);
      assert (names.length == 2);
      long bucketObjectId = getBucketObjectId(names);
      basicResponse.setTotalDir(getTotalDirCount(bucketObjectId));
      basicResponse.setTotalKey(getTotalKeyCount(bucketObjectId));
      break;
    }
    case DIRECTORY: {
      // path should exist so we don't need any extra verification/null check
      assert (names.length >= 3);
      long dirObjectId = getDirObjectId(names);
      basicResponse = new BasicResponse(EntityType.DIRECTORY);
      basicResponse.setTotalDir(getTotalDirCount(dirObjectId));
      basicResponse.setTotalKey(getTotalKeyCount(dirObjectId));
      break;
    }
    case KEY:
      basicResponse = new BasicResponse(EntityType.KEY);
      break;
    case INVALID:
      basicResponse = new BasicResponse(EntityType.INVALID);
      basicResponse.setPathNotFound(true);
      break;
    }
    return Response.ok(basicResponse).build();
  }

  /**
   * DU endpoint.
   * @param path in a format with leading slash, and without trailing slash
   * @return DU response
   * @throws IOException
   */
  @GET
  @Path("/du")
  public Response getDiskUsage(@QueryParam("path") String path)
          throws IOException {
    String[] names = parseRequestPath(path);
    EntityType type = getEntityType(names);
    DUResponse duResponse = new DUResponse();
    switch (type) {
    case VOLUME: {
      String volName = names[0];
      List<OmBucketInfo> buckets = omMetadataManager.listBuckets(volName,
              null, null, Integer.MAX_VALUE);
      duResponse.setCount(buckets.size());
      List<DUResponse.DiskUsage> duData = new ArrayList<>();
      for (OmBucketInfo bucket : buckets) {
        String bucketName = bucket.getBucketName();
        long objectId = bucket.getObjectID();
        String subpath = omMetadataManager.getBucketKey(volName, bucketName);
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(subpath);
        long dataSize = getTotalSize(objectId);
        diskUsage.setSize(dataSize);
        duData.add(diskUsage);
      }
      duResponse.setDuData(duData);
      break;
    }
    case BUCKET: {
      long bucketObjectId = getBucketObjectId(names);
      NSSummary nsSummary =
              reconNamespaceSummaryManager.getNSSummary(bucketObjectId);
      List<Long> subdirs = nsSummary.getChildDir();
      duResponse.setCount(subdirs.size());
      List<DUResponse.DiskUsage> duData = new ArrayList<>();
      for (long subdirObjectId: subdirs) {
        NSSummary subdirNSSummary = reconNamespaceSummaryManager
                .getNSSummary(subdirObjectId);
        String dirName = subdirNSSummary.getDirName();
        String subpath = path + OM_KEY_PREFIX + dirName;

        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(subpath);
        long dataSize = getTotalSize(subdirObjectId);
        diskUsage.setSize(dataSize);
        duData.add(diskUsage);
      }
      duResponse.setDuData(duData);
      break;
    }
    case DIRECTORY: {
      long dirObjectId = getDirObjectId(names);
      NSSummary nsSummary =
              reconNamespaceSummaryManager.getNSSummary(dirObjectId);
      List<Long> subdirs = nsSummary.getChildDir();

      duResponse = new DUResponse();
      duResponse.setCount(subdirs.size());
      List<DUResponse.DiskUsage> duData = new ArrayList<>();
      for (long subdirObjectId: subdirs) {
        NSSummary dirNSSummary =
                reconNamespaceSummaryManager.getNSSummary(subdirObjectId);
        String subdirName = dirNSSummary.getDirName();
        // build the path for subdirectory
        String subpath = path + OM_KEY_PREFIX + subdirName;
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(subpath);
        long dataSize = getTotalSize(subdirObjectId);
        diskUsage.setSize(dataSize);
        duData.add(diskUsage);
      }
      duResponse.setDuData(duData);
      break;
    }
    case KEY: {
      // DU for key is the data size
      duResponse.setCount(1);
      DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
      String keyName = getKeyName(names);
      String ozoneKey = omMetadataManager
              .getOzoneKey(names[0], names[1], keyName);
      OmKeyInfo keyInfo = omMetadataManager.getKeyTable().get(ozoneKey);
      diskUsage.setSubpath(path);
      diskUsage.setSize(keyInfo.getDataSize());
      duResponse.setDuData(Collections.singletonList(diskUsage));
      break;
    }
    case INVALID:
      duResponse.setPathNotFound(true);
      break;
    }
    return Response.ok(duResponse).build();
  }

  /**
   * Quora usage endpoint that summarize the quota allowed and quota used in
   * bytes.
   * @param path in a format with leading slash, and without trailing slash
   * @return Quota Usage response
   * @throws IOException
   */
  @GET
  @Path("/qu")
  public Response getQuotaUsage(@QueryParam("path") String path)
          throws IOException {
    String[] names = parseRequestPath(path);
    EntityType type = getEntityType(names);
    QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
    if (type == EntityType.VOLUME) {
      List<OmBucketInfo> buckets = omMetadataManager.listBuckets(names[0],
              null, null, Integer.MAX_VALUE);
      String volKey = omMetadataManager.getVolumeKey(names[0]);
      OmVolumeArgs volumeArgs = omMetadataManager.getVolumeTable().get(volKey);
      long quotaInBytes = volumeArgs.getQuotaInBytes();
      long quotaUsedInBytes = 0L;
      for (OmBucketInfo bucketInfo: buckets) {
        long bucketObjectId = bucketInfo.getObjectID();
        quotaUsedInBytes += getTotalSize(bucketObjectId);
      }
      quotaUsageResponse.setQuota(quotaInBytes);
      quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
    } else if (type == EntityType.BUCKET) {
      String bucketKey = omMetadataManager.getBucketKey(names[0], names[1]);
      OmBucketInfo bucketInfo = omMetadataManager
              .getBucketTable().get(bucketKey);
      long bucketObjectId = bucketInfo.getObjectID();
      NSSummary nsSummary =
              reconNamespaceSummaryManager.getNSSummary(bucketObjectId);
      long quotaInBytes = bucketInfo.getQuotaInBytes();
      long quotaUsedInBytes = nsSummary.getSizeOfFiles() // direct keys' size
              + getTotalSize(bucketObjectId); //keys in all subdirectories
      quotaUsageResponse.setQuota(quotaInBytes);
      quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
    } else if (type == EntityType.INVALID) {
      quotaUsageResponse.setPathNotFound(true);
    } else {
      quotaUsageResponse.setNamespaceNotApplicable(true);
    }
    return Response.ok(quotaUsageResponse).build();
  }

  /**
   * Endpoint that returns aggregate file size distribution under a path
   * @param path in a format with leading slash, and without trailing slash
   * @return File size distribution response
   * @throws IOException
   */
  @GET
  @Path("/dist")
  public Response getFileSizeDistribution(@QueryParam("path") String path)
          throws IOException {
    String[] names = parseRequestPath(path);
    EntityType type = getEntityType(names);
    FileSizeDistributionResponse distReponse =
            new FileSizeDistributionResponse();
    switch (type) {
    case VOLUME: {
      List<OmBucketInfo> buckets = omMetadataManager.listBuckets(names[0],
              null, null, Integer.MAX_VALUE);
      int[] fileSizeDist = new int[ReconConstants.NUM_OF_BINS];
      for (OmBucketInfo bucket : buckets) {
        long bucketObjectId = bucket.getObjectID();
        int[] bucketSizeDist = getTotalFileSizeDist(bucketObjectId);
        for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
          fileSizeDist[i] += bucketSizeDist[i];
        }
      }
      distReponse.setFileSizeDist(fileSizeDist);
      break;
    }
    case BUCKET: {
      long objectId = getBucketObjectId(names);
      int[] fileSizeDist = getTotalFileSizeDist(objectId);
      distReponse.setFileSizeDist(fileSizeDist);
      break;
    }
    case DIRECTORY: {
      long dirObjectId = getDirObjectId(names);
      int[] fileSizeDist = getTotalFileSizeDist(dirObjectId);
      distReponse.setFileSizeDist(fileSizeDist);
      break;
    }
    case KEY:
      distReponse.setNamespaceNotApplicable(true);
      break;
    case INVALID:
      distReponse.setPathNotFound(true);
      break;
    }
    return Response.ok(distReponse).build();
  }

  /**
   * Return the entity type of client's request, check path existence.
   * If path doesn't exist, return Entity.INVALID
   * @param names the client's parsed request
   * @return the entity type, invalid if path not found
   */
  private EntityType getEntityType(String[] names) throws IOException {
    if (names.length == 0) {
      return EntityType.INVALID;
    } else if (names.length == 1) { // volume level check
      String volName = names[0];
      if (!checkVolumeExistence(volName)) {
        return EntityType.INVALID;
      }
      return EntityType.VOLUME;
    } else if (names.length == 2) { // bucket level check
      String volName = names[0];
      String bucketName = names[1];
      if (!checkBucketExistence(volName, bucketName)) {
        return EntityType.INVALID;
      }
      return EntityType.BUCKET;
    } else { // length > 3. check dir or key existence (FSO-enabled)
      // E.x. /vol1/buck1/a/b/c/d/e/file1.txt
      // keyName = a/b/c/d/e/file1.txt
      String volName = names[0];
      String bucketName = names[1];
      String keyName = getKeyName(names);
      if (!checkVolumeExistence(volName)
              || !checkBucketExistence(volName, bucketName)) {
        return EntityType.INVALID;
      }
      OzoneFileStatus fileStatus = null;
      omMetadataManager.getLock().acquireReadLock(BUCKET_LOCK, volName,
              bucketName);

      try {
        fileStatus = OMFileRequest.getOMKeyInfoIfExists(omMetadataManager,
                volName, bucketName, keyName, 0);
      } finally {
        omMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volName,
                bucketName);
      }
      if (fileStatus == null) { // path not exist
        return EntityType.INVALID;
      }
      // exist, but we need to distinguish dir from key
      if (fileStatus.isDirectory()) {
        return EntityType.DIRECTORY;
      }
      return EntityType.KEY;
    }
  }

  /**
   * Given a existent path, get the bucket object ID.
   * @param names valid path request
   * @return bucket objectID
   * @throws IOException
   */
  private long getBucketObjectId(String[] names) throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(names[0], names[1]);
    OmBucketInfo bucketInfo = omMetadataManager
            .getBucketTable().get(bucketKey);
    return bucketInfo.getObjectID();
  }

  /**
   * Given a valid path request for a directory,
   * return the directory object ID.
   * @param names parsed path request in a list of names
   * @return directory object ID
   */
  private long getDirObjectId(String[] names)
          throws IOException {
    long dirObjectId = getBucketObjectId(names);
    String dirKey = null;
    for (int i = 2; i < names.length; ++i) {
      dirKey = omMetadataManager.getOzonePathKey(dirObjectId, names[i]);
      OmDirectoryInfo dirInfo =
              omMetadataManager.getDirectoryTable().get(dirKey);
      dirObjectId = dirInfo.getObjectID();
    }
    return dirObjectId;
  }

  @VisibleForTesting
  public static String[] parseRequestPath(String path) {
    if (path.startsWith(OM_KEY_PREFIX)) {
      path = path.substring(1);
    }
    String[] names = path.split(OM_KEY_PREFIX);
    return names;
  }

  @VisibleForTesting
  public static String getKeyName(String[] names) {
    String[] keyArr = Arrays.copyOfRange(names, 2, names.length);
    return String.join(OM_KEY_PREFIX, keyArr);
  }

  private boolean checkVolumeExistence(String volName) throws IOException {
    assert (omMetadataManager != null);
    String volDBKey = omMetadataManager.getVolumeKey(volName);
    if (omMetadataManager.getVolumeTable().get(volDBKey) == null) {
      return false;
    }
    return true;
  }

  private boolean checkBucketExistence(String volName, String bucketName)
          throws IOException {
    String bucketDBKey = omMetadataManager.getBucketKey(volName, bucketName);
    // Check if bucket exists
    if (omMetadataManager.getBucketTable().get(bucketDBKey) == null) {
      return false;
    }
    return true;
  }

  private int getTotalKeyCount(long objectId) throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0;
    }
    int totalCnt = nsSummary.getNumOfFiles();
    for (long childId: nsSummary.getChildDir()) {
      totalCnt += getTotalKeyCount(childId);
    }
    return totalCnt;
  }

  private int getTotalDirCount(long objectId) throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0;
    }
    List<Long> subdirs = nsSummary.getChildDir();
    int totalCnt = subdirs.size();
    for (long subdir: subdirs) {
      totalCnt += getTotalDirCount(subdir);
    }
    return totalCnt;
  }

  private long getTotalSize(long objectId) throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0L;
    }
    long totalSize = nsSummary.getSizeOfFiles();
    for (long childId: nsSummary.getChildDir()) {
      totalSize += getTotalSize(childId);
    }
    return totalSize;
  }

  private int[] getTotalFileSizeDist(long objectId) throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return new int[ReconConstants.NUM_OF_BINS];
    }
    int[] res = nsSummary.getFileSizeBucket();
    for (long childId: nsSummary.getChildDir()) {
      int[] subDirFileSizeDist = getTotalFileSizeDist(childId);
      for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
        res[i] += subDirFileSizeDist[i];
      }
    }
    return res;
  }
}
