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
import com.google.common.base.Strings;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.BasicResponse;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.NamespaceResponseCode;
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
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.removeTrailingSlashIfNeeded;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * REST APIs for namespace metadata summary.
 */
@Path("/namespace")
@Produces(MediaType.APPLICATION_JSON)
public class NSSummaryEndpoint {
  @Inject
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  /**
   * This endpoint will return the entity type and aggregate count of objects.
   * @param path the request path.
   * @return HTTP response with basic info: entity type, num of objects
   * @throws IOException IOE
   */
  @GET
  @Path("/summary")
  public Response getBasicInfo(
          @QueryParam("path") String path) throws IOException {

    String[] names = parseRequestPath(path);
    EntityType type = getEntityType(names);

    BasicResponse basicResponse = null;
    switch (type) {
    case VOLUME:
      basicResponse = new BasicResponse(EntityType.VOLUME);
      List<OmBucketInfo> buckets = listBucketsUnderVolume(names[0]);
      basicResponse.setNumTotalBucket(buckets.size());
      int totalDir = 0;
      int totalKey = 0;

      // iterate all buckets to collect the total object count.
      for (OmBucketInfo bucket : buckets) {
        long bucketObjectId = bucket.getObjectID();
        totalDir += getTotalDirCount(bucketObjectId);
        totalKey += getTotalKeyCount(bucketObjectId);
      }
      basicResponse.setNumTotalDir(totalDir);
      basicResponse.setNumTotalKey(totalKey);
      break;
    case BUCKET:
      basicResponse = new BasicResponse(EntityType.BUCKET);
      assert (names.length == 2);
      long bucketObjectId = getBucketObjectId(names);
      basicResponse.setNumTotalDir(getTotalDirCount(bucketObjectId));
      basicResponse.setNumTotalKey(getTotalKeyCount(bucketObjectId));
      break;
    case DIRECTORY:
      // path should exist so we don't need any extra verification/null check
      long dirObjectId = getDirObjectId(names);
      basicResponse = new BasicResponse(EntityType.DIRECTORY);
      basicResponse.setNumTotalDir(getTotalDirCount(dirObjectId));
      basicResponse.setNumTotalKey(getTotalKeyCount(dirObjectId));
      break;
    case KEY:
      basicResponse = new BasicResponse(EntityType.KEY);
      break;
    case UNKNOWN:
      basicResponse = new BasicResponse(EntityType.UNKNOWN);
      basicResponse.setStatus(NamespaceResponseCode.PATH_NOT_FOUND);
      break;
    default:
      break;
    }
    return Response.ok(basicResponse).build();
  }

  /**
   * DU endpoint to return datasize for subdirectory (bucket for volume).
   * @param path request path
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
    case VOLUME:
      String volName = names[0];
      List<OmBucketInfo> buckets = listBucketsUnderVolume(volName);
      duResponse.setCount(buckets.size());

      // List of DiskUsage data for all buckets
      List<DUResponse.DiskUsage> bucketDuData = new ArrayList<>();
      for (OmBucketInfo bucket : buckets) {
        String bucketName = bucket.getBucketName();
        long bucketObjectID = bucket.getObjectID();
        String subpath = omMetadataManager.getBucketKey(volName, bucketName);
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(subpath);
        long dataSize = getTotalSize(bucketObjectID);
        diskUsage.setSize(dataSize);
        bucketDuData.add(diskUsage);
      }
      duResponse.setDuData(bucketDuData);
      break;
    case BUCKET:
      long bucketObjectId = getBucketObjectId(names);
      NSSummary bucketNSSummary =
              reconNamespaceSummaryManager.getNSSummary(bucketObjectId);

      // get object IDs for all its subdirectories
      Set<Long> bucketSubdirs = bucketNSSummary.getChildDir();
      duResponse.setCount(bucketSubdirs.size());
      List<DUResponse.DiskUsage> dirDUData = new ArrayList<>();
      for (long subdirObjectId: bucketSubdirs) {
        NSSummary subdirNSSummary = reconNamespaceSummaryManager
                .getNSSummary(subdirObjectId);

        // get directory's name and generate the next-level subpath.
        String dirName = subdirNSSummary.getDirName();
        String subpath = path + OM_KEY_PREFIX + dirName;
        // we need to reformat the subpath in the response in a
        // format with leading slash and without trailing slash
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(reformatString(subpath));
        long dataSize = getTotalSize(subdirObjectId);
        diskUsage.setSize(dataSize);
        dirDUData.add(diskUsage);
      }
      duResponse.setDuData(dirDUData);
      break;
    case DIRECTORY:
      long dirObjectId = getDirObjectId(names);
      NSSummary dirNSSummary =
              reconNamespaceSummaryManager.getNSSummary(dirObjectId);
      Set<Long> subdirs = dirNSSummary.getChildDir();

      duResponse = new DUResponse();
      duResponse.setCount(subdirs.size());
      List<DUResponse.DiskUsage> subdirDUData = new ArrayList<>();
      // iterate all subdirectories to get disk usage data
      for (long subdirObjectId: subdirs) {
        NSSummary subdirNSSummary =
                reconNamespaceSummaryManager.getNSSummary(subdirObjectId);
        String subdirName = subdirNSSummary.getDirName();
        // build the path for subdirectory
        String subpath = path + OM_KEY_PREFIX + subdirName;
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        // reformat the response
        diskUsage.setSubpath(reformatString(subpath));
        long dataSize = getTotalSize(subdirObjectId);
        diskUsage.setSize(dataSize);
        subdirDUData.add(diskUsage);
      }
      duResponse.setDuData(subdirDUData);
      break;
    case KEY:
      // DU for key is the data size
      duResponse.setCount(1);
      DUResponse.DiskUsage keyDU = new DUResponse.DiskUsage();
      // The object ID for the directory that the key is directly in
      long parentObjectId = getDirObjectId(names, names.length - 1);
      String fileName = names[names.length - 1];
      String ozoneKey =
              omMetadataManager.getOzonePathKey(parentObjectId, fileName);
      OmKeyInfo keyInfo =
              omMetadataManager.getFileTable().getSkipCache(ozoneKey);
      keyDU.setSubpath(reformatString(path));
      keyDU.setSize(keyInfo.getDataSize());
      duResponse.setDuData(Collections.singletonList(keyDU));
      break;
    case UNKNOWN:
      duResponse.setStatus(NamespaceResponseCode.PATH_NOT_FOUND);
      break;
    default:
      break;
    }
    return Response.ok(duResponse).build();
  }

  /**
   * Quota usage endpoint that summarize the quota allowed and quota used in
   * bytes.
   * @param path request path
   * @return Quota Usage response
   * @throws IOException
   */
  @GET
  @Path("/quota")
  public Response getQuotaUsage(@QueryParam("path") String path)
          throws IOException {
    String[] names = parseRequestPath(path);
    EntityType type = getEntityType(names);
    QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
    if (type == EntityType.VOLUME) {
      List<OmBucketInfo> buckets = listBucketsUnderVolume(names[0]);
      String volKey = omMetadataManager.getVolumeKey(names[0]);
      OmVolumeArgs volumeArgs =
              omMetadataManager.getVolumeTable().getSkipCache(volKey);
      long quotaInBytes = volumeArgs.getQuotaInBytes();
      long quotaUsedInBytes = 0L;

      // Get the total data size used by all buckets
      for (OmBucketInfo bucketInfo: buckets) {
        long bucketObjectId = bucketInfo.getObjectID();
        quotaUsedInBytes += getTotalSize(bucketObjectId);
      }
      quotaUsageResponse.setQuota(quotaInBytes);
      quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
    } else if (type == EntityType.BUCKET) {
      String bucketKey = omMetadataManager.getBucketKey(names[0], names[1]);
      OmBucketInfo bucketInfo = omMetadataManager
              .getBucketTable().getSkipCache(bucketKey);
      long bucketObjectId = bucketInfo.getObjectID();
      long quotaInBytes = bucketInfo.getQuotaInBytes();
      long quotaUsedInBytes = getTotalSize(bucketObjectId);
      quotaUsageResponse.setQuota(quotaInBytes);
      quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
    } else if (type == EntityType.UNKNOWN) {
      quotaUsageResponse.setResponseCode(NamespaceResponseCode.PATH_NOT_FOUND);
    } else { // directory and key are not applicable for this request
      quotaUsageResponse.setResponseCode(
              NamespaceResponseCode.TYPE_NOT_APPLICABLE);
    }
    return Response.ok(quotaUsageResponse).build();
  }

  /**
   * Endpoint that returns aggregate file size distribution under a path.
   * @param path request path
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
    case VOLUME:
      List<OmBucketInfo> buckets = listBucketsUnderVolume(names[0]);
      int[] volumeFileSizeDist = new int[ReconConstants.NUM_OF_BINS];

      // accumulate file size distribution arrays from all buckets
      for (OmBucketInfo bucket : buckets) {
        long bucketObjectId = bucket.getObjectID();
        int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
        // add on each bin
        for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
          volumeFileSizeDist[i] += bucketFileSizeDist[i];
        }
      }
      distReponse.setFileSizeDist(volumeFileSizeDist);
      break;
    case BUCKET:
      long bucketObjectId = getBucketObjectId(names);
      int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
      distReponse.setFileSizeDist(bucketFileSizeDist);
      break;
    case DIRECTORY:
      long dirObjectId = getDirObjectId(names);
      int[] dirFileSizeDist = getTotalFileSizeDist(dirObjectId);
      distReponse.setFileSizeDist(dirFileSizeDist);
      break;
    case KEY:
      // key itself doesn't have file size distribution
      distReponse.setStatus(NamespaceResponseCode.TYPE_NOT_APPLICABLE);
      break;
    case UNKNOWN:
      distReponse.setStatus(NamespaceResponseCode.PATH_NOT_FOUND);
      break;
    default:
      break;
    }
    return Response.ok(distReponse).build();
  }

  /**
   * Return the entity type of client's request, check path existence.
   * If path doesn't exist, return Entity.UNKNOWN
   * @param names the client's parsed request
   * @return the entity type, unknown if path not found
   */
  @VisibleForTesting
  public EntityType getEntityType(String[] names) throws IOException {
    if (names.length == 0) {
      return EntityType.UNKNOWN;
    } else if (names.length == 1) { // volume level check
      String volName = names[0];
      if (!checkVolumeExistence(volName)) {
        return EntityType.UNKNOWN;
      }
      return EntityType.VOLUME;
    } else if (names.length == 2) { // bucket level check
      String volName = names[0];
      String bucketName = names[1];
      if (!checkBucketExistence(volName, bucketName)) {
        return EntityType.UNKNOWN;
      }
      return EntityType.BUCKET;
    } else { // length > 3. check dir or key existence (FSO-enabled)
      String volName = names[0];
      String bucketName = names[1];
      String keyName = getKeyName(names);
      // check if either volume or bucket doesn't exist
      if (!checkVolumeExistence(volName)
              || !checkBucketExistence(volName, bucketName)) {
        return EntityType.UNKNOWN;
      }
      long bucketObjectId = getBucketObjectId(names);
      return determineKeyPath(volName, bucketName, keyName, bucketObjectId);
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
            .getBucketTable().getSkipCache(bucketKey);
    return bucketInfo.getObjectID();
  }

  /**
   * Given a valid path request for a directory,
   * return the directory object ID.
   * @param names parsed path request in a list of names
   * @return directory object ID
   */
  private long getDirObjectId(String[] names) throws IOException {
    return getDirObjectId(names, names.length);
  }

  /**
   * Given a valid path request and a cutoff length where should be iterated
   * up to.
   * return the directory object ID for the object at the cutoff length
   * @param names parsed path request in a list of names
   * @param cutoff cannot be larger than the names' length. If equals,
   *               return the directory object id for the whole path
   * @return directory object ID
   */
  private long getDirObjectId(String[] names, int cutoff) throws IOException {
    long dirObjectId = getBucketObjectId(names);
    String dirKey = null;
    for (int i = 2; i < cutoff; ++i) {
      dirKey = omMetadataManager.getOzonePathKey(dirObjectId, names[i]);
      OmDirectoryInfo dirInfo =
              omMetadataManager.getDirectoryTable().getSkipCache(dirKey);
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

  /**
   * Example: /vol1/buck1/a/b/c/d/e/file1.txt -> a/b/c/d/e/file1.txt.
   * @param names parsed request
   * @return key name
   */
  @VisibleForTesting
  public static String getKeyName(String[] names) {
    String[] keyArr = Arrays.copyOfRange(names, 2, names.length);
    return String.join(OM_KEY_PREFIX, keyArr);
  }

  /**
   * Format the path in a nice format with leading slash and without trailing
   * slash.
   * @param path
   * @return
   */
  @VisibleForTesting
  public static String reformatString(String path) {
    if (!path.startsWith(OM_KEY_PREFIX)) {
      path = OM_KEY_PREFIX + path;
    }
    return removeTrailingSlashIfNeeded(path);
  }

  private boolean checkVolumeExistence(String volName) throws IOException {
    String volDBKey = omMetadataManager.getVolumeKey(volName);
    if (omMetadataManager.getVolumeTable().getSkipCache(volDBKey) == null) {
      return false;
    }
    return true;
  }

  private boolean checkBucketExistence(String volName, String bucketName)
          throws IOException {
    String bucketDBKey = omMetadataManager.getBucketKey(volName, bucketName);
    // Check if bucket exists
    if (omMetadataManager.getBucketTable().getSkipCache(bucketDBKey) == null) {
      return false;
    }
    return true;
  }

  /**
   * Given an object ID, return total count of keys under this object.
   * @param objectId the object's ID
   * @return count of keys
   * @throws IOException ioEx
   */
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

  /**
   * Given an object ID, return total count of directories under this object.
   * @param objectId the object's ID
   * @return count of directories
   * @throws IOException ioEx
   */
  private int getTotalDirCount(long objectId) throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0;
    }
    Set<Long> subdirs = nsSummary.getChildDir();
    int totalCnt = subdirs.size();
    for (long subdir: subdirs) {
      totalCnt += getTotalDirCount(subdir);
    }
    return totalCnt;
  }

  /**
   * Given an object ID, return total data size (no replication)
   * under this object.
   * @param objectId the object's ID
   * @return total used data size in bytes
   * @throws IOException ioEx
   */
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

  /**
   * Given an object ID, return the file size distribution.
   * @param objectId the object's ID
   * @return int array indicating file size distribution
   * @throws IOException ioEx
   */
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

  private List<OmBucketInfo> listBucketsUnderVolume(final String volumeName)
          throws IOException {
    List<OmBucketInfo> result = new ArrayList<>();
    if (Strings.isNullOrEmpty(volumeName)
            || !checkVolumeExistence(volumeName)) {
      return null;
    }
    Table bucketTable = omMetadataManager.getBucketTable();
    String seekPrefix =
            omMetadataManager.getVolumeKey(volumeName + OM_KEY_PREFIX);

    TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
            iterator = bucketTable.iterator();


    while (iterator.hasNext()) {
      Table.KeyValue<String, OmBucketInfo> kv = iterator.next();

      String key = kv.getKey();
      OmBucketInfo omBucketInfo = kv.getValue();

      if (omBucketInfo != null) {
        // We should return only the keys, whose keys match with the seek prefix
        if (key.startsWith(seekPrefix)) {
          result.add(omBucketInfo);
        }
      }
    }
    return result;
  }

  /**
   * Helper function to check if a path is a directory, key, or invalid.
   * @param volName volume name
   * @param bucketName bucket name
   * @param keyName key name
   * @return DIRECTORY, KEY, or UNKNOWN
   * @throws IOException
   */
  private EntityType determineKeyPath(String volName, String bucketName,
                                      String keyName, long bucketObjectId)
          throws IOException {
    omMetadataManager.getLock().acquireReadLock(BUCKET_LOCK, volName,
            bucketName);

    try {
      java.nio.file.Path keyPath = Paths.get(keyName);
      Iterator<java.nio.file.Path> elements = keyPath.iterator();

      long lastKnownParentId = bucketObjectId;
      OmDirectoryInfo omDirInfo = null;
      while (elements.hasNext()) {
        String fileName = elements.next().toString();

        // For example, /vol1/buck1/a/b/c/d/e/file1.txt
        // 1. Do lookup path component on directoryTable starting from bucket
        // 'buck1' to the leaf node component, which is 'file1.txt'.
        // 2. If there is no dir exists for the leaf node component 'file1.txt'
        // then do look it on fileTable.
        String dbNodeName = omMetadataManager.getOzonePathKey(
                lastKnownParentId, fileName);
        omDirInfo = omMetadataManager.getDirectoryTable()
                .getSkipCache(dbNodeName);

        if (omDirInfo != null) {
          lastKnownParentId = omDirInfo.getObjectID();
        } else if (!elements.hasNext()) {
          // reached last path component. Check file exists for the given path.
          OmKeyInfo omKeyInfo = omMetadataManager.getFileTable()
                  .getSkipCache(dbNodeName);
          // The path exists as a file
          if (omKeyInfo != null) {
            omKeyInfo.setKeyName(keyName);
            return EntityType.KEY;
          }
        } else {
          // Missing intermediate directory and just return null;
          // key not found in DB
          return EntityType.UNKNOWN;
        }
      }

      if (omDirInfo != null) {
        return EntityType.DIRECTORY;
      }

    } finally {
      omMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volName,
              bucketName);
    }
    return EntityType.UNKNOWN;
  }
}
