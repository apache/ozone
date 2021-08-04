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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
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

  @Inject
  public NSSummaryEndpoint(ReconNamespaceSummaryManager namespaceSummaryManager,
                           ReconOMMetadataManager omMetadataManager) {
    this.reconNamespaceSummaryManager = namespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
  }

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

    if (path == null || path.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    String normalizedPath = normalizePath(path);
    String[] names = parseRequestPath(normalizedPath);

    EntityType type = getEntityType(normalizedPath, names);

    NamespaceSummaryResponse namespaceSummaryResponse = null;
    switch (type) {
    case ROOT:
      namespaceSummaryResponse = new NamespaceSummaryResponse(EntityType.ROOT);
      List<OmVolumeArgs> volumes = listVolumes();
      namespaceSummaryResponse.setNumVolume(volumes.size());
      List<OmBucketInfo> allBuckets = listBucketsUnderVolume(null);
      namespaceSummaryResponse.setNumBucket(allBuckets.size());
      int totalNumDir = 0;
      int totalNumKey = 0;
      for (OmBucketInfo bucket : allBuckets) {
        long bucketObjectId = bucket.getObjectID();
        totalNumDir += getTotalDirCount(bucketObjectId);
        totalNumKey += getTotalKeyCount(bucketObjectId);
      }
      namespaceSummaryResponse.setNumTotalDir(totalNumDir);
      namespaceSummaryResponse.setNumTotalKey(totalNumKey);
      break;
    case VOLUME:
      namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.VOLUME);
      List<OmBucketInfo> buckets = listBucketsUnderVolume(names[0]);
      namespaceSummaryResponse.setNumBucket(buckets.size());
      int totalDir = 0;
      int totalKey = 0;

      // iterate all buckets to collect the total object count.
      for (OmBucketInfo bucket : buckets) {
        long bucketObjectId = bucket.getObjectID();
        totalDir += getTotalDirCount(bucketObjectId);
        totalKey += getTotalKeyCount(bucketObjectId);
      }
      namespaceSummaryResponse.setNumTotalDir(totalDir);
      namespaceSummaryResponse.setNumTotalKey(totalKey);
      break;
    case BUCKET:
      namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.BUCKET);
      assert (names.length == 2);
      long bucketObjectId = getBucketObjectId(names);
      namespaceSummaryResponse.setNumTotalDir(getTotalDirCount(bucketObjectId));
      namespaceSummaryResponse.setNumTotalKey(getTotalKeyCount(bucketObjectId));
      break;
    case DIRECTORY:
      // path should exist so we don't need any extra verification/null check
      long dirObjectId = getDirObjectId(names);
      namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.DIRECTORY);
      namespaceSummaryResponse.setNumTotalDir(getTotalDirCount(dirObjectId));
      namespaceSummaryResponse.setNumTotalKey(getTotalKeyCount(dirObjectId));
      break;
    case KEY:
      namespaceSummaryResponse = new NamespaceSummaryResponse(EntityType.KEY);
      break;
    case UNKNOWN:
      namespaceSummaryResponse =
              new NamespaceSummaryResponse(EntityType.UNKNOWN);
      namespaceSummaryResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);
      break;
    default:
      break;
    }
    return Response.ok(namespaceSummaryResponse).build();
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
    if (path == null || path.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    String normalizedPath = normalizePath(path);
    String[] names = parseRequestPath(normalizedPath);
    EntityType type = getEntityType(normalizedPath, names);

    DUResponse duResponse = new DUResponse();
    switch (type) {
    case ROOT:
      List<OmVolumeArgs> volumes = listVolumes();
      duResponse.setCount(volumes.size());

      List<DUResponse.DiskUsage> volumeDuData = new ArrayList<>();
      for (OmVolumeArgs volume: volumes) {
        String volumeName = volume.getVolume();
        String subpath = omMetadataManager.getVolumeKey(volumeName);
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        long dataSize = 0;
        diskUsage.setSubpath(subpath);
        // iterate all buckets per volume to get total data size
        for (OmBucketInfo bucket: listBucketsUnderVolume(volumeName)) {
          long bucketObjectID = bucket.getObjectID();
          dataSize += getTotalSize(bucketObjectID);
        }
        diskUsage.setSize(dataSize);
        volumeDuData.add(diskUsage);
      }
      duResponse.setDuData(volumeDuData);
      break;
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
      duResponse.setKeySize(bucketNSSummary.getSizeOfFiles());
      List<DUResponse.DiskUsage> dirDUData = new ArrayList<>();
      for (long subdirObjectId: bucketSubdirs) {
        NSSummary subdirNSSummary = reconNamespaceSummaryManager
                .getNSSummary(subdirObjectId);

        // get directory's name and generate the next-level subpath.
        String dirName = subdirNSSummary.getDirName();
        String subpath = buildSubpath(normalizedPath, dirName);
        // we need to reformat the subpath in the response in a
        // format with leading slash and without trailing slash
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        diskUsage.setSubpath(subpath);
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

      duResponse.setCount(subdirs.size());
      duResponse.setKeySize(dirNSSummary.getSizeOfFiles());
      List<DUResponse.DiskUsage> subdirDUData = new ArrayList<>();
      // iterate all subdirectories to get disk usage data
      for (long subdirObjectId: subdirs) {
        NSSummary subdirNSSummary =
                reconNamespaceSummaryManager.getNSSummary(subdirObjectId);
        String subdirName = subdirNSSummary.getDirName();
        // build the path for subdirectory
        String subpath = buildSubpath(normalizedPath, subdirName);
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        // reformat the response
        diskUsage.setSubpath(subpath);
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
      String subpath = buildSubpath(normalizedPath, null);
      keyDU.setSubpath(subpath);
      keyDU.setSize(keyInfo.getDataSize());
      duResponse.setDuData(Collections.singletonList(keyDU));
      break;
    case UNKNOWN:
      duResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);
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

    if (path == null || path.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    String normalizedPath = normalizePath(path);
    String[] names = parseRequestPath(normalizedPath);
    EntityType type = getEntityType(normalizedPath, names);

    QuotaUsageResponse quotaUsageResponse = new QuotaUsageResponse();
    if (type == EntityType.ROOT) {
      List<OmVolumeArgs> volumes = listVolumes();
      List<OmBucketInfo> buckets = listBucketsUnderVolume(null);
      long quotaInBytes = 0L;
      long quotaUsedInBytes = 0L;

      for (OmVolumeArgs volume: volumes) {
        final long quota = volume.getQuotaInBytes();
        assert(quota >= -1L);
        if (quota == -1L) {
          // If one volume has unlimited quota, the "root" quota is unlimited.
          quotaInBytes = -1L;
          break;
        }
        quotaInBytes += quota;
      }
      for (OmBucketInfo bucket: buckets) {
        long bucketObjectId = bucket.getObjectID();
        quotaUsedInBytes += getTotalSize(bucketObjectId);
      }

      quotaUsageResponse.setQuota(quotaInBytes);
      quotaUsageResponse.setQuotaUsed(quotaUsedInBytes);
    } else if (type == EntityType.VOLUME) {
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
      quotaUsageResponse.setResponseCode(ResponseStatus.PATH_NOT_FOUND);
    } else { // directory and key are not applicable for this request
      quotaUsageResponse.setResponseCode(
              ResponseStatus.TYPE_NOT_APPLICABLE);
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

    if (path == null || path.length() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    String normalizedPath = normalizePath(path);
    String[] names = parseRequestPath(normalizedPath);
    EntityType type = getEntityType(normalizedPath, names);

    FileSizeDistributionResponse distResponse =
            new FileSizeDistributionResponse();
    switch (type) {
    case ROOT:
      List<OmBucketInfo> allBuckets = listBucketsUnderVolume(null);
      int[] fileSizeDist = new int[ReconConstants.NUM_OF_BINS];

      // accumulate file size distribution arrays from all buckets
      for (OmBucketInfo bucket : allBuckets) {
        long bucketObjectId = bucket.getObjectID();
        int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
        // add on each bin
        for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
          fileSizeDist[i] += bucketFileSizeDist[i];
        }
      }
      distResponse.setFileSizeDist(fileSizeDist);
      break;
    case VOLUME:
      List<OmBucketInfo> buckets = listBucketsUnderVolume(names[0]);
      int[] volumeFileSizeDist = new int[ReconConstants.NUM_OF_BINS];

      // accumulate file size distribution arrays from all buckets under volume
      for (OmBucketInfo bucket : buckets) {
        long bucketObjectId = bucket.getObjectID();
        int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
        // add on each bin
        for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
          volumeFileSizeDist[i] += bucketFileSizeDist[i];
        }
      }
      distResponse.setFileSizeDist(volumeFileSizeDist);
      break;
    case BUCKET:
      long bucketObjectId = getBucketObjectId(names);
      int[] bucketFileSizeDist = getTotalFileSizeDist(bucketObjectId);
      distResponse.setFileSizeDist(bucketFileSizeDist);
      break;
    case DIRECTORY:
      long dirObjectId = getDirObjectId(names);
      int[] dirFileSizeDist = getTotalFileSizeDist(dirObjectId);
      distResponse.setFileSizeDist(dirFileSizeDist);
      break;
    case KEY:
      // key itself doesn't have file size distribution
      distResponse.setStatus(ResponseStatus.TYPE_NOT_APPLICABLE);
      break;
    case UNKNOWN:
      distResponse.setStatus(ResponseStatus.PATH_NOT_FOUND);
      break;
    default:
      break;
    }
    return Response.ok(distResponse).build();
  }

  /**
   * Return the entity type of client's request, check path existence.
   * If path doesn't exist, return Entity.UNKNOWN
   * @param path the original path request used to identify root level
   * @param names the client's parsed request
   * @return the entity type, unknown if path not found
   */
  @VisibleForTesting
  public EntityType getEntityType(String path, String[] names)
          throws IOException {
    if (path.equals(OM_KEY_PREFIX)) {
      return EntityType.ROOT;
    }

    if (names.length == 0) {
      return EntityType.UNKNOWN;
    } else if (names.length == 1) { // volume level check
      String volName = names[0];
      if (!volumeExists(volName)) {
        return EntityType.UNKNOWN;
      }
      return EntityType.VOLUME;
    } else if (names.length == 2) { // bucket level check
      String volName = names[0];
      String bucketName = names[1];
      if (!bucketExists(volName, bucketName)) {
        return EntityType.UNKNOWN;
      }
      return EntityType.BUCKET;
    } else { // length > 3. check dir or key existence (FSO-enabled)
      String volName = names[0];
      String bucketName = names[1];
      String keyName = getKeyName(names);
      // check if either volume or bucket doesn't exist
      if (!volumeExists(volName)
              || !bucketExists(volName, bucketName)) {
        return EntityType.UNKNOWN;
      }
      long bucketObjectId = getBucketObjectId(names);
      return determineKeyPath(keyName, bucketObjectId);
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

  static String[] parseRequestPath(String path) {
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
  static String getKeyName(String[] names) {
    String[] keyArr = Arrays.copyOfRange(names, 2, names.length);
    return String.join(OM_KEY_PREFIX, keyArr);
  }

  /**
   *
   * @param path
   * @param nextLevel
   * @return
   */
  static String buildSubpath(String path, String nextLevel) {
    String subpath = path;
    if (!subpath.startsWith(OM_KEY_PREFIX)) {
      subpath = OM_KEY_PREFIX + subpath;
    }
    subpath = removeTrailingSlashIfNeeded(subpath);
    if (nextLevel != null) {
      subpath = subpath + OM_KEY_PREFIX + nextLevel;
    }
    return subpath;
  }

  private boolean volumeExists(String volName) throws IOException {
    String volDBKey = omMetadataManager.getVolumeKey(volName);
    return omMetadataManager.getVolumeTable().getSkipCache(volDBKey) != null;
  }

  private boolean bucketExists(String volName, String bucketName)
          throws IOException {
    String bucketDBKey = omMetadataManager.getBucketKey(volName, bucketName);
    // Check if bucket exists
    return omMetadataManager.getBucketTable().getSkipCache(bucketDBKey) != null;
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

  /**
   * Return all volumes in the file system.
   * This method can be optimized by using username as a filter.
   * @return a list of volume names under the system
   */
  private List<OmVolumeArgs> listVolumes() throws IOException {
    List<OmVolumeArgs> result = new ArrayList<>();
    Table volumeTable = omMetadataManager.getVolumeTable();
    TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
            iterator = volumeTable.iterator();

    while (iterator.hasNext()) {
      Table.KeyValue<String, OmVolumeArgs> kv = iterator.next();

      OmVolumeArgs omVolumeArgs = kv.getValue();
      if (omVolumeArgs != null) {
        result.add(omVolumeArgs);
      }
    }
    return result;
  }

  /**
   * List all buckets under a volume, if volume name is null, return all buckets
   * under the system.
   * @param volumeName volume name
   * @return a list of buckets
   * @throws IOException IOE
   */
  private List<OmBucketInfo> listBucketsUnderVolume(final String volumeName)
          throws IOException {
    List<OmBucketInfo> result = new ArrayList<>();
    // if volume name is null, seek prefix is an empty string
    String seekPrefix = "";

    Table bucketTable = omMetadataManager.getBucketTable();

    TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
            iterator = bucketTable.iterator();

    if (volumeName != null) {
      if (!volumeExists(volumeName)) {
        return result;
      }
      seekPrefix = omMetadataManager.getVolumeKey(volumeName + OM_KEY_PREFIX);
    }

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
   * @param keyName key name
   * @return DIRECTORY, KEY, or UNKNOWN
   * @throws IOException
   */
  private EntityType determineKeyPath(String keyName, long bucketObjectId)
          throws IOException {

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
    return EntityType.UNKNOWN;
  }

  private static String normalizePath(String path) {
    return OM_KEY_PREFIX + OmUtils.normalizeKey(path, false);
  }
}
