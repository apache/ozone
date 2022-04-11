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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Class for handling all entity types.
 */
public abstract class EntityHandler {

  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  private ReconOMMetadataManager omMetadataManager;

  private BucketHandler bucketHandler;

  private OzoneStorageContainerManager reconSCM;

  private static String normalizedPath;
  private static String[] names;

  public EntityHandler(
          ReconNamespaceSummaryManager reconNamespaceSummaryManager,
          ReconOMMetadataManager omMetadataManager,
          OzoneStorageContainerManager reconSCM,
          BucketHandler bucketHandler) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.reconSCM = reconSCM;
    this.bucketHandler = bucketHandler;
  }

  public abstract NamespaceSummaryResponse getSummaryResponse()
          throws IOException;

  public abstract DUResponse getDuResponse(
          boolean listFile, boolean withReplica)
          throws IOException;

  public abstract QuotaUsageResponse getQuotaResponse()
          throws IOException;

  public abstract FileSizeDistributionResponse getDistResponse()
          throws IOException;

  public ReconOMMetadataManager getOmMetadataManager() {
    return omMetadataManager;
  }

  public OzoneStorageContainerManager getReconSCM() {
    return reconSCM;
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  public BucketHandler getBucketHandler() {
    return bucketHandler;
  }

  public String getNormalizedPath() {
    return normalizedPath;
  }

  public String[] getNames() {
    return names;
  }

  /**
   * Return the entity type of client's request, check path existence.
   * If path doesn't exist, return Entity.UNKNOWN
   * @param path the original path request used to identify root level
   * @return the entity type of client's request, check path existence
   */
  public static EntityHandler getEntityHandler(
          ReconNamespaceSummaryManager reconNamespaceSummaryManager,
          ReconOMMetadataManager omMetadataManager,
          OzoneStorageContainerManager reconSCM,
          String path) throws IOException {
    BucketHandler bucketHandler;

    normalizedPath = normalizePath(path);
    names = parseRequestPath(normalizedPath);

    if (path.equals(OM_KEY_PREFIX)) {
      return EntityType.ROOT.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null);
    }

    if (names.length == 0) {
      return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null);
    } else if (names.length == 1) { // volume level check
      String volName = names[0];
      if (!volumeExists(omMetadataManager, volName)) {
        return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
                omMetadataManager, reconSCM, null);
      }
      return EntityType.VOLUME.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null);
    } else if (names.length == 2) { // bucket level check
      bucketHandler = BucketHandler.getBucketHandler(
              reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, path);
      String volName = names[0];
      String bucketName = names[1];
      if (!bucketHandler.bucketExists(volName, bucketName)) {
        return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
                omMetadataManager, reconSCM, null);
      }
      return EntityType.BUCKET.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, bucketHandler);
    } else { // length > 3. check dir or key existence (FSO-enabled)
      bucketHandler = BucketHandler.getBucketHandler(
              reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, path);
      String volName = names[0];
      String bucketName = names[1];
      String keyName = BucketHandler.getKeyName(names);
      // check if either volume or bucket doesn't exist
      if (!volumeExists(omMetadataManager, volName)
          || !bucketHandler.bucketExists(volName, bucketName)) {
        return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
                omMetadataManager, reconSCM, null);
      }
      long bucketObjectId = bucketHandler.getBucketObjectId(names);
      return bucketHandler.determineKeyPath(keyName,
          bucketObjectId).create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, bucketHandler);
    }
  }

  /**
   * Given an object ID, return the file size distribution.
   * @param objectId the object's ID
   * @return int array indicating file size distribution
   * @throws IOException ioEx
   */
  protected int[] getTotalFileSizeDist(long objectId) throws IOException {
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
  protected List<OmVolumeArgs> listVolumes() throws IOException {
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
  protected List<OmBucketInfo> listBucketsUnderVolume(final String volumeName)
      throws IOException {
    List<OmBucketInfo> result = new ArrayList<>();
    // if volume name is null, seek prefix is an empty string
    String seekPrefix = "";

    Table bucketTable = omMetadataManager.getBucketTable();

    TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
        iterator = bucketTable.iterator();

    if (volumeName != null) {
      if (!volumeExists(omMetadataManager, volumeName)) {
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

  static boolean volumeExists(ReconOMMetadataManager omMetadataManager,
                              String volName) throws IOException {
    String volDBKey = omMetadataManager.getVolumeKey(volName);
    return omMetadataManager.getVolumeTable().getSkipCache(volDBKey) != null;
  }

  /**
   * Given an object ID, return total count of keys under this object.
   * @param objectId the object's ID
   * @return count of keys
   * @throws IOException ioEx
   */
  protected long getTotalKeyCount(long objectId) throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0L;
    }
    long totalCnt = nsSummary.getNumOfFiles();
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
  protected int getTotalDirCount(long objectId) throws IOException {
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
  protected long getTotalSize(long objectId) throws IOException {
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

  protected long calculateDUForVolume(String volumeName)
      throws IOException {
    long result = 0L;

    Table keyTable = omMetadataManager.getFileTable();

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = keyTable.iterator();

    while (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      OmKeyInfo keyInfo = kv.getValue();

      if (keyInfo != null) {
        if (volumeName.equals(keyInfo.getVolumeName())) {
          result += bucketHandler.getKeySizeWithReplication(keyInfo);
        }
      }
    }
    return result;
  }

  public static String[] parseRequestPath(String path) {
    if (path.startsWith(OM_KEY_PREFIX)) {
      path = path.substring(1);
    }
    names = path.split(OM_KEY_PREFIX);
    return names;
  }

  private static String normalizePath(String path) {
    return OM_KEY_PREFIX + OmUtils.normalizeKey(path, false);
  }
}
