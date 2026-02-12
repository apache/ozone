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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

/**
 * Class for handling all entity types.
 */
public abstract class EntityHandler {

  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  private final ReconOMMetadataManager omMetadataManager;

  private final BucketHandler bucketHandler;

  private final OzoneStorageContainerManager reconSCM;

  private final String normalizedPath;
  private final String[] names;

  public EntityHandler(
          ReconNamespaceSummaryManager reconNamespaceSummaryManager,
          ReconOMMetadataManager omMetadataManager,
          OzoneStorageContainerManager reconSCM,
          BucketHandler bucketHandler, String path) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.reconSCM = reconSCM;
    this.bucketHandler = bucketHandler;

    // Defaulting to FILE_SYSTEM_OPTIMIZED if bucketHandler is null
    BucketLayout layout =
        (bucketHandler != null) ? bucketHandler.getBucketLayout() :
            BucketLayout.FILE_SYSTEM_OPTIMIZED;

    // Normalize the path based on the determined layout
    normalizedPath = normalizePath(path, layout);

    // Choose the parsing method based on the bucket layout
    names = (layout == BucketLayout.OBJECT_STORE) ?
        parseObjectStorePath(normalizedPath) : parseRequestPath(normalizedPath);
  }

  public abstract NamespaceSummaryResponse getSummaryResponse()
          throws IOException;

  public abstract DUResponse getDuResponse(
      boolean listFile, boolean withReplica, boolean sort)
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
    return names.clone();
  }

  /**
   * Return the entity handler of client's request, check path existence.
   * If path doesn't exist, return UnknownEntityHandler
   * @param reconNamespaceSummaryManager ReconNamespaceSummaryManager
   * @param omMetadataManager ReconOMMetadataManager
   * @param reconSCM OzoneStorageContainerManager
   * @param path the original path request used to identify root level
   * @return the entity handler of client's request
   */
  public static EntityHandler getEntityHandler(
          ReconNamespaceSummaryManager reconNamespaceSummaryManager,
          ReconOMMetadataManager omMetadataManager,
          OzoneStorageContainerManager reconSCM,
          String path) throws IOException {
    BucketHandler bucketHandler;

    String normalizedPath =
        normalizePath(path, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String[] names = parseRequestPath(normalizedPath);
    if (path.equals(OM_KEY_PREFIX)) {
      return EntityType.ROOT.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null, path);
    }

    if (names.length == 0) {
      return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null, path);
    } else if (names.length == 1) { // volume level check
      String volName = names[0];
      if (!omMetadataManager.volumeExists(volName)) {
        return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
                omMetadataManager, reconSCM, null, path);
      }
      return EntityType.VOLUME.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null, path);
    } else if (names.length == 2) { // bucket level check
      String volName = names[0];
      String bucketName = names[1];

      bucketHandler = BucketHandler.getBucketHandler(
              reconNamespaceSummaryManager,
              omMetadataManager, reconSCM,
              volName, bucketName);

      if (bucketHandler == null
          || !bucketHandler.bucketExists(volName, bucketName)) {
        return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
                omMetadataManager, reconSCM, null, path);
      }
      return EntityType.BUCKET.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, bucketHandler, path);
    } else { // length > 3. check dir or key existence
      String volName = names[0];
      String bucketName = names[1];

      // Assuming getBucketHandler already validates volume and bucket existence
      bucketHandler = BucketHandler.getBucketHandler(
          reconNamespaceSummaryManager, omMetadataManager, reconSCM, volName,
          bucketName);

      if (bucketHandler == null) {
        return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
            omMetadataManager, reconSCM, null, path);
      }

      // Directly handle path normalization and parsing based on the layout
      if (bucketHandler.getBucketLayout() == BucketLayout.OBJECT_STORE) {
        String[] parsedObjectLayoutPath = parseObjectStorePath(
            normalizePath(path, bucketHandler.getBucketLayout()));
        if (parsedObjectLayoutPath == null) {
          return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null, path);
        }
        // Use the key part directly from the parsed path
        return bucketHandler.determineKeyPath(parsedObjectLayoutPath[2])
            .create(reconNamespaceSummaryManager, omMetadataManager, reconSCM,
                bucketHandler, path);
      } else {
        // Use the existing names array for non-OBJECT_STORE layouts to derive
        // the keyName
        String keyName = BucketHandler.getKeyName(names);
        return bucketHandler.determineKeyPath(keyName)
            .create(reconNamespaceSummaryManager, omMetadataManager, reconSCM,
                bucketHandler, path);
      }
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
      return new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
    }
    int[] res = nsSummary.getFileSizeBucket();
    for (long childId: nsSummary.getChildDir()) {
      int[] subDirFileSizeDist = getTotalFileSizeDist(childId);
      for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
        res[i] += subDirFileSizeDist[i];
      }
    }
    return res;
  }

  protected int getTotalDirCount(long objectId) throws IOException {
    NSSummary nsSummary =
        getReconNamespaceSummaryManager().getNSSummary(objectId);
    if (nsSummary == null) {
      return 0;
    }
    Set<Long> subdirs = nsSummary.getChildDir();
    int totalCnt = subdirs.size();
    for (long subdir : subdirs) {
      totalCnt += getTotalDirCount(subdir);
    }
    return totalCnt;
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
    // With materialized optimization: getNumOfFiles now returns total count (including all subdirectories)
    return nsSummary.getNumOfFiles();
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
    // With materialized optimization: getSizeOfFiles now returns total size (including all subdirectories)
    return nsSummary.getSizeOfFiles();
  }

  public static String[] parseRequestPath(String path) {
    if (path.startsWith(OM_KEY_PREFIX)) {
      path = path.substring(1);
    }
    String[] names = path.split(OM_KEY_PREFIX);
    return names;
  }

  /**
   * Splits an object store path into volume, bucket, and key name components.
   *
   * This method parses a path of the format "/volumeName/bucketName/keyName",
   * including paths with additional '/' characters within the key name. It's
   * designed for object store paths where the first three '/' characters
   * separate the root, volume and bucket names from the key name.
   *
   * @param path The object store path to parse, starting with a slash.
   * @return A String array with three elements: volume name, bucket name, and
   * key name, or {null} if the path format is invalid.
   */
  public static String[] parseObjectStorePath(String path) {
    // Removing the leading slash for correct splitting
    path = path.substring(1);

    // Splitting the modified path by "/", limiting to 3 parts
    String[] parts = path.split("/", 3);

    // Checking if we correctly obtained 3 parts after removing the leading slash
    if (parts.length <= 3) {
      return parts;
    } else {
      return null;
    }
  }

  /**
   * Normalizes a given path based on the specified bucket layout.
   *
   * This method adjusts the path according to the bucket layout.
   * For {OBJECT_STORE Layout}, it normalizes the path up to the bucket level
   * using OmUtils.normalizePathUptoBucket. For other layouts, it
   * normalizes the entire path, including the key, using
   * OmUtils.normalizeKey, and does not preserve any trailing slashes.
   * The normalized path will always be prefixed with OM_KEY_PREFIX to ensure it
   * is consistent with the expected format for object storage paths in Ozone.
   *
   * @param path
   * @param bucketLayout
   * @return A normalized path
   */
  public static String normalizePath(String path, BucketLayout bucketLayout) {
    if (bucketLayout == BucketLayout.OBJECT_STORE) {
      return OM_KEY_PREFIX + OmUtils.normalizePathUptoBucket(path);
    }
    return OM_KEY_PREFIX + OmUtils.normalizeKey(path, false);
  }
}
