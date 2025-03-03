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
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.removeTrailingSlashIfNeeded;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for handling all bucket types.
 * The abstract methods have different implementation for each bucket type.
 */
public abstract class BucketHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      BucketHandler.class);

  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  private final ReconOMMetadataManager omMetadataManager;

  public BucketHandler(
          ReconNamespaceSummaryManager reconNamespaceSummaryManager,
          ReconOMMetadataManager omMetadataManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
  }

  public ReconOMMetadataManager getOmMetadataManager() {
    return omMetadataManager;
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  public abstract EntityType determineKeyPath(String keyName)
      throws IOException;

  public abstract long calculateDUUnderObject(long parentId)
      throws IOException;

  public abstract long handleDirectKeys(long parentId,
                       boolean withReplica, boolean listFile,
                       List<DUResponse.DiskUsage> duData,
                       String normalizedPath) throws IOException;

  public abstract long getDirObjectId(String[] names)
          throws IOException;

  public abstract long getDirObjectId(String[] names, int cutoff)
          throws IOException;

  public abstract BucketLayout getBucketLayout();

  public abstract OmKeyInfo getKeyInfo(String[] names)
      throws IOException;

  public abstract OmDirectoryInfo getDirInfo(String[] names)
      throws IOException;

  /**
   * Fixing the existing path and appending the next level entity to it.
   * @param path
   * @param nextLevel
   * @return subpath
   */
  public static String buildSubpath(String path, String nextLevel) {
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

  /**
   * Example: {@literal /vol1/buck1/a/b/c/d/e/file1.txt -> a/b/c/d/e/file1.txt} .
   * @param names parsed request
   * @return key name
   */
  public static String getKeyName(String[] names) {
    String[] keyArr = Arrays.copyOfRange(names, 2, names.length);
    return String.join(OM_KEY_PREFIX, keyArr);
  }

  boolean bucketExists(String volName, String bucketName)
      throws IOException {
    String bucketDBKey = omMetadataManager.getBucketKey(volName, bucketName);
    // Check if bucket exists
    return omMetadataManager.getBucketTable().getSkipCache(bucketDBKey) != null;
  }

  /**
   * Given a existent path, get the volume object ID.
   * @param names valid path request
   * @return volume objectID
   * @throws IOException
   */
  public long getVolumeObjectId(String[] names) throws IOException {
    String volumeKey = omMetadataManager.getVolumeKey(names[0]);
    OmVolumeArgs volumeInfo = omMetadataManager
            .getVolumeTable().getSkipCache(volumeKey);
    return volumeInfo.getObjectID();
  }

  /**
   * Given a existent path, get the bucket object ID.
   * @param names valid path request
   * @return bucket objectID
   * @throws IOException
   */
  public long getBucketObjectId(String[] names) throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(names[0], names[1]);
    OmBucketInfo bucketInfo = omMetadataManager
        .getBucketTable().getSkipCache(bucketKey);
    return bucketInfo.getObjectID();
  }

  public static BucketHandler getBucketHandler(
                ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                ReconOMMetadataManager omMetadataManager,
                OzoneStorageContainerManager reconSCM,
                OmBucketInfo bucketInfo) throws IOException {
    // Check if enableFileSystemPaths flag is set to true.
    boolean enableFileSystemPaths = isEnableFileSystemPaths(omMetadataManager);

    // If bucketInfo is null then entity type is UNKNOWN
    if (Objects.isNull(bucketInfo)) {
      return null;
    } else {
      if (bucketInfo.getBucketLayout()
          .equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
        return new FSOBucketHandler(reconNamespaceSummaryManager,
            omMetadataManager, bucketInfo);
      } else if (bucketInfo.getBucketLayout().equals(BucketLayout.LEGACY)) {
        // Choose handler based on enableFileSystemPaths flag for legacy layout.
        // If enableFileSystemPaths is false, then the legacy bucket is treated
        // as an OBS bucket.
        if (enableFileSystemPaths) {
          return new LegacyBucketHandler(reconNamespaceSummaryManager,
              omMetadataManager, bucketInfo);
        } else {
          return new OBSBucketHandler(reconNamespaceSummaryManager,
              omMetadataManager, bucketInfo);
        }
      } else if (bucketInfo.getBucketLayout()
          .equals(BucketLayout.OBJECT_STORE)) {
        return new OBSBucketHandler(reconNamespaceSummaryManager,
            omMetadataManager, bucketInfo);
      } else {
        LOG.error("Unsupported bucket layout: " +
            bucketInfo.getBucketLayout());
        return null;
      }
    }
  }

  /**
   * Determines whether FileSystemPaths are enabled for Legacy Buckets
   * based on the Ozone configuration.
   *
   * @param ReconOMMetadataManager Instance
   * @return True if FileSystemPaths are enabled, false otherwise.
   */
  private static boolean isEnableFileSystemPaths(ReconOMMetadataManager omMetadataManager) {
    OzoneConfiguration configuration = omMetadataManager.getOzoneConfiguration();
    if (configuration == null) {
      configuration = new OzoneConfiguration();
    }
    return configuration.getBoolean(OmConfig.Keys.ENABLE_FILESYSTEM_PATHS,
        OmConfig.Defaults.ENABLE_FILESYSTEM_PATHS);
  }

  public static BucketHandler getBucketHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      String volumeName, String bucketName) throws IOException {

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    Table<String, OmBucketInfo> bucketTable =
        omMetadataManager.getBucketTable();
    OmBucketInfo bucketInfo = null;
    if (null != bucketTable) {
      bucketInfo = bucketTable.getSkipCache(bucketKey);
    }

    return getBucketHandler(reconNamespaceSummaryManager,
        omMetadataManager, reconSCM, bucketInfo);
  }
}
