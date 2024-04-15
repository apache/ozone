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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.Stats;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Class for handling FSO buckets NameSpaceSummaries.
 */
public class FSOBucketHandler extends BucketHandler {
  private final long volumeId;
  private final long bucketId;

  public FSOBucketHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      OmBucketInfo bucketInfo) throws IOException {
    super(reconNamespaceSummaryManager, omMetadataManager,
        reconSCM);

    String vol = bucketInfo.getVolumeName();
    String bucket = bucketInfo.getBucketName();

    String[] names = {vol, bucket};

    this.volumeId = getVolumeObjectId(names);
    this.bucketId = getBucketObjectId(names);
  }

  /**
   * Helper function to check if a path is a directory, key, or invalid.
   * @param keyName key name
   * @return DIRECTORY, KEY, or UNKNOWN
   * @throws IOException
   */
  @Override
  public EntityType determineKeyPath(String keyName)
      throws IOException {
    java.nio.file.Path keyPath = Paths.get(keyName);
    Iterator<java.nio.file.Path> elements = keyPath.iterator();

    long lastKnownParentId = bucketId;
    OmDirectoryInfo omDirInfo = null;
    while (elements.hasNext()) {
      String fileName = elements.next().toString();

      // For example, /vol1/buck1/a/b/c/d/e/file1.txt
      // 1. Do lookup path component on directoryTable starting from bucket
      // 'buck1' to the leaf node component, which is 'file1.txt'.
      // 2. If there is no dir exists for the leaf node component 'file1.txt'
      // then do look it on fileTable.
      String dbNodeName = getOmMetadataManager().getOzonePathKey(volumeId,
          bucketId, lastKnownParentId, fileName);
      omDirInfo = getOmMetadataManager().getDirectoryTable()
          .getSkipCache(dbNodeName);

      if (omDirInfo != null) {
        lastKnownParentId = omDirInfo.getObjectID();
      } else if (!elements.hasNext()) {
        // reached last path component. Check file exists for the given path.
        OmKeyInfo omKeyInfo = getOmMetadataManager().getFileTable()
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

  // FileTable's key is in the format of "volumeId/bucketId/parentId/fileName"
  // Make use of RocksDB's order to seek to the prefix and avoid full iteration
  @Override
  public long calculateDUUnderObject(long parentId, boolean recursive, List<DUResponse.DiskUsage> diskUsageList,
                                     Stats stats)
      throws IOException {
    Table<String, OmKeyInfo> keyTable = getOmMetadataManager().getFileTable();

    long totalDU = 0L;
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = keyTable.iterator()) {

      String seekPrefix = OM_KEY_PREFIX +
          volumeId +
          OM_KEY_PREFIX +
          bucketId +
          OM_KEY_PREFIX +
          parentId +
          OM_KEY_PREFIX;
      iterator.seek(seekPrefix);
      // handle direct keys
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
        String dbKey = kv.getKey();
        // since the RocksDB is ordered, seek until the prefix isn't matched
        if (!dbKey.startsWith(seekPrefix)) {
          break;
        }
        OmKeyInfo keyInfo = kv.getValue();
        if (recursive) {
          if (stats.getLimit() == -1) {
            populateDiskUsage(keyInfo, diskUsageList);
          } else {
            if (stats.getCurrentCount() < stats.getLimit()) {
              populateDiskUsage(keyInfo, diskUsageList);
              stats.setCurrentCount(stats.getCurrentCount() + 1);
              stats.setLastKey(kv.getKey());
            }
          }
        }
        if (keyInfo != null) {
          stats.setTotalCount(stats.getTotalCount() + 1);
          totalDU += keyInfo.getReplicatedSize();
        }
      }
    }

    // handle nested keys (DFS)
    NSSummary nsSummary = getReconNamespaceSummaryManager()
        .getNSSummary(parentId);
    // empty bucket
    if (nsSummary == null) {
      return 0;
    }

    Set<Long> subDirIds = nsSummary.getChildDir();
    for (long subDirId: subDirIds) {
      totalDU += calculateDUUnderObject(subDirId, recursive, diskUsageList, stats);
    }
    return totalDU;
  }

  private void populateDiskUsage(OmKeyInfo keyInfo, List<DUResponse.DiskUsage> diskUsageList) throws IOException {
    DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
    diskUsage.setKey(keyInfo.isFile());
    diskUsage.setSubpath(constructFullPath(keyInfo, getReconNamespaceSummaryManager()));
    diskUsage.setSize(keyInfo.getDataSize());
    diskUsage.setSizeWithReplica(keyInfo.getReplicatedSize());
    diskUsage.setReplicationType(keyInfo.getReplicationConfig().getReplicationType().name());
    diskUsage.setCreationTime(keyInfo.getCreationTime());
    diskUsage.setModificationTime(keyInfo.getModificationTime());

    diskUsageList.add(diskUsage);
  }

  /**
   * Constructs the full path of a key from its OmKeyInfo using a bottom-up approach, starting from the leaf node.
   * <p>
   * The method begins with the leaf node (the key itself) and recursively prepends parent directory names, fetched
   * via NSSummary objects, until reaching the parent bucket (parentId is -1). It effectively builds the path from
   * bottom to top, finally prepending the volume and bucket names to complete the full path.
   *
   * @param omKeyInfo The OmKeyInfo object for the key
   * @return The constructed full path of the key as a String.
   * @throws IOException
   */
  public static String constructFullPath(OmKeyInfo omKeyInfo,
                                         ReconNamespaceSummaryManager reconNamespaceSummaryManager)
      throws IOException {
    StringBuilder fullPath = new StringBuilder(omKeyInfo.getKeyName());
    long parentId = omKeyInfo.getParentObjectID();
    boolean isDirectoryPresent = false;
    while (parentId != -1) {
      NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(parentId);
      if (nsSummary == null) {
        break;
      }
      // Prepend the directory name to the path
      fullPath.insert(0, nsSummary.getDirName() + OM_KEY_PREFIX);

      // Move to the parent ID of the current directory
      parentId = nsSummary.getParentId();
      isDirectoryPresent = true;
    }

    // Prepend the volume and bucket to the constructed path
    String volumeName = omKeyInfo.getVolumeName();
    String bucketName = omKeyInfo.getBucketName();
    fullPath.insert(0, volumeName + OM_KEY_PREFIX + bucketName + OM_KEY_PREFIX);
    if (isDirectoryPresent) {
      return OmUtils.normalizeKey(fullPath.toString(), true);
    }
    return fullPath.toString();
  }

  /**
   * This method handles disk usage of direct keys.
   *
   * @param parentId       parent directory/bucket
   * @param withReplica    if withReplica is enabled, set sizeWithReplica
   *                       for each direct key's DU
   * @param listFile       if listFile is enabled, append key DU as a subpath
   * @param duData         the current DU data
   * @param normalizedPath the normalized path request
   * @param stats
   * @return the total DU of all direct keys
   * @throws IOException IOE
   */
  @Override
  public long handleDirectKeys(long parentId, boolean withReplica,
                               boolean listFile,
                               List<DUResponse.DiskUsage> duData,
                               String normalizedPath, Stats stats) throws IOException {

    Table<String, OmKeyInfo> keyTable = getOmMetadataManager().getFileTable();
    long keyDataSizeWithReplica = 0L;

    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = keyTable.iterator()) {

      String seekPrefix = OM_KEY_PREFIX +
          volumeId +
          OM_KEY_PREFIX +
          bucketId +
          OM_KEY_PREFIX +
          parentId +
          OM_KEY_PREFIX;
      iterator.seek(seekPrefix);

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
        String dbKey = kv.getKey();

        if (!dbKey.startsWith(seekPrefix)) {
          break;
        }
        OmKeyInfo keyInfo = kv.getValue();
        if (keyInfo != null) {
          DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
          String subpath = buildSubpath(normalizedPath,
              keyInfo.getFileName());
          diskUsage.setSubpath(subpath);
          diskUsage.setKey(true);
          diskUsage.setSize(keyInfo.getDataSize());
          diskUsage.setReplicationType(keyInfo.getReplicationConfig().getReplicationType().name());
          diskUsage.setCreationTime(keyInfo.getCreationTime());
          diskUsage.setModificationTime(keyInfo.getModificationTime());

          if (withReplica) {
            long keyDU = keyInfo.getReplicatedSize();
            keyDataSizeWithReplica += keyDU;
            diskUsage.setSizeWithReplica(keyDU);
          }
          // list the key as a subpath
          if (listFile) {
            verifyStatsAndAddDURecord(duData, stats, kv, diskUsage);
          }
        }
      }
    }

    return keyDataSizeWithReplica;
  }

  /**
   * Given a valid path request for a directory,
   * return the directory object ID.
   * @param names parsed path request in a list of names
   * @return directory object ID
   */
  @Override
  public long getDirObjectId(String[] names) throws IOException {
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
  @Override
  public long getDirObjectId(String[] names, int cutoff) throws IOException {
    long dirObjectId = getBucketObjectId(names);
    String dirKey;
    for (int i = 2; i < cutoff; ++i) {
      dirKey = getOmMetadataManager().getOzonePathKey(getVolumeObjectId(names),
          getBucketObjectId(names), dirObjectId, names[i]);
      OmDirectoryInfo dirInfo =
          getOmMetadataManager().getDirectoryTable().getSkipCache(dirKey);
      if (null != dirInfo) {
        dirObjectId = dirInfo.getObjectID();
      }
    }
    return dirObjectId;
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  public OmKeyInfo getKeyInfo(String[] names) throws IOException {
    // The object ID for the directory that the key is directly in
    long parentObjectId = getDirObjectId(names, names.length - 1);
    String fileName = names[names.length - 1];
    String ozoneKey =
        getOmMetadataManager().getOzonePathKey(volumeId, bucketId,
            parentObjectId, fileName);
    return getOmMetadataManager().getFileTable().getSkipCache(ozoneKey);
  }

  @Override
  public OmDirectoryInfo getDirInfo(String[] names) throws IOException {
    String path = OM_KEY_PREFIX;
    path += String.join(OM_KEY_PREFIX, names);
    Preconditions.checkArgument(
        names.length >= 3,
        "Path should be a directory: %s", path);
    long parentObjectId = getDirObjectId(names, names.length - 1);
    String dirKey = getOmMetadataManager().getOzonePathKey(
        getVolumeObjectId(names),
        getBucketObjectId(names),
        parentObjectId,
        names[names.length - 1]);
    OmDirectoryInfo dirInfo = getOmMetadataManager()
        .getDirectoryTable().getSkipCache(dirKey);
    return dirInfo;
  }

}
