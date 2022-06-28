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
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Class for handling Legacy buckets.
 */
public class LegacyBucketHandler extends BucketHandler {

  private String vol;
  private String bucket;
  private OmBucketInfo omBucketInfo;

  public LegacyBucketHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      OmBucketInfo bucketInfo) {
    super(reconNamespaceSummaryManager, omMetadataManager,
        reconSCM);
    this.omBucketInfo = bucketInfo;
    this.vol = omBucketInfo.getVolumeName();
    this.bucket = omBucketInfo.getBucketName();
  }

  /**
   * Helper function to check if a path is a directory, key, or invalid.
   * @param keyName key name
   * @return DIRECTORY, KEY, or UNKNOWN
   * @throws IOException
   */
  @Override
  public EntityType determineKeyPath(String keyName, long bucketObjectId)
          throws IOException {

    String filename = OzoneFSUtils.removeTrailingSlashIfNeeded(keyName);
    // For example, /vol1/buck1/a/b/c/d/e/file1.txt
    // Look in the KeyTable for the key path,
    // if the first one we seek to is the same as the seek key,
    // it is a key;
    // if it is the seekKey with a trailing slash, it is a directory
    // else it is unknown
    String key = OM_KEY_PREFIX + vol +
        OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + filename;

    Table keyTable = getKeyTable();

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = keyTable.iterator();

    iterator.seek(key);
    if (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      String dbKey = kv.getKey();
      if (dbKey.equals(key)) {
        return EntityType.KEY;
      }
      if (dbKey.equals(key + OM_KEY_PREFIX)) {
        return EntityType.DIRECTORY;
      }
    }
    return EntityType.UNKNOWN;
  }

  @Override
  public long calculateDUForVolume(String volumeName)
      throws IOException {
    long result = 0L;

    Table keyTable = getKeyTable();

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = keyTable.iterator();

    while (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      OmKeyInfo keyInfo = kv.getValue();

      if (keyInfo != null) {
        if (!keyInfo.getKeyName().endsWith(OM_KEY_PREFIX)) {
          if (volumeName.equals(keyInfo.getVolumeName())) {
            result += getKeySizeWithReplication(keyInfo);
          }
        }
      }
    }
    return result;
  }

  // KeyTable's key is in the format of "vol/bucket/keyName"
  // Make use of RocksDB's order to seek to the prefix and avoid full iteration
  @Override
  public long calculateDUUnderObject(long parentId)
      throws IOException {
    Table keyTable = getKeyTable();

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = keyTable.iterator();

    String seekPrefix =
        OM_KEY_PREFIX + vol + OM_KEY_PREFIX + bucket + OM_KEY_PREFIX;

    // handle nested keys (DFS)
    NSSummary nsSummary = getReconNamespaceSummaryManager()
        .getNSSummary(parentId);
    // empty bucket
    if (nsSummary == null) {
      return 0;
    }

    if (omBucketInfo.getObjectID() != parentId) {
      String dirName = nsSummary.getDirName();
      seekPrefix += dirName;
    }

    String[] seekKeys = seekPrefix.split(OM_KEY_PREFIX);
    iterator.seek(seekPrefix);
    long totalDU = 0L;
    // handle direct keys
    while (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      String dbKey = kv.getKey();
      // since the RocksDB is ordered, seek until the prefix isn't matched
      if (!dbKey.startsWith(seekPrefix)) {
        break;
      }

      String[] keys = dbKey.split(OM_KEY_PREFIX);

      // iteration moved to the next level
      // and not handling direct keys
      if (keys.length - seekKeys.length > 1) {
        continue;
      }

      OmKeyInfo keyInfo = kv.getValue();
      if (keyInfo != null) {
        // skip directory markers, just include directKeys
        if (keyInfo.getKeyName().endsWith(OM_KEY_PREFIX)) {
          continue;
        }
        totalDU += getKeySizeWithReplication(keyInfo);
      }
    }

    Set<Long> subDirIds = nsSummary.getChildDir();
    for (long subDirId: subDirIds) {
      totalDU += calculateDUUnderObject(subDirId);
    }
    return totalDU;
  }

  /**
   * This method handles disk usage of direct keys.
   * @param parentId parent directory/bucket
   * @param withReplica if withReplica is enabled, set sizeWithReplica
   * for each direct key's DU
   * @param listFile if listFile is enabled, append key DU as a subpath
   * @param duData the current DU data
   * @param normalizedPath the normalized path request
   * @return the total DU of all direct keys
   * @throws IOException IOE
   */
  @Override
  public long handleDirectKeys(long parentId, boolean withReplica,
                               boolean listFile,
                               List<DUResponse.DiskUsage> duData,
                               String normalizedPath) throws IOException {

    Table keyTable = getKeyTable();
    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = keyTable.iterator();

    String seekPrefix =
        OM_KEY_PREFIX + vol + OM_KEY_PREFIX + bucket + OM_KEY_PREFIX;

    NSSummary nsSummary = getReconNamespaceSummaryManager()
        .getNSSummary(parentId);
    // empty bucket
    if (nsSummary == null) {
      return 0;
    }

    if (omBucketInfo.getObjectID() != parentId) {
      String dirName = nsSummary.getDirName();
      seekPrefix += dirName;
    }
    String[] seekKeys = seekPrefix.split(OM_KEY_PREFIX);

    iterator.seek(seekPrefix);

    long keyDataSizeWithReplica = 0L;

    while (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      String dbKey = kv.getKey();

      if (!dbKey.startsWith(seekPrefix)) {
        break;
      }

      String[] keys = dbKey.split(OM_KEY_PREFIX);

      // iteration moved to the next level
      // and not handling direct keys
      if (keys.length - seekKeys.length > 1) {
        continue;
      }

      OmKeyInfo keyInfo = kv.getValue();
      if (keyInfo != null) {
        // skip directory markers, just include directKeys
        if (keyInfo.getKeyName().endsWith(OM_KEY_PREFIX)) {
          continue;
        }
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        String subpath = buildSubpath(normalizedPath,
            keyInfo.getFileName());
        diskUsage.setSubpath(subpath);
        diskUsage.setKey(true);
        diskUsage.setSize(keyInfo.getDataSize());

        if (withReplica) {
          long keyDU = getKeySizeWithReplication(keyInfo);
          keyDataSizeWithReplica += keyDU;
          diskUsage.setSizeWithReplica(keyDU);
        }
        // list the key as a subpath
        if (listFile) {
          duData.add(diskUsage);
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
    StringBuilder bld = new StringBuilder();
    for (int i = 0; i < cutoff; ++i) {
      bld.append(OM_KEY_PREFIX)
          .append(names[i]);
    }
    bld.append(OM_KEY_PREFIX);
    String dirKey = bld.toString();
    OmKeyInfo dirInfo = getKeyTable().getSkipCache(dirKey);
    
    if (dirInfo != null) {
      dirObjectId = dirInfo.getObjectID();
    }
    return dirObjectId;
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.LEGACY;
  }

  @Override
  public int getTotalDirCount(long objectId) throws IOException {
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

  @Override
  public OmKeyInfo getKeyInfo(String[] names) throws IOException {
    StringBuilder bld = new StringBuilder();
    for (int i = 0; i < names.length; i++) {
      bld.append(OM_KEY_PREFIX)
          .append(names[i]);
    }
    String ozoneKey = bld.toString();
    OmKeyInfo keyInfo = getKeyTable().get(ozoneKey);
    return keyInfo;
  }

  @Override
  public Table<String, OmKeyInfo> getKeyTable() {
    Table keyTable =
        getOmMetadataManager().getKeyTable(getBucketLayout());
    return keyTable;
  }
}
