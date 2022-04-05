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
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Class for handling FSO buckets.
 */
public class FSOBucketHandler extends BucketHandler {

  public FSOBucketHandler(
          ReconNamespaceSummaryManager reconNamespaceSummaryManager,
          ReconOMMetadataManager omMetadataManager,
          OzoneStorageContainerManager reconSCM) {
    super(reconNamespaceSummaryManager, omMetadataManager, reconSCM);
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
    java.nio.file.Path keyPath = Paths.get(keyName);
    Iterator<Path> elements = keyPath.iterator();

    long lastKnownParentId = bucketObjectId;
    OmDirectoryInfo omDirInfo = null;
    while (elements.hasNext()) {
      String fileName = elements.next().toString();

      // For example, /vol1/buck1/a/b/c/d/e/file1.txt
      // 1. Do lookup path component on directoryTable starting from bucket
      // 'buck1' to the leaf node component, which is 'file1.txt'.
      // 2. If there is no dir exists for the leaf node component 'file1.txt'
      // then do look it on fileTable.
      String dbNodeName = getOmMetadataManager()
              .getOzonePathKey(lastKnownParentId, fileName);
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


  // FileTable's key is in the format of "parentId/fileName"
  // Make use of RocksDB's order to seek to the prefix and avoid full iteration
  @Override
  public long calculateDUUnderObject(long parentId) throws IOException {
    Table keyTable = getOmMetadataManager().getFileTable();

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            iterator = keyTable.iterator();

    String seekPrefix = parentId + OM_KEY_PREFIX;
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
      OmKeyInfo keyInfo = kv.getValue();
      if (keyInfo != null) {
        totalDU += getKeySizeWithReplication(keyInfo);
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

    Table keyTable = getOmMetadataManager().getFileTable();
    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            iterator = keyTable.iterator();

    String seekPrefix = parentId + OM_KEY_PREFIX;
    iterator.seek(seekPrefix);

    long keyDataSizeWithReplica = 0L;

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
    String dirKey = null;
    for (int i = 2; i < cutoff; ++i) {
      dirKey = getOmMetadataManager().getOzonePathKey(dirObjectId, names[i]);
      OmDirectoryInfo dirInfo =
              getOmMetadataManager().getDirectoryTable().getSkipCache(dirKey);
      dirObjectId = dirInfo.getObjectID();
    }
    return dirObjectId;
  }

}
