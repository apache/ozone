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
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

/**
 * Class for handling OBS buckets NameSpaceSummaries.
 */
public class OBSBucketHandler extends BucketHandler {

  private final String vol;
  private final String bucket;
  private final OmBucketInfo omBucketInfo;

  public OBSBucketHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OmBucketInfo bucketInfo) {
    super(reconNamespaceSummaryManager, omMetadataManager);
    this.omBucketInfo = bucketInfo;
    this.vol = omBucketInfo.getVolumeName();
    this.bucket = omBucketInfo.getBucketName();
  }

  /**
   * Helper function to check if a path is a key, or invalid.
   *
   * @param keyName key name
   * @return KEY, or UNKNOWN
   * @throws IOException
   */
  @Override
  public EntityType determineKeyPath(String keyName) throws IOException {
    String key = OM_KEY_PREFIX + vol +
        OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + keyName;

    Table<String, OmKeyInfo> keyTable = getKeyTable();

    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            iterator = keyTable.iterator()) {
      iterator.seek(key);
      if (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
        String dbKey = kv.getKey();
        if (dbKey.equals(key)) {
          return EntityType.KEY;
        }
      }
    }
    return EntityType.UNKNOWN;
  }

  /**
   * This method handles disk usage of direct keys.
   *
   * @param parentId       The identifier for the parent bucket.
   * @param withReplica    if withReplica is enabled, set sizeWithReplica
   *                       for each direct key's DU
   * @param listFile       if listFile is enabled, append key DU as a children
   *                       keys
   * @param duData         the current DU data
   * @param normalizedPath the normalized path request
   * @return the total DU of all direct keys
   * @throws IOException IOE
   */
  @Override
  public long handleDirectKeys(long parentId, boolean withReplica,
                               boolean listFile,
                               List<DUResponse.DiskUsage> duData,
                               String normalizedPath) throws IOException {

    NSSummary nsSummary = getReconNamespaceSummaryManager()
        .getNSSummary(parentId);
    // Handle the case of an empty bucket.
    if (nsSummary == null) {
      return 0;
    }

    Table<String, OmKeyInfo> keyTable = getKeyTable();
    long keyDataSizeWithReplica = 0L;

    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            iterator = keyTable.iterator()) {

      String seekPrefix = OM_KEY_PREFIX +
          vol +
          OM_KEY_PREFIX +
          bucket +
          OM_KEY_PREFIX;

      iterator.seek(seekPrefix);

      while (iterator.hasNext()) {
        // KeyName : OmKeyInfo-Object
        Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
        String dbKey = kv.getKey();

        // Exit loop if the key doesn't match the seekPrefix.
        if (!dbKey.startsWith(seekPrefix)) {
          break;
        }

        OmKeyInfo keyInfo = kv.getValue();
        if (keyInfo != null) {
          DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
          String objectName = keyInfo.getKeyName();
          diskUsage.setSubpath(objectName);
          diskUsage.setKey(true);
          diskUsage.setSize(keyInfo.getDataSize());

          if (withReplica) {
            long keyDU = keyInfo.getReplicatedSize();
            keyDataSizeWithReplica += keyDU;
            diskUsage.setSizeWithReplica(keyDU);
          }
          // List all the keys for the OBS bucket if requested.
          if (listFile) {
            duData.add(diskUsage);
          }
        }
      }
    }

    return keyDataSizeWithReplica;
  }

  /**
   * Calculates the total disk usage (DU) for an Object Store Bucket (OBS) by
   * summing the sizes of all keys contained within the bucket.
   * Since OBS buckets operate on a flat hierarchy, this method iterates through
   * all the keys in the bucket without the need to traverse directories.
   *
   * @param parentId The identifier for the parent bucket.
   * @return The total disk usage of all keys within the specified OBS bucket.
   * @throws IOException
   */
  @Override
  public long calculateDUUnderObject(long parentId) throws IOException {
    NSSummary nsSummary = getReconNamespaceSummaryManager().getNSSummary(parentId);
    return nsSummary != null ? nsSummary.getReplicatedSizeOfFiles() : 0L;
  }

  /**
   * Object stores do not support directories.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public long getDirObjectId(String[] names)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Object stores do not support directories.");
  }

  /**
   * Object stores do not support directories.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public long getDirObjectId(String[] names, int cutoff)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Object stores do not support directories.");
  }

  /**
   * Returns the keyInfo object from the KEY table.
   * @return OmKeyInfo
   */
  @Override
  public OmKeyInfo getKeyInfo(String[] names) throws IOException {
    String ozoneKey = OM_KEY_PREFIX;
    ozoneKey += String.join(OM_KEY_PREFIX, names);

    return getKeyTable().getSkipCache(ozoneKey);
  }

  /**
   * Object stores do not support directories.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public OmDirectoryInfo getDirInfo(String[] names) throws IOException {
    throw new UnsupportedOperationException(
        "Object stores do not support directories.");
  }

  public Table<String, OmKeyInfo> getKeyTable() {
    return getOmMetadataManager().getKeyTable(getBucketLayout());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }

}
