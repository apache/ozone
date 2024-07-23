/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.DBStoreHAManager;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.storage.proto.
    OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;

import com.google.common.annotations.VisibleForTesting;
import org.apache.ozone.compaction.log.CompactionLogEntry;

/**
 * OM metadata manager interface.
 */
public interface OMMetadataManager extends DBStoreHAManager {
  /**
   * Start metadata manager.
   *
   * @param configuration
   * @throws IOException
   */
  void start(OzoneConfiguration configuration) throws IOException;

  /**
   * Stop metadata manager.
   */
  void stop() throws Exception;

  /**
   * Get metadata store.
   *
   * @return metadata store.
   */
  @VisibleForTesting
  DBStore getStore();

  /**
   * Returns the OzoneManagerLock used on Metadata DB.
   *
   * @return OzoneManagerLock
   */
  IOzoneManagerLock getLock();

  /**
   * Returns the epoch associated with current OM process.
   */
  long getOmEpoch();

  /**
   * Given a volume return the corresponding DB key.
   *
   * @param volume - Volume name
   */
  String getVolumeKey(String volume);

  /**
   * Given a user return the corresponding DB key.
   *
   * @param user - User name
   */
  String getUserKey(String user);

  /**
   * Given a volume and bucket, return the corresponding DB key.
   *
   * @param volume - User name
   * @param bucket - Bucket name
   */
  String getBucketKey(String volume, String bucket);

  /**
   * Given a volume, bucket and a key, return the corresponding DB key.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key    - key name
   * @return DB key as String.
   */
  String getOzoneKey(String volume, String bucket, String key);

  /**
   * Given a volume, bucket and a key, return the corresponding DB directory
   * key.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key    - key name
   * @return DB directory key as String.
   */
  String getOzoneDirKey(String volume, String bucket, String key);


  /**
   * Returns the DB key name of a open key in OM metadata store. Should be
   * #open# prefix followed by actual key name.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @param id - the id for this open
   * @return bytes of DB key.
   */
  default String getOpenKey(String volume, String bucket, String key, long id) {
    return getOpenKey(volume, bucket, key, String.valueOf(id));
  }

  /**
   * Returns the DB key name of a open key in OM metadata store. Should be
   * #open# prefix followed by actual key name.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @param clientId - client Id String for this open key
   * @return bytes of DB key.
   */
  String getOpenKey(String volume, String bucket, String key, String clientId);

  /**
   * Given a volume, check if it is empty, i.e there are no buckets inside it.
   *
   * @param volume - Volume name
   */
  boolean isVolumeEmpty(String volume) throws IOException;

  /**
   * Given a volume/bucket, check if it is empty, i.e there are no keys inside
   * it.
   *
   * @param volume - Volume name
   * @param bucket - Bucket name
   * @return true if the bucket is empty
   */
  boolean isBucketEmpty(String volume, String bucket) throws IOException;

  /**
   * Returns a list of buckets represented by {@link OmBucketInfo} in the given
   * volume.
   *
   * @param volumeName the name of the volume. This argument is required, this
   * method returns buckets in this given volume.
   * @param startBucket the start bucket name. Only the buckets whose name is
   * after this value will be included in the result. This key is excluded from
   * the result.
   * @param bucketPrefix bucket name prefix. Only the buckets whose name has
   * this prefix will be included in the result.
   * @param maxNumOfBuckets the maximum number of buckets to return. It ensures
   * the size of the result will not exceed this limit.
   * @param hasSnapshot set the flag to list buckets which have snapshot.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listBuckets(String volumeName, String startBucket,
                                 String bucketPrefix, int maxNumOfBuckets,
                                 boolean hasSnapshot)
      throws IOException;

  /**
   * Returns a list of keys represented by {@link OmKeyInfo} in the given
   * bucket.
   *
   * @param volumeName the name of the volume.
   * @param bucketName the name of the bucket.
   * @param startKey the start key name, only the keys whose name is after this
   * value will be included in the result. This key is excluded from the
   * result.
   * @param keyPrefix key name prefix, only the keys whose name has this prefix
   * will be included in the result.
   * @param maxKeys the maximum number of keys to return. It ensures the size of
   * the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  ListKeysResult listKeys(String volumeName,
                          String bucketName, String startKey, String keyPrefix,
                          int maxKeys)
      throws IOException;

  /**
   * List trash allows the user to list the keys that were marked as deleted,
   * but not actually deleted by Ozone Manager. This allows a user to recover
   * keys within a configurable window.
   * @param volumeName - The volume name, which can also be a wild card
   *                   using '*'.
   * @param bucketName - The bucket name, which can also be a wild card
   *                   using '*'.
   * @param startKeyName - List keys from a specific key name.
   * @param keyPrefix - List keys using a specific prefix.
   * @param maxKeys - The number of keys to be returned. This must be below
   *                the cluster level set by admins.
   * @return The list of keys that are deleted from the deleted table.
   * @throws IOException
   */
  List<RepeatedOmKeyInfo> listTrash(String volumeName, String bucketName,
      String startKeyName, String keyPrefix, int maxKeys) throws IOException;

  /**
   * Returns snapshot info for volume/bucket snapshot path.
   * @param volumeName volume name
   * @param bucketName bucket name
   * @param snapshotName snapshot name
   * @return snapshot info for volume/bucket snapshot path.
   * @throws IOException
   */
  SnapshotInfo getSnapshotInfo(String volumeName, String bucketName,
                               String snapshotName) throws IOException;

  /**
   * List snapshots in a volume/bucket.
   * @param volumeName     volume name
   * @param bucketName     bucket name
   * @param snapshotPrefix snapshot prefix to match
   * @param prevSnapshot   snapshots will be listed after this snapshot name
   * @param maxListResult  max number of snapshots to return
   * @return list of snapshot
   */
  ListSnapshotResponse listSnapshot(
      String volumeName, String bucketName, String snapshotPrefix,
      String prevSnapshot, int maxListResult) throws IOException;

  /**
   * Recover trash allows the user to recover the keys
   * that were marked as deleted, but not actually deleted by Ozone Manager.
   * @param volumeName - The volume name.
   * @param bucketName - The bucket name.
   * @param keyName - The key user want to recover.
   * @param destinationBucket - The bucket user want to recover to.
   * @return The result of recovering operation is success or not.
   */
  boolean recoverTrash(String volumeName, String bucketName,
      String keyName, String destinationBucket) throws IOException;

  /**
   * Returns a list of volumes owned by a given user; if user is null, returns
   * all volumes.
   *
   * @param userName volume owner
   * @param prefix the volume prefix used to filter the listing result.
   * @param startKey the start volume name determines where to start listing
   * from, this key is excluded from the result.
   * @param maxKeys the maximum number of volumes to return.
   * @return a list of {@link OmVolumeArgs}
   * @throws IOException
   */
  List<OmVolumeArgs> listVolumes(String userName, String prefix,
      String startKey, int maxKeys) throws IOException;

  /**
   * Returns the names of up to {@code count} open keys whose age is
   * greater than or equal to {@code expireThreshold}.
   *
   * @param count The maximum number of open keys to return.
   * @param expireThreshold The threshold of open key expire age.
   * @param bucketLayout The type of open keys to get (e.g. DEFAULT or FSO).
   * @return the expired open keys.
   * @throws IOException
   */
  ExpiredOpenKeys getExpiredOpenKeys(Duration expireThreshold, int count,
      BucketLayout bucketLayout) throws IOException;

  /**
   * Returns the names of up to {@code count} MPU key whose age is greater
   * than or equal to {@code expireThreshold}.
   *
   * @param expireThreshold The threshold of MPU key expiration age.
   * @param maxParts The threshold of number of MPU parts to return.
   *                 This is a soft limit, which means if the last MPU has
   *                 parts that exceed this limit, it will be included
   *                 in the returned expired MPUs. This is to handle
   *                 situation where MPU that has more parts than
   *                 maxParts never get deleted.
   * @return a {@link List} of {@link ExpiredMultipartUploadsBucket}, the
   *         expired multipart uploads, grouped by volume and bucket.
   */
  List<ExpiredMultipartUploadsBucket> getExpiredMultipartUploads(
      Duration expireThreshold, int maxParts) throws IOException;

  /**
   * Returns the user Table.
   *
   * @return UserTable.
   */
  Table<String, PersistedUserVolumeInfo> getUserTable();

  /**
   * Returns the Volume Table.
   *
   * @return VolumeTable.
   */
  Table<String, OmVolumeArgs> getVolumeTable();

  /**
   * Returns the BucketTable.
   *
   * @return BucketTable.
   */
  Table<String, OmBucketInfo> getBucketTable();

  /**
   * Returns the KeyTable.
   *
   * @return KeyTable.
   */

  Table<String, OmKeyInfo> getKeyTable(BucketLayout bucketLayout);

  /**
   * Returns the FileTable.
   *
   * @return FileTable.
   */
  Table<String, OmKeyInfo> getFileTable();

  /**
   * Get Deleted Table.
   *
   * @return Deleted Table.
   */
  Table<String, RepeatedOmKeyInfo> getDeletedTable();

  /**
   * Gets the OpenKeyTable.
   *
   * @return Table.
   */
  Table<String, OmKeyInfo> getOpenKeyTable(BucketLayout buckLayout);

  /**
   * Gets the DelegationTokenTable.
   *
   * @return Table.
   */
  Table<OzoneTokenIdentifier, Long> getDelegationTokenTable();

  /**
   * Gets the Ozone prefix path to its acl mapping table.
   * @return Table.
   */
  Table<String, OmPrefixInfo> getPrefixTable();

  /**
   * Returns the DB key name of a multipart upload key in OM metadata store
   * for LEGACY/OBS buckets.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @param uploadId - the upload id for this key
   * @return bytes of DB key.
   */
  String getMultipartKey(String volume, String bucket, String key, String
      uploadId);

  /**
   * Returns the DB key name of a multipart upload key in OM metadata store
   * for FSO-enabled buckets.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @param uploadId - the upload id for this key
   * @return bytes of DB key.
   */
  String getMultipartKeyFSO(String volume, String bucket, String key, String
          uploadId) throws IOException;


  /**
   * Gets the multipart info table which holds the information about
   * multipart upload information of the keys.
   * @return Table
   */
  Table<String, OmMultipartKeyInfo> getMultipartInfoTable();

  Table<String, TransactionInfo> getTransactionInfoTable();

  Table<String, OmDBAccessIdInfo> getTenantAccessIdTable();

  Table<String, OmDBUserPrincipalInfo> getPrincipalToAccessIdsTable();

  Table<String, OmDBTenantState> getTenantStateTable();

  Table<String, SnapshotInfo> getSnapshotInfoTable();

  Table<String, String> getSnapshotRenamedTable();

  Table<String, CompactionLogEntry> getCompactionLogTable();
  /**
   * Gets the OM Meta table.
   * @return meta table reference.
   */
  Table<String, String> getMetaTable();

  /**
   * Returns number of rows in a table.  This should not be used for very
   * large tables.
   * @param table
   * @return long
   * @throws IOException
   */
  <KEY, VALUE> long countRowsInTable(Table<KEY, VALUE> table)
      throws IOException;

  /**
   * Returns an estimated number of rows in a table.  This is much quicker
   * than {@link OMMetadataManager#countRowsInTable} but the result can be
   * inaccurate.
   * @param table Table
   * @return long Estimated number of rows in the table.
   * @throws IOException
   */
  <KEY, VALUE> long countEstimatedRowsInTable(Table<KEY, VALUE> table)
      throws IOException;

  /**
   * Return the existing upload keys which includes volumeName, bucketName,
   * keyName.
   */
  Set<String> getMultipartUploadKeys(String volumeName,
      String bucketName, String prefix) throws IOException;

  /**
   * Gets the DirectoryTable.
   * @return Table.
   */
  Table<String, OmDirectoryInfo> getDirectoryTable();

  /**
   * Return table mapped to the specified table name.
   * @param tableName
   * @return Table
   */
  Table getTable(String tableName);

  /**
   * Return a map of tableName and table in OM DB.
   * @return map of table and table name.
   */
  Map<String, Table> listTables();

  /**
   * Return Set of table names created in OM DB.
   * @return table names in OM DB.
   */
  Set<String> listTableNames();

  Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>>
      getBucketIterator();

  TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
      getKeyIterator() throws IOException;

  /**
   * Given parent object id and path component name, return the corresponding
   * DB 'prefixKey' key.
   *
   * @param volumeId - ID of the volume
   * @param bucketId - ID of the bucket
   * @param parentObjectId - parent object Id
   * @param pathComponentName   - path component name
   * @return DB directory key as String.
   */
  String getOzonePathKey(long volumeId, long bucketId,
                         long parentObjectId, String pathComponentName);

  default String getOzonePathKey(long volumeId, long bucketId,
      OmDirectoryInfo dir) {
    return getOzonePathKey(volumeId, bucketId,
        dir.getParentObjectID(), dir.getName());
  }

  /**
   * Given ozone path key, component id, return the corresponding 
   * DB path key for delete table.
   *
   * @param objectId - object Id
   * @param pathKey   - path key of component
   * @return DB Delete directory key as String.
   */
  String getOzoneDeletePathKey(long objectId, String pathKey);

  /**
   * Given ozone delete path key return the corresponding
   * DB path key for directory table.
   *
   * @param ozoneDeletePath - ozone delete path
   * @return DB directory key as String.
   */
  String getOzoneDeletePathDirKey(String ozoneDeletePath);

  /**
   * Returns DB key name of an open file in OM metadata store. Should be
   * #open# prefix followed by actual leaf node name.
   *
   * @param volumeId       - ID of the volume
   * @param bucketId       - ID of the bucket
   * @param parentObjectId - parent object Id
   * @param fileName       - file name
   * @param id             - client id for this open request
   * @return DB directory key as String.
   */
  default String getOpenFileName(long volumeId, long bucketId, long parentObjectId, String fileName, long id) {
    return getOpenFileName(volumeId, bucketId, parentObjectId, fileName, String.valueOf(id));
  }

  /**
   * Returns DB key name of an open file in OM metadata store. Should be
   * #open# prefix followed by actual leaf node name.
   *
   * @param volumeId       - ID of the volume
   * @param bucketId       - ID of the bucket
   * @param parentObjectId - parent object Id
   * @param fileName       - file name
   * @param clientId       - client id String for this open request
   * @return DB directory key as String.
   */
  String getOpenFileName(long volumeId, long bucketId, long parentObjectId, String fileName, String clientId);

  /**
   * Given a volume, bucket and a objectID, return the DB key name in
   * snapshotRenamedTable.
   *
   * @param volume   - volume name
   * @param bucket   - bucket name
   * @param objectID - objectID of the key
   * @return DB rename key as String.
   */
  String getRenameKey(String volume, String bucket, long objectID);

  /**
   * Returns the DB key name of a multipart upload key in OM metadata store
   * for FSO-enabled buckets.
   *
   * @param volumeId       - ID of the volume
   * @param bucketId       - ID of the bucket
   * @param parentObjectId - parent object Id
   * @param fileName       - file name
   * @param uploadId       - the upload id for this key
   * @return bytes of DB key.
   */
  String getMultipartKey(long volumeId, long bucketId,
                         long parentObjectId, String fileName,
                         String uploadId);

  /**
   * Get Deleted Directory Table.
   *
   * @return Deleted Directory Table.
   */
  Table<String, OmKeyInfo> getDeletedDirTable();

  /**
   * Get the ID of given volume.
   *
   * @param volume volume name
   * @return ID of the volume
   * @throws IOException
   */
  long getVolumeId(String volume) throws IOException;

  /**
   * Get the ID of given bucket.
   *
   * @param volume volume name
   * @param bucket bucket name
   * @return ID of the bucket
   * @throws IOException
   */
  long getBucketId(String volume, String bucket) throws IOException;

  /**
   * Returns List<{@link BlockGroup}> for a key in the deletedTable.
   * @param deletedKey - key to be purged from the deletedTable
   * @return {@link BlockGroup}
   */
  List<BlockGroup> getBlocksForKeyDelete(String deletedKey) throws IOException;

  /**
   * Given a volume/bucket, check whether it contains incomplete MPUs.
   *
   * @param volume - Volume name
   * @param bucket - Bucket name
   * @return true if the bucket is empty
   */
  boolean containsIncompleteMPUs(String volume, String bucket)
      throws IOException;
}
