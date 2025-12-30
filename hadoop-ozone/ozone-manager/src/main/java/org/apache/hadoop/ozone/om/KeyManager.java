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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.fs.OzoneManagerFS;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.service.CompactionService;
import org.apache.hadoop.ozone.om.service.DirectoryDeletingService;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.ozone.om.service.KeyLifecycleService;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.apache.hadoop.ozone.om.snapshot.defrag.SnapshotDefragService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * Handles key level commands.
 */
public interface KeyManager extends OzoneManagerFS, IOzoneAcl {

  /**
   * Start key manager.
   *
   * @param configuration
   */
  void start(OzoneConfiguration configuration);

  /**
   * Stop key manager.
   */
  void stop() throws IOException;

  /**
   * Look up an existing key. Return the info of the key to client side, which
   * DistributedStorageHandler will use to access the data on datanode.
   *
   * @param args the args of the key provided by client.
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return a OmKeyInfo instance client uses to talk to container.
   * @throws IOException
   */
  OmKeyInfo lookupKey(OmKeyArgs args, ResolvedBucket bucketLayout,
      String clientAddress) throws IOException;

  /**
   * Return info of an existing key to client side to access to data on
   * datanodes.
   * @param args the args of the key provided by client.
   * @param clientAddress a hint to key manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return a OmKeyInfo instance client uses to talk to container.
   * @throws IOException
   */
  OmKeyInfo getKeyInfo(OmKeyArgs args, ResolvedBucket buctket,
      String clientAddress) throws IOException;

  /**
   * Returns a list of keys represented by {@link OmKeyInfo}
   * in the given bucket.
   *
   * @param volumeName
   *   the name of the volume.
   * @param bucketName
   *   the name of the bucket.
   * @param startKey
   *   the start key name, only the keys whose name is
   *   after this value will be included in the result.
   *   This key is excluded from the result.
   * @param keyPrefix
   *   key name prefix, only the keys whose name has
   *   this prefix will be included in the result.
   * @param maxKeys
   *   the maximum number of keys to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  ListKeysResult listKeys(String volumeName, String bucketName, String startKey,
                          String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * Retrieves pending deletion keys that match a given filter function.
   *
   * @param filter a functional interface specifying the filter condition to apply
   *               to the keys. It takes a KeyValue pair containing a string key and
   *               an OmKeyInfo object, and returns a boolean value indicating whether
   *               the key meets the filter criteria.
   * @param count  the maximum number of keys to retrieve.
   * @return a PendingKeysDeletion object containing the keys that satisfy the filter
   *         criteria, up to the specified count.
   * @throws IOException if an I/O error occurs while fetching the keys.
   */
  PendingKeysDeletion getPendingDeletionKeys(
      CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, IOException> filter, int count)
      throws IOException;

  /**
   * Retrieves the keys that are pending deletion in a specified bucket and volume.
   *
   * @param volume the name of the volume that contains the bucket.
   * @param bucket the name of the bucket within the volume where keys are located.
   * @param startKey the key from which to start retrieving pending deletions.
   * @param filter a filter function to determine which keys should be included
   *               in the pending deletion list.
   * @param count the maximum number of keys to retrieve that are pending deletion.
   * @return a PendingKeysDeletion object containing the list of keys
   *         pending deletion based on the specified parameters.
   * @throws IOException if an I/O error occurs during the operation.
   */
  PendingKeysDeletion getPendingDeletionKeys(
      String volume, String bucket, String startKey,
      CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, IOException> filter, int count)
      throws IOException;

  /**
   * Returns a list rename entries from the snapshotRenamedTable.
   *
   * @param count max number of keys to return.
   * @param filter filter to apply on the entries.
   * @return a Pair of list of {@link org.apache.hadoop.hdds.utils.db.Table.KeyValue} representing the keys in the
   * underlying metadataManager.
   * @throws IOException
   */
  List<Table.KeyValue<String, String>> getRenamesKeyEntries(
      String volume, String bucket, String startKey,
      CheckedFunction<Table.KeyValue<String, String>, Boolean, IOException> filter, int count)
      throws IOException;


  /**
   * Returns the previous snapshot's ozone directorInfo corresponding for the object.
   */
  CheckedFunction<KeyManager, OmDirectoryInfo, IOException> getPreviousSnapshotOzoneDirInfo(
      long volumeId, OmBucketInfo bucketInfo, OmDirectoryInfo directoryInfo) throws IOException;

  /**
   * Returns the previous snapshot's ozone directoryInfo corresponding for the object.
   */
  CheckedFunction<KeyManager, OmDirectoryInfo, IOException> getPreviousSnapshotOzoneDirInfo(
      long volumeId, OmBucketInfo bucketInfo, OmKeyInfo directoryInfo) throws IOException;

  /**
   * Returns the previous snapshot's ozone keyInfo corresponding for the object.
   */
  CheckedFunction<KeyManager, OmKeyInfo, IOException> getPreviousSnapshotOzoneKeyInfo(
      long volumeId, OmBucketInfo bucketInfo, OmKeyInfo keyInfo) throws IOException;

  /**
   * Returns a list deleted entries from the deletedTable.
   *
   * @param count max number of keys to return.
   * @param filter filter to apply on the entries.
   * @return a Pair of list of {@link org.apache.hadoop.hdds.utils.db.Table.KeyValue} representing the keys in the
   * underlying metadataManager.
   * @throws IOException
   */
  List<Table.KeyValue<String, List<OmKeyInfo>>> getDeletedKeyEntries(
      String volume, String bucket, String startKey,
      CheckedFunction<Table.KeyValue<String, RepeatedOmKeyInfo>, Boolean, IOException> filter,
      int count) throws IOException;

  /**
   * Returns the names of up to {@code count} open keys whose age is
   * greater than or equal to {@code expireThreshold}.
   *
   * @param count The maximum number of expired open keys to return.
   * @param expireThreshold The threshold of open key expiration age.
   * @param bucketLayout The type of open keys to get (e.g. DEFAULT or FSO).
   * @param leaseThreshold The threshold of hsync key.
   * @return the expired open keys.
   * @throws IOException
   */
  ExpiredOpenKeys getExpiredOpenKeys(Duration expireThreshold, int count,
      BucketLayout bucketLayout, Duration leaseThreshold) throws IOException;

  /**
   * Returns the MPU infos of up to {@code count} whose age is greater
   * than or equal to {@code expireThreshold}.
   *
   * @param expireThreshold The threshold of expired multipart uploads
   *                        to return.
   * @param maxParts The threshold of number of MPU parts to return.
   *                 This is a soft limit, which means if the last MPU has
   *                 parts that exceed this limit, it will be included
   *                 in the returned expired MPUs. This is to handle
   *                 situation where MPU that has more parts than
   *                 maxParts never get deleted.
   * @return a {@link List} of the expired
   *        {@link ExpiredMultipartUploadsBucket}, the expired multipart
   *        uploads, grouped by volume and bucket.
   */
  List<ExpiredMultipartUploadsBucket> getExpiredMultipartUploads(
      Duration expireThreshold, int maxParts) throws IOException;

  /**
   * Look up an existing key from the OM table and retrieve the tags from
   * the key info.
   *
   * @param args the args of the key provided by client.
   * @param bucket the resolved parent bucket of the key.
   * @return Map of the tag set associated with the key.
   * @throws IOException
   */
  Map<String, String> getObjectTagging(OmKeyArgs args, ResolvedBucket bucket) throws IOException;

  /**
   * Returns the metadataManager.
   * @return OMMetadataManager.
   */
  OMMetadataManager getMetadataManager();

  /**
   * Returns the instance of Deleting Service.
   * @return Background service.
   */
  KeyDeletingService getDeletingService();

  OmMultipartUploadList listMultipartUploads(String volumeName,
          String bucketName, String prefix,
          String keyMarker, String uploadIdMarker, int maxUploads, boolean withPagination) throws OMException;

  /**
   * Returns list of parts of a multipart upload key.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param uploadID
   * @param partNumberMarker
   * @param maxParts
   * @return OmMultipartUploadListParts
   */
  OmMultipartUploadListParts listParts(String volumeName, String bucketName,
      String keyName, String uploadID, int partNumberMarker,
      int maxParts)  throws IOException;

  /**
   * Refresh the key block location information by get latest info from SCM.
   * @param key
   */
  void refresh(OmKeyInfo key) throws IOException;

  /**
   * Returns an iterator for pending deleted directories all buckets.
   */
  default TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> getDeletedDirEntries() throws IOException {
    return getDeletedDirEntries(null, null);
  }

  /**
   * Returns an iterator for pending deleted directories for volume and bucket.
   * @throws IOException
   */
  TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> getDeletedDirEntries(
      String volume, String bucket) throws IOException;

  default List<Table.KeyValue<String, OmKeyInfo>> getDeletedDirEntries(String volume, String bucket, int size)
      throws IOException {
    List<Table.KeyValue<String, OmKeyInfo>> deletedDirEntries = new ArrayList<>(size);
    try (TableIterator<String, ? extends  Table.KeyValue<String, OmKeyInfo>> iterator =
             getDeletedDirEntries(volume, bucket)) {
      while (deletedDirEntries.size() < size && iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
        deletedDirEntries.add(Table.newKeyValue(kv.getKey(), kv.getValue()));
      }
      return deletedDirEntries;
    }
  }

  /**
   * Returns all sub directories under the given parent directory.
   *
   * @param parentInfo
   * @return list of dirs
   * @throws IOException
   */
  DeleteKeysResult getPendingDeletionSubDirs(long volumeId, long bucketId, OmKeyInfo parentInfo,
      CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, IOException> filter, int remainingNum)
      throws IOException;

  /**
   * Returns all sub files under the given parent directory.
   *
   * @param parentInfo
   * @return list of files
   * @throws IOException
   */
  DeleteKeysResult getPendingDeletionSubFiles(long volumeId, long bucketId, OmKeyInfo parentInfo,
      CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, IOException> filter, int remainingNum)
      throws IOException;

  /**
   * Returns the instance of Directory Deleting Service.
   * @return Background service.
   */
  DirectoryDeletingService getDirDeletingService();

  /**
   * Returns the instance of Open Key Cleanup Service.
   * @return Background service.
   */
  BackgroundService getOpenKeyCleanupService();

  /**
   * Returns the instance of Multipart Upload Cleanup Service.
   * @return Background service.
   */
  BackgroundService getMultipartUploadCleanupService();

  /**
   * Returns the instance of Snapshot SST Filtering service.
   * @return Background service.
   */
  SstFilteringService getSnapshotSstFilteringService();

  /**
   * Returns the instance of Snapshot Defrag service.
   * @return Background service.
   */
  SnapshotDefragService getSnapshotDefragService();

  /**
   * Returns the instance of Snapshot Deleting service.
   * @return Background service.
   */
  SnapshotDeletingService getSnapshotDeletingService();

  /**
   * Returns the instance of CompactionService.
   * @return BackgroundService
   */
  CompactionService getCompactionService();

  /**
   * Returns the instance of key/object lifecycle service.
   * @return Background service.
   */
  KeyLifecycleService getKeyLifecycleService();
}
