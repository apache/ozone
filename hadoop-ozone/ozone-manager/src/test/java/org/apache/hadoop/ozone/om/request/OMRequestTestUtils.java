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

package org.apache.hadoop.ozone.om.request;

import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.getOmKeyInfo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfigValidator;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.KeyValue;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3VolumeContextRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignAdminRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignUserAccessIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantGetUserInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantListUserRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeAdminRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeUserAccessIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.logging.log4j.util.Strings;

/**
 * Helper class to test OMClientRequest classes.
 */
public final class OMRequestTestUtils {

  private OMRequestTestUtils() {
    //Do nothing
  }

  /**
   * Add's volume and bucket creation entries to OM DB.
   * @param volumeName
   * @param bucketName
   * @param omMetadataManager
   * @throws Exception
   */
  public static OmBucketInfo addVolumeAndBucketToDB(String volumeName,
      String bucketName, OMMetadataManager omMetadataManager) throws Exception {
    return addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager,
        BucketLayout.DEFAULT);
  }

  public static OmBucketInfo addVolumeAndBucketToDB(String volumeName,
      String bucketName, OMMetadataManager omMetadataManager,
      BucketLayout bucketLayout) throws Exception {

    if (!omMetadataManager.getVolumeTable().isExist(
            omMetadataManager.getVolumeKey(volumeName))) {
      addVolumeToDB(volumeName, omMetadataManager);
    }
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    if (omBucketInfo == null) {
      return addBucketToDB(volumeName, bucketName, omMetadataManager, bucketLayout);
    }
    return omBucketInfo;
  }

  public static void addVolumeAndBucketToDB(
      String volumeName, OMMetadataManager omMetadataManager,
      OmBucketInfo.Builder builder)
      throws Exception {
    if (!omMetadataManager.getVolumeTable().isExist(
        omMetadataManager.getVolumeKey(volumeName))) {
      addVolumeToDB(volumeName, omMetadataManager);
    }

    addBucketToDB(omMetadataManager, builder);
  }

  @SuppressWarnings("parameterNumber")
  public static void addKeyToTableAndCache(String volumeName, String bucketName,
      String keyName, long clientID, ReplicationConfig replicationConfig, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws Exception {
    addKeyToTable(false, true, volumeName, bucketName, keyName, clientID,
        replicationConfig, trxnLogIndex, omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   *
   * @param openKeyTable
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param clientID
   * @param replicationConfig
   * @param omMetadataManager
   * @param locationList
   * @throws Exception
   */
  @SuppressWarnings("parameterNumber")
  public static void addKeyToTable(boolean openKeyTable, String volumeName,
      String bucketName, String keyName, long clientID,
      ReplicationConfig replicationConfig,
      OMMetadataManager omMetadataManager,
      List<OmKeyLocationInfo> locationList, long version) throws Exception {
    addKeyToTable(openKeyTable, false, volumeName, bucketName, keyName,
        clientID, replicationConfig, 0L, omMetadataManager,
        locationList, version);
  }


  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   *
   * @param openKeyTable
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param clientID
   * @param replicationConfig
   * @param omMetadataManager
   * @throws Exception
   */
  @SuppressWarnings("parameterNumber")
  public static OmKeyInfo addKeyToTable(boolean openKeyTable, String volumeName,
      String bucketName, String keyName, long clientID,
      ReplicationConfig replicationConfig,
      OMMetadataManager omMetadataManager) throws Exception {
    return addKeyToTable(openKeyTable, false, volumeName, bucketName, keyName,
        clientID, replicationConfig, 0L, omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable. isMultipartKey flag indicates
   * whether the key is a MPU key.
   *
   * @param openKeyTable
   * @param isMultipartKey
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param clientID
   * @param replicationConfig
   * @param omMetadataManager
   * @throws Exception
   */
  @SuppressWarnings("parameterNumber")
  public static void addKeyToTable(boolean openKeyTable, boolean isMultipartKey,
      String volumeName, String bucketName, String keyName, long clientID,
      ReplicationConfig replicationConfig,
      OMMetadataManager omMetadataManager) throws Exception {
    addKeyToTable(openKeyTable, isMultipartKey, false,
        volumeName, bucketName, keyName, clientID, replicationConfig, 0L, omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @throws Exception
   */
  @SuppressWarnings("parameternumber")
  public static void addKeyToTable(boolean openKeyTable, boolean addToCache,
      String volumeName, String bucketName, String keyName, long clientID, ReplicationConfig replicationConfig,
      long trxnLogIndex,
      OMMetadataManager omMetadataManager,
      List<OmKeyLocationInfo> locationList, long version) throws Exception {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationConfig, new OmKeyLocationInfoGroup(version, new ArrayList<>(), false))
        .setObjectID(trxnLogIndex)
        .build();

    omKeyInfo.appendNewBlocks(locationList, false);

    addKeyToTable(openKeyTable, addToCache, omKeyInfo, clientID, trxnLogIndex,
        omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @throws Exception
   */
  @SuppressWarnings("parameternumber")
  public static OmKeyInfo addKeyToTable(boolean openKeyTable, boolean addToCache,
      String volumeName, String bucketName, String keyName, long clientID,
      ReplicationConfig replicationConfig, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws Exception {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName, replicationConfig)
        .setObjectID(trxnLogIndex).build();

    addKeyToTable(openKeyTable, addToCache, omKeyInfo, clientID, trxnLogIndex,
        omMetadataManager);

    return omKeyInfo;
  }

  /**
   * Add key entry to SnapshotRenamedTable.
   */
  public static String addRenamedEntryToTable(long trxnLogIndex, String volumeName, String bucketName, String key,
      OMMetadataManager omMetadataManager) throws Exception {
    String renameKey = omMetadataManager.getRenameKey(volumeName, bucketName, trxnLogIndex);
    omMetadataManager.getSnapshotRenamedTable().put(renameKey, key);
    return renameKey;
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @throws Exception
   */
  @SuppressWarnings("parameternumber")
  public static void addKeyToTable(boolean openKeyTable, boolean isMultipartKey,
      boolean addToCache, String volumeName, String bucketName, String keyName,
      long clientID, ReplicationConfig replicationConfig, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws Exception {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationConfig, new OmKeyLocationInfoGroup(0, new ArrayList<>(), isMultipartKey))
        .setObjectID(trxnLogIndex)
        .build();

    addKeyToTable(openKeyTable, addToCache, omKeyInfo, clientID, trxnLogIndex,
        omMetadataManager);
  }

  @SuppressWarnings("parameternumber")
  public static void addKeyToTable(boolean openKeyTable, boolean isMultipartKey,
      boolean addToCache, String volumeName, String bucketName, String keyName,
      long clientID, ReplicationConfig replicationConfig, long trxnLogIndex,
      OMMetadataManager omMetadataManager, List<OmKeyLocationInfo> locationList, long version) throws Exception {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationConfig, new OmKeyLocationInfoGroup(version, new ArrayList<>(), isMultipartKey))
        .setObjectID(trxnLogIndex)
        .build();

    omKeyInfo.appendNewBlocks(locationList, false);

    addKeyToTable(openKeyTable, addToCache, omKeyInfo, clientID, trxnLogIndex,
        omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @throws Exception
   */
  public static void addKeyToTable(boolean openKeyTable, boolean addToCache,
                                   OmKeyInfo omKeyInfo,  long clientID,
                                   long trxnLogIndex,
                                   OMMetadataManager omMetadataManager)
      throws Exception {
    String volumeName = omKeyInfo.getVolumeName();
    String bucketName = omKeyInfo.getBucketName();
    String keyName = omKeyInfo.getKeyName();

    if (openKeyTable) {
      String ozoneKey = omMetadataManager.getOpenKey(volumeName, bucketName,
          keyName, clientID);
      if (addToCache) {
        omMetadataManager.getOpenKeyTable(getDefaultBucketLayout())
            .addCacheEntry(new CacheKey<>(ozoneKey),
                CacheValue.get(trxnLogIndex, omKeyInfo));
      }
      omMetadataManager.getOpenKeyTable(getDefaultBucketLayout())
          .put(ozoneKey, omKeyInfo);
    } else {
      String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);

      // Simulate bucket quota (usage) update done in OMKeyCommitRequest
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo omBucketInfo = omMetadataManager.getBucketTable().get(
          bucketKey);
      // omBucketInfo can be null if some mocked tests doesn't use the table
      if (omBucketInfo != null) {
        omBucketInfo.incrUsedBytes(omKeyInfo.getReplicatedSize());
      }

      if (addToCache) {
        omMetadataManager.getKeyTable(getDefaultBucketLayout())
            .addCacheEntry(new CacheKey<>(ozoneKey),
                CacheValue.get(trxnLogIndex, omKeyInfo));
        if (omBucketInfo != null) {
          omMetadataManager.getBucketTable()
              .addCacheEntry(new CacheKey<>(bucketKey),
                  CacheValue.get(trxnLogIndex + 1, omBucketInfo));
        }
      }
      omMetadataManager.getKeyTable(getDefaultBucketLayout())
          .put(ozoneKey, omKeyInfo);

      if (omBucketInfo != null) {
        omMetadataManager.getBucketTable().put(bucketKey, omBucketInfo);
      }
    }
  }

  /**
   * Add open multipart key to openKeyTable.
   *
   * @throws Exception DB failure
   */
  public static void addMultipartKeyToOpenKeyTable(boolean addToCache,
      OmKeyInfo omKeyInfo, String uploadId, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws IOException {

    String multipartKey = omMetadataManager.getMultipartKey(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
        omKeyInfo.getKeyName(), uploadId);

    if (addToCache) {
      omMetadataManager.getOpenKeyTable(BucketLayout.DEFAULT)
          .addCacheEntry(new CacheKey<>(multipartKey),
              CacheValue.get(trxnLogIndex, omKeyInfo));
    }
    omMetadataManager.getOpenKeyTable(BucketLayout.DEFAULT)
        .put(multipartKey, omKeyInfo);
  }

  /**
   * Add multipart info entry to the multipartInfoTable.
   * @throws Exception
   */
  public static String addMultipartInfoToTable(boolean addToCache,
      OmKeyInfo omKeyInfo, OmMultipartKeyInfo omMultipartKeyInfo,
      long trxnLogIndex, OMMetadataManager omMetadataManager)
      throws IOException {
    String ozoneDBKey = omMetadataManager.getMultipartKey(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
        omKeyInfo.getKeyName(), omMultipartKeyInfo.getUploadID());

    if (addToCache) {
      omMetadataManager.getMultipartInfoTable()
          .addCacheEntry(new CacheKey<>(ozoneDBKey),
              CacheValue.get(trxnLogIndex, omMultipartKeyInfo));
    }

    omMetadataManager.getMultipartInfoTable().put(ozoneDBKey,
        omMultipartKeyInfo);

    return ozoneDBKey;
  }

  /**
   * Add multipart info entry to the multipartInfoTable.
   * 
   * @throws Exception
   */
  public static String addMultipartInfoToTableCache(
      OmKeyInfo omKeyInfo, OmMultipartKeyInfo omMultipartKeyInfo,
      long trxnLogIndex, OMMetadataManager omMetadataManager) throws IOException {
    String ozoneDBKey = omMetadataManager.getMultipartKey(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
        omKeyInfo.getKeyName(), omMultipartKeyInfo.getUploadID());

    omMetadataManager.getMultipartInfoTable()
        .addCacheEntry(new CacheKey<>(ozoneDBKey),
            CacheValue.get(trxnLogIndex, omMultipartKeyInfo));

    return ozoneDBKey;
  }

  public static PartKeyInfo createPartKeyInfo(String volumeName,
      String bucketName, String keyName, String uploadId, int partNumber) {
    return PartKeyInfo.newBuilder()
        .setPartNumber(partNumber)
        .setPartName(OmMultipartUpload.getDbKey(
            volumeName, bucketName, keyName, uploadId))
        .setPartKeyInfo(KeyInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setDataSize(100L) // Just set dummy size for testing
            .setCreationTime(Time.now())
            .setModificationTime(Time.now())
            .setType(HddsProtos.ReplicationType.RATIS)
            .setFactor(HddsProtos.ReplicationFactor.ONE).build()).build();
  }

  /**
   * Append a {@link PartKeyInfo} to an {@link OmMultipartKeyInfo}.
   */
  public static void addPart(PartKeyInfo partKeyInfo,
       OmMultipartKeyInfo omMultipartKeyInfo) {
    omMultipartKeyInfo.addPartKeyInfo(partKeyInfo);
  }

  /**
   * Add key entry to key table cache.
   *
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param replicationConfig
   * @param omMetadataManager
   */
  @SuppressWarnings("parameterNumber")
  public static OmKeyInfo addKeyToTableCache(String volumeName,
      String bucketName,
      String keyName,
      ReplicationConfig replicationConfig,
      OMMetadataManager omMetadataManager) {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationConfig).build();

    omMetadataManager.getKeyTable(getDefaultBucketLayout()).addCacheEntry(
        new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName,
            keyName)), CacheValue.get(1L, omKeyInfo));

    return omKeyInfo;
  }

  /**
   * Adds one block to {@code keyInfo} with the provided size and offset.
   */
  public static void addKeyLocationInfo(
      OmKeyInfo keyInfo, long offset, long keyLength) throws IOException {

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(keyInfo.getReplicationConfig())
        .setNodes(new ArrayList<>())
        .build();

    OmKeyLocationInfo locationInfo = new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(100L, 1000L))
          .setOffset(offset)
          .setLength(keyLength)
          .setPipeline(pipeline)
          .build();

    keyInfo.appendNewBlocks(Collections.singletonList(locationInfo), false);
  }

  /**
   * Add dir key entry to DirectoryTable.
   *
   * @throws Exception
   */
  public static String addDirKeyToDirTable(boolean addToCache, OmDirectoryInfo omDirInfo, String volume, String bucket,
      long trxnLogIndex, OMMetadataManager omMetadataManager) throws Exception {
    final long volumeId = omMetadataManager.getVolumeId(volume);
    final long bucketId = omMetadataManager.getBucketId(volume, bucket);
    final String ozoneKey = omMetadataManager.getOzonePathKey(volumeId,
            bucketId, omDirInfo.getParentObjectID(), omDirInfo.getName());
    if (addToCache) {
      omMetadataManager.getDirectoryTable().addCacheEntry(
              new CacheKey<>(ozoneKey),
              CacheValue.get(trxnLogIndex, omDirInfo));
    }
    omMetadataManager.getDirectoryTable().put(ozoneKey, omDirInfo);
    return ozoneKey;
  }

  /**
   * Add snapshot entry to DB.
   */
  public static SnapshotInfo addSnapshotToTable(
      String volumeName, String bucketName, String snapshotName,
      OMMetadataManager omMetadataManager) throws IOException {
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volumeName,
        bucketName, snapshotName, UUID.randomUUID(), Time.now());
    addSnapshotToTable(false, 0L, snapshotInfo, omMetadataManager);
    return snapshotInfo;
  }

  /**
   * Add snapshot entry to snapshot table cache.
   */
  public static SnapshotInfo addSnapshotToTableCache(
      String volumeName, String bucketName, String snapshotName,
      OMMetadataManager omMetadataManager) throws IOException {
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volumeName, bucketName,
        snapshotName, UUID.randomUUID(), Time.now());
    addSnapshotToTable(true, 0L, snapshotInfo, omMetadataManager);
    return snapshotInfo;
  }

  /**
   * Add snapshot entry to snapshotInfoTable. If addToCache flag set true,
   * add it to cache table, else add it to DB.
   */
  public static void addSnapshotToTable(
      Boolean addToCache, long txnID, SnapshotInfo snapshotInfo,
      OMMetadataManager omMetadataManager) throws IOException {
    String key = snapshotInfo.getTableKey();
    if (addToCache) {
      omMetadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(key),
          CacheValue.get(txnID, snapshotInfo));
    }
    omMetadataManager.getSnapshotInfoTable().put(key, snapshotInfo);
  }

  /**
   * Create OmKeyInfo.
   * Initializes most values to a sensible default.
   */
  public static OmKeyInfo.Builder createOmKeyInfo(String volumeName, String bucketName,
      String keyName, ReplicationConfig replicationConfig, OmKeyLocationInfoGroup omKeyLocationInfoGroup) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setReplicationConfig(replicationConfig)
        .setObjectID(0L)
        .setUpdateID(0L)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .addOmKeyLocationInfoGroup(omKeyLocationInfoGroup)
        .setDataSize(1000L);
  }

  public static OmKeyInfo.Builder createOmKeyInfo(String volumeName, String bucketName,
      String keyName, ReplicationConfig replicationConfig) {
    return createOmKeyInfo(volumeName, bucketName, keyName, replicationConfig,
        new OmKeyLocationInfoGroup(0L, new ArrayList<>(), false));
  }

  /**
   * Create OmDirectoryInfo.
   */
  public static OmDirectoryInfo createOmDirectoryInfo(String keyName,
      long objectID,
      long parentObjID) {
    return new OmDirectoryInfo.Builder()
        .setName(keyName)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setObjectID(objectID)
        .setParentObjectID(parentObjID)
        .setUpdateID(50)
        .build();
  }

  /**
   * Create OmMultipartKeyInfo for OBS/LEGACY bucket.
   */
  public static OmMultipartKeyInfo createOmMultipartKeyInfo(String uploadId,
      long creationTime, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, long objectID) {
    return new OmMultipartKeyInfo.Builder()
        .setUploadID(uploadId)
        .setCreationTime(creationTime)
        .setReplicationConfig(
            ReplicationConfig
                .fromProtoTypeAndFactor(replicationType, replicationFactor))
        .setPartKeyInfoList(Collections.emptySortedMap())
        .setObjectID(objectID)
        .setUpdateID(objectID)
        .build();
  }

  /**
   * Add volume creation entry to OM DB.
   * @param volumeName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeToDB(String volumeName,
      OMMetadataManager omMetadataManager) throws Exception {
    addVolumeToDB(volumeName, UUID.randomUUID().toString(), omMetadataManager);
  }

  /**
   * Add volume creation entry to OM DB.
   * @param volumeName
   * @param omMetadataManager
   * @param quotaInBytes
   * @throws Exception
   */
  public static void addVolumeToDB(String volumeName,
      OMMetadataManager omMetadataManager, long quotaInBytes) throws Exception {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(volumeName)
            .setOwnerName(volumeName).setQuotaInBytes(quotaInBytes)
            .setQuotaInNamespace(10000L).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);

    // Add to cache.
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
        CacheValue.get(1L, omVolumeArgs));
  }

  /**
   * Add volume creation entry to OM DB.
   * @param volumeName
   * @param ownerName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeToDB(String volumeName, String ownerName,
      OMMetadataManager omMetadataManager) throws Exception {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(ownerName)
            .setObjectID(System.currentTimeMillis())
            .setOwnerName(ownerName).setQuotaInBytes(Long.MAX_VALUE)
            .setQuotaInNamespace(10000L).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);

    // Add to cache.
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
            CacheValue.get(1L, omVolumeArgs));
  }

  /**
   * Add bucket creation entry to OM DB.
   * @param volumeName
   * @param bucketName
   * @param omMetadataManager
   * @throws Exception
   */
  public static long addBucketToDB(String volumeName, String bucketName,
      OMMetadataManager omMetadataManager) throws Exception {
    return addBucketToDB(volumeName, bucketName, omMetadataManager,
        BucketLayout.DEFAULT).getObjectID();
  }

  public static OmBucketInfo addBucketToDB(String volumeName,
      String bucketName, OMMetadataManager omMetadataManager,
      BucketLayout bucketLayout)
      throws Exception {
    return addBucketToDB(omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setObjectID(System.currentTimeMillis())
            .setBucketName(bucketName)
            .setBucketLayout(bucketLayout)
    );
  }

  public static OmBucketInfo addBucketToDB(OMMetadataManager omMetadataManager,
      OmBucketInfo.Builder builder) throws Exception {

    OmBucketInfo omBucketInfo = builder
        .setObjectID(System.currentTimeMillis())
        .setCreationTime(Time.now())
        .build();

    String volumeName = omBucketInfo.getVolumeName();
    String bucketName = omBucketInfo.getBucketName();

    // Add to cache.
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        CacheValue.get(1L, omBucketInfo));

    return omBucketInfo;
  }

  /**
   * Add bucket creation entry to OM DB.
   * @param volumeName
   * @param bucketName
   * @param omMetadataManager
   * @param quotaInBytes
   * @throws Exception
   */
  public static void addBucketToDB(String volumeName, String bucketName,
      OMMetadataManager omMetadataManager, long quotaInBytes) throws Exception {

    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now())
            .setQuotaInBytes(quotaInBytes).build();

    // Add to cache.
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        CacheValue.get(1L, omBucketInfo));
  }

  public static BucketInfo.Builder newBucketInfoBuilder(
      String bucketName, String volumeName) {
    return BucketInfo.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setStorageType(HddsProtos.StorageTypeProto.SSD)
        .setIsVersionEnabled(false)
        .addAllMetadata(getMetadataList());
  }

  public static OMRequest.Builder newCreateBucketRequest(
      BucketInfo.Builder bucketInfo) {
    OzoneManagerProtocolProtos.CreateBucketRequest.Builder req =
        OzoneManagerProtocolProtos.CreateBucketRequest.newBuilder();
    req.setBucketInfo(bucketInfo);
    return OMRequest.newBuilder()
        .setCreateBucketRequest(req)
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
        .setClientId(UUID.randomUUID().toString());
  }

  public static OMRequest newDeleteBucketRequest(String volumeName, String bucketName) {
    OzoneManagerProtocolProtos.DeleteBucketRequest.Builder req =
        OzoneManagerProtocolProtos.DeleteBucketRequest.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName);

    return OMRequest.newBuilder()
        .setDeleteBucketRequest(req)
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static List< KeyValue> getMetadataList() {
    List<KeyValue> metadataList = new ArrayList<>();
    metadataList.add(KeyValue.newBuilder().setKey("key1").setValue(
        "value1").build());
    metadataList.add(KeyValue.newBuilder().setKey("key2").setValue(
        "value2").build());
    return metadataList;
  }

  public static KeyValue fsoMetadata() {
    return KeyValue.newBuilder()
        .setKey(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS)
        .setValue(Boolean.FALSE.toString())
        .build();
  }

  /**
   * Add user to user table.
   * @param volumeName
   * @param ownerName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addUserToDB(String volumeName, String ownerName,
      OMMetadataManager omMetadataManager) throws Exception {

    OzoneManagerStorageProtos.PersistedUserVolumeInfo userVolumeInfo =
        omMetadataManager.getUserTable().get(
            omMetadataManager.getUserKey(ownerName));
    if (userVolumeInfo == null) {
      userVolumeInfo = OzoneManagerStorageProtos.PersistedUserVolumeInfo
          .newBuilder()
          .addVolumeNames(volumeName)
          .setObjectID(1)
          .setUpdateID(1)
          .build();
    } else {
      userVolumeInfo = userVolumeInfo.toBuilder()
          .addVolumeNames(volumeName)
          .build();
    }

    omMetadataManager.getUserTable().put(
        omMetadataManager.getUserKey(ownerName), userVolumeInfo);
  }

  /**
   * Create OMRequest for set volume property request with owner set.
   * @param volumeName
   * @param newOwner
   * @return OMRequest
   */
  public static OMRequest createSetVolumePropertyRequest(String volumeName,
      String newOwner) {
    SetVolumePropertyRequest setVolumePropertyRequest =
        SetVolumePropertyRequest.newBuilder().setVolumeName(volumeName)
            .setOwnerName(newOwner).setModificationTime(Time.now()).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(setVolumePropertyRequest).build();
  }

  public static OMRequest createSetVolumePropertyRequest(String volumeName) {
    SetVolumePropertyRequest setVolumePropertyRequest =
        SetVolumePropertyRequest.newBuilder().setVolumeName(volumeName)
            .setModificationTime(Time.now()).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(setVolumePropertyRequest).build();
  }

  /**
   * Create OMRequest for set volume property request with quota set.
   * @param volumeName
   * @param quotaInBytes
   * @param quotaInNamespace
   * @return OMRequest
   */
  public static OMRequest createSetVolumePropertyRequest(String volumeName,
      long quotaInBytes, long quotaInNamespace) {
    SetVolumePropertyRequest setVolumePropertyRequest =
        SetVolumePropertyRequest.newBuilder().setVolumeName(volumeName)
            .setQuotaInBytes(quotaInBytes)
            .setQuotaInNamespace(quotaInNamespace)
            .setModificationTime(Time.now()).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(setVolumePropertyRequest).build();
  }

  /**
   * Create OMRequest for set volume property request with namespace quota set.
   * @param volumeName
   * @param quotaInNamespace
   * @return OMRequest
   */
  public static OMRequest createSetVolumePropertyRequest(
      String volumeName, long quotaInNamespace) {
    SetVolumePropertyRequest setVolumePropertyRequest =
        SetVolumePropertyRequest.newBuilder().setVolumeName(volumeName)
            .setQuotaInNamespace(quotaInNamespace)
            .setModificationTime(Time.now()).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(setVolumePropertyRequest).build();
  }

  public static OMRequest createVolumeAddAclRequest(String volumeName,
      OzoneAcl acl) {
    AddAclRequest.Builder addAclRequestBuilder = AddAclRequest.newBuilder();
    addAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(ResourceType.VOLUME)
        .setStoreType(StoreType.OZONE)
        .build()));
    if (acl != null) {
      addAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }
    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AddAcl)
        .setAddAclRequest(addAclRequestBuilder.build()).build();
  }

  public static OMRequest createVolumeRemoveAclRequest(String volumeName,
      OzoneAcl acl) {
    RemoveAclRequest.Builder removeAclRequestBuilder =
        RemoveAclRequest.newBuilder();
    removeAclRequestBuilder.setObj(OzoneObj.toProtobuf(
        new OzoneObjInfo.Builder()
            .setVolumeName(volumeName)
            .setResType(ResourceType.VOLUME)
            .setStoreType(StoreType.OZONE)
            .build()));
    if (acl != null) {
      removeAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }
    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.RemoveAcl)
        .setRemoveAclRequest(removeAclRequestBuilder.build()).build();
  }

  public static OMRequest createVolumeSetAclRequest(String volumeName,
      List<OzoneAcl> acls) {
    SetAclRequest.Builder setAclRequestBuilder = SetAclRequest.newBuilder();
    setAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(ResourceType.VOLUME)
        .setStoreType(StoreType.OZONE)
        .build()));
    if (acls != null) {
      acls.forEach(
          acl -> setAclRequestBuilder.addAcl(OzoneAcl.toProtobuf(acl)));
    }

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetAcl)
        .setSetAclRequest(setAclRequestBuilder.build()).build();
  }

  // Create OMRequest for testing adding acl of bucket.
  public static OMRequest createBucketAddAclRequest(String volumeName,
      String bucketName, OzoneAcl acl) {
    AddAclRequest.Builder addAclRequestBuilder = AddAclRequest.newBuilder();
    addAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setResType(ResourceType.BUCKET)
        .setStoreType(StoreType.OZONE)
        .build()));

    if (acl != null) {
      addAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AddAcl)
        .setAddAclRequest(addAclRequestBuilder.build()).build();
  }

  // Create OMRequest for testing removing acl of bucket.
  public static OMRequest createBucketRemoveAclRequest(String volumeName,
      String bucketName, OzoneAcl acl) {
    RemoveAclRequest.Builder removeAclRequestBuilder =
        RemoveAclRequest.newBuilder();
    removeAclRequestBuilder.setObj(OzoneObj.toProtobuf(
        new OzoneObjInfo.Builder()
            .setVolumeName(volumeName).setBucketName(bucketName)
            .setResType(ResourceType.BUCKET)
            .setStoreType(StoreType.OZONE)
            .build()));

    if (acl != null) {
      removeAclRequestBuilder.setAcl(OzoneAcl.toProtobuf(acl));
    }

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.RemoveAcl)
        .setRemoveAclRequest(removeAclRequestBuilder.build()).build();
  }

  // Create OMRequest for testing setting acls of bucket.
  public static OMRequest createBucketSetAclRequest(String volumeName,
      String bucketName, List<OzoneAcl> acls) {
    SetAclRequest.Builder setAclRequestBuilder = SetAclRequest.newBuilder();
    setAclRequestBuilder.setObj(OzoneObj.toProtobuf(new OzoneObjInfo.Builder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setResType(ResourceType.BUCKET)
        .setStoreType(StoreType.OZONE)
        .build()));

    if (acls != null) {
      acls.forEach(
          acl -> setAclRequestBuilder.addAcl(OzoneAcl.toProtobuf(acl)));
    }

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetAcl)
        .setSetAclRequest(setAclRequestBuilder.build()).build();
  }

  /**
   * Deletes key from Key table and adds it to DeletedKeys table.
   * @return the deletedKey name
   */
  public static String deleteKey(String ozoneKey, long bucketId, OMMetadataManager omMetadataManager,
      long trxnLogIndex) throws IOException {
    // Retrieve the keyInfo
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getDefaultBucketLayout()).get(ozoneKey);

    // Delete key from KeyTable and put in DeletedKeyTable
    omMetadataManager.getKeyTable(getDefaultBucketLayout()).delete(ozoneKey);

    RepeatedOmKeyInfo repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(bucketId, omKeyInfo, trxnLogIndex);

    omMetadataManager.getDeletedTable().put(ozoneKey, repeatedOmKeyInfo);

    return ozoneKey;
  }

  /**
   * Deletes directory from directory table and adds it to DeletedDirectory table.
   * @return the deletedDirectoryTable key.
   */
  public static String deleteDir(String ozoneKey, String volume, String bucket,
      OMMetadataManager omMetadataManager)
      throws IOException {
    // Retrieve the keyInfo
    OmDirectoryInfo omDirectoryInfo = omMetadataManager.getDirectoryTable().get(ozoneKey);
    OmKeyInfo omKeyInfo = getOmKeyInfo(volume, bucket, omDirectoryInfo,
        omDirectoryInfo.getName());
    omMetadataManager.getDeletedDirTable().put(ozoneKey, omKeyInfo);
    omMetadataManager.getDirectoryTable().delete(ozoneKey);
    return ozoneKey;
  }

  /**
   * Create OMRequest which encapsulates InitiateMultipartUpload request.
   * @param volumeName
   * @param bucketName
   * @param keyName
   */
  public static OMRequest createInitiateMPURequest(String volumeName,
      String bucketName, String keyName) {
    return createInitiateMPURequest(volumeName, bucketName, keyName, Collections.emptyMap(),
        Collections.emptyMap());
  }

  /**
   * Create OMRequest which encapsulates InitiateMultipartUpload request.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param metadata
   */
  public static OMRequest createInitiateMPURequest(String volumeName,
      String bucketName, String keyName, Map<String, String> metadata,
      Map<String, String> tags) {
    MultipartInfoInitiateRequest
        multipartInfoInitiateRequest =
        MultipartInfoInitiateRequest.newBuilder().setKeyArgs(
            KeyArgs.newBuilder()
                .setVolumeName(volumeName)
                .setKeyName(keyName)
                .setBucketName(bucketName)
                .addAllMetadata(KeyValueUtil.toProtobuf(metadata))
                .addAllTags(KeyValueUtil.toProtobuf(tags))
            )
            .build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.InitiateMultiPartUpload)
        .setInitiateMultiPartUploadRequest(multipartInfoInitiateRequest)
        .build();
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static OMRequest createCommitPartMPURequest(String volumeName,
      String bucketName, String keyName, long clientID, long size,
      String multipartUploadID, int partNumber, List<KeyLocation> keyLocations) {
    MessageDigest eTagProvider;
    try {
      eTagProvider = MessageDigest.getInstance(OzoneConsts.MD5_HASH);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    // Just set dummy size.
    KeyArgs.Builder  keyArgs = KeyArgs.newBuilder().setVolumeName(volumeName)
        .setKeyName(keyName)
        .setBucketName(bucketName)
        .setDataSize(size)
        .setMultipartNumber(partNumber)
        .setMultipartUploadID(multipartUploadID)
        .addAllKeyLocations(keyLocations)
        .addMetadata(KeyValue.newBuilder()
            .setKey(OzoneConsts.ETAG)
            .setValue(DatatypeConverter.printHexBinary(
                new DigestInputStream(
                    new ByteArrayInputStream(
                        RandomStringUtils.secure().nextAlphanumeric((int) size)
                            .getBytes(StandardCharsets.UTF_8)),
                    eTagProvider)
                    .getMessageDigest().digest()))
            .build());
    // Just adding dummy list. As this is for UT only.

    MultipartCommitUploadPartRequest multipartCommitUploadPartRequest =
        MultipartCommitUploadPartRequest.newBuilder()
            .setKeyArgs(keyArgs).setClientID(clientID).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitMultiPartUpload)
        .setCommitMultiPartUploadRequest(multipartCommitUploadPartRequest)
        .build();
  }

  public static OMRequest createAbortMPURequest(String volumeName,
      String bucketName, String keyName, String multipartUploadID) {
    KeyArgs.Builder keyArgs =
        KeyArgs.newBuilder().setVolumeName(volumeName)
            .setKeyName(keyName)
            .setBucketName(bucketName)
            .setMultipartUploadID(multipartUploadID);

    MultipartUploadAbortRequest multipartUploadAbortRequest =
        MultipartUploadAbortRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AbortMultiPartUpload)
        .setAbortMultiPartUploadRequest(multipartUploadAbortRequest).build();
  }

  public static OMRequest createCompleteMPURequest(String volumeName,
      String bucketName, String keyName, String multipartUploadID,
      List<OzoneManagerProtocolProtos.Part> partList) {
    KeyArgs.Builder keyArgs =
        KeyArgs.newBuilder().setVolumeName(volumeName)
            .setKeyName(keyName)
            .setBucketName(bucketName)
            .setMultipartUploadID(multipartUploadID);

    MultipartUploadCompleteRequest multipartUploadCompleteRequest =
        MultipartUploadCompleteRequest.newBuilder().setKeyArgs(keyArgs)
            .addAllPartsList(partList).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CompleteMultiPartUpload)
        .setCompleteMultiPartUploadRequest(multipartUploadCompleteRequest)
        .build();

  }

  /**
   * Create OMRequest for create volume.
   * @param volumeName
   * @param adminName
   * @param ownerName
   * @return OMRequest
   */
  public static OMRequest createVolumeRequest(String volumeName,
      String adminName, String ownerName) {
    OzoneManagerProtocolProtos.VolumeInfo volumeInfo =
        OzoneManagerProtocolProtos.VolumeInfo.newBuilder().setVolume(volumeName)
        .setAdminName(adminName).setOwnerName(ownerName)
        .setQuotaInNamespace(OzoneConsts.QUOTA_RESET).build();
    OzoneManagerProtocolProtos.CreateVolumeRequest createVolumeRequest =
        OzoneManagerProtocolProtos.CreateVolumeRequest.newBuilder()
            .setVolumeInfo(volumeInfo).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume)
        .setCreateVolumeRequest(createVolumeRequest).build();
  }

  /**
   * Create OMRequest for delete bucket.
   * @param volumeName
   * @param bucketName
   */
  public static OMRequest createDeleteBucketRequest(String volumeName,
      String bucketName) {
    return OMRequest.newBuilder().setDeleteBucketRequest(
        OzoneManagerProtocolProtos.DeleteBucketRequest.newBuilder()
            .setBucketName(bucketName).setVolumeName(volumeName))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  public static OMRequest createTenantRequest(String tenantId,
      boolean forceCreationWhenVolumeExists) {

    final CreateTenantRequest.Builder requestBuilder =
        CreateTenantRequest.newBuilder()
            .setTenantId(tenantId)
            .setVolumeName(tenantId)
            .setForceCreationWhenVolumeExists(forceCreationWhenVolumeExists);

    return OMRequest.newBuilder()
        .setCreateTenantRequest(requestBuilder)
        .setCmdType(Type.CreateTenant)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static OMRequest deleteTenantRequest(String tenantId) {

    final DeleteTenantRequest.Builder requestBuilder =
        DeleteTenantRequest.newBuilder()
            .setTenantId(tenantId);

    return OMRequest.newBuilder()
        .setDeleteTenantRequest(requestBuilder)
        .setCmdType(Type.DeleteTenant)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static OMRequest tenantAssignUserAccessIdRequest(
      String username, String tenantId, String accessId) {

    final TenantAssignUserAccessIdRequest.Builder requestBuilder =
        TenantAssignUserAccessIdRequest.newBuilder()
            .setUserPrincipal(username)
            .setTenantId(tenantId)
            .setAccessId(accessId);

    return OMRequest.newBuilder()
        .setTenantAssignUserAccessIdRequest(requestBuilder)
        .setCmdType(Type.TenantAssignUserAccessId)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static OMRequest tenantRevokeUserAccessIdRequest(String accessId) {

    final TenantRevokeUserAccessIdRequest.Builder requestBuilder =
        TenantRevokeUserAccessIdRequest.newBuilder()
            .setAccessId(accessId);

    return OMRequest.newBuilder()
        .setTenantRevokeUserAccessIdRequest(requestBuilder)
        .setCmdType(Type.TenantRevokeUserAccessId)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static OMRequest tenantAssignAdminRequest(
      String accessId, String tenantId, boolean delegated) {

    final TenantAssignAdminRequest.Builder requestBuilder =
        TenantAssignAdminRequest.newBuilder()
            .setAccessId(accessId)
            .setDelegated(delegated);

    if (tenantId != null) {
      requestBuilder.setTenantId(tenantId);
    }

    return OMRequest.newBuilder()
        .setTenantAssignAdminRequest(requestBuilder)
        .setCmdType(Type.TenantAssignAdmin)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static OMRequest tenantRevokeAdminRequest(
      String accessId, String tenantId) {

    final TenantRevokeAdminRequest.Builder requestBuilder =
        TenantRevokeAdminRequest.newBuilder()
            .setAccessId(accessId);

    if (tenantId != null) {
      requestBuilder.setTenantId(tenantId);
    }

    return OMRequest.newBuilder()
        .setTenantRevokeAdminRequest(requestBuilder)
        .setCmdType(Type.TenantRevokeAdmin)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static OMRequest tenantGetUserInfoRequest(String userPrincipal) {

    final TenantGetUserInfoRequest.Builder requestBuilder =
        TenantGetUserInfoRequest.newBuilder()
            .setUserPrincipal(userPrincipal);

    return OMRequest.newBuilder()
        .setTenantGetUserInfoRequest(requestBuilder)
        .setCmdType(Type.TenantGetUserInfo)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static OMRequest listUsersInTenantRequest(String tenantId) {

    final TenantListUserRequest.Builder requestBuilder =
        TenantListUserRequest.newBuilder()
            .setTenantId(tenantId);

    return OMRequest.newBuilder()
        .setTenantListUserRequest(requestBuilder)
        .setCmdType(Type.TenantListUser)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static OMRequest listTenantRequest() {

    final ListTenantRequest.Builder requestBuilder =
        ListTenantRequest.newBuilder();

    return OMRequest.newBuilder()
        .setListTenantRequest(requestBuilder)
        .setCmdType(Type.ListTenant)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  public static OMRequest getS3VolumeContextRequest() {

    final GetS3VolumeContextRequest.Builder requestBuilder =
        GetS3VolumeContextRequest.newBuilder();

    return OMRequest.newBuilder()
        .setGetS3VolumeContextRequest(requestBuilder)
        .setCmdType(Type.GetS3VolumeContext)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  /**
   * Create OMRequest for Create Snapshot.
   * @param volumeName vol to be used
   * @param bucketName bucket to be used
   * @param snapshotName name to be used
   */
  public static OMRequest createSnapshotRequest(String volumeName,
                                                String bucketName,
                                                String snapshotName) {
    OzoneManagerProtocolProtos.CreateSnapshotRequest createSnapshotRequest =
        OzoneManagerProtocolProtos.CreateSnapshotRequest.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setSnapshotId(HddsUtils.toProtobuf(UUID.randomUUID()))
            .setSnapshotName(snapshotName)
            .build();

    OzoneManagerProtocolProtos.UserInfo userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder()
            .setUserName("user")
            .setHostName("host")
            .setRemoteAddress("remote-address")
            .build();

    return OMRequest.newBuilder()
        .setCreateSnapshotRequest(createSnapshotRequest)
        .setCmdType(Type.CreateSnapshot)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(userInfo)
        .build();
  }

  public static OMRequest moveSnapshotTableKeyRequest(UUID snapshotId,
                                                      List<Pair<String, List<OmKeyInfo>>> deletedKeys,
                                                      List<Pair<String, List<OmKeyInfo>>> deletedDirs,
                                                      List<Pair<String, String>> renameKeys) {
    List<OzoneManagerProtocolProtos.SnapshotMoveKeyInfos> deletedMoveKeys = new ArrayList<>();
    for (Pair<String, List<OmKeyInfo>> deletedKey : deletedKeys) {
      OzoneManagerProtocolProtos.SnapshotMoveKeyInfos snapshotMoveKeyInfos =
          OzoneManagerProtocolProtos.SnapshotMoveKeyInfos.newBuilder()
              .setKey(deletedKey.getKey())
              .addAllKeyInfos(
                  deletedKey.getValue().stream()
                  .map(omKeyInfo -> omKeyInfo.getProtobuf(ClientVersion.CURRENT_VERSION)).collect(Collectors.toList()))
              .build();
      deletedMoveKeys.add(snapshotMoveKeyInfos);
    }

    List<OzoneManagerProtocolProtos.SnapshotMoveKeyInfos> deletedDirMoveKeys = new ArrayList<>();
    for (Pair<String, List<OmKeyInfo>> deletedKey : deletedDirs) {
      OzoneManagerProtocolProtos.SnapshotMoveKeyInfos snapshotMoveKeyInfos =
          OzoneManagerProtocolProtos.SnapshotMoveKeyInfos.newBuilder()
              .setKey(deletedKey.getKey())
              .addAllKeyInfos(
                  deletedKey.getValue().stream()
                      .map(omKeyInfo -> omKeyInfo.getProtobuf(ClientVersion.CURRENT_VERSION))
                      .collect(Collectors.toList()))
              .build();
      deletedDirMoveKeys.add(snapshotMoveKeyInfos);
    }

    List<KeyValue> renameKeyList = new ArrayList<>();
    for (Pair<String, String> renameKey : renameKeys) {
      KeyValue.Builder keyValue = KeyValue.newBuilder();
      keyValue.setKey(renameKey.getKey());
      if (!Strings.isBlank(renameKey.getValue())) {
        keyValue.setValue(renameKey.getValue());
      }
      renameKeyList.add(keyValue.build());
    }


    OzoneManagerProtocolProtos.SnapshotMoveTableKeysRequest snapshotMoveTableKeysRequest =
        OzoneManagerProtocolProtos.SnapshotMoveTableKeysRequest.newBuilder()
            .setFromSnapshotID(HddsUtils.toProtobuf(snapshotId))
            .addAllDeletedKeys(deletedMoveKeys)
            .addAllDeletedDirs(deletedDirMoveKeys)
            .addAllRenamedKeys(renameKeyList)
            .build();

    OzoneManagerProtocolProtos.UserInfo userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder()
            .setUserName("user")
            .setHostName("host")
            .setRemoteAddress("remote-address")
            .build();

    return OMRequest.newBuilder()
        .setSnapshotMoveTableKeysRequest(snapshotMoveTableKeysRequest)
        .setCmdType(Type.SnapshotMoveTableKeys)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(userInfo)
        .build();
  }

  /**
   * Create OMRequest for Rename Snapshot.
   *
   * @param volumeName vol to be used
   * @param bucketName bucket to be used
   * @param snapshotOldName Old name of the snapshot
   * @param snapshotNewName New name of the snapshot
   */
  public static OMRequest renameSnapshotRequest(String volumeName,
                                                String bucketName,
                                                String snapshotOldName,
                                                String snapshotNewName) {
    OzoneManagerProtocolProtos.RenameSnapshotRequest renameSnapshotRequest =
        OzoneManagerProtocolProtos.RenameSnapshotRequest.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setSnapshotOldName(snapshotOldName)
            .setSnapshotNewName(snapshotNewName)
            .build();

    OzoneManagerProtocolProtos.UserInfo userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder()
            .setUserName("user")
            .setHostName("host")
            .setRemoteAddress("remote-address")
            .build();

    return OMRequest.newBuilder()
        .setRenameSnapshotRequest(renameSnapshotRequest)
        .setCmdType(Type.RenameSnapshot)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(userInfo)
        .build();
  }

  /**
   * Create OMRequest for Delete Snapshot.
   * @param volumeName vol to be used
   * @param bucketName bucket to be used
   * @param snapshotName name of the snapshot to be deleted
   */
  public static OMRequest deleteSnapshotRequest(String volumeName,
      String bucketName, String snapshotName) {
    OzoneManagerProtocolProtos.DeleteSnapshotRequest deleteSnapshotRequest =
        OzoneManagerProtocolProtos.DeleteSnapshotRequest.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setSnapshotName(snapshotName)
            .setDeletionTime(Time.now())
            .build();

    OzoneManagerProtocolProtos.UserInfo userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder()
            .setUserName("user")
            .setHostName("host")
            .setRemoteAddress("0.0.0.0")
            .build();

    return OMRequest.newBuilder()
        .setDeleteSnapshotRequest(deleteSnapshotRequest)
        .setCmdType(Type.DeleteSnapshot)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(userInfo)
        .build();
  }

  /**
   * Add the Key information to OzoneManager DB and cache.
   * @param omMetadataManager
   * @param omKeyInfo
   * @throws IOException
   */
  public static void addKeyToOM(final OMMetadataManager omMetadataManager,
                                final OmKeyInfo omKeyInfo) throws IOException {
    final String dbKey = omMetadataManager.getOzoneKey(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
        omKeyInfo.getKeyName());
    omMetadataManager.getKeyTable(getDefaultBucketLayout())
        .put(dbKey, omKeyInfo);
    omMetadataManager.getKeyTable(getDefaultBucketLayout()).addCacheEntry(
        new CacheKey<>(dbKey),
        CacheValue.get(1L, omKeyInfo));
  }

  /**
   * Add the Bucket information to OzoneManager DB and cache.
   * @param omMetadataManager
   * @param omBucketInfo
   * @throws IOException
   */
  public static void addBucketToOM(OMMetadataManager omMetadataManager,
      OmBucketInfo omBucketInfo) throws IOException {
    String dbBucketKey =
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName());
    omMetadataManager.getBucketTable().put(dbBucketKey, omBucketInfo);
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(dbBucketKey),
        CacheValue.get(1L, omBucketInfo));
  }

  /**
   * Add the Volume information to OzoneManager DB and Cache.
   * @param omMetadataManager
   * @param omVolumeArgs
   * @throws IOException
   */
  public static void addVolumeToOM(OMMetadataManager omMetadataManager,
      OmVolumeArgs omVolumeArgs) throws IOException {
    String dbVolumeKey =
        omMetadataManager.getVolumeKey(omVolumeArgs.getVolume());
    omMetadataManager.getVolumeTable().put(dbVolumeKey, omVolumeArgs);
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(dbVolumeKey),
        CacheValue.get(1L, omVolumeArgs));
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   *
   * @throws Exception DB failure
   */
  public static String addFileToKeyTable(boolean openKeyTable,
                                       boolean addToCache, String fileName,
                                       OmKeyInfo omKeyInfo,
                                       long clientID, long trxnLogIndex,
                                       OMMetadataManager omMetadataManager)
          throws Exception {
    String ozoneDBKey;
    if (openKeyTable) {
      final long volumeId = omMetadataManager.getVolumeId(
              omKeyInfo.getVolumeName());
      final long bucketId = omMetadataManager.getBucketId(
              omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
      ozoneDBKey = omMetadataManager.getOpenFileName(
              volumeId, bucketId, omKeyInfo.getParentObjectID(),
              fileName, clientID);
      if (addToCache) {
        omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
            .addCacheEntry(new CacheKey<>(ozoneDBKey),
                CacheValue.get(trxnLogIndex, omKeyInfo));
      }
      omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
          .put(ozoneDBKey, omKeyInfo);
    } else {
      ozoneDBKey = omMetadataManager.getOzonePathKey(
              omMetadataManager.getVolumeId(omKeyInfo.getVolumeName()),
              omMetadataManager.getBucketId(omKeyInfo.getVolumeName(),
                      omKeyInfo.getBucketName()),
              omKeyInfo.getParentObjectID(), fileName);
      if (addToCache) {
        omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
            .addCacheEntry(new CacheKey<>(ozoneDBKey),
                CacheValue.get(trxnLogIndex, omKeyInfo));
      }
      omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
          .put(ozoneDBKey, omKeyInfo);
    }
    return ozoneDBKey;
  }

  /**
   * Add open multipart key to openFileTable.
   *
   * @throws Exception DB failure
   */
  public static void addMultipartKeyToOpenFileTable(boolean addToCache,
      String fileName, OmKeyInfo omKeyInfo, String uploadId, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(
        omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
    String multipartKey = omMetadataManager.getMultipartKey(volumeId, bucketId,
        omKeyInfo.getParentObjectID(), fileName, uploadId);

    if (addToCache) {
      omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
          .addCacheEntry(new CacheKey<>(multipartKey),
              CacheValue.get(trxnLogIndex, omKeyInfo));
    }
    omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put(multipartKey, omKeyInfo);
  }

  /**
   * Add path components to the directory table and returns last directory's
   * object id.
   *
   * @param volumeName volume name
   * @param bucketName bucket name
   * @param key        key name
   * @param omMetaMgr  metdata manager
   * @return last directory object id
   * @throws Exception
   */
  public static long addParentsToDirTable(String volumeName, String bucketName,
                                    String key, OMMetadataManager omMetaMgr)
          throws Exception {
    long bucketId = omMetaMgr.getBucketId(volumeName, bucketName);
    if (org.apache.commons.lang3.StringUtils.isBlank(key)) {
      return bucketId;
    }
    String[] pathComponents = StringUtils.split(key, '/');
    long objectId = bucketId + 10;
    long parentId = bucketId;
    long txnID = 50;
    for (String pathElement : pathComponents) {
      OmDirectoryInfo omDirInfo =
              OMRequestTestUtils.createOmDirectoryInfo(pathElement, ++objectId,
                      parentId);
      OMRequestTestUtils.addDirKeyToDirTable(true, omDirInfo,
              volumeName, bucketName, ++txnID, omMetaMgr);
      parentId = omDirInfo.getObjectID();
    }
    return parentId;
  }

  public static void configureFSOptimizedPaths(Configuration conf,
                                               boolean enableFileSystemPaths) {
    configureFSOptimizedPaths(conf, enableFileSystemPaths,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  public static void configureFSOptimizedPaths(Configuration conf,
                                               boolean enableFileSystemPaths,
                                               BucketLayout bucketLayout) {
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
            enableFileSystemPaths);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
            bucketLayout.name());
  }

  private static BucketLayout getDefaultBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  public static void setupReplicationConfigValidation(
      OzoneManager ozoneManager, ConfigurationSource ozoneConfiguration)
      throws OMException {
    ReplicationConfigValidator validator =
        ozoneConfiguration.getObject(ReplicationConfigValidator.class);
    when(ozoneManager.getReplicationConfigValidator())
        .thenReturn(validator);
    doCallRealMethod().when(ozoneManager).validateReplicationConfig(any());
  }

  public static OMRequest createRequestWithS3Credentials(String accessId,
                                                         String signature,
                                                         String stringToSign) {
    return OMRequest.newBuilder()
        .setS3Authentication(
            OzoneManagerProtocolProtos.S3Authentication.newBuilder()
                .setAccessId(accessId)
                .setSignature(signature)
                .setStringToSign(stringToSign)
                .build())
        .setCmdType(Type.CommitKey)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  /**
   * Add key entry to PrefixTable.
   * @throws Exception
   */
  public static void addPrefixToTable(String volumeName, String bucketName, String prefixName, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws Exception {

    OmPrefixInfo omPrefixInfo = createOmPrefixInfo(volumeName, bucketName,
        prefixName, trxnLogIndex);

    addPrefixToTable(false, omPrefixInfo, trxnLogIndex,
        omMetadataManager);
  }

  /**
   * Add key entry to PrefixTable.
   * @throws Exception
   */
  public static void addPrefixToTable(boolean addToCache, OmPrefixInfo omPrefixInfo, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws Exception {
    String prefixName = omPrefixInfo.getName();

    if (addToCache) {
      omMetadataManager.getPrefixTable()
          .addCacheEntry(new CacheKey<>(omPrefixInfo.getName()),
              CacheValue.get(trxnLogIndex, omPrefixInfo));
    }
    omMetadataManager.getPrefixTable().put(prefixName, omPrefixInfo);
  }

  /**
   * Create OmPrefixInfo.
   */
  public static OmPrefixInfo createOmPrefixInfo(String volumeName, String bucketName, String prefixName,
        long trxnLogIndex) {
    OzoneObjInfo prefixObj = OzoneObjInfo.Builder
        .newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefixName)
        .setResType(ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    return OmPrefixInfo.newBuilder()
        .setName(prefixObj.getPath())
        .setObjectID(System.currentTimeMillis())
        .setUpdateID(trxnLogIndex)
        .build();
  }

}
