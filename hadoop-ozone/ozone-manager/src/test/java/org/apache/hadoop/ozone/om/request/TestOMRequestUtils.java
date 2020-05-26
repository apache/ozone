/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RemoveAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetAclRequest;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;

import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

/**
 * Helper class to test OMClientRequest classes.
 */
public final class TestOMRequestUtils {

  private TestOMRequestUtils() {
    //Do nothing
  }

  /**
   * Add's volume and bucket creation entries to OM DB.
   * @param volumeName
   * @param bucketName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeAndBucketToDB(String volumeName,
      String bucketName, OMMetadataManager omMetadataManager) throws Exception {

    addVolumeToDB(volumeName, omMetadataManager);

    addBucketToDB(volumeName, bucketName, omMetadataManager);
  }

  @SuppressWarnings("parameterNumber")
  public static void addKeyToTableAndCache(String volumeName, String bucketName,
      String keyName, long clientID, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws Exception {
    addKeyToTable(false, true, volumeName, bucketName, keyName, clientID,
        replicationType, replicationFactor, trxnLogIndex, omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @param openKeyTable
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param clientID
   * @param replicationType
   * @param replicationFactor
   * @param omMetadataManager
   * @throws Exception
   */
  @SuppressWarnings("parameterNumber")
  public static void addKeyToTable(boolean openKeyTable, String volumeName,
      String bucketName, String keyName, long clientID,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor,
      OMMetadataManager omMetadataManager) throws Exception {
    addKeyToTable(openKeyTable, false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, 0L, omMetadataManager);
  }

  /**
   * Add key entry to KeyTable. if openKeyTable flag is true, add's entries
   * to openKeyTable, else add's it to keyTable.
   * @throws Exception
   */
  @SuppressWarnings("parameternumber")
  public static void addKeyToTable(boolean openKeyTable, boolean addToCache,
      String volumeName, String bucketName, String keyName, long clientID,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, long trxnLogIndex,
      OMMetadataManager omMetadataManager) throws Exception {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationType, replicationFactor, trxnLogIndex);

    if (openKeyTable) {
      String ozoneKey = omMetadataManager.getOpenKey(volumeName, bucketName,
          keyName, clientID);
      if (addToCache) {
        omMetadataManager.getOpenKeyTable().addCacheEntry(
            new CacheKey<>(ozoneKey),
            new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));
      }
      omMetadataManager.getOpenKeyTable().put(ozoneKey, omKeyInfo);
    } else {
      String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      if (addToCache) {
        omMetadataManager.getKeyTable().addCacheEntry(new CacheKey<>(ozoneKey),
            new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));
      }
      omMetadataManager.getKeyTable().put(ozoneKey, omKeyInfo);
    }
  }

  /**
   * Add key entry to key table cache.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param replicationType
   * @param replicationFactor
   * @param omMetadataManager
   */
  @SuppressWarnings("parameterNumber")
  public static void addKeyToTableCache(String volumeName,
      String bucketName,
      String keyName,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor,
      OMMetadataManager omMetadataManager) {

    OmKeyInfo omKeyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationType, replicationFactor);

    omMetadataManager.getKeyTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName,
            keyName)), new CacheValue<>(Optional.of(omKeyInfo), 1L));
  }

  private OmKeyInfo createKeyInfo(String volumeName, String bucketName,
      String keyName, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(1000L)
        .setReplicationType(replicationType)
        .setReplicationFactor(replicationFactor).build();
  }

  /**
   * Create OmKeyInfo.
   */
  public static OmKeyInfo createOmKeyInfo(String volumeName, String bucketName,
      String keyName, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor) {
    return createOmKeyInfo(volumeName, bucketName, keyName, replicationType,
        replicationFactor, 0L);
  }

  /**
   * Create OmKeyInfo.
   */
  public static OmKeyInfo createOmKeyInfo(String volumeName, String bucketName,
      String keyName, HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, long objectID) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(1000L)
        .setReplicationType(replicationType)
        .setReplicationFactor(replicationFactor)
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
   * @param ownerName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addVolumeToDB(String volumeName, String ownerName,
      OMMetadataManager omMetadataManager) throws Exception {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(ownerName)
            .setOwnerName(ownerName).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);

    // Add to cache.
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
            new CacheValue<>(Optional.of(omVolumeArgs), 1L));
  }

  /**
   * Add bucket creation entry to OM DB.
   * @param volumeName
   * @param bucketName
   * @param omMetadataManager
   * @throws Exception
   */
  public static void addBucketToDB(String volumeName, String bucketName,
      OMMetadataManager omMetadataManager) throws Exception {

    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName).setCreationTime(Time.now()).build();

    // Add to cache.
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
  }

  public static OzoneManagerProtocolProtos.OMRequest createBucketRequest(
      String bucketName, String volumeName, boolean isVersionEnabled,
      OzoneManagerProtocolProtos.StorageTypeProto storageTypeProto) {
    OzoneManagerProtocolProtos.BucketInfo bucketInfo =
        OzoneManagerProtocolProtos.BucketInfo.newBuilder()
            .setBucketName(bucketName)
            .setVolumeName(volumeName)
            .setIsVersionEnabled(isVersionEnabled)
            .setStorageType(storageTypeProto)
            .addAllMetadata(getMetadataList())
            .setTrashEnabled(false)
            .setRecoverWindow(0).build();
    OzoneManagerProtocolProtos.CreateBucketRequest.Builder req =
        OzoneManagerProtocolProtos.CreateBucketRequest.newBuilder();
    req.setBucketInfo(bucketInfo);
    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCreateBucketRequest(req)
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  public static List< HddsProtos.KeyValue> getMetadataList() {
    List<HddsProtos.KeyValue> metadataList = new ArrayList<>();
    metadataList.add(HddsProtos.KeyValue.newBuilder().setKey("key1").setValue(
        "value1").build());
    metadataList.add(HddsProtos.KeyValue.newBuilder().setKey("key2").setValue(
        "value2").build());
    return metadataList;
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

    OzoneManagerProtocolProtos.UserVolumeInfo userVolumeInfo = omMetadataManager
        .getUserTable().get(omMetadataManager.getUserKey(ownerName));
    if (userVolumeInfo == null) {
      userVolumeInfo = OzoneManagerProtocolProtos.UserVolumeInfo
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
            .setOwnerName(newOwner).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(setVolumePropertyRequest).build();
  }


  /**
   * Create OMRequest for set volume property request with quota set.
   * @param volumeName
   * @param quota
   * @return OMRequest
   */
  public static OMRequest createSetVolumePropertyRequest(String volumeName,
      long quota) {
    SetVolumePropertyRequest setVolumePropertyRequest =
        SetVolumePropertyRequest.newBuilder().setVolumeName(volumeName)
            .setQuotaInBytes(quota).build();

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

  /**
   * Deletes key from Key table and adds it to deletedTable and trashTable.
   * @return the deletedKey name
   */
  public static String deleteKey(String ozoneKey,
      OMMetadataManager omMetadataManager, long trxnLogIndex)
      throws IOException {
    // Retrieve the keyInfo
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    // Delete key from KeyTable and put in DeletedKeyTable
    omMetadataManager.getKeyTable().delete(ozoneKey);

    RepeatedOmKeyInfo repeatedOmKeyInfo =
        omMetadataManager.getDeletedTable().get(ozoneKey);

    repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(omKeyInfo,
        repeatedOmKeyInfo, trxnLogIndex, true);

    omMetadataManager.getDeletedTable().put(ozoneKey, repeatedOmKeyInfo);

    // Update cache of trashTable and update trashTable in OM DB.
    omMetadataManager.getTrashTable().addCacheEntry(
        new CacheKey<>(ozoneKey),
        new CacheValue<>(Optional.of(repeatedOmKeyInfo), trxnLogIndex));
    omMetadataManager.getTrashTable().put(ozoneKey, repeatedOmKeyInfo);

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
    MultipartInfoInitiateRequest
        multipartInfoInitiateRequest =
        MultipartInfoInitiateRequest.newBuilder().setKeyArgs(
            KeyArgs.newBuilder().setVolumeName(volumeName).setKeyName(keyName)
                .setBucketName(bucketName)).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.InitiateMultiPartUpload)
        .setInitiateMultiPartUploadRequest(multipartInfoInitiateRequest)
        .build();
  }

  /**
   * Create OMRequest which encapsulates InitiateMultipartUpload request.
   * @param volumeName
   * @param bucketName
   * @param keyName
   */
  public static OMRequest createCommitPartMPURequest(String volumeName,
      String bucketName, String keyName, long clientID, long size,
      String multipartUploadID, int partNumber) {

    // Just set dummy size.
    KeyArgs.Builder keyArgs =
        KeyArgs.newBuilder().setVolumeName(volumeName).setKeyName(keyName)
            .setBucketName(bucketName)
            .setDataSize(size)
            .setMultipartNumber(partNumber)
            .setMultipartUploadID(multipartUploadID)
            .addAllKeyLocations(new ArrayList<>());
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
        .setAdminName(adminName).setOwnerName(ownerName).build();
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
    omMetadataManager.getKeyTable().put(dbKey, omKeyInfo);
    omMetadataManager.getKeyTable().addCacheEntry(
        new CacheKey<>(dbKey),
        new CacheValue<>(Optional.of(omKeyInfo), 1L));
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
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
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
        new CacheValue<>(Optional.of(omVolumeArgs), 1L));
  }
}
