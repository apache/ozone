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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Base test class for S3 MPU response.
 */

@SuppressWarnings("VisibilityModifier")
public class TestS3MultipartResponse {

  @TempDir
  private Path folder;

  protected OMMetadataManager omMetadataManager;
  protected BatchOperation batchOperation;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  public S3InitiateMultipartUploadResponse createS3InitiateMPUResponse(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID) throws IOException {
    OmMultipartKeyInfo multipartKeyInfo = new OmMultipartKeyInfo.Builder()
        .setUploadID(multipartUploadID)
        .setCreationTime(Time.now())
        .setReplicationConfig(RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE))
        .build();

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setReplicationConfig(RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE))
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>(), true)))
        .build();

    OMResponse omResponse = OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.InitiateMultiPartUpload)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true).setInitiateMultiPartUploadResponse(
            OzoneManagerProtocolProtos.MultipartInfoInitiateResponse
                .newBuilder().setVolumeName(volumeName)
                .setBucketName(bucketName)
                .setKeyName(keyName)
                .setMultipartUploadID(multipartUploadID)).build();

    // some volID and buckID as these values are not used in legacy buckets
    return getS3InitiateMultipartUploadResp(multipartKeyInfo, omKeyInfo,
        omResponse, -1, -1);
  }

  public S3MultipartUploadAbortResponse createS3AbortMPUResponse(
      String multipartKey, String multipartOpenKey,
      OmMultipartKeyInfo omMultipartKeyInfo, OmBucketInfo omBucketInfo) {
    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AbortMultiPartUpload)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true)
        .setAbortMultiPartUploadResponse(
            MultipartUploadAbortResponse.newBuilder().build()).build();

    return getS3MultipartUploadAbortResp(multipartKey,
        multipartOpenKey, omMultipartKeyInfo, omBucketInfo, omResponse);
  }

  public void addPart(int partNumber, PartKeyInfo partKeyInfo,
      OmMultipartKeyInfo omMultipartKeyInfo) {
    omMultipartKeyInfo.addPartKeyInfo(partKeyInfo);
  }

  public PartKeyInfo createPartKeyInfo(String volumeName, String bucketName,
                                       String keyName, int partNumber)
          throws IOException {
    return PartKeyInfo.newBuilder()
        .setPartNumber(partNumber)
        .setPartName(omMetadataManager.getMultipartKey(volumeName,
            bucketName, keyName, UUID.randomUUID().toString()))
        .setPartKeyInfo(KeyInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setDataSize(100L) // Just set dummy size for testing
            .setCreationTime(Time.now())
            .setModificationTime(Time.now())
            .setObjectID(UUID.randomUUID().hashCode())
            .setType(HddsProtos.ReplicationType.RATIS)
            .setFactor(HddsProtos.ReplicationFactor.ONE).build()).build();
  }

  public PartKeyInfo createPartKeyInfoFSO(
      String volumeName, String bucketName, long parentID, String fileName,
      int partNumber) throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return PartKeyInfo.newBuilder()
        .setPartNumber(partNumber)
        .setPartName(omMetadataManager.getOzonePathKey(volumeId, bucketId,
                parentID, fileName +
                UUID.randomUUID().toString()))
        .setPartKeyInfo(KeyInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(fileName)
            .setDataSize(100L) // Just set dummy size for testing
            .setCreationTime(Time.now())
            .setModificationTime(Time.now())
            .setObjectID(UUID.randomUUID().hashCode())
            .setParentID(parentID)
            .setType(HddsProtos.ReplicationType.RATIS)
            .setFactor(HddsProtos.ReplicationFactor.ONE).build()).build();
  }

  @SuppressWarnings("parameternumber")
  public S3InitiateMultipartUploadResponse createS3InitiateMPUResponseFSO(
      String volumeName, String bucketName, long parentID, String keyName,
      String multipartUploadID, List<OmDirectoryInfo> parentDirInfos,
      long volumeId, long bucketId) throws IOException {
    OmMultipartKeyInfo multipartKeyInfo = new OmMultipartKeyInfo.Builder()
            .setUploadID(multipartUploadID)
            .setCreationTime(Time.now())
            .setReplicationConfig(RatisReplicationConfig.getInstance(
                    HddsProtos.ReplicationFactor.ONE))
            .setParentID(parentID)
            .build();

    String fileName = OzoneFSUtils.getFileName(keyName);

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(fileName)
            .setCreationTime(Time.now())
            .setModificationTime(Time.now())
            .setReplicationConfig(RatisReplicationConfig.getInstance(
                    HddsProtos.ReplicationFactor.ONE))
            .setOmKeyLocationInfos(Collections.singletonList(
                    new OmKeyLocationInfoGroup(0, new ArrayList<>(), true)))
            .setParentObjectID(parentID)
            .build();

    OMResponse omResponse = OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.InitiateMultiPartUpload)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true).setInitiateMultiPartUploadResponse(
                    OzoneManagerProtocolProtos.MultipartInfoInitiateResponse
                            .newBuilder().setVolumeName(volumeName)
                            .setBucketName(bucketName)
                            .setKeyName(keyName)
                            .setMultipartUploadID(multipartUploadID)).build();

    String mpuKey = omMetadataManager.getMultipartKey(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName(),
        keyName, multipartUploadID);

    String buckDBKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(buckDBKey);

    return new S3InitiateMultipartUploadResponseWithFSO(omResponse,
        multipartKeyInfo, omKeyInfo, mpuKey, parentDirInfos, getBucketLayout(),
        volumeId, bucketId, omBucketInfo);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public S3MultipartUploadCommitPartResponse createS3CommitMPUResponseFSO(
          String volumeName, String bucketName, long parentID, String keyName,
          String multipartUploadID,
          OzoneManagerProtocolProtos.PartKeyInfo oldPartKeyInfo,
          OmMultipartKeyInfo multipartKeyInfo,
          OzoneManagerProtocolProtos.Status status, String openKey)
          throws IOException {
    if (multipartKeyInfo == null) {
      multipartKeyInfo = new OmMultipartKeyInfo.Builder()
              .setUploadID(multipartUploadID)
              .setCreationTime(Time.now())
              .setReplicationConfig(RatisReplicationConfig.getInstance(
                      HddsProtos.ReplicationFactor.ONE))
              .setParentID(parentID)
              .build();
    }

    String fileName = OzoneFSUtils.getFileName(keyName);

    String multipartKey = omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName, multipartUploadID);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);

    OmKeyInfo openPartKeyInfoToBeDeleted = new OmKeyInfo.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(fileName)
            .setCreationTime(Time.now())
            .setModificationTime(Time.now())
            .setReplicationConfig(RatisReplicationConfig.getInstance(
                    HddsProtos.ReplicationFactor.ONE))
            .setOmKeyLocationInfos(Collections.singletonList(
                    new OmKeyLocationInfoGroup(0, new ArrayList<>(), true)))
            .build();

    OMResponse omResponse = OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CommitMultiPartUpload)
            .setStatus(status).setSuccess(true)
            .setCommitMultiPartUploadResponse(
                    OzoneManagerProtocolProtos.MultipartCommitUploadPartResponse
                            .newBuilder().setETag(volumeName).setPartName(volumeName)).build();

    Map<String, RepeatedOmKeyInfo> keyToDeleteMap = new HashMap<>();
    if (oldPartKeyInfo != null) {
      OmKeyInfo partKeyToBeDeleted =
          OmKeyInfo.getFromProtobuf(oldPartKeyInfo.getPartKeyInfo());
      String delKeyName = omMetadataManager.getOzoneDeletePathKey(
          partKeyToBeDeleted.getObjectID(), multipartKey);

      keyToDeleteMap.put(delKeyName, new RepeatedOmKeyInfo(partKeyToBeDeleted, omBucketInfo.getObjectID()));
    }

    return new S3MultipartUploadCommitPartResponseWithFSO(omResponse,
        multipartKey, openKey, multipartKeyInfo, keyToDeleteMap,
        openPartKeyInfoToBeDeleted, omBucketInfo, omBucketInfo.getObjectID(),
        getBucketLayout());
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public S3MultipartUploadCompleteResponse createS3CompleteMPUResponseFSO(
          String volumeName, String bucketName, long parentID, String keyName,
          String multipartUploadID, OmKeyInfo omKeyInfo,
          OzoneManagerProtocolProtos.Status status,
          List<OmKeyInfo>  allKeyInfoToRemove,
          OmBucketInfo omBucketInfo) throws IOException {


    String multipartKey = omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName, multipartUploadID);
    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager
        .getMultipartInfoTable().get(multipartKey);

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    String fileName = OzoneFSUtils.getFileName(keyName);
    String multipartOpenKey = omMetadataManager.getMultipartKey(
            volumeId, bucketId, parentID, fileName, multipartUploadID);

    OMResponse omResponse = OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CompleteMultiPartUpload)
            .setStatus(status).setSuccess(true)
            .setCompleteMultiPartUploadResponse(
                    OzoneManagerProtocolProtos.MultipartUploadCompleteResponse
                            .newBuilder().setBucket(bucketName)
                            .setVolume(volumeName).setKey(keyName)).build();

    return new S3MultipartUploadCompleteResponseWithFSO(omResponse,
        multipartKey, multipartOpenKey, omKeyInfo,  allKeyInfoToRemove,
        getBucketLayout(), omBucketInfo, volumeId, bucketId, null,
        multipartKeyInfo);
  }

  protected S3InitiateMultipartUploadResponse getS3InitiateMultipartUploadResp(
      OmMultipartKeyInfo multipartKeyInfo, OmKeyInfo omKeyInfo,
      OMResponse omResponse, long volumeId, long bucketId) throws IOException {
    return new S3InitiateMultipartUploadResponse(omResponse, multipartKeyInfo,
        omKeyInfo, getBucketLayout());
  }

  protected S3MultipartUploadAbortResponse getS3MultipartUploadAbortResp(
      String multipartKey, String multipartOpenKey,
      OmMultipartKeyInfo omMultipartKeyInfo, OmBucketInfo omBucketInfo,
      OMResponse omResponse) {
    return new S3MultipartUploadAbortResponse(omResponse, multipartKey,
        multipartOpenKey, omMultipartKeyInfo, omBucketInfo,
        getBucketLayout());
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  public void addVolumeToDB(String volumeName) throws IOException {
    final OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
            .setVolume(volumeName)
            .setAdminName("admin")
            .setOwnerName("owner")
            .setObjectID(System.currentTimeMillis())
            .build();

    omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
            CacheValue.get(1, volumeArgs));
  }

  public void addBucketToDB(String volumeName, String bucketName)
          throws IOException {
    final OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setObjectID(System.currentTimeMillis())
            .setStorageType(StorageType.DISK)
            .setIsVersionEnabled(false)
            .build();

    omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getBucketKey(
                    volumeName, bucketName)),
            CacheValue.get(1, omBucketInfo));
  }
}
