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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCommitPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests S3 Multipart upload commit part request.
 */
public class TestS3MultipartUploadCommitPartRequest
    extends S3MultipartRequestTests {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    doPreExecuteCommitMPU(volumeName, bucketName, keyName, Time.now(),
        UUID.randomUUID().toString(), 1);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    // Add key to open key table.
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);


    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.OK);

    String multipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));
    assertEquals(1, omMetadataManager.getMultipartInfoTable()
        .get(multipartKey).getPartKeyInfoMap().size());

    OmKeyInfo mpuOpenKeyInfo = omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(multipartOpenKey);
    assertNotNull(mpuOpenKeyInfo);
    assertNotNull(mpuOpenKeyInfo.getLatestVersionLocations());
    assertTrue(mpuOpenKeyInfo.getLatestVersionLocations()
        .isMultipartKey());

    String partKey = getOpenKey(volumeName, bucketName, keyName, clientID);
    assertNull(omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(partKey));
  }

  @Test
  public void testValidateAndUpdateCacheMultipartNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    // Add key to open key table.
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    assertNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));

  }

  @Test
  public void testValidateAndUpdateCacheKeyNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());


    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    // Don't add key to open table entry, and we are trying to commit this MPU
    // part. It will fail with KEY_NOT_FOUND

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);


    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    if (getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      assertSame(omClientResponse.getOMResponse().getStatus(),
          OzoneManagerProtocolProtos.Status.DIRECTORY_NOT_FOUND);
    } else {
      assertSame(omClientResponse.getOMResponse().getStatus(),
          OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND);
    }

  }

  @Test
  public void testValidateAndUpdateCacheBucketFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);


    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    // Don't add key to open table entry, and we are trying to commit this MPU
    // part. It will fail with KEY_NOT_FOUND

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);


    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheOnOverwrite() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    // Create part key to be overwritten
    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    // Take the first 2 blocks for the part key to be overwritten
    List<KeyLocation> originalKeyLocationList = getKeyLocation(5).subList(0, 2);

    List<OmKeyLocationInfo> originalKeyLocationInfos = originalKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    // Add key to open key table.
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, originalKeyLocationInfos);

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, originalKeyLocationList);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());
    PartKeyInfo partKeyInfo = multipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    OmKeyInfo partOmKeyInfo = OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());

    // Overwrite the key

    // New client ID for the overwritten key
    clientID = Time.now();

    // Take the last 3 blocks for the overwrite key
    List<KeyLocation> overwriteKeyLocationList = getKeyLocation(5).subList(2, 5);

    List<OmKeyLocationInfo> overwriteKeyLocationInfos = overwriteKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequest overwriteOMRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, overwriteKeyLocationList);

    S3MultipartUploadCommitPartRequest overwriteRequest = getS3MultipartUploadCommitReq(overwriteOMRequest);

    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, overwriteKeyLocationInfos);

    omClientResponse =
        overwriteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertSame(OzoneManagerProtocolProtos.Status.OK, omClientResponse.getOMResponse().getStatus());

    OmMultipartKeyInfo newMultipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    // Part key still remains the same
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());

    PartKeyInfo newPartKeyInfo = newMultipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    // Check modification time
    assertEquals(overwriteOMRequest.getCommitMultiPartUploadRequest()
        .getKeyArgs().getModificationTime(), newPartKeyInfo.getPartKeyInfo().getModificationTime());

    OmKeyInfo newPartOmKeyInfo = OmKeyInfo.getFromProtobuf(newPartKeyInfo.getPartKeyInfo());

    assertNotEquals(partOmKeyInfo, newPartOmKeyInfo);

    // Check block location
    List<OmKeyLocationInfo> locationsInfoListFromCommitPartRequest =
        overwriteOMRequest.getCommitMultiPartUploadRequest().getKeyArgs()
            .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());

    assertEquals(overwriteKeyLocationInfos, locationsInfoListFromCommitPartRequest);
    assertEquals(overwriteKeyLocationInfos, newPartOmKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(1, newPartOmKeyInfo.getKeyLocationVersions().size());

    Map<String, RepeatedOmKeyInfo> toDeleteKeyList =
        ((S3MultipartUploadCommitPartResponse) omClientResponse).getKeyToDelete();

    // Since there are no uncommitted blocks, only the overwritten (original) key should be deleted
    assertEquals(1, toDeleteKeyList.size());
    assertEquals(originalKeyLocationList.size(), toDeleteKeyList.values().stream()
        .findFirst().get().cloneOmKeyInfoList().get(0).getKeyLocationVersions()
        .get(0).getLocationList().size());
  }

  @Test
  public void testValidateAndUpdateCacheWithUncommittedBlocks() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    // Allocated block list (5 blocks)
    List<KeyLocation> allocatedKeyLocationList = getKeyLocation(5);

    List<OmKeyLocationInfo> allocatedBlockList = allocatedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    // Put the open key to simulate the part key upload using OMKeyCreateRequest
    String openMpuPartKey = addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, allocatedBlockList);

    OmKeyInfo openMpuPartKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openMpuPartKey);
    assertNotNull(openMpuPartKeyInfo);

    // Commit only the first 3 allocated blocks
    List<KeyLocation> committedKeyLocationList = allocatedKeyLocationList.subList(0, 3);

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, committedKeyLocationList);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    omClientResponse = s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    Map<String, RepeatedOmKeyInfo> toDeleteKeyList =
        ((S3MultipartUploadCommitPartResponse) omClientResponse).getKeyToDelete();

    // Since this part key is not overwritten, only the allocated but uncommitted
    // blocks should be deleted.
    assertEquals(1, toDeleteKeyList.size());
    assertEquals(2, toDeleteKeyList.values().stream()
        .findFirst().get().cloneOmKeyInfoList().get(0).getKeyLocationVersions()
        .get(0).getLocationList().size());

    String multipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));
    assertEquals(1, omMetadataManager.getMultipartInfoTable()
        .get(multipartKey).getPartKeyInfoMap().size());

    OmKeyInfo mpuOpenKeyInfo = omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(multipartOpenKey);
    assertNotNull(mpuOpenKeyInfo);
    assertNotNull(mpuOpenKeyInfo.getLatestVersionLocations());
    assertTrue(mpuOpenKeyInfo.getLatestVersionLocations()
        .isMultipartKey());

    assertNull(omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(openMpuPartKey));
  }

  @Test
  public void testValidateAndUpdateCacheOnOverWriteWithUncommittedBlocks() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    // Create key to be overwritten
    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    List<KeyLocation> originalKeyLocationList = getKeyLocation(5).subList(0, 2);

    List<OmKeyLocationInfo> originalKeyLocationInfos = originalKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, originalKeyLocationList);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, originalKeyLocationInfos);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());
    PartKeyInfo partKeyInfo = multipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    // Overwrite the key, at the same time there are some uncommitted blocks

    // New client ID for the overwritten key
    clientID = Time.now();

    // Allocate 3 blocks for the overwritten key
    List<KeyLocation> overwriteAllocatedKeyLocationList = getKeyLocation(5).subList(2, 5);

    List<OmKeyLocationInfo> overwriteAllocatedBlockList = overwriteAllocatedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    // Put the open key to simulate the part key upload using OMKeyCreateRequest
    String openMpuPartKey = addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID,
        overwriteAllocatedBlockList);

    OmKeyInfo openMpuPartKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openMpuPartKey);
    assertNotNull(openMpuPartKeyInfo);

    // Commit only the first allocated blocks
    List<KeyLocation> overwriteCommittedKeyLocationList = overwriteAllocatedKeyLocationList.subList(0, 1);

    List<OmKeyLocationInfo> overwriteCommittedBlockList = overwriteCommittedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequest overwriteOMRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, overwriteCommittedKeyLocationList);

    S3MultipartUploadCommitPartRequest overwriteRequest = getS3MultipartUploadCommitReq(overwriteOMRequest);

    omClientResponse =
        overwriteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertSame(OzoneManagerProtocolProtos.Status.OK, omClientResponse.getOMResponse().getStatus());

    OmMultipartKeyInfo newMultipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());

    PartKeyInfo newPartKeyInfo = newMultipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    // Check modification time
    assertEquals(overwriteOMRequest.getCommitMultiPartUploadRequest()
        .getKeyArgs().getModificationTime(), newPartKeyInfo.getPartKeyInfo().getModificationTime());

    OmKeyInfo newPartOmKeyInfo = OmKeyInfo.getFromProtobuf(newPartKeyInfo.getPartKeyInfo());

    // Check block location
    List<OmKeyLocationInfo> locationsInfoListFromCommitPartRequest =
        overwriteOMRequest.getCommitMultiPartUploadRequest().getKeyArgs()
            .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());

    assertEquals(overwriteCommittedBlockList, locationsInfoListFromCommitPartRequest);
    assertEquals(overwriteCommittedBlockList, newPartOmKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(1, newPartOmKeyInfo.getKeyLocationVersions().size());

    Map<String, RepeatedOmKeyInfo> toDeleteKeyMap =
        ((S3MultipartUploadCommitPartResponse) omClientResponse).getKeyToDelete();

    // Since there are both uncommitted blocks and overwritten key blocks, there are two keys to delete
    assertEquals(2, toDeleteKeyMap.size());
  }

  @Test
  public void testValidateAndUpdateCacheWithUncommittedBlockForEmptyPart() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    // Create key to be overwritten
    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    List<KeyLocation> emptyKeyLocationInfos = new ArrayList<>();
    List<KeyLocation> originalKeyLocationList = getKeyLocation(1);
    List<OmKeyLocationInfo> originalKeyLocationInfos = originalKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, emptyKeyLocationInfos);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, originalKeyLocationInfos);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());
    PartKeyInfo partKeyInfo = multipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    Map<String, RepeatedOmKeyInfo> toDeleteKeyMap =
        ((S3MultipartUploadCommitPartResponse) omClientResponse).getKeyToDelete();
    assertNull(toDeleteKeyMap);
  }

  @Test
  public void testSplitSchemaCommitWritesToPartsTable() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    String uploadId = UUID.randomUUID().toString();
    createSplitSchemaMpuEntry(volumeName, bucketName, keyName, uploadId, 1L);

    long clientID = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID,
        UUID.randomUUID().toString());

    OMRequest commitRequest = doPreExecuteCommitMPU(volumeName, bucketName,
        keyName, clientID, uploadId, 1);
    S3MultipartUploadCommitPartRequest request = getS3MultipartUploadCommitReq(commitRequest);

    OMClientResponse response = request.validateAndUpdateCache(ozoneManager, 2L);
    assertSame(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus());

    OmMultipartPartKey partKey = OmMultipartPartKey.of(uploadId, 1);
    OmMultipartPartInfo partInfo = omMetadataManager.getMultipartPartsTable().get(partKey);
    assertNotNull(partInfo);
    assertNotNull(partInfo.getETag());
    assertEquals(1, partInfo.getPartNumber());

    // Split schema must NOT write parts inline in multipartInfoTable
    String multipartKey = omMetadataManager.getMultipartKey(volumeName, bucketName, keyName, uploadId);
    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(0, multipartKeyInfo.getPartKeyInfoMap().size());
  }

  @Test
  public void testSplitSchemaOverwriteQueuesOldBlocksForDeletion() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    String uploadId = UUID.randomUUID().toString();
    createSplitSchemaMpuEntry(volumeName, bucketName, keyName, uploadId, 1L);

    // First commit of part 1
    long clientID1 = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID1,
        UUID.randomUUID().toString());

    OMRequest commitRequest1 = doPreExecuteCommitMPU(volumeName, bucketName,
        keyName, clientID1, uploadId, 1);
    getS3MultipartUploadCommitReq(commitRequest1).validateAndUpdateCache(ozoneManager, 2L);

    // Overwrite part 1
    long clientID2 = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID2,
        UUID.randomUUID().toString());

    OMRequest commitRequest2 = doPreExecuteCommitMPU(volumeName, bucketName,
        keyName, clientID2, uploadId, 1);
    OMClientResponse response =
        getS3MultipartUploadCommitReq(commitRequest2).validateAndUpdateCache(ozoneManager, 3L);

    assertSame(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus());

    // Part should be updated in the parts table
    OmMultipartPartKey partKey = OmMultipartPartKey.of(uploadId, 1);
    OmMultipartPartInfo partInfo = omMetadataManager.getMultipartPartsTable().get(partKey);
    assertNotNull(partInfo);

    // Old blocks must be queued for deletion
    Map<String, RepeatedOmKeyInfo> toDelete =
        ((S3MultipartUploadCommitPartResponse) response).getKeyToDelete();
    assertNotNull(toDelete);
    assertEquals(1, toDelete.size());
  }

  @Test
  public void testSplitSchemaCommitFailsWithoutETag() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    String uploadId = UUID.randomUUID().toString();
    createSplitSchemaMpuEntry(volumeName, bucketName, keyName, uploadId, 1L);

    long clientID = Time.now();
    // Add key WITHOUT ETag to open key table
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    // Build a commit request WITHOUT ETag metadata
    OzoneManagerProtocolProtos.MultipartCommitUploadPartRequest multipartRequest =
        OzoneManagerProtocolProtos.MultipartCommitUploadPartRequest.newBuilder()
            .setKeyArgs(OzoneManagerProtocolProtos.KeyArgs.newBuilder()
                .setVolumeName(volumeName).setBucketName(bucketName).setKeyName(keyName)
                .setMultipartUploadID(uploadId).setMultipartNumber(1)
                .setDataSize(0).setModificationTime(Time.now()))
            .setClientID(clientID)
            .build();
    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitMultiPartUpload)
        .setClientId(UUID.randomUUID().toString())
        .setCommitMultiPartUploadRequest(multipartRequest)
        .build();
    S3MultipartUploadCommitPartRequest request = getS3MultipartUploadCommitReq(omRequest);

    OMClientResponse response = request.validateAndUpdateCache(ozoneManager, 2L);
    assertSame(OzoneManagerProtocolProtos.Status.INVALID_REQUEST, response.getOMResponse().getStatus());
  }

  @Test
  public void testScanPartsReturnsCommittedPart() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    String uploadId = UUID.randomUUID().toString();
    createSplitSchemaMpuEntry(volumeName, bucketName, keyName, uploadId, 1L);

    long clientID = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID,
        UUID.randomUUID().toString());
    OMRequest commitRequest = doPreExecuteCommitMPU(volumeName, bucketName,
        keyName, clientID, uploadId, 1);
    getS3MultipartUploadCommitReq(commitRequest).validateAndUpdateCache(ozoneManager, 2L);

    SortedMap<Integer, OmMultipartPartInfo> parts =
        OMMultipartUploadUtils.scanParts(omMetadataManager, uploadId);
    assertEquals(1, parts.size());
    assertTrue(parts.containsKey(1));
    assertNotNull(parts.get(1).getETag());
  }

  @Test
  public void testScanPartsTombstonePreventsDeletedPartFromReappearing() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    String uploadId = UUID.randomUUID().toString();
    createSplitSchemaMpuEntry(volumeName, bucketName, keyName, uploadId, 1L);

    // Commit two parts
    long clientID1 = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID1,
        UUID.randomUUID().toString());
    OMRequest commit1 = doPreExecuteCommitMPU(volumeName, bucketName, keyName, clientID1, uploadId, 1);
    getS3MultipartUploadCommitReq(commit1).validateAndUpdateCache(ozoneManager, 2L);

    long clientID2 = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID2,
        UUID.randomUUID().toString());
    OMRequest commit2 = doPreExecuteCommitMPU(volumeName, bucketName, keyName, clientID2, uploadId, 2);
    getS3MultipartUploadCommitReq(commit2).validateAndUpdateCache(ozoneManager, 3L);

    // Flush parts to DB so they exist on disk
    OmMultipartPartKey partKey1 = OmMultipartPartKey.of(uploadId, 1);
    OmMultipartPartKey partKey2 = OmMultipartPartKey.of(uploadId, 2);
    OmMultipartPartInfo info1 = omMetadataManager.getMultipartPartsTable().get(partKey1);
    OmMultipartPartInfo info2 = omMetadataManager.getMultipartPartsTable().get(partKey2);
    omMetadataManager.getMultipartPartsTable().put(partKey1, info1);
    omMetadataManager.getMultipartPartsTable().put(partKey2, info2);

    // Tombstone part 1 in cache (simulates a delete that hasn't flushed)
    omMetadataManager.getMultipartPartsTable().addCacheEntry(
        new CacheKey<>(partKey1), CacheValue.get(4L));

    SortedMap<Integer, OmMultipartPartInfo> parts =
        OMMultipartUploadUtils.scanParts(omMetadataManager, uploadId);
    assertEquals(1, parts.size());
    assertFalse(parts.containsKey(1));
    assertTrue(parts.containsKey(2));
  }

  @Test
  public void testScanPartsCacheOverridesDbEntry() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    String uploadId = UUID.randomUUID().toString();
    createSplitSchemaMpuEntry(volumeName, bucketName, keyName, uploadId, 1L);

    // Commit part 1 first time
    long clientID1 = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID1,
        UUID.randomUUID().toString());
    OMRequest commit1 = doPreExecuteCommitMPU(volumeName, bucketName, keyName, clientID1, uploadId, 1);
    getS3MultipartUploadCommitReq(commit1).validateAndUpdateCache(ozoneManager, 2L);

    // Flush to DB
    OmMultipartPartKey partKey1 = OmMultipartPartKey.of(uploadId, 1);
    OmMultipartPartInfo originalInfo = omMetadataManager.getMultipartPartsTable().get(partKey1);
    omMetadataManager.getMultipartPartsTable().put(partKey1, originalInfo);

    // Overwrite part 1 with different data size
    long clientID2 = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID2,
        UUID.randomUUID().toString());
    OMRequest commit2 = doPreExecuteCommitMPU(volumeName, bucketName, keyName, clientID2, uploadId, 1);
    getS3MultipartUploadCommitReq(commit2).validateAndUpdateCache(ozoneManager, 3L);

    // scanParts should return the newer cached version, not the DB version
    SortedMap<Integer, OmMultipartPartInfo> parts =
        OMMultipartUploadUtils.scanParts(omMetadataManager, uploadId);
    assertEquals(1, parts.size());
    OmMultipartPartInfo scannedInfo = parts.get(1);
    assertNotNull(scannedInfo);
    // The cache entry (from the second commit) should take precedence
    assertNotNull(scannedInfo.getETag());
  }

  @Test
  public void testScanPartsIsolatesUploadIds() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    String uploadId1 = UUID.randomUUID().toString();
    String uploadId2 = UUID.randomUUID().toString();
    createSplitSchemaMpuEntry(volumeName, bucketName, keyName, uploadId1, 1L);
    createSplitSchemaMpuEntry(volumeName, bucketName, keyName, uploadId2, 2L);

    // Commit a part under uploadId1
    long clientID1 = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID1,
        UUID.randomUUID().toString());
    OMRequest commit1 = doPreExecuteCommitMPU(volumeName, bucketName, keyName, clientID1, uploadId1, 1);
    getS3MultipartUploadCommitReq(commit1).validateAndUpdateCache(ozoneManager, 3L);

    // Commit a part under uploadId2
    long clientID2 = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID2,
        UUID.randomUUID().toString());
    OMRequest commit2 = doPreExecuteCommitMPU(volumeName, bucketName, keyName, clientID2, uploadId2, 5);
    getS3MultipartUploadCommitReq(commit2).validateAndUpdateCache(ozoneManager, 4L);

    SortedMap<Integer, OmMultipartPartInfo> parts1 =
        OMMultipartUploadUtils.scanParts(omMetadataManager, uploadId1);
    assertEquals(1, parts1.size());
    assertTrue(parts1.containsKey(1));

    SortedMap<Integer, OmMultipartPartInfo> parts2 =
        OMMultipartUploadUtils.scanParts(omMetadataManager, uploadId2);
    assertEquals(1, parts2.size());
    assertTrue(parts2.containsKey(5));
  }

  @Test
  public void testScanPartsEmptyForUnknownUploadId() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    String uploadId = UUID.randomUUID().toString();
    createSplitSchemaMpuEntry(volumeName, bucketName, keyName, uploadId, 1L);

    // Commit a part
    long clientID = Time.now();
    addKeyToOpenKeyTableWithETag(volumeName, bucketName, keyName, clientID,
        UUID.randomUUID().toString());
    OMRequest commit = doPreExecuteCommitMPU(volumeName, bucketName, keyName, clientID, uploadId, 1);
    getS3MultipartUploadCommitReq(commit).validateAndUpdateCache(ozoneManager, 2L);

    // Scan with a different upload ID should find nothing
    SortedMap<Integer, OmMultipartPartInfo> parts =
        OMMultipartUploadUtils.scanParts(omMetadataManager, UUID.randomUUID().toString());
    assertTrue(parts.isEmpty());
  }

  private void createSplitSchemaMpuEntry(String volumeName, String bucketName,
      String keyName, String uploadId, long trxnIdx) throws IOException {
    OmMultipartKeyInfo multipartKeyInfo = new OmMultipartKeyInfo.Builder()
        .setUploadID(uploadId)
        .setCreationTime(Time.now())
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setObjectID(trxnIdx)
        .setUpdateID(trxnIdx)
        .setSchemaVersion(OmMultipartKeyInfo.SPLIT_PARTS_TABLE_SCHEMA_VERSION)
        .build();

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>(), true)))
        .build();

    OMRequestTestUtils.addMultipartInfoToTable(false, omKeyInfo, multipartKeyInfo, trxnIdx, omMetadataManager);
  }

  private void addKeyToOpenKeyTableWithETag(String volumeName, String bucketName,
      String keyName, long clientID, String eTag) throws Exception {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>(), true)))
        .addMetadata(OzoneConsts.ETAG, eTag)
        .build();

    String openKey = getOpenKey(volumeName, bucketName, keyName, clientID);
    omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
        new CacheKey<>(openKey), CacheValue.get(clientID, keyInfo));
    omMetadataManager.getOpenKeyTable(getBucketLayout()).put(openKey, keyInfo);
  }

  protected void addKeyToOpenKeyTable(String volumeName, String bucketName,
      String keyName, long clientID) throws Exception {
    OMRequestTestUtils.addKeyToTable(true, true, volumeName, bucketName,
        keyName, clientID, RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE), omMetadataManager);
  }

  protected String addKeyToOpenKeyTable(String volumeName, String bucketName,
      String keyName, long clientID, List<OmKeyLocationInfo> locationList) throws Exception {
    OMRequestTestUtils.addKeyToTable(true, true, false,
        volumeName, bucketName, keyName,
        clientID, RatisReplicationConfig.getInstance(ReplicationFactor.ONE), 0L,
        omMetadataManager, locationList, 0L);

    return getOpenKey(volumeName, bucketName, keyName, clientID);
  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }

  protected String getMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    return omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName, multipartUploadID);
  }

  protected String getOpenKey(String volumeName, String bucketName,
      String keyName, long clientID) throws IOException {
    return omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, clientID);
  }

  protected void createParentPath(String volumeName, String bucketName)
          throws Exception {
    // no parent hierarchy
  }

  /**
   * Create KeyLocation list.
   */
  protected List<KeyLocation> getKeyLocation(int count) {
    List<KeyLocation> keyLocations = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      KeyLocation keyLocation =
          KeyLocation.newBuilder()
              .setBlockID(HddsProtos.BlockID.newBuilder()
                  .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                      .setContainerID(i + 1000).setLocalID(i + 100).build()))
              .setOffset(0).setLength(200).setCreateVersion(0L).build();
      keyLocations.add(keyLocation);
    }
    return keyLocations;
  }
}
