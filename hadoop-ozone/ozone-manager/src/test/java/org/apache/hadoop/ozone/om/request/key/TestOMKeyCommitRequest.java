/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

/**
 * Class tests OMKeyCommitRequest class.
 */
public class TestOMKeyCommitRequest extends TestOMKeyRequest {

  private String parentDir;

  @Test
  public void testPreExecute() throws Exception {
    doPreExecute(createCommitKeyRequest());
  }

  @Test
  public void testValidateAndUpdateCacheWithUnknownBlockId() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
        getOmKeyCommitRequest(modifiedOmRequest);

    // Append 3 blocks locations.
    List<OmKeyLocationInfo> allocatedLocationList = getKeyLocation(3)
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, omKeyCommitRequest.getBucketLayout());

    String openKey = addKeyToOpenKeyTable(allocatedLocationList);
    String ozoneKey = getOzonePathKey();

    OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(
                    omKeyCommitRequest.getBucketLayout()).get(openKey);
    Assert.assertNotNull(omKeyInfo);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Entry should be deleted from openKey Table.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(openKey);
    Assert.assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNotNull(omKeyInfo);

    // Check modification time

    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assert.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
        omKeyInfo.getModificationTime());

    // Check block location.
    Assert.assertEquals(allocatedLocationList,
        omKeyInfo.getLatestVersionLocations().getLocationList());

  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
            getOmKeyCommitRequest(modifiedOmRequest);


    KeyArgs keyArgs = modifiedOmRequest.getCommitKeyRequest().getKeyArgs();

    // Append new blocks
    List<OmKeyLocationInfo> allocatedLocationList =
        keyArgs.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, omKeyCommitRequest.getBucketLayout());

    String openKey = addKeyToOpenKeyTable(allocatedLocationList);
    String ozoneKey = getOzonePathKey();

    OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(
                    omKeyCommitRequest.getBucketLayout()).get(openKey);
    Assert.assertNotNull(omKeyInfo);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
        100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Entry should be deleted from openKey Table.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(openKey);
    Assert.assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);
    Assert.assertNotNull(omKeyInfo);
    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time

    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assert.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
        omKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs()
        .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    Assert.assertEquals(locationInfoListFromCommitKeyRequest,
        omKeyInfo.getLatestVersionLocations().getLocationList());
    Assert.assertEquals(allocatedLocationList,
        omKeyInfo.getLatestVersionLocations().getLocationList());
  }

  @Test
  public void testValidateAndUpdateCacheWithUncommittedBlocks()
      throws Exception {

    // allocated block list
    List<KeyLocation> allocatedKeyLocationList = getKeyLocation(5);

    List<OmKeyLocationInfo> allocatedBlockList = allocatedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    // committed block list, with three blocks different with the allocated
    List<KeyLocation> committedKeyLocationList = getKeyLocation(3);

    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest(
        committedKeyLocationList, false));

    OMKeyCommitRequest omKeyCommitRequest =
        getOmKeyCommitRequest(modifiedOmRequest);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, omKeyCommitRequest.getBucketLayout());

    String openKey = addKeyToOpenKeyTable(allocatedBlockList);
    String ozoneKey = getOzonePathKey();

    OmKeyInfo omKeyInfo =
            omMetadataManager.getOpenKeyTable(
                    omKeyCommitRequest.getBucketLayout()).get(openKey);
    Assert.assertNotNull(omKeyInfo);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    List<OmKeyInfo> toDeleteKeyList = ((OMKeyCommitResponse) omClientResponse).
        getKeysToDelete().cloneOmKeyInfoList();

    // This is the first time to commit key, only the allocated but uncommitted
    // blocks should be deleted.
    Assert.assertEquals(1, toDeleteKeyList.size());
    Assert.assertEquals(2, toDeleteKeyList.get(0).
        getKeyLocationVersions().get(0).getLocationList().size());

    // Entry should be deleted from openKey Table.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(openKey);
    Assert.assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNotNull(omKeyInfo);

    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time
    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assert.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
        omKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs()
            .getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());

    List<OmKeyLocationInfo> intersection = new ArrayList<>(allocatedBlockList);
    intersection.retainAll(locationInfoListFromCommitKeyRequest);

    // Key table should have three blocks.
    Assert.assertEquals(intersection,
        omKeyInfo.getLatestVersionLocations().getLocationList());
    Assert.assertEquals(3, intersection.size());

  }

  @Test
  public void testCommitWithHsyncIncrementalUsages() throws Exception {
    BucketLayout bucketLayout = getBucketLayout();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, bucketLayout);
    List<KeyLocation> allocatedKeyLocationList = getKeyLocation(10);
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable()
        .get(bucketKey);
    long usedBytes = bucketInfo.getUsedBytes();

    performHsyncCommit(allocatedKeyLocationList.subList(0, 5));
    bucketInfo = omMetadataManager.getBucketTable()
        .get(bucketKey);
    long firstCommitUsedBytes = bucketInfo.getUsedBytes();
    Assert.assertEquals(500, firstCommitUsedBytes - usedBytes);

    performHsyncCommit(allocatedKeyLocationList);
    bucketInfo = omMetadataManager.getBucketTable()
        .get(bucketKey);
    long nextCommitUsedBytes = bucketInfo.getUsedBytes();

    Assert.assertEquals(1000, nextCommitUsedBytes - usedBytes);
  }
  
  private List<KeyLocation> performHsyncCommit(
      List<KeyLocation> keyLocations) throws Exception {
    // allocated block list
    dataSize = keyLocations.size() * 100;
    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest(
        keyLocations, true));
    OMKeyCommitRequest omKeyCommitRequest =
        getOmKeyCommitRequest(modifiedOmRequest);

    List<OmKeyLocationInfo> allocatedBlockList = keyLocations
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());
    String openKey = addKeyToOpenKeyTable(allocatedBlockList);
    String ozoneKey = getOzonePathKey();
    
    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // key must be prsent in both open key table and key table for hsync
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(
            omKeyCommitRequest.getBucketLayout()).get(openKey);
    Assert.assertNotNull(omKeyInfo);
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);
    Assert.assertNotNull(omKeyInfo);
    return keyLocations;
  }

  @Test
  public void testValidateAndUpdateCacheWithSubDirs() throws Exception {
    parentDir = "dir1/dir2/dir3/";
    keyName = parentDir + UUID.randomUUID().toString();

    testValidateAndUpdateCache();
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
            getOmKeyCommitRequest(modifiedOmRequest);

    final long volumeId = 100L;
    final long bucketID = 1000L;
    final String fileName = OzoneFSUtils.getFileName(keyName);
    final String ozoneKey = omMetadataManager.getOzonePathKey(volumeId,
            bucketID, bucketID, fileName);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNull(omKeyInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
            getOmKeyCommitRequest(modifiedOmRequest);

    OMRequestTestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE,
        omMetadataManager);
    String ozoneKey = getOzonePathKey();

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNull(omKeyInfo);
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketQuotaExceeds()
      throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
        getOmKeyCommitRequest(modifiedOmRequest);

    KeyArgs keyArgs = modifiedOmRequest.getCommitKeyRequest().getKeyArgs();

    // Append new blocks
    List<OmKeyLocationInfo> allocatedLocationList =
        keyArgs.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());


    OMRequestTestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE,
        omMetadataManager);
    OmBucketInfo.Builder bucketBuilder = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(omKeyCommitRequest.getBucketLayout())
        .setQuotaInBytes(0L);
    OMRequestTestUtils.addBucketToDB(omMetadataManager, bucketBuilder);

    addKeyToOpenKeyTable(allocatedLocationList);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED,
        omClientResponse.getOMResponse().getStatus());

    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));
    Assert.assertEquals(0, bucketInfo.getUsedNamespace());
  }

  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
            getOmKeyCommitRequest(modifiedOmRequest);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, omKeyCommitRequest.getBucketLayout());

    String ozoneKey = getOzonePathKey();

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNull(omKeyInfo);
  }

  @Test
  public void testValidateAndUpdateCacheOnOverwrite() throws Exception {
    testValidateAndUpdateCache();

    // Become a new client and set next version number
    clientID = Time.now();
    version += 1;

    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest());

    OMKeyCommitRequest omKeyCommitRequest =
            getOmKeyCommitRequest(modifiedOmRequest);

    KeyArgs keyArgs = modifiedOmRequest.getCommitKeyRequest().getKeyArgs();

    String ozoneKey = getOzonePathKey();
    // Key should be there in key table, as validateAndUpdateCache is called.
    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNotNull(omKeyInfo);
    // Previously committed version
    Assert.assertEquals(0L,
            omKeyInfo.getLatestVersionLocations().getVersion());

    // Append new blocks
    List<OmKeyLocationInfo> allocatedLocationList =
            keyArgs.getKeyLocationsList().stream()
                    .map(OmKeyLocationInfo::getFromProtobuf)
                    .collect(Collectors.toList());
    addKeyToOpenKeyTable(allocatedLocationList);

    OMClientResponse omClientResponse =
            omKeyCommitRequest.validateAndUpdateCache(ozoneManager,
                    102L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
            omClientResponse.getOMResponse().getStatus());

    // New entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assert.assertNotNull(omKeyInfo);
    Assert.assertEquals(version,
            omKeyInfo.getLatestVersionLocations().getVersion());
    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time
    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assert.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
            omKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs()
        .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    Assert.assertEquals(locationInfoListFromCommitKeyRequest,
            omKeyInfo.getLatestVersionLocations().getLocationList());
    Assert.assertEquals(allocatedLocationList,
            omKeyInfo.getLatestVersionLocations().getLocationList());
    Assert.assertEquals(1, omKeyInfo.getKeyLocationVersions().size());
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {

    OMKeyCommitRequest omKeyCommitRequest =
            getOmKeyCommitRequest(originalOMRequest);

    OMRequest modifiedOmRequest = omKeyCommitRequest.preExecute(ozoneManager);

    Assert.assertTrue(modifiedOmRequest.hasCommitKeyRequest());
    KeyArgs originalKeyArgs =
        originalOMRequest.getCommitKeyRequest().getKeyArgs();
    KeyArgs modifiedKeyArgs =
        modifiedOmRequest.getCommitKeyRequest().getKeyArgs();
    verifyKeyArgs(originalKeyArgs, modifiedKeyArgs);
    return modifiedOmRequest;
  }

  /**
   * Verify KeyArgs.
   * @param originalKeyArgs
   * @param modifiedKeyArgs
   */
  private void verifyKeyArgs(KeyArgs originalKeyArgs, KeyArgs modifiedKeyArgs) {

    // Check modification time is set or not.
    Assert.assertTrue(modifiedKeyArgs.getModificationTime() > 0);
    Assert.assertTrue(originalKeyArgs.getModificationTime() == 0);

    Assert.assertEquals(originalKeyArgs.getVolumeName(),
        modifiedKeyArgs.getVolumeName());
    Assert.assertEquals(originalKeyArgs.getBucketName(),
        modifiedKeyArgs.getBucketName());
    Assert.assertEquals(originalKeyArgs.getKeyName(),
        modifiedKeyArgs.getKeyName());
    Assert.assertEquals(originalKeyArgs.getDataSize(),
        modifiedKeyArgs.getDataSize());
    Assert.assertEquals(originalKeyArgs.getKeyLocationsList(),
        modifiedKeyArgs.getKeyLocationsList());
    Assert.assertEquals(originalKeyArgs.getType(),
        modifiedKeyArgs.getType());
    Assert.assertEquals(originalKeyArgs.getFactor(),
        modifiedKeyArgs.getFactor());
  }

  private OMRequest createCommitKeyRequest() {
    return createCommitKeyRequest(false);
  }

  private OMRequest createCommitKeyRequest(boolean isHsync) {
    return createCommitKeyRequest(getKeyLocation(5), isHsync);
  }

  /**
   * Create OMRequest which encapsulates CommitKeyRequest.
   */
  private OMRequest createCommitKeyRequest(
      List<KeyLocation> keyLocations, boolean isHsync) {
    KeyArgs keyArgs =
        KeyArgs.newBuilder().setDataSize(dataSize).setVolumeName(volumeName)
            .setKeyName(keyName).setBucketName(bucketName)
            .setType(replicationType).setFactor(replicationFactor)
            .addAllKeyLocations(keyLocations).build();

    CommitKeyRequest commitKeyRequest =
        CommitKeyRequest.newBuilder().setKeyArgs(keyArgs)
            .setClientID(clientID).setHsync(isHsync).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
        .setCommitKeyRequest(commitKeyRequest)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  /**
   * Create KeyLocation list.
   */
  private List<KeyLocation> getKeyLocation(int count) {
    List<KeyLocation> keyLocations = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      KeyLocation keyLocation =
          KeyLocation.newBuilder()
              .setBlockID(HddsProtos.BlockID.newBuilder()
                  .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                      .setContainerID(i + 1000).setLocalID(i + 100).build()))
              .setOffset(0).setLength(200).setCreateVersion(version).build();
      keyLocations.add(keyLocation);
    }
    return keyLocations;
  }

  protected String getParentDir() {
    return parentDir;
  }

  @NotNull
  protected String getOzonePathKey() throws IOException {
    return omMetadataManager.getOzoneKey(volumeName, bucketName,
            keyName);
  }

  @NotNull
  protected String addKeyToOpenKeyTable(List<OmKeyLocationInfo> locationList)
      throws Exception {
    OMRequestTestUtils.addKeyToTable(true, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, omMetadataManager,
        locationList, version);

    return omMetadataManager.getOpenKey(volumeName, bucketName,
              keyName, clientID);
  }

  @NotNull
  protected OMKeyCommitRequest getOmKeyCommitRequest(OMRequest omRequest) {
    return new OMKeyCommitRequest(omRequest, BucketLayout.DEFAULT);
  }

  protected void verifyKeyName(OmKeyInfo omKeyInfo) {
    Assert.assertEquals("Incorrect KeyName", keyName,
            omKeyInfo.getKeyName());
    String fileName = OzoneFSUtils.getFileName(keyName);
    Assert.assertEquals("Incorrect FileName", fileName,
            omKeyInfo.getFileName());
  }
}
