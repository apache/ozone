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
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

import static org.junit.jupiter.api.Assertions.assertThrows;

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
    Assertions.assertNotNull(omKeyInfo);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assertions.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Entry should be deleted from openKey Table.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(openKey);
    Assertions.assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assertions.assertNotNull(omKeyInfo);

    // Check modification time

    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assertions.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
        omKeyInfo.getModificationTime());

    // Check block location.
    Assertions.assertEquals(allocatedLocationList,
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
    Assertions.assertNotNull(omKeyInfo);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assertions.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Entry should be deleted from openKey Table.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(openKey);
    Assertions.assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);
    Assertions.assertNotNull(omKeyInfo);
    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time

    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assertions.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
        omKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs()
        .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    Assertions.assertEquals(locationInfoListFromCommitKeyRequest,
        omKeyInfo.getLatestVersionLocations().getLocationList());
    Assertions.assertEquals(allocatedLocationList,
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
    Assertions.assertNotNull(omKeyInfo);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assertions.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    Map<String, RepeatedOmKeyInfo> toDeleteKeyList
        = ((OMKeyCommitResponse) omClientResponse).getKeysToDelete();

    // This is the first time to commit key, only the allocated but uncommitted
    // blocks should be deleted.
    Assertions.assertEquals(1, toDeleteKeyList.size());
    Assertions.assertEquals(2, toDeleteKeyList.values().stream()
        .findFirst().get().cloneOmKeyInfoList().get(0).getKeyLocationVersions()
        .get(0).getLocationList().size());

    // Entry should be deleted from openKey Table.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(openKey);
    Assertions.assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assertions.assertNotNull(omKeyInfo);

    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time
    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assertions.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
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
    Assertions.assertEquals(intersection,
        omKeyInfo.getLatestVersionLocations().getLocationList());
    Assertions.assertEquals(3, intersection.size());

  }

  @Test
  public void testRejectHsyncIfNotEnabled() throws Exception {
    OzoneConfiguration conf = ozoneManager.getConfiguration();
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, false);
    BucketLayout bucketLayout = getBucketLayout();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, bucketLayout);
    List<KeyLocation> allocatedKeyLocationList = getKeyLocation(10);

    // hsync should throw OMException
    assertThrows(OMException.class, () ->
        doKeyCommit(true, allocatedKeyLocationList.subList(0, 5)));

    // Regular key commit should still work
    doKeyCommit(false, allocatedKeyLocationList.subList(0, 5));

    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
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

    // 1st commit of 3 blocks, HSync = true
    Map<String, RepeatedOmKeyInfo> keyToDeleteMap =
        doKeyCommit(true, allocatedKeyLocationList.subList(0, 3));
    Assertions.assertNull(keyToDeleteMap);
    bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    long firstCommitUsedBytes = bucketInfo.getUsedBytes();
    Assertions.assertEquals(300, firstCommitUsedBytes - usedBytes);

    // 2nd commit of 6 blocks, HSync = true
    keyToDeleteMap = doKeyCommit(true, allocatedKeyLocationList.subList(0, 6));
    Assertions.assertNull(keyToDeleteMap);
    bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    long secondCommitUsedBytes = bucketInfo.getUsedBytes();
    Assertions.assertEquals(600, secondCommitUsedBytes - usedBytes);

    // 3rd and final commit of all 10 blocks, HSync = false
    keyToDeleteMap = doKeyCommit(false, allocatedKeyLocationList);
    // keyToDeleteMap should be null / empty because none of the previous blocks
    // should be deleted.
    Assertions.assertTrue(keyToDeleteMap == null || keyToDeleteMap.isEmpty());
    bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    long thirdCommitUsedBytes = bucketInfo.getUsedBytes();
    Assertions.assertEquals(1000, thirdCommitUsedBytes - usedBytes);
  }

  private Map<String, RepeatedOmKeyInfo> doKeyCommit(boolean isHSync,
      List<KeyLocation> keyLocations) throws Exception {
    // allocated block list
    dataSize = keyLocations.size() * 100;
    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest(
        keyLocations, isHSync));
    OMKeyCommitRequest omKeyCommitRequest =
        getOmKeyCommitRequest(modifiedOmRequest);

    List<OmKeyLocationInfo> allocatedBlockList = keyLocations
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());
    String openKey = addKeyToOpenKeyTable(allocatedBlockList);
    String ozoneKey = getOzonePathKey();
    
    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Key should be present in both OpenKeyTable and KeyTable with HSync commit
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(
            omKeyCommitRequest.getBucketLayout()).get(openKey);
    if (isHSync) {
      Assertions.assertNotNull(omKeyInfo);
    } else {
      // Key should not exist in OpenKeyTable anymore with non-HSync commit
      Assertions.assertNull(omKeyInfo);
    }
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);
    Assertions.assertNotNull(omKeyInfo);

    return ((OMKeyCommitResponse) omClientResponse).getKeysToDelete();
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

    Assertions.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assertions.assertNull(omKeyInfo);
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

    Assertions.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assertions.assertNull(omKeyInfo);
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
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED,
        omClientResponse.getOMResponse().getStatus());

    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));
    Assertions.assertEquals(0, bucketInfo.getUsedNamespace());
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

    Assertions.assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assertions.assertNull(omKeyInfo);
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

    Assertions.assertNotNull(omKeyInfo);
    // Previously committed version
    Assertions.assertEquals(0L,
            omKeyInfo.getLatestVersionLocations().getVersion());

    // Append new blocks
    List<OmKeyLocationInfo> allocatedLocationList =
            keyArgs.getKeyLocationsList().stream()
                    .map(OmKeyLocationInfo::getFromProtobuf)
                    .collect(Collectors.toList());
    addKeyToOpenKeyTable(allocatedLocationList);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 102L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
            omClientResponse.getOMResponse().getStatus());

    // New entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    Assertions.assertNotNull(omKeyInfo);
    Assertions.assertEquals(version,
            omKeyInfo.getLatestVersionLocations().getVersion());
    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time
    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    Assertions.assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
            omKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs()
        .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    Assertions.assertEquals(locationInfoListFromCommitKeyRequest,
            omKeyInfo.getLatestVersionLocations().getLocationList());
    Assertions.assertEquals(allocatedLocationList,
            omKeyInfo.getLatestVersionLocations().getLocationList());
    Assertions.assertEquals(1, omKeyInfo.getKeyLocationVersions().size());
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

    Assertions.assertTrue(modifiedOmRequest.hasCommitKeyRequest());
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
    Assertions.assertTrue(modifiedKeyArgs.getModificationTime() > 0);
    Assertions.assertEquals(0, originalKeyArgs.getModificationTime());

    Assertions.assertEquals(originalKeyArgs.getVolumeName(),
        modifiedKeyArgs.getVolumeName());
    Assertions.assertEquals(originalKeyArgs.getBucketName(),
        modifiedKeyArgs.getBucketName());
    Assertions.assertEquals(originalKeyArgs.getKeyName(),
        modifiedKeyArgs.getKeyName());
    Assertions.assertEquals(originalKeyArgs.getDataSize(),
        modifiedKeyArgs.getDataSize());
    Assertions.assertEquals(originalKeyArgs.getKeyLocationsList(),
        modifiedKeyArgs.getKeyLocationsList());
    Assertions.assertEquals(originalKeyArgs.getType(),
        modifiedKeyArgs.getType());
    Assertions.assertEquals(originalKeyArgs.getFactor(),
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
    Assertions.assertEquals(keyName, omKeyInfo.getKeyName(),
        "Incorrect KeyName");
    String fileName = OzoneFSUtils.getFileName(keyName);
    Assertions.assertEquals(fileName, omKeyInfo.getFileName(),
        "Incorrect FileName");
  }
}
