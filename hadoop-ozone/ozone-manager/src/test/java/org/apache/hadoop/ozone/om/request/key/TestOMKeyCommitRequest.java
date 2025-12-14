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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Class tests OMKeyCommitRequest class.
 */
public class TestOMKeyCommitRequest extends TestOMKeyRequest {

  private static final int DEFAULT_COMMIT_BLOCK_SIZE = 5;

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
    assertNotNull(omKeyInfo);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OK,
        omClientResponse.getOMResponse().getStatus());

    // Entry should be deleted from openKey Table.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(openKey);
    assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    assertNotNull(omKeyInfo);

    // Check modification time

    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
        omKeyInfo.getModificationTime());

    // Check block location.
    assertEquals(allocatedLocationList,
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
    assertNotNull(omKeyInfo);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OK,
        omClientResponse.getOMResponse().getStatus());

    // Entry should be deleted from openKey Table.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(openKey);
    assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);
    assertNotNull(omKeyInfo);
    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time

    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
        omKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs()
        .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    assertEquals(locationInfoListFromCommitKeyRequest,
        omKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(allocatedLocationList,
        omKeyInfo.getLatestVersionLocations().getLocationList());
  }

  @Test
  public void testAtomicRewrite() throws Exception {
    Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
    Table<String, OmKeyInfo> closedKeyTable = omMetadataManager.getKeyTable(getBucketLayout());

    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest());
    OMKeyCommitRequest omKeyCommitRequest = getOmKeyCommitRequest(modifiedOmRequest);
    KeyArgs keyArgs = modifiedOmRequest.getCommitKeyRequest().getKeyArgs();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, omKeyCommitRequest.getBucketLayout());

    // Append new blocks
    List<OmKeyLocationInfo> allocatedLocationList =
        keyArgs.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());

    List<OzoneAcl> acls = Collections.singletonList(OzoneAcl.parseAcl("user:foo:rw"));
    OmKeyInfo.Builder omKeyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(
        volumeName, bucketName, keyName, replicationConfig, new OmKeyLocationInfoGroup(version, new ArrayList<>()));
    omKeyInfoBuilder.setExpectedDataGeneration(1L);
    omKeyInfoBuilder.addAcl(acls.get(0));

    String openKey = addKeyToOpenKeyTable(allocatedLocationList, omKeyInfoBuilder);
    OmKeyInfo openKeyInfo = openKeyTable.get(openKey);
    assertNotNull(openKeyInfo);
    assertEquals(acls, openKeyInfo.getAcls());
    // At this stage, we have an openKey, with rewrite generation of 1.
    // However there is no closed key entry, so the commit should fail.
    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(KEY_NOT_FOUND, omClientResponse.getOMResponse().getStatus());

    // Now add the key to the key table, and try again, but with different generation
    omKeyInfoBuilder.setExpectedDataGeneration(null);
    omKeyInfoBuilder.setUpdateID(0L);
    OmKeyInfo invalidKeyInfo = omKeyInfoBuilder.build();
    closedKeyTable.put(getOzonePathKey(), invalidKeyInfo);
    // This should fail as the updateID ia zero and the open key has rewrite generation of 1.
    omClientResponse = omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(KEY_NOT_FOUND, omClientResponse.getOMResponse().getStatus());

    omKeyInfoBuilder.setUpdateID(1L);
    OmKeyInfo closedKeyInfo = omKeyInfoBuilder.build();

    closedKeyTable.delete(getOzonePathKey());
    closedKeyTable.put(getOzonePathKey(), closedKeyInfo);

    // Now the key should commit as the updateID and rewrite Generation match.
    omClientResponse = omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OK, omClientResponse.getOMResponse().getStatus());

    OmKeyInfo committedKey = closedKeyTable.get(getOzonePathKey());
    assertNull(committedKey.getExpectedDataGeneration());
    // Generation should be changed
    assertNotEquals(closedKeyInfo.getGeneration(), committedKey.getGeneration());
    assertEquals(acls, committedKey.getAcls());
  }

  @Test
  public void testValidateAndUpdateCacheWithUncommittedBlocks()
      throws Exception {

    // Allocated block list (5 blocks)
    List<KeyLocation> allocatedKeyLocationList = getKeyLocation(5);

    List<OmKeyLocationInfo> allocatedBlockList = allocatedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    // Commit only the first 3 allocated blocks
    List<KeyLocation> committedKeyLocationList = allocatedKeyLocationList.subList(0, 3);

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
    assertNotNull(omKeyInfo);

    // Key should not be there in key table, as validateAndUpdateCache is
    // still not called.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OK,
        omClientResponse.getOMResponse().getStatus());

    Map<String, RepeatedOmKeyInfo> toDeleteKeyList
        = ((OMKeyCommitResponse) omClientResponse).getKeysToDelete();

    // This is the first time to commit key, only the allocated but uncommitted
    // blocks should be deleted.
    assertEquals(1, toDeleteKeyList.size());
    assertEquals(allocatedKeyLocationList.size() - committedKeyLocationList.size(),
        toDeleteKeyList.values().stream()
        .findFirst().get().cloneOmKeyInfoList().get(0).getKeyLocationVersions()
        .get(0).getLocationList().size());

    // Entry should be deleted from openKey Table.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(openKey);
    assertNull(omKeyInfo);

    // Now entry should be created in key Table.
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    assertNotNull(omKeyInfo);

    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time
    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(),
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
    assertEquals(intersection,
        omKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(3, intersection.size());

  }

  /**
   * In these scenarios below, OM should reject key commit with HSync requested from a client:
   * 1. ozone.hbase.enhancements.allowed = false, ozone.fs.hsync.enabled = false
   * 2. ozone.hbase.enhancements.allowed = false, ozone.fs.hsync.enabled = true
   * 3. ozone.hbase.enhancements.allowed = true,  ozone.fs.hsync.enabled = false
   */
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false"})
  public void testRejectHsyncIfNotEnabled(boolean hbaseEnhancementsEnabled, boolean fsHsyncEnabled) throws Exception {
    OzoneConfiguration conf = ozoneManager.getConfiguration();
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, hbaseEnhancementsEnabled);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, fsHsyncEnabled);
    BucketLayout bucketLayout = getBucketLayout();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, bucketLayout);
    List<KeyLocation> allocatedKeyLocationList = getKeyLocation(10);

    // hsync should throw OMException
    assertThrows(OMException.class, () ->
        doKeyCommit(true, allocatedKeyLocationList.subList(0, 5)));

    // Regular key commit should still work
    doKeyCommit(false, allocatedKeyLocationList.subList(0, 5));

    // Restore config after this test run
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
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
    assertNull(keyToDeleteMap);
    bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    long firstCommitUsedBytes = bucketInfo.getUsedBytes();
    assertEquals(300, firstCommitUsedBytes - usedBytes);

    // 2nd commit of 6 blocks, HSync = true
    keyToDeleteMap = doKeyCommit(true, allocatedKeyLocationList.subList(0, 6));
    assertNull(keyToDeleteMap);
    bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    long secondCommitUsedBytes = bucketInfo.getUsedBytes();
    assertEquals(600, secondCommitUsedBytes - usedBytes);

    // 3rd and final commit of all 10 blocks, HSync = false
    keyToDeleteMap = doKeyCommit(false, allocatedKeyLocationList);
    // keyToDeleteMap should be null / empty because none of the previous blocks
    // should be deleted.
    assertTrue(keyToDeleteMap == null || keyToDeleteMap.isEmpty());
    bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    long thirdCommitUsedBytes = bucketInfo.getUsedBytes();
    assertEquals(1000, thirdCommitUsedBytes - usedBytes);
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
    assertEquals(OK,
        omClientResponse.getOMResponse().getStatus());

    // Key should be present in both OpenKeyTable and KeyTable with HSync commit
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(
            omKeyCommitRequest.getBucketLayout()).get(openKey);
    if (isHSync) {
      assertNotNull(omKeyInfo);
    } else {
      // Key should not exist in OpenKeyTable anymore with non-HSync commit
      assertNull(omKeyInfo);
    }
    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);
    assertNotNull(omKeyInfo);

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

    assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    assertNull(omKeyInfo);
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

    assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    assertNull(omKeyInfo);
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

    assertEquals(OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED,
        omClientResponse.getOMResponse().getStatus());

    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));
    assertEquals(0, bucketInfo.getUsedNamespace());
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

    assertNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    assertNull(omKeyInfo);
  }

  @Test
  public void testValidateAndUpdateCacheOnOverwrite() throws Exception {
    testValidateAndUpdateCache();

    // This is used to generate the pseudo object ID for the suffix in deletedTable for uniqueness
    when(ozoneManager.getObjectIdFromTxId(anyLong())).thenAnswer(tx ->
        OmUtils.getObjectIdFromTxId(2, tx.getArgument(0)));

    // Become a new client and set next version number
    clientID = Time.now();
    version += 1;

    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest(getKeyLocation(10).subList(4, 10), false));

    OMKeyCommitRequest omKeyCommitRequest = getOmKeyCommitRequest(modifiedOmRequest);

    KeyArgs keyArgs = modifiedOmRequest.getCommitKeyRequest().getKeyArgs();

    String ozoneKey = getOzonePathKey();
    // Key should be there in key table, as validateAndUpdateCache is called.
    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout())
            .get(ozoneKey);

    assertNotNull(omKeyInfo);
    // Previously committed version
    assertEquals(0L, omKeyInfo.getLatestVersionLocations().getVersion());

    // Append new blocks
    List<OmKeyLocationInfo> allocatedLocationList =
        keyArgs.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());
    addKeyToOpenKeyTable(allocatedLocationList);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 102L);

    assertEquals(OK, omClientResponse.getOMResponse().getStatus());

    // New entry should be created in key Table.
    omKeyInfo = omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout()).get(ozoneKey);

    assertNotNull(omKeyInfo);
    assertEquals(version, omKeyInfo.getLatestVersionLocations().getVersion());
    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time
    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs().getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());

    assertEquals(locationInfoListFromCommitKeyRequest, omKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(allocatedLocationList, omKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(1, omKeyInfo.getKeyLocationVersions().size());

    // flush response content to db
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMKeyCommitResponse) omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // verify deleted key is unique generated
    String deletedKey = omMetadataManager.getOzoneKey(volumeName, omKeyInfo.getBucketName(), keyName);
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
        = omMetadataManager.getDeletedTable().getRangeKVs(null, 100, deletedKey);
    assertThat(rangeKVs.size()).isGreaterThan(0);
    Table.KeyValue<String, RepeatedOmKeyInfo> keyValue = rangeKVs.get(0);
    String key = keyValue.getKey();
    List<OmKeyInfo> omKeyInfoList = keyValue.getValue().getOmKeyInfoList();
    assertEquals(1, omKeyInfoList.size());
    assertThat(key).doesNotEndWith(String.valueOf(omKeyInfoList.get(0).getObjectID()));
  }

  @Test
  public void testValidateAndUpdateCacheOnOverwriteWithUncommittedBlocks() throws Exception {
    // Do a normal commit key (this will allocate 5 blocks
    testValidateAndUpdateCache();

    // This is used to generate the pseudo object ID for the suffix in deletedTable for uniqueness
    when(ozoneManager.getObjectIdFromTxId(anyLong())).thenAnswer(tx ->
        OmUtils.getObjectIdFromTxId(2, tx.getArgument(0)));

    // Prepare the key to overwrite
    // Become a new client and set next version number
    clientID = Time.now();
    version += 1;

    String ozoneKey = getOzonePathKey();
    // Previous key should be there in key table, as validateAndUpdateCache is called.
    OmKeyInfo originalKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout())
            .get(ozoneKey);

    assertNotNull(originalKeyInfo);
    // Previously committed version
    assertEquals(0L, originalKeyInfo.getLatestVersionLocations().getVersion());

    // Allocate the subsequent 5 blocks (this should not include the initial 5 blocks from the original key)
    List<KeyLocation> allocatedKeyLocationList = getKeyLocation(10).subList(5, 10);

    List<OmKeyLocationInfo> allocatedBlockList = allocatedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    // Commit only the first 3 allocated blocks (the other 2 blocks are uncommitted)
    List<KeyLocation> committedKeyLocationList = allocatedKeyLocationList.subList(0, 3);

    List<OmKeyLocationInfo> committedBlockList = committedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequest modifiedOmRequest = doPreExecute(createCommitKeyRequest(committedKeyLocationList, false));

    OMKeyCommitRequest omKeyCommitRequest = getOmKeyCommitRequest(modifiedOmRequest);

    addKeyToOpenKeyTable(allocatedBlockList);

    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 102L);

    assertEquals(OK, omClientResponse.getOMResponse().getStatus());

    // New entry should be created in key Table.
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(omKeyCommitRequest.getBucketLayout()).get(ozoneKey);

    assertNotNull(omKeyInfo);
    assertEquals(version, omKeyInfo.getLatestVersionLocations().getVersion());
    // DB keyInfo format
    verifyKeyName(omKeyInfo);

    // Check modification time
    CommitKeyRequest commitKeyRequest = modifiedOmRequest.getCommitKeyRequest();
    assertEquals(commitKeyRequest.getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    // Check block location.
    List<OmKeyLocationInfo> locationInfoListFromCommitKeyRequest =
        commitKeyRequest.getKeyArgs().getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());

    assertEquals(locationInfoListFromCommitKeyRequest, omKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(committedBlockList, omKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(1, omKeyInfo.getKeyLocationVersions().size());

    Map<String, RepeatedOmKeyInfo> toDeleteKeyList
        = ((OMKeyCommitResponse) omClientResponse).getKeysToDelete();

    // Both the overwritten key and the uncommitted blocks should be deleted
    // Both uses the same key (unlike MPU part commit request)
    assertEquals(1, toDeleteKeyList.size());
    List<OmKeyInfo> keysToDelete = toDeleteKeyList.values().stream()
        .findFirst().get().cloneOmKeyInfoList();
    assertEquals(2, keysToDelete.size());
    OmKeyInfo overwrittenKey = keysToDelete.get(0);
    OmKeyInfo uncommittedPseudoKey = keysToDelete.get(1);
    assertEquals(DEFAULT_COMMIT_BLOCK_SIZE, overwrittenKey.getLatestVersionLocations().getLocationList().size());
    assertEquals(allocatedKeyLocationList.size() - committedKeyLocationList.size(),
        uncommittedPseudoKey.getLatestVersionLocations().getLocationList().size());

    // flush response content to db
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMKeyCommitResponse) omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // verify deleted keys are stored in the deletedTable
    String deletedKey = omMetadataManager.getOzoneKey(volumeName, omKeyInfo.getBucketName(), keyName);
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
        = omMetadataManager.getDeletedTable().getRangeKVs(null, 100, deletedKey);
    assertThat(rangeKVs.size()).isGreaterThan(0);
    Table.KeyValue<String, RepeatedOmKeyInfo> keyValue = rangeKVs.get(0);
    String key = keyValue.getKey();
    List<OmKeyInfo> omKeyInfoList = keyValue.getValue().getOmKeyInfoList();
    assertEquals(2, omKeyInfoList.size());
    assertThat(key).doesNotEndWith(String.valueOf(omKeyInfoList.get(0).getObjectID()));
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

    assertTrue(modifiedOmRequest.hasCommitKeyRequest());
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
    assertThat(modifiedKeyArgs.getModificationTime()).isGreaterThan(0);
    assertEquals(0, originalKeyArgs.getModificationTime());

    assertEquals(originalKeyArgs.getVolumeName(),
        modifiedKeyArgs.getVolumeName());
    assertEquals(originalKeyArgs.getBucketName(),
        modifiedKeyArgs.getBucketName());
    assertEquals(originalKeyArgs.getKeyName(),
        modifiedKeyArgs.getKeyName());
    assertEquals(originalKeyArgs.getDataSize(),
        modifiedKeyArgs.getDataSize());
    assertEquals(originalKeyArgs.getKeyLocationsList(),
        modifiedKeyArgs.getKeyLocationsList());
    assertEquals(originalKeyArgs.getType(),
        modifiedKeyArgs.getType());
    assertEquals(originalKeyArgs.getFactor(),
        modifiedKeyArgs.getFactor());
  }

  private OMRequest createCommitKeyRequest() {
    return createCommitKeyRequest(false);
  }

  private OMRequest createCommitKeyRequest(boolean isHsync) {
    return createCommitKeyRequest(getKeyLocation(DEFAULT_COMMIT_BLOCK_SIZE), isHsync);
  }

  /**
   * Create OMRequest which encapsulates CommitKeyRequest.
   */
  private OMRequest createCommitKeyRequest(
      List<KeyLocation> keyLocations, boolean isHsync) {
    KeyArgs keyArgs =
        KeyArgs.newBuilder().setDataSize(dataSize).setVolumeName(volumeName)
            .setKeyName(keyName).setBucketName(bucketName)
            .setType(replicationConfig.getReplicationType())
            .setFactor(((RatisReplicationConfig) replicationConfig).getReplicationFactor())
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

  @Nonnull
  protected String getOzonePathKey() throws IOException {
    return omMetadataManager.getOzoneKey(volumeName, bucketName,
            keyName);
  }

  @Nonnull
  protected String addKeyToOpenKeyTable(List<OmKeyLocationInfo> locationList)
      throws Exception {
    OMRequestTestUtils.addKeyToTable(true, volumeName, bucketName, keyName,
        clientID, replicationConfig, omMetadataManager,
        locationList, version);

    return omMetadataManager.getOpenKey(volumeName, bucketName,
              keyName, clientID);
  }

  @Nonnull
  protected String addKeyToOpenKeyTable(List<OmKeyLocationInfo> locationList, OmKeyInfo.Builder keyInfoBuilder)
      throws Exception {
    OmKeyInfo keyInfo = keyInfoBuilder.build();
    keyInfo.appendNewBlocks(locationList, false);
    OMRequestTestUtils.addKeyToTable(true, false, keyInfo, clientID, 0, omMetadataManager);

    return omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, clientID);
  }

  @Nonnull
  protected OMKeyCommitRequest getOmKeyCommitRequest(OMRequest omRequest) {
    return new OMKeyCommitRequest(omRequest, BucketLayout.DEFAULT);
  }

  protected void verifyKeyName(OmKeyInfo omKeyInfo) {
    assertEquals(keyName, omKeyInfo.getKeyName(),
        "Incorrect KeyName");
    String fileName = OzoneFSUtils.getFileName(keyName);
    assertEquals(fileName, omKeyInfo.getFileName(),
        "Incorrect FileName");
  }
}
