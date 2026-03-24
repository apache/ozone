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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addVolumeAndBucketToDB;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createOmKeyInfo;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_A_FILE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.KeyValue;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.lock.OzoneLockProvider;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This class tests the OM Key Create Request.
 */
public class TestOMKeyCreateRequest extends TestOMKeyRequest {

  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{true, true},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{false, false});
  }

  @Test
  public void testPreExecuteWithNormalKey() throws Exception {
    ReplicationConfig ratis3Config =
        ReplicationConfig.fromProtoTypeAndFactor(RATIS, THREE);
    preExecuteTest(false, 0, ratis3Config);
  }

  @Test
  public void testPreExecuteWithECKey() throws Exception {
    ReplicationConfig ec3Plus2Config = new ECReplicationConfig("rs-3-2-1024k");
    preExecuteTest(false, 0, ec3Plus2Config);
  }

  @Test
  public void testPreExecuteWithMultipartKey() throws Exception {
    ReplicationConfig ratis3Config =
        ReplicationConfig.fromProtoTypeAndFactor(RATIS, THREE);
    preExecuteTest(true, 1, ratis3Config);
  }

  private void preExecuteTest(boolean isMultipartKey, int partNumber,
                              ReplicationConfig repConfig) throws Exception {
    long scmBlockSize = ozoneManager.getScmBlockSize();
    for (int i = 0; i <= repConfig.getRequiredNodes(); i++) {
      doPreExecute(createKeyRequest(isMultipartKey, partNumber,
          scmBlockSize * i, repConfig, null));
      doPreExecute(createKeyRequest(isMultipartKey, partNumber,
          scmBlockSize * i + 1, repConfig, null));
    }
  }

  @Test
  public void preExecuteRejectsInvalidReplication() {
    ECReplicationConfig invalidReplication = new ECReplicationConfig(1, 2);
    OMException e = assertThrows(OMException.class,
        () -> preExecuteTest(false, 0, invalidReplication));

    assertEquals(OMException.ResultCodes.INVALID_REQUEST, e.getResult());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testValidateAndUpdateCache(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");

    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0, emptyMap(), tags));

    OMKeyCreateRequest omKeyCreateRequest =
        getOMKeyCreateRequest(modifiedOmRequest);

    // Add volume and bucket entries to DB.
    addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();

    String openKey = getOpenKey(id);

    // Before calling
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(
                omKeyCreateRequest.getBucketLayout())
            .get(openKey);

    assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    checkResponse(modifiedOmRequest, omKeyCreateResponse, id, false,
        omKeyCreateRequest.getBucketLayout());

    // Network returns only latest version.
    assertEquals(1, omKeyCreateResponse.getOMResponse()
        .getCreateKeyResponse().getKeyInfo().getKeyLocationListCount());

    // Disk should have 1 version, as it is fresh key create.
    OmKeyInfo openKeyInfo = omMetadataManager.getOpenKeyTable(omKeyCreateRequest.getBucketLayout()).get(openKey);

    assertEquals(1, openKeyInfo.getKeyLocationVersions().size());
    assertThat(openKeyInfo.getTags()).containsAllEntriesOf(tags);

    // Write to DB like key commit.
    omMetadataManager.getKeyTable(omKeyCreateRequest.getBucketLayout())
        .put(getOzoneKey(), omMetadataManager
            .getOpenKeyTable(omKeyCreateRequest.getBucketLayout())
            .get(openKey));

    tags.remove("tag-key1");
    tags.remove("tag-key2");
    tags.put("tag-key3", "tag-value3");

    // Override same key again
    modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0, emptyMap(), tags));

    id = modifiedOmRequest.getCreateKeyRequest().getClientID();
    openKey = getOpenKey(id);

    // Before calling
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(
                omKeyCreateRequest.getBucketLayout())
            .get(openKey);
    assertNull(omKeyInfo);

    omKeyCreateRequest =
        getOMKeyCreateRequest(modifiedOmRequest);

    omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 101L);

    checkResponse(modifiedOmRequest, omKeyCreateResponse, id, true,
        omKeyCreateRequest.getBucketLayout());

    // Network returns only latest version
    assertEquals(1, omKeyCreateResponse.getOMResponse()
        .getCreateKeyResponse().getKeyInfo().getKeyLocationListCount());

    // Disk should have 1 versions when bucket versioning is off.
    assertEquals(1,
        omMetadataManager.getOpenKeyTable(
                omKeyCreateRequest.getBucketLayout())
            .get(openKey).getKeyLocationVersions().size());
    openKeyInfo = omMetadataManager.getOpenKeyTable(omKeyCreateRequest.getBucketLayout()).get(openKey);

    assertEquals(1, openKeyInfo.getKeyLocationVersions().size());
    assertThat(openKeyInfo.getTags()).containsAllEntriesOf(tags);
    assertThat(openKeyInfo.getTags()).doesNotContainKeys("tag-key1", "tag-key2");

  }

  @ParameterizedTest
  @MethodSource("data")
  public void testValidateAndUpdateCacheWithNamespaceQuotaExceeded(
      boolean setKeyPathLock, boolean setFileSystemPaths)throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));
    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0, "test/" + keyName));

    // test with FSO type
    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(
        modifiedOmRequest, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // create bucket with quota limit 1
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
            .setQuotaInNamespace(1));

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertSame(omKeyCreateResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED);
  }

  private void checkResponse(
      OMRequest modifiedOmRequest, OMClientResponse omKeyCreateResponse,
      long id, boolean override, BucketLayout bucketLayout) throws Exception {

    assertEquals(OK, omKeyCreateResponse.getOMResponse().getStatus());

    String openKey = getOpenKey(id);

    // Check open table whether key is added or not.

    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(bucketLayout).get(openKey);

    assertNotNull(omKeyInfo);
    assertNotNull(omKeyInfo.getLatestVersionLocations());

    List<OmKeyLocationInfo> omKeyLocationInfoList =
        omKeyInfo.getLatestVersionLocations().getLocationList();
    if (modifiedOmRequest.getCreateKeyRequest().getKeyArgs().getDataSize() > 0) {
      // As our data size is 100, and scmBlockSize is default to 1000, so we
      // shall have only one block.
      assertEquals(1, omKeyLocationInfoList.size());

      OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);
      // Check data of the block
      OzoneManagerProtocolProtos.KeyLocation keyLocation =
          modifiedOmRequest.getCreateKeyRequest().getKeyArgs().getKeyLocations(0);

      assertEquals(keyLocation.getBlockID().getContainerBlockID()
          .getContainerID(), omKeyLocationInfo.getContainerID());
      assertEquals(keyLocation.getBlockID().getContainerBlockID()
          .getLocalID(), omKeyLocationInfo.getLocalID());
    } else {
      // When creating an empty key, there should not be any blocks
      assertEquals(0, omKeyLocationInfoList.size());
    }


    // Check modification time
    assertEquals(modifiedOmRequest.getCreateKeyRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    if (!override) {
      assertEquals(omKeyInfo.getModificationTime(),
          omKeyInfo.getCreationTime());
    } else {
      assertNotEquals(omKeyInfo.getModificationTime(),
          omKeyInfo.getCreationTime());
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testValidateAndUpdateCacheWithNoSuchMultipartUploadError(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));
    int partNumber = 1;
    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(true, partNumber));

    OMKeyCreateRequest omKeyCreateRequest =
        getOMKeyCreateRequest(modifiedOmRequest);

    // Add volume and bucket entries to DB.
    addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);

    // Before calling
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omKeyCreateResponse.getOMResponse().getStatus());

    // As we got error, no entry should be created in openKeyTable.

    omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    assertNull(omKeyInfo);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testValidateAndUpdateCacheWithVolumeNotFound(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));
    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0));

    OMKeyCreateRequest omKeyCreateRequest =
        getOMKeyCreateRequest(modifiedOmRequest);


    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);


    // Before calling
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omKeyCreateResponse.getOMResponse().getStatus());


    // As We got an error, openKey Table should not have entry.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    assertNull(omKeyInfo);

  }

  @ParameterizedTest
  @MethodSource("data")
  public void testValidateAndUpdateCacheWithBucketNotFound(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));
    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(
            false, 0));

    OMKeyCreateRequest omKeyCreateRequest =
        getOMKeyCreateRequest(modifiedOmRequest);


    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();

    String openKey = getOpenKey(id);

    OMRequestTestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE,
        omMetadataManager);

    // Before calling
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omKeyCreateResponse.getOMResponse().getStatus());


    // As We got an error, openKey Table should not have entry.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    assertNull(omKeyInfo);

  }

  @ParameterizedTest
  @MethodSource("data")
  public void testValidateAndUpdateCacheWithInvalidPath(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    PrefixManager prefixManager = new PrefixManagerImpl(ozoneManager,
        ozoneManager.getMetadataManager(), true);
    when(ozoneManager.getPrefixManager()).thenReturn(prefixManager);
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));
    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(
            false, 0, String.valueOf('\u0000')));

    OMKeyCreateRequest omKeyCreateRequest =
        getOMKeyCreateRequest(modifiedOmRequest);


    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();

    String openKey = getOpenKey(id);

    // Add volume and bucket entries to DB.
    addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    // Before calling
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.INVALID_PATH,
        omKeyCreateResponse.getOMResponse().getStatus());



    // As We got an error, openKey Table should not have entry.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    assertNull(omKeyInfo);

  }

  @ParameterizedTest
  @MethodSource("data")
  public void testOverwritingExistingMetadata(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));

    addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager,
        getBucketLayout());

    Map<String, String> initialMetadata =
        Collections.singletonMap("initialKey", "initialValue");
    OMRequest initialRequest =
        createKeyRequest(false, 0, keyName, initialMetadata);
    OMKeyCreateRequest initialOmKeyCreateRequest =
        new OMKeyCreateRequest(initialRequest, getBucketLayout());
    initialOmKeyCreateRequest.setUGI(UserGroupInformation.getCurrentUser());
    OMClientResponse initialResponse =
        initialOmKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    verifyMetadataInResponse(initialResponse, initialMetadata);

    // We have to add the key to the key table, as validateAndUpdateCache only
    // updates the cache and not the DB.
    OmKeyInfo keyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationConfig).setMetadata(initialMetadata).build();
    omMetadataManager.getKeyTable(initialOmKeyCreateRequest.getBucketLayout())
        .put(getOzoneKey(), keyInfo);

    Map<String, String> updatedMetadata =
        Collections.singletonMap("initialKey", "updatedValue");
    OMRequest updatedRequest =
        createKeyRequest(false, 0, keyName, updatedMetadata);
    OMKeyCreateRequest updatedOmKeyCreateRequest =
        new OMKeyCreateRequest(updatedRequest, getBucketLayout());
    updatedOmKeyCreateRequest.setUGI(UserGroupInformation.getCurrentUser());

    OMClientResponse updatedResponse =
        updatedOmKeyCreateRequest.validateAndUpdateCache(ozoneManager, 101L);
    verifyMetadataInResponse(updatedResponse, updatedMetadata);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCreationWithoutMetadataFollowedByOverwriteWithMetadata(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));
    addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager,
        getBucketLayout());

    // Create the key request without any initial metadata
    OMRequest createRequestWithoutMetadata = createKeyRequest(false, 0, keyName,
        null, emptyMap(), emptyList()); // Passing 'null' for metadata
    OMKeyCreateRequest createOmKeyCreateRequest =
        new OMKeyCreateRequest(createRequestWithoutMetadata, getBucketLayout());

    // Perform the create operation without any metadata
    OMClientResponse createResponse =
        createOmKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    // Verify that no metadata exists in the response
    assertThat(
        createResponse.getOMResponse().getCreateKeyResponse().getKeyInfo()
            .getMetadataList()).isEmpty();

    OmKeyInfo keyInfo = createOmKeyInfo(volumeName, bucketName, keyName,
        replicationConfig).build();
    omMetadataManager.getKeyTable(createOmKeyCreateRequest.getBucketLayout())
        .put(getOzoneKey(), keyInfo);

    // Define new metadata for the overwrite operation
    Map<String, String> overwriteMetadata = new HashMap<>();
    overwriteMetadata.put("newKey", "newValue");

    // Overwrite the previously created key with new metadata
    OMRequest overwriteRequestWithMetadata =
        createKeyRequest(false, 0, keyName, overwriteMetadata, emptyMap(), emptyList());
    OMKeyCreateRequest overwriteOmKeyCreateRequest =
        new OMKeyCreateRequest(overwriteRequestWithMetadata, getBucketLayout());
    overwriteOmKeyCreateRequest.setUGI(UserGroupInformation.getCurrentUser());

    // Perform the overwrite operation and capture the response
    OMClientResponse overwriteResponse =
        overwriteOmKeyCreateRequest.validateAndUpdateCache(ozoneManager, 101L);
    // Verify the new metadata is correctly applied in the response
    verifyMetadataInResponse(overwriteResponse, overwriteMetadata);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testIgnoreClientACL(boolean ignoreClientACLs) throws Exception {
    ozoneManager.getConfig().setIgnoreClientACLs(ignoreClientACLs);
    when(ozoneManager.getOzoneLockProvider()).thenReturn(new OzoneLockProvider(true, true));
    addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    // create file
    String ozoneAll = "user:ozone:a";
    List<OzoneAcl> aclList = new ArrayList<>();
    aclList.add(OzoneAcl.parseAcl(ozoneAll));
    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0, keyName, emptyMap(), emptyMap(), aclList));
    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(modifiedOmRequest);
    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();
    String openKey = getOpenKey(id);
    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    checkResponse(modifiedOmRequest, omKeyCreateResponse, id, false,
        omKeyCreateRequest.getBucketLayout());

    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);
    if (ignoreClientACLs) {
      assertFalse(omKeyInfo.getAcls().contains(OzoneAcl.parseAcl(ozoneAll)));
    } else {
      assertTrue(omKeyInfo.getAcls().contains(OzoneAcl.parseAcl(ozoneAll)));
    }
  }

  private void verifyMetadataInResponse(OMClientResponse response,
                                        Map<String, String> expectedMetadata) {
    // Extract metadata from the response
    List<KeyValue> metadataList =
        response.getOMResponse().getCreateKeyResponse().getKeyInfo()
            .getMetadataList();
    assertEquals(expectedMetadata.size(), metadataList.size());
    metadataList.forEach(kv -> {
      String expectedValue = expectedMetadata.get(kv.getKey());
      assertEquals(expectedValue, kv.getValue(),
          "Metadata value mismatch for key: " + kv.getKey());
    });
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOMRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {

    OMKeyCreateRequest omKeyCreateRequest =
        getOMKeyCreateRequest(originalOMRequest);

    OMRequest modifiedOmRequest =
        omKeyCreateRequest.preExecute(ozoneManager);

    assertEquals(originalOMRequest.getCmdType(), modifiedOmRequest.getCmdType());
    assertEquals(originalOMRequest.getClientId(), modifiedOmRequest.getClientId());

    assertTrue(modifiedOmRequest.hasCreateKeyRequest());

    CreateKeyRequest createKeyRequest =
        modifiedOmRequest.getCreateKeyRequest();

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    int dataGroupSize = keyArgs.hasEcReplicationConfig() ?
        keyArgs.getEcReplicationConfig().getData() : 1;
    long blockSize = ozoneManager.getScmBlockSize();
    long preAllocatedBlocks = Math.min(ozoneManager.getPreallocateBlocksMax(),
        keyArgs.getDataSize() > 0 ?
            (keyArgs.getDataSize() - 1) / (blockSize * dataGroupSize) + 1 : 0);

    // Time should be set
    assertThat(keyArgs.getModificationTime()).isGreaterThan(0);


    // Client ID should be set.
    assertTrue(createKeyRequest.hasClientID());
    assertThat(createKeyRequest.getClientID()).isGreaterThan(0);


    if (!originalOMRequest.getCreateKeyRequest().getKeyArgs()
        .getIsMultipartKey()) {

      List<OzoneManagerProtocolProtos.KeyLocation> keyLocations =
          keyArgs.getKeyLocationsList();
      // KeyLocation should be set.
      assertEquals(preAllocatedBlocks, keyLocations.size());
      if (preAllocatedBlocks > 0) {
        assertEquals(CONTAINER_ID,
            keyLocations.get(0).getBlockID().getContainerBlockID()
                .getContainerID());
        assertEquals(LOCAL_ID,
            keyLocations.get(0).getBlockID().getContainerBlockID()
                .getLocalID());
        assertTrue(keyLocations.get(0).hasPipeline());

        assertEquals(0, keyLocations.get(0).getOffset());

        assertEquals(scmBlockSize, keyLocations.get(0).getLength());
      }
    } else {
      // We don't create blocks for multipart key in createKey preExecute.
      assertEquals(0, keyArgs.getKeyLocationsList().size());
    }

    return modifiedOmRequest;

  }

  /**
   * Create OMRequest which encapsulates CreateKeyRequest.
   * @param isMultipartKey
   * @param partNumber
   * @return OMRequest.
   */

  @SuppressWarnings("parameterNumber")
  protected OMRequest createKeyRequest(boolean isMultipartKey, int partNumber) {
    return createKeyRequest(isMultipartKey, partNumber, emptyMap(), emptyMap());
  }

  protected OMRequest createKeyRequest(boolean isMultipartKey, int partNumber,
                                       Map<String, String> metadata, Map<String, String> tags) {
    return createKeyRequest(isMultipartKey, partNumber, keyName, metadata, tags, emptyList());
  }

  private OMRequest createKeyRequest(boolean isMultipartKey, int partNumber,
                                     String keyName) {
    return createKeyRequest(isMultipartKey, partNumber, keyName, emptyMap());
  }

  protected OMRequest createKeyRequest(boolean isMultipartKey, int partNumber,
                                       String keyName,
                                       Map<String, String> metadata) {
    return createKeyRequest(isMultipartKey, partNumber, keyName, metadata, emptyMap(), emptyList());
  }

  /**
   * Create OMRequest which encapsulates a CreateKeyRequest, optionally
   * with metadata.
   *
   * @param isMultipartKey Indicates if the key is part of a multipart upload.
   * @param partNumber     The part number for multipart uploads, ignored if
   *                       isMultipartKey is false.
   * @param keyName        The name of the key to create or update.
   * @param metadata       Optional metadata for the key. Pass null or an empty
   *                       map if no metadata is to be set.
   * @param tags           Optional tags for the key. Pass null or an empty
   *                       map if no tags is to be set.
   * @return OMRequest configured with the provided parameters.
   */
  protected OMRequest createKeyRequest(boolean isMultipartKey, int partNumber,
                                       String keyName,
                                       Map<String, String> metadata,
                                       Map<String, String> tags,
                                       List<OzoneAcl> acls) {
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setIsMultipartKey(isMultipartKey)
        .setFactor(
            ((RatisReplicationConfig) replicationConfig).getReplicationFactor())
        .setType(replicationConfig.getReplicationType())
        .setLatestVersionLocation(true);

    for (OzoneAcl acl : acls) {
      keyArgs.addAcls(OzoneAcl.toProtobuf(acl));
    }
    // Configure for multipart upload, if applicable
    if (isMultipartKey) {
      keyArgs.setDataSize(dataSize).setMultipartNumber(partNumber);
    }

    // Include metadata, if provided
    if (metadata != null && !metadata.isEmpty()) {
      metadata.forEach((key, value) -> keyArgs.addMetadata(KeyValue.newBuilder()
          .setKey(key)
          .setValue(value)
          .build()));
    }

    if (tags != null && !tags.isEmpty()) {
      keyArgs.addAllTags(KeyValueUtil.toProtobuf(tags));
    }

    OzoneManagerProtocolProtos.CreateKeyRequest createKeyRequest =
        CreateKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .setClientId(UUID.randomUUID().toString())
        .setCreateKeyRequest(createKeyRequest)
        .build();
  }

  private OMRequest createKeyRequest(
      boolean isMultipartKey, int partNumber, long keyLength,
      ReplicationConfig repConfig, Long expectedDataGeneration) {
    return createKeyRequest(isMultipartKey, partNumber, keyLength, repConfig,
        expectedDataGeneration, null);
  }

  private OMRequest createKeyRequest(
      boolean isMultipartKey, int partNumber, long keyLength,
      ReplicationConfig repConfig, Long expectedDataGeneration, Map<String, String> metaData) {

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setIsMultipartKey(isMultipartKey)
        .setType(repConfig.getReplicationType())
        .setLatestVersionLocation(true)
        .setDataSize(keyLength);

    if (repConfig.getReplicationType() == EC) {
      keyArgs.setEcReplicationConfig(
          ((ECReplicationConfig) repConfig).toProto());
    } else {
      keyArgs.setFactor(ReplicationConfig.getLegacyFactor(repConfig));
    }

    if (isMultipartKey) {
      keyArgs.setMultipartNumber(partNumber);
    }
    if (expectedDataGeneration != null) {
      keyArgs.setExpectedDataGeneration(expectedDataGeneration);
    }
    if (metaData != null) {
      metaData.forEach((key, value) -> keyArgs.addMetadata(KeyValue.newBuilder()
          .setKey(key)
          .setValue(value)
          .build()));
    }

    CreateKeyRequest createKeyRequest =
        CreateKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .setClientId(UUID.randomUUID().toString())
        .setCreateKeyRequest(createKeyRequest).build();
  }

  @ParameterizedTest
  @ValueSource(booleans =  {true, false})
  public void testKeyCreateWithFileSystemPathsEnabled(
      boolean setKeyPathLock) throws Exception {
    OzoneConfiguration configuration = getOzoneConfiguration();
    configuration.setBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    when(ozoneManager.getConfiguration()).thenReturn(configuration);
    when(ozoneManager.getConfig()).thenReturn(configuration.getObject(OmConfig.class));
    when(ozoneManager.getEnableFileSystemPaths()).thenReturn(true);
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, true));

    // Add volume and bucket entries to DB.
    addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    String keyName = "dir1/dir2/dir3/file1";
    createAndCheck(keyName);

    // Key with leading '/'.
    keyName = "/a/b/c/file1";
    createAndCheck(keyName);

    // Commit openKey entry.
    addToKeyTable(keyName);

    // Now create another file in same dir path.
    keyName = "/a/b/c/file2";
    createAndCheck(keyName);

    // Create key with multiple /'s
    // converted to a/b/c/file5
    keyName = "///a/b///c///file5";
    createAndCheck(keyName);

    // converted to a/b/c/.../file3
    keyName = "///a/b///c//.../file3";
    createAndCheck(keyName);

    // converted to r1/r2
    keyName = "././r1/r2/";
    createAndCheck(keyName);

    // converted to ..d1/d2/d3
    keyName = "..d1/d2/d3/";
    createAndCheck(keyName);

    // Create a file, where a file already exists in the path.
    // Now try with a file exists in path. Should fail.
    keyName = "/a/b/c/file1/file3";
    checkNotAFile(keyName);

    // Empty keyName.
    keyName = "";
    checkNotAValidPath(keyName);

    // Key name ends with /
    keyName = "/a/./";
    checkNotAValidPath(keyName);

    keyName = "/////";
    checkNotAValidPath(keyName);

    keyName = "../../b/c";
    checkNotAValidPath(keyName);

    keyName = "../../b/c/";
    checkNotAValidPath(keyName);

    keyName = "../../b:/c/";
    checkNotAValidPath(keyName);

    keyName = ":/c/";
    checkNotAValidPath(keyName);

    keyName = "";
    checkNotAValidPath(keyName);

    keyName = "../a/b";
    checkNotAValidPath(keyName);

    keyName = "/../a/b";
    checkNotAValidPath(keyName);

  }

  @Test
  public void testPreExecuteWithInvalidKeyPrefix() throws Exception {
    Map<String, String> invalidKeyScenarios = new HashMap<String, String>() {
      {
        put(OM_SNAPSHOT_INDICATOR + "/" + keyName,
            "Cannot create key under path reserved for snapshot: "
                + OM_SNAPSHOT_INDICATOR + OM_KEY_PREFIX);
        put(OM_SNAPSHOT_INDICATOR + "/a/" + keyName,
            "Cannot create key under path reserved for snapshot: "
                + OM_SNAPSHOT_INDICATOR + OM_KEY_PREFIX);
        put(OM_SNAPSHOT_INDICATOR + "/a/b" + keyName,
            "Cannot create key under path reserved for snapshot: "
                + OM_SNAPSHOT_INDICATOR + OM_KEY_PREFIX);
        put(OM_SNAPSHOT_INDICATOR,
            "Cannot create key with reserved name: " + OM_SNAPSHOT_INDICATOR);
      }
    };

    for (Map.Entry<String, String> entry : invalidKeyScenarios.entrySet()) {
      String invalidKeyName = entry.getKey();
      String expectedErrorMessage = entry.getValue();

      KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
          .setVolumeName(volumeName).setBucketName(bucketName)
          .setKeyName(invalidKeyName);

      OzoneManagerProtocolProtos.CreateKeyRequest createKeyRequest =
          CreateKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
          .setClientId(UUID.randomUUID().toString())
          .setCreateKeyRequest(createKeyRequest).build();

      OMException ex = assertThrows(OMException.class,
          () -> getOMKeyCreateRequest(omRequest).preExecute(ozoneManager)
      );
      assertThat(ex.getMessage()).contains(expectedErrorMessage);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testKeyCreateInheritParentDefaultAcls(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));

    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(OzoneAcl.parseAcl("user:newUser:rw[DEFAULT]"));
    acls.add(OzoneAcl.parseAcl("user:noInherit:rw"));
    acls.add(OzoneAcl.parseAcl("group:newGroup:rwl[DEFAULT]"));

    // create bucket with DEFAULT acls
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setAcls(acls));

    // Verify bucket has DEFAULT acls.
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    List<OzoneAcl> bucketAcls = omMetadataManager.getBucketTable()
        .get(bucketKey).getAcls();
    assertEquals(acls, bucketAcls);

    // create file inherit bucket DEFAULT acls
    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0));

    OMKeyCreateRequest omKeyCreateRequest =
        getOMKeyCreateRequest(modifiedOmRequest);

    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();
    String openKey = getOpenKey(id);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    checkResponse(modifiedOmRequest, omKeyCreateResponse, id, false,
        omKeyCreateRequest.getBucketLayout());

    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    verifyKeyInheritAcls(omKeyInfo.getAcls(), bucketAcls);

  }

  @ParameterizedTest
  @MethodSource("data")
  public void testAtomicRewrite(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout()));

    // First, create a key with the rewrite ID - this should fail as no key exists
    OMRequest omRequest = createKeyRequest(false, 0, 100,
        RatisReplicationConfig.getInstance(THREE), 1L);
    omRequest = doPreExecute(omRequest);
    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);
    OMClientResponse response = omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 105L);
    assertEquals(KEY_NOT_FOUND, response.getOMResponse().getStatus());

    // Now pre-create the key in the system so we can rewrite it.
    Map<String, String> metadata = Collections.singletonMap("metakey", "metavalue");
    Map<String, String> reWriteMetadata = Collections.singletonMap("metakey", "rewriteMetavalue");

    List<OzoneAcl> acls = Collections.singletonList(OzoneAcl.parseAcl("user:foo:rw"));
    OmKeyInfo createdKeyInfo = createAndCheck(keyName, metadata, acls);
    // Commit openKey entry.
    omMetadataManager.getKeyTable(getBucketLayout()).put(getOzoneKey(), createdKeyInfo);

    // Retrieve the committed key info
    OmKeyInfo existingKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(getOzoneKey());
    List<OzoneAcl> existingAcls = existingKeyInfo.getAcls();
    assertThat(existingAcls.containsAll(acls));

    // Create a request with a generation which doesn't match the current key
    omRequest = createKeyRequest(false, 0, 100,
        RatisReplicationConfig.getInstance(THREE), existingKeyInfo.getGeneration() + 1, reWriteMetadata);
    omRequest = doPreExecute(omRequest);
    omKeyCreateRequest = getOMKeyCreateRequest(omRequest);
    response = omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 105L);
    // Still fails, as the matching key is not present.
    assertEquals(KEY_NOT_FOUND, response.getOMResponse().getStatus());

    // Now create the key with the correct rewrite generation
    omRequest = createKeyRequest(false, 0, 100,
        RatisReplicationConfig.getInstance(THREE), existingKeyInfo.getGeneration(), reWriteMetadata);
    omRequest = doPreExecute(omRequest);
    omKeyCreateRequest = getOMKeyCreateRequest(omRequest);
    response = omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 105L);
    assertEquals(OK, response.getOMResponse().getStatus());

    OmKeyInfo openKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(getOpenKey(omRequest.getCreateKeyRequest().getClientID()));

    assertEquals(existingKeyInfo.getGeneration(), openKeyInfo.getExpectedDataGeneration());
    // Creation time should remain the same on rewrite.
    assertEquals(existingKeyInfo.getCreationTime(), openKeyInfo.getCreationTime());
    // Update ID should change
    assertNotEquals(existingKeyInfo.getGeneration(), openKeyInfo.getGeneration());
    assertEquals(metadata, existingKeyInfo.getMetadata());
    // The metadata should not be copied from the existing key. It should be passed in the request.
    assertEquals(reWriteMetadata, openKeyInfo.getMetadata());
    // Ensure the ACLS are copied over from the existing key.
    assertEquals(existingAcls, openKeyInfo.getAcls());
  }

  /**
   * Leaf file has ACCESS scope acls which inherited
   * from parent DEFAULT acls.
   */
  private void verifyKeyInheritAcls(List<OzoneAcl> keyAcls,
                                    List<OzoneAcl> bucketAcls) {

    List<OzoneAcl> parentDefaultAcl = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.DEFAULT)
        .collect(Collectors.toList());

    OzoneAcl parentAccessAcl = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.ACCESS)
        .findAny().orElse(null);

    // Should inherit parent DEFAULT Acls
    assertTrue(keyAcls.containsAll(parentDefaultAcl.stream()
            .map(acl -> acl.withScope(OzoneAcl.AclScope.ACCESS))
            .collect(Collectors.toList())),
        "Failed to inherit parent DEFAULT acls!,");

    // Should not inherit parent ACCESS Acls
    assertThat(keyAcls).doesNotContain(parentAccessAcl);
  }

  /**
   * Test that SCM's allocateBlock is not called when creating an empty key.
   */
  @Test
  public void testEmptyKeyKeyDoesNotCallScmAllocateBlock() throws Exception {
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setIsMultipartKey(false)
        .setFactor(
            ((RatisReplicationConfig) replicationConfig).getReplicationFactor())
        .setType(replicationConfig.getReplicationType())
        .setLatestVersionLocation(true)
        .setDataSize(0); // explicitly set data size to 0

    CreateKeyRequest createKeyRequest =
        CreateKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .setClientId(UUID.randomUUID().toString())
        .setCreateKeyRequest(createKeyRequest).build();

    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omKeyCreateRequest.preExecute(ozoneManager);

    // Verify that SCM's allocateBlock was never called
    verify(scmBlockLocationProtocol, never())
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class), anyString(),
            any(ExcludeList.class), anyString());

    verify(scmBlockLocationProtocol, never())
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class), anyString(),
            any(ExcludeList.class), anyString());

    assertTrue(modifiedOmRequest.hasCreateKeyRequest());
    CreateKeyRequest responseCreateKeyRequest =
        modifiedOmRequest.getCreateKeyRequest();
    assertEquals(0,
        responseCreateKeyRequest.getKeyArgs().getKeyLocationsCount(),
        "Empty key should have no key locations");

    assertEquals(0,
        responseCreateKeyRequest.getKeyArgs().getDataSize(),
        "Empty key should have dataSize of 0");
  }

  /**
   * Test that SCM's allocateBlock is not called when creating a key without specifying the dataSize.
   */
  @Test
  public void testKeyWithoutDataSizeCallsScmAllocateBlock() throws Exception {
    // Setup mock to return valid blocks
    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(
                org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE))
        .setNodes(new ArrayList<>())
        .build();

    AllocatedBlock.Builder blockBuilder = new AllocatedBlock.Builder()
        .setPipeline(pipeline)
        .setContainerBlockID(new ContainerBlockID(CONTAINER_ID, LOCAL_ID));

    when(scmBlockLocationProtocol.allocateBlock(
            anyLong(), anyInt(),
            any(ReplicationConfig.class), anyString(),
            any(ExcludeList.class), anyString()))
        .thenAnswer(invocation -> {
          int num = invocation.getArgument(1);
          List<AllocatedBlock> allocatedBlocks = new ArrayList<>(num);
          for (int i = 0; i < num; i++) {
            blockBuilder.setContainerBlockID(
                new ContainerBlockID(CONTAINER_ID + i, LOCAL_ID + i));
            allocatedBlocks.add(blockBuilder.build());
          }
          return allocatedBlocks;
        });

    // Create a key request without setting dataSize (should default to scmBlockSize)
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setIsMultipartKey(false)
        .setFactor(
            ((RatisReplicationConfig) replicationConfig).getReplicationFactor())
        .setType(replicationConfig.getReplicationType())
        .setLatestVersionLocation(true);

    CreateKeyRequest createKeyRequest =
        CreateKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .setClientId(UUID.randomUUID().toString())
        .setCreateKeyRequest(createKeyRequest).build();

    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omKeyCreateRequest.preExecute(ozoneManager);

    verify(scmBlockLocationProtocol, never())
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class), anyString(),
            any(ExcludeList.class), anyString());

    verify(scmBlockLocationProtocol, never())
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class), anyString(),
            any(ExcludeList.class), anyString());

    assertTrue(modifiedOmRequest.hasCreateKeyRequest());
    CreateKeyRequest responseCreateKeyRequest =
        modifiedOmRequest.getCreateKeyRequest();
    assertEquals(0,
        responseCreateKeyRequest.getKeyArgs().getKeyLocationsCount(),
        "Empty key should have no key locations");

    assertEquals(0,
        responseCreateKeyRequest.getKeyArgs().getDataSize(),
        "Empty key should have dataSize of 0");
  }

  protected void addToKeyTable(String keyName) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        keyName.substring(1), 0L, RatisReplicationConfig.getInstance(THREE), omMetadataManager);
  }

  private void checkNotAValidPath(String keyName) throws IOException {
    OMRequest omRequest = createKeyRequest(false, 0, keyName);
    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);
    OMException ex =
        assertThrows(OMException.class, () -> omKeyCreateRequest.preExecute(ozoneManager),
            "checkNotAValidPath failed for path" + keyName);
    assertEquals(OMException.ResultCodes.INVALID_KEY_NAME,
        ex.getResult());
  }

  private void checkNotAFile(String keyName) throws Exception {
    OMRequest omRequest = createKeyRequest(false, 0, keyName);

    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    omRequest = omKeyCreateRequest.preExecute(ozoneManager);

    omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    OMClientResponse omClientResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 101L);

    assertEquals(NOT_A_FILE, omClientResponse.getOMResponse().getStatus());
  }

  private void createAndCheck(String keyName) throws Exception {
    createAndCheck(keyName, emptyMap(), emptyList());
  }

  private OmKeyInfo createAndCheck(String keyName, Map<String, String> metadata, List<OzoneAcl> acls)
      throws Exception {
    OMRequest omRequest = createKeyRequest(false, 0, keyName, metadata, emptyMap(), acls);

    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    omRequest = omKeyCreateRequest.preExecute(ozoneManager);

    omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    OMClientResponse omClientResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 101L);

    assertEquals(OK, omClientResponse.getOMResponse().getStatus());

    return checkCreatedPaths(omKeyCreateRequest, omRequest, keyName);
  }

  protected OmKeyInfo checkCreatedPaths(
      OMKeyCreateRequest omKeyCreateRequest, OMRequest omRequest,
      String keyName) throws Exception {
    keyName = omKeyCreateRequest.validateAndNormalizeKey(true, keyName);

    // Check open key entry
    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, omRequest.getCreateKeyRequest().getClientID());
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCreateRequest.getBucketLayout())
            .get(openKey);
    assertNotNull(omKeyInfo);
    return omKeyInfo;
  }

  protected long checkIntermediatePaths(Path keyPath) throws Exception {
    // Check intermediate paths are created
    keyPath = keyPath.getParent();
    while (keyPath != null) {
      assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
          .get(omMetadataManager.getOzoneDirKey(
              volumeName, bucketName, keyPath.toString())));
      keyPath = keyPath.getParent();
    }
    return -1;
  }

  protected String getOpenKey(long id) throws IOException {
    return omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);
  }

  protected String getOzoneKey() throws IOException {
    return omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
  }

  protected OMKeyCreateRequest getOMKeyCreateRequest(OMRequest omRequest) throws IOException {
    OMKeyCreateRequest request = new OMKeyCreateRequest(omRequest, getBucketLayout());
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

  protected OMKeyCreateRequest getOMKeyCreateRequest(
      OMRequest omRequest, BucketLayout layout) throws IOException {
    OMKeyCreateRequest request = new OMKeyCreateRequest(omRequest, layout);
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }
}
