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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.lock.OzoneLockProvider;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addVolumeAndBucketToDB;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_A_FILE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

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
          scmBlockSize * i, repConfig));
      doPreExecute(createKeyRequest(isMultipartKey, partNumber,
          scmBlockSize * i + 1, repConfig));
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

    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0));

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
    assertEquals(1,
        omMetadataManager.getOpenKeyTable(
                omKeyCreateRequest.getBucketLayout())
            .get(openKey).getKeyLocationVersions().size());

    // Write to DB like key commit.
    omMetadataManager.getKeyTable(omKeyCreateRequest.getBucketLayout())
        .put(getOzoneKey(), omMetadataManager
            .getOpenKeyTable(omKeyCreateRequest.getBucketLayout())
            .get(openKey));

    // Override same key again
    modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0));

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

    // As our data size is 100, and scmBlockSize is default to 1000, so we
    // shall have only one block.
    List<OmKeyLocationInfo> omKeyLocationInfoList =
        omKeyInfo.getLatestVersionLocations().getLocationList();
    assertEquals(1, omKeyLocationInfoList.size());

    OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

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

    // Check data of the block
    OzoneManagerProtocolProtos.KeyLocation keyLocation =
        modifiedOmRequest.getCreateKeyRequest().getKeyArgs().getKeyLocations(0);

    assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omKeyLocationInfo.getContainerID());
    assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getLocalID(), omKeyLocationInfo.getLocalID());
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
    PrefixManager prefixManager = new PrefixManagerImpl(
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
        (keyArgs.getDataSize() - 1) / (blockSize * dataGroupSize) + 1);

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
      assertEquals(CONTAINER_ID,
          keyLocations.get(0).getBlockID().getContainerBlockID()
              .getContainerID());
      assertEquals(LOCAL_ID,
          keyLocations.get(0).getBlockID().getContainerBlockID()
              .getLocalID());
      assertTrue(keyLocations.get(0).hasPipeline());

      assertEquals(0, keyLocations.get(0).getOffset());

      assertEquals(scmBlockSize, keyLocations.get(0).getLength());
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
    return createKeyRequest(isMultipartKey, partNumber, keyName);
  }

  private OMRequest createKeyRequest(boolean isMultipartKey, int partNumber,
                                     String keyName) {

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setIsMultipartKey(isMultipartKey)
        .setFactor(replicationFactor).setType(replicationType)
        .setLatestVersionLocation(true);

    if (isMultipartKey) {
      keyArgs.setDataSize(dataSize).setMultipartNumber(partNumber);
    }

    OzoneManagerProtocolProtos.CreateKeyRequest createKeyRequest =
        CreateKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .setClientId(UUID.randomUUID().toString())
        .setCreateKeyRequest(createKeyRequest).build();
  }

  private OMRequest createKeyRequest(
      boolean isMultipartKey, int partNumber, long keyLength,
      ReplicationConfig repConfig) {

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

    OzoneManagerProtocolProtos.CreateKeyRequest createKeyRequest =
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
    assertEquals(parentDefaultAcl.stream()
            .map(acl -> acl.setAclScope(OzoneAcl.AclScope.ACCESS))
            .collect(Collectors.toList()), keyAcls,
        "Failed to inherit parent DEFAULT acls!,");

    // Should not inherit parent ACCESS Acls
    assertThat(keyAcls).doesNotContain(parentAccessAcl);
  }

  protected void addToKeyTable(String keyName) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        keyName.substring(1), 0L, RATIS, THREE, omMetadataManager);
  }


  private void checkNotAValidPath(String keyName) {
    OMRequest omRequest = createKeyRequest(false, 0, keyName);
    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    try {
      omKeyCreateRequest.preExecute(ozoneManager);
      fail("checkNotAValidPath failed for path" + keyName);
    } catch (IOException ex) {
      OMException omException = assertInstanceOf(OMException.class, ex);
      assertEquals(OMException.ResultCodes.INVALID_KEY_NAME,
          omException.getResult());
    }


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
    OMRequest omRequest = createKeyRequest(false, 0, keyName);

    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    omRequest = omKeyCreateRequest.preExecute(ozoneManager);

    omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    OMClientResponse omClientResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 101L);

    assertEquals(OK, omClientResponse.getOMResponse().getStatus());

    checkCreatedPaths(omKeyCreateRequest, omRequest, keyName);
  }

  protected void checkCreatedPaths(
      OMKeyCreateRequest omKeyCreateRequest, OMRequest omRequest,
      String keyName) throws Exception {
    keyName = omKeyCreateRequest.validateAndNormalizeKey(true, keyName);
    // Check intermediate directories created or not.
    Path keyPath = Paths.get(keyName);
    checkIntermediatePaths(keyPath);

    // Check open key entry
    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, omRequest.getCreateKeyRequest().getClientID());
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCreateRequest.getBucketLayout())
            .get(openKey);
    assertNotNull(omKeyInfo);
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

  protected OMKeyCreateRequest getOMKeyCreateRequest(OMRequest omRequest) {
    return new OMKeyCreateRequest(omRequest, BucketLayout.DEFAULT);
  }

  protected OMKeyCreateRequest getOMKeyCreateRequest(
      OMRequest omRequest, BucketLayout layout) {
    return new OMKeyCreateRequest(omRequest, layout);
  }

}
