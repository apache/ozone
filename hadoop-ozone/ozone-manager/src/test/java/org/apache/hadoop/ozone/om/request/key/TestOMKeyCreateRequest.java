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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.lock.OzoneLockProvider;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.Assert;
import org.junit.Test;

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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addVolumeAndBucketToDB;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_A_FILE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Tests OMCreateKeyRequest class.
 */
@RunWith(Parameterized.class)
public class TestOMKeyCreateRequest extends TestOMKeyRequest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{true, true},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{false, false});
  }

  public TestOMKeyCreateRequest(boolean setKeyPathLock,
                                boolean setFileSystemPaths) {
    // Ignored. Actual init done in initParam().
    // This empty constructor is still required to avoid argument exception.
  }

  @Parameterized.BeforeParam
  public static void initParam(boolean setKeyPathLock,
                               boolean setFileSystemPaths) {
    keyPathLockEnabled = setKeyPathLock;
    enableFileSystemPaths = setFileSystemPaths;
  }

  private static boolean keyPathLockEnabled;
  private static boolean enableFileSystemPaths;

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
  public void testValidateAndUpdateCache() throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(keyPathLockEnabled, enableFileSystemPaths));
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
        omMetadataManager.getOpenKeyTable(omKeyCreateRequest.getBucketLayout())
            .get(openKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    checkResponse(modifiedOmRequest, omKeyCreateResponse, id, false,
        omKeyCreateRequest.getBucketLayout());

    // Network returns only latest version.
    Assert.assertEquals(1, omKeyCreateResponse.getOMResponse()
        .getCreateKeyResponse().getKeyInfo().getKeyLocationListCount());

    // Disk should have 1 version, as it is fresh key create.
    Assert.assertEquals(1,
        omMetadataManager.getOpenKeyTable(omKeyCreateRequest.getBucketLayout())
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
        omMetadataManager.getOpenKeyTable(omKeyCreateRequest.getBucketLayout())
            .get(openKey);
    Assert.assertNull(omKeyInfo);

    omKeyCreateRequest =
        getOMKeyCreateRequest(modifiedOmRequest);

    omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 101L,
            ozoneManagerDoubleBufferHelper);

    checkResponse(modifiedOmRequest, omKeyCreateResponse, id, true,
        omKeyCreateRequest.getBucketLayout());

    // Network returns only latest version
    Assert.assertEquals(1, omKeyCreateResponse.getOMResponse()
        .getCreateKeyResponse().getKeyInfo().getKeyLocationListCount());

    // Disk should have 1 versions when bucket versioning is off.
    Assert.assertEquals(1,
        omMetadataManager.getOpenKeyTable(omKeyCreateRequest.getBucketLayout())
            .get(openKey).getKeyLocationVersions().size());

  }

  private void checkResponse(OMRequest modifiedOmRequest,
      OMClientResponse omKeyCreateResponse, long id, boolean override,
      BucketLayout bucketLayout) throws Exception {

    Assert.assertEquals(OK,
        omKeyCreateResponse.getOMResponse().getStatus());

    String openKey = getOpenKey(id);

    // Check open table whether key is added or not.

    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(bucketLayout).get(openKey);

    Assert.assertNotNull(omKeyInfo);
    Assert.assertNotNull(omKeyInfo.getLatestVersionLocations());

    // As our data size is 100, and scmBlockSize is default to 1000, so we
    // shall have only one block.
    List<OmKeyLocationInfo> omKeyLocationInfoList =
        omKeyInfo.getLatestVersionLocations().getLocationList();
    Assert.assertEquals(1, omKeyLocationInfoList.size());

    OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

    // Check modification time
    Assert.assertEquals(modifiedOmRequest.getCreateKeyRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    if (!override) {
      Assert.assertEquals(omKeyInfo.getModificationTime(),
          omKeyInfo.getCreationTime());
    } else {
      Assert.assertNotEquals(omKeyInfo.getModificationTime(),
          omKeyInfo.getCreationTime());
    }

    // Check data of the block
    OzoneManagerProtocolProtos.KeyLocation keyLocation =
        modifiedOmRequest.getCreateKeyRequest().getKeyArgs().getKeyLocations(0);

    Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omKeyLocationInfo.getContainerID());
    Assert.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getLocalID(), omKeyLocationInfo.getLocalID());
  }

  @Test
  public void testValidateAndUpdateCacheWithNoSuchMultipartUploadError()
      throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(keyPathLockEnabled, enableFileSystemPaths));
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

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omKeyCreateResponse.getOMResponse().getStatus());

    // As we got error, no entry should be created in openKeyTable.

    omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    Assert.assertNull(omKeyInfo);
  }



  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(keyPathLockEnabled, enableFileSystemPaths));
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

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omKeyCreateResponse.getOMResponse().getStatus());


    // As We got an error, openKey Table should not have entry.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    Assert.assertNull(omKeyInfo);

  }


  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(keyPathLockEnabled, enableFileSystemPaths));
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

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omKeyCreateResponse.getOMResponse().getStatus());


    // As We got an error, openKey Table should not have entry.
    omKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);

    Assert.assertNull(omKeyInfo);

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

    Assert.assertEquals(originalOMRequest.getCmdType(),
        modifiedOmRequest.getCmdType());
    Assert.assertEquals(originalOMRequest.getClientId(),
        modifiedOmRequest.getClientId());

    Assert.assertTrue(modifiedOmRequest.hasCreateKeyRequest());

    CreateKeyRequest createKeyRequest =
        modifiedOmRequest.getCreateKeyRequest();

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    int dataGroupSize = keyArgs.hasEcReplicationConfig() ?
        keyArgs.getEcReplicationConfig().getData() : 1;
    long blockSize = ozoneManager.getScmBlockSize();
    long preAllocatedBlocks = Math.min(ozoneManager.getPreallocateBlocksMax(),
        (keyArgs.getDataSize() - 1) / (blockSize * dataGroupSize) + 1);

    // Time should be set
    Assert.assertTrue(keyArgs.getModificationTime() > 0);


    // Client ID should be set.
    Assert.assertTrue(createKeyRequest.hasClientID());
    Assert.assertTrue(createKeyRequest.getClientID() > 0);


    if (!originalOMRequest.getCreateKeyRequest().getKeyArgs()
        .getIsMultipartKey()) {

      List<OzoneManagerProtocolProtos.KeyLocation> keyLocations =
          keyArgs.getKeyLocationsList();
      // KeyLocation should be set.
      Assert.assertEquals(preAllocatedBlocks, keyLocations.size());
      Assert.assertEquals(CONTAINER_ID,
          keyLocations.get(0).getBlockID().getContainerBlockID()
              .getContainerID());
      Assert.assertEquals(LOCAL_ID,
          keyLocations.get(0).getBlockID().getContainerBlockID()
              .getLocalID());
      Assert.assertTrue(keyLocations.get(0).hasPipeline());

      Assert.assertEquals(0, keyLocations.get(0).getOffset());

      Assert.assertEquals(scmBlockSize, keyLocations.get(0).getLength());
    } else {
      // We don't create blocks for multipart key in createKey preExecute.
      Assert.assertEquals(0, keyArgs.getKeyLocationsList().size());
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
  private OMRequest createKeyRequest(boolean isMultipartKey, int partNumber) {
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

  private OMRequest createKeyRequest(boolean isMultipartKey, int partNumber,
      long keyLength, ReplicationConfig repConfig) {

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

  @Test
  public void testKeyCreateWithFileSystemPathsEnabled() throws Exception {

    OzoneConfiguration configuration = getOzoneConfiguration();
    configuration.setBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    when(ozoneManager.getConfiguration()).thenReturn(configuration);
    when(ozoneManager.getEnableFileSystemPaths()).thenReturn(true);
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(keyPathLockEnabled, true));

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
      Assert.assertTrue(ex instanceof OMException);
      OMException omException = (OMException) ex;
      Assert.assertEquals(OMException.ResultCodes.INVALID_KEY_NAME,
          omException.getResult());
    }


  }
  private void checkNotAFile(String keyName) throws Exception {
    OMRequest omRequest = createKeyRequest(false, 0, keyName);

    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    omRequest = omKeyCreateRequest.preExecute(ozoneManager);

    omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    OMClientResponse omClientResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager,
            101L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(NOT_A_FILE,
        omClientResponse.getOMResponse().getStatus());
  }


  private void createAndCheck(String keyName) throws Exception {
    OMRequest omRequest = createKeyRequest(false, 0, keyName);

    OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    omRequest = omKeyCreateRequest.preExecute(ozoneManager);

    omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

    OMClientResponse omClientResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager,
            101L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OK, omClientResponse.getOMResponse().getStatus());

    checkCreatedPaths(omKeyCreateRequest, omRequest, keyName);
  }

  protected void checkCreatedPaths(OMKeyCreateRequest omKeyCreateRequest,
      OMRequest omRequest, String keyName) throws Exception {
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
    Assert.assertNotNull(omKeyInfo);
  }

  protected long checkIntermediatePaths(Path keyPath) throws Exception {
    // Check intermediate paths are created
    keyPath = keyPath.getParent();
    while (keyPath != null) {
      Assert.assertNotNull(omMetadataManager.getKeyTable(getBucketLayout()).get(
          omMetadataManager
              .getOzoneDirKey(volumeName, bucketName, keyPath.toString())));
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
}
