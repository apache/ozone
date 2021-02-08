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
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.request.TestOMRequestUtils.addVolumeAndBucketToDB;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_A_FILE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Tests OMCreateKeyRequest class.
 */
public class TestOMKeyCreateRequest extends TestOMKeyRequest {

  @Test
  public void testPreExecuteWithNormalKey() throws Exception {
    doPreExecute(createKeyRequest(false, 0));
  }

  @Test
  public void testPreExecuteWithMultipartKey() throws Exception {
    doPreExecute(createKeyRequest(true, 1));
  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0));

    OMKeyCreateRequest omKeyCreateRequest =
        new OMKeyCreateRequest(modifiedOmRequest);

    // Add volume and bucket entries to DB.
    addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);

    // Before calling
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OK,
        omKeyCreateResponse.getOMResponse().getStatus());

    // Check open table whether key is added or not.

    omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNotNull(omKeyInfo);

    List<OmKeyLocationInfo> omKeyLocationInfoList =
        omKeyInfo.getLatestVersionLocations().getLocationList();
    Assert.assertTrue(omKeyLocationInfoList.size() == 1);

    OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

    // Check modification time
    Assert.assertEquals(modifiedOmRequest.getCreateKeyRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    Assert.assertEquals(omKeyInfo.getModificationTime(),
        omKeyInfo.getCreationTime());


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


    int partNumber = 1;
    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(true, partNumber));

    OMKeyCreateRequest omKeyCreateRequest =
        new OMKeyCreateRequest(modifiedOmRequest);

    // Add volume and bucket entries to DB.
    addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);

    // Before calling
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omKeyCreateResponse.getOMResponse().getStatus());

    // As we got error, no entry should be created in openKeyTable.

    omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);
  }



  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {

    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(false, 0));

    OMKeyCreateRequest omKeyCreateRequest =
        new OMKeyCreateRequest(modifiedOmRequest);


    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);


    // Before calling
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omKeyCreateResponse.getOMResponse().getStatus());


    // As We got an error, openKey Table should not have entry.
    omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

  }


  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {


    OMRequest modifiedOmRequest =
        doPreExecute(createKeyRequest(
            false, 0));

    OMKeyCreateRequest omKeyCreateRequest =
        new OMKeyCreateRequest(modifiedOmRequest);


    long id = modifiedOmRequest.getCreateKeyRequest().getClientID();

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, id);

    TestOMRequestUtils.addVolumeToDB(volumeName, OzoneConsts.OZONE,
        omMetadataManager);

    // Before calling
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

    Assert.assertNull(omKeyInfo);

    OMClientResponse omKeyCreateResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omKeyCreateResponse.getOMResponse().getStatus());


    // As We got an error, openKey Table should not have entry.
    omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);

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
        new OMKeyCreateRequest(originalOMRequest);

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
    // Time should be set
    Assert.assertTrue(keyArgs.getModificationTime() > 0);


    // Client ID should be set.
    Assert.assertTrue(createKeyRequest.hasClientID());
    Assert.assertTrue(createKeyRequest.getClientID() > 0);


    if (!originalOMRequest.getCreateKeyRequest().getKeyArgs()
        .getIsMultipartKey()) {

      // As our data size is 100, and scmBlockSize is default to 1000, so we
      // shall have only one block.
      List< OzoneManagerProtocolProtos.KeyLocation> keyLocations =
          keyArgs.getKeyLocationsList();
      // KeyLocation should be set.
      Assert.assertTrue(keyLocations.size() == 1);
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
      Assert.assertTrue(keyArgs.getKeyLocationsList().size() == 0);
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
        .setFactor(replicationFactor).setType(replicationType);

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

  @Test
  public void testKeyCreateWithFileSystemPathsEnabled() throws Exception {

    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    when(ozoneManager.getConfiguration()).thenReturn(configuration);
    when(ozoneManager.getEnableFileSystemPaths()).thenReturn(true);

    // Add volume and bucket entries to DB.
    addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);


    keyName = "dir1/dir2/dir3/file1";
    createAndCheck(keyName);

    // Key with leading '/'.
    String keyName = "/a/b/c/file1";
    createAndCheck(keyName);

    // Commit openKey entry.
    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName,
        keyName.substring(1), 0L, RATIS, THREE, omMetadataManager);

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


  private void checkNotAValidPath(String keyName) {
    OMRequest omRequest = createKeyRequest(false, 0, keyName);
    OMKeyCreateRequest omKeyCreateRequest = new OMKeyCreateRequest(omRequest);

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

    OMKeyCreateRequest omKeyCreateRequest = new OMKeyCreateRequest(omRequest);

    omRequest = omKeyCreateRequest.preExecute(ozoneManager);

    omKeyCreateRequest = new OMKeyCreateRequest(omRequest);

    OMClientResponse omClientResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager,
            101L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(NOT_A_FILE,
        omClientResponse.getOMResponse().getStatus());
  }


  private void createAndCheck(String keyName) throws Exception {
    OMRequest omRequest = createKeyRequest(false, 0, keyName);

    OMKeyCreateRequest omKeyCreateRequest = new OMKeyCreateRequest(omRequest);

    omRequest = omKeyCreateRequest.preExecute(ozoneManager);

    omKeyCreateRequest = new OMKeyCreateRequest(omRequest);

    OMClientResponse omClientResponse =
        omKeyCreateRequest.validateAndUpdateCache(ozoneManager,
            101L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OK, omClientResponse.getOMResponse().getStatus());

    checkCreatedPaths(omKeyCreateRequest, omRequest, keyName);
  }

  private void checkCreatedPaths(OMKeyCreateRequest omKeyCreateRequest,
      OMRequest omRequest, String keyName) throws Exception {
    keyName = omKeyCreateRequest.validateAndNormalizeKey(true, keyName);
    // Check intermediate directories created or not.
    Path keyPath = Paths.get(keyName);
    checkIntermediatePaths(keyPath);

    // Check open key entry
    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, omRequest.getCreateKeyRequest().getClientID());
    OmKeyInfo omKeyInfo = omMetadataManager.getOpenKeyTable().get(openKey);
    Assert.assertNotNull(omKeyInfo);
  }



  private void checkIntermediatePaths(Path keyPath) throws Exception {
    // Check intermediate paths are created
    keyPath = keyPath.getParent();
    while(keyPath != null) {
      Assert.assertNotNull(omMetadataManager.getKeyTable().get(
          omMetadataManager.getOzoneDirKey(volumeName, bucketName,
              keyPath.toString())));
      keyPath = keyPath.getParent();
    }
  }

}
