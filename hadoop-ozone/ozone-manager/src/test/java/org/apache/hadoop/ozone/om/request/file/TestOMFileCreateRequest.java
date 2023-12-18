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

package org.apache.hadoop.ozone.om.request.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_A_FILE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.DIRECTORY_NOT_FOUND;

/**
 * Tests OMFileCreateRequest.
 */
public class TestOMFileCreateRequest extends TestOMKeyRequest {

  @Test
  public void testPreExecute() throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, keyName,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, false);

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);
    Assertions.assertNotEquals(omRequest, modifiedOmRequest);

    // Check clientID and modification time is set or not.
    Assertions.assertTrue(modifiedOmRequest.hasCreateFileRequest());
    Assertions.assertTrue(
        modifiedOmRequest.getCreateFileRequest().getClientID() > 0);

    KeyArgs keyArgs = modifiedOmRequest.getCreateFileRequest().getKeyArgs();
    Assertions.assertNotNull(keyArgs);
    Assertions.assertTrue(keyArgs.getModificationTime() > 0);

    // As our data size is 100, and scmBlockSize is default to 1000, so we
    // shall have only one block.
    List< OzoneManagerProtocolProtos.KeyLocation> keyLocations =
        keyArgs.getKeyLocationsList();

    // KeyLocation should be set.
    Assertions.assertEquals(1, keyLocations.size());
    Assertions.assertEquals(CONTAINER_ID,
        keyLocations.get(0).getBlockID().getContainerBlockID()
            .getContainerID());
    Assertions.assertEquals(LOCAL_ID,
        keyLocations.get(0).getBlockID().getContainerBlockID()
            .getLocalID());
    Assertions.assertTrue(keyLocations.get(0).hasPipeline());

    Assertions.assertEquals(0, keyLocations.get(0).getOffset());

    Assertions.assertEquals(scmBlockSize, keyLocations.get(0).getLength());
  }

  @Test
  public void testPreExecuteWithBlankKey() throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, "",
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, false);

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);
    Assertions.assertNotEquals(omRequest, modifiedOmRequest);

    // When KeyName is root, nothing will be set.
    Assertions.assertTrue(modifiedOmRequest.hasCreateFileRequest());
    Assertions.assertFalse(
        modifiedOmRequest.getCreateFileRequest().getClientID() > 0);

    KeyArgs keyArgs = modifiedOmRequest.getCreateFileRequest().getKeyArgs();
    Assertions.assertNotNull(keyArgs);
    Assertions.assertEquals(0, keyArgs.getModificationTime());
    Assertions.assertEquals(0, keyArgs.getKeyLocationsList().size());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, keyName,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, true);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    long id = modifiedOmRequest.getCreateFileRequest().getClientID();

    // Before calling
    OmKeyInfo omKeyInfo = verifyPathInOpenKeyTable(keyName, id, false);
    Assertions.assertNull(omKeyInfo);

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);

    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omFileCreateResponse.getOMResponse().getStatus());

    // Check open table whether key is added or not.

    omKeyInfo = verifyPathInOpenKeyTable(keyName, id, true);

    List< OmKeyLocationInfo > omKeyLocationInfoList =
        omKeyInfo.getLatestVersionLocations().getLocationList();
    Assertions.assertEquals(1, omKeyLocationInfoList.size());

    OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

    // Check modification time
    Assertions.assertEquals(modifiedOmRequest.getCreateFileRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    Assertions.assertEquals(omKeyInfo.getModificationTime(),
        omKeyInfo.getCreationTime());

    // Check data of the block
    OzoneManagerProtocolProtos.KeyLocation keyLocation =
        modifiedOmRequest.getCreateFileRequest().getKeyArgs()
            .getKeyLocations(0);

    Assertions.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omKeyLocationInfo.getContainerID());
    Assertions.assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getLocalID(), omKeyLocationInfo.getLocalID());
  }

  @Test
  public void testValidateAndUpdateCacheWithNamespaceQuotaExceeded()
      throws Exception {
    keyName = "test/" + keyName;
    OMRequest omRequest = createFileRequest(volumeName, bucketName, keyName,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, true);

    // add volume and create bucket with quota limit 1
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setQuotaInNamespace(1));
    
    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);
    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);
    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    Assertions.assertSame(omFileCreateResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED);
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, keyName,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
            false, true);

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);

    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    Assertions.assertEquals(VOLUME_NOT_FOUND,
        omFileCreateResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, keyName,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, true);

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);

    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    Assertions.assertEquals(BUCKET_NOT_FOUND,
        omFileCreateResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithNonRecursive() throws Exception {
    testNonRecursivePath(UUID.randomUUID().toString(), false, false, false);
    testNonRecursivePath("a/b", false, false, true);

    // Create some child keys for the path
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/c/d", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/c/", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);

    // cannot create file if directory of same name exists
    testNonRecursivePath("a/b/c", false, false, true);

    // Delete child key but retain path "a/b/ in the key table
    omMetadataManager.getKeyTable(getBucketLayout()).delete(
        omMetadataManager.getOzoneKey(volumeName, bucketName, "a/b/c/d"));
    omMetadataManager.getKeyTable(getBucketLayout()).delete(
        omMetadataManager.getOzoneKey(volumeName, bucketName, "a/b/c/"));

    // can create non-recursive because parents already exist.
    testNonRecursivePath("a/b/e", false, false, false);
  }

  @Test
  public void testValidateAndUpdateCacheWithRecursive() throws Exception {
    // Should be able to create file even if parent directories does not
    // exist and key already exist, as this is with overwrite enabled.
    testNonRecursivePath(UUID.randomUUID().toString(), false, false, false);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/d/e/f", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    testNonRecursivePath("c/d/e/f", true, true, false);
    // Create some child keys for the path
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/c/d", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    testNonRecursivePath("a/b/c", false, true, false);
  }

  @Test
  public void testValidateAndUpdateCacheWithRecursiveAndOverWrite()
      throws Exception {

    String key = "c/d/e/f";
    // Should be able to create file even if parent directories does not exist
    testNonRecursivePath(key, false, true, false);
    
    // 3 parent directory created c/d/e
    Assertions.assertEquals(omMetadataManager.getBucketTable().get(
            omMetadataManager.getBucketKey(volumeName, bucketName))
        .getUsedNamespace(), 3);
    
    // Add the key to key table
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        key, 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);

    // Even if key exists, should be able to create file as overwrite is set
    // to true
    testNonRecursivePath(key, true, true, false);
    testNonRecursivePath(key, false, true, true);
  }

  @Test
  public void testValidateAndUpdateCacheWithNonRecursiveAndOverWrite()
      throws Exception {

    String key = "c/d/e/f";
    // Need to add the path which starts with "c/d/e" to keyTable as this is
    // non-recursive parent should exist.
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/d/", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/d/e/", 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);
    testNonRecursivePath(key, false, false, false);

    // Add the key to key table
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        key, 0L,  HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);

    // Even if key exists, should be able to create file as overwrite is set
    // to true
    testNonRecursivePath(key, true, false, false);
    testNonRecursivePath(key, false, false, true);
  }

  @Test
  public void testCreateFileInheritParentDefaultAcls()
      throws Exception {
    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    String prefix = "a/b/c/";
    List<String> dirs = new ArrayList<>();
    dirs.add("a");
    dirs.add("b");
    dirs.add("c");
    String keyName = prefix + UUID.randomUUID();
    List<OzoneAcl> bucketAclResults = new ArrayList<>();

    OmKeyInfo omKeyInfo = createFileWithInheritAcls(keyName, bucketAclResults);

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);

    verifyInheritAcls(dirs, omKeyInfo, volumeId, bucketId, bucketAclResults);
  }

  protected OmKeyInfo createFileWithInheritAcls(String keyName,
      List<OzoneAcl> bucketAclResults) throws Exception {
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(OzoneAcl.parseAcl("user:newUser:rw[DEFAULT]"));
    acls.add(OzoneAcl.parseAcl("user:noInherit:rw"));
    acls.add(OzoneAcl.parseAcl("group:newGroup:rwl[DEFAULT]"));

    // Create bucket with DEFAULT acls
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setAcls(acls));

    // Verify bucket has DEFAULT acls.
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    bucketAclResults.addAll(omMetadataManager.getBucketTable()
        .get(bucketKey).getAcls());
    Assertions.assertEquals(acls, bucketAclResults);

    // Recursive create file with acls inherited from bucket DEFAULT acls
    OMRequest omRequest = createFileRequest(volumeName, bucketName,
        keyName, HddsProtos.ReplicationFactor.ONE,
        HddsProtos.ReplicationType.RATIS, false, true);

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);
    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);
    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omFileCreateResponse.getOMResponse().getStatus());

    long id = modifiedOmRequest.getCreateFileRequest().getClientID();
    return verifyPathInOpenKeyTable(keyName, id, true);
  }

  /**
   * The following layout should inherit the parent DEFAULT acls:
   *  (1) FSO
   *  (2) Legacy when EnableFileSystemPaths
   *
   *  The following layout should inherit the bucket DEFAULT acls:
   *  (1) OBS
   *  (2) Legacy when DisableFileSystemPaths
   *
   * Note: Acl which dir inherited itself has DEFAULT scope,
   * and acl which leaf file inherited itself has ACCESS scope.
   */
  protected void verifyInheritAcls(List<String> dirs, OmKeyInfo omKeyInfo,
      long volumeId, long bucketId, List<OzoneAcl> bucketAcls)
      throws IOException {

    if (getBucketLayout().shouldNormalizePaths(
        ozoneManager.getEnableFileSystemPaths())) {

      // bucketID is the parent
      long parentID = bucketId;
      List<OzoneAcl> expectedInheritAcls = bucketAcls.stream()
          .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.DEFAULT)
          .collect(Collectors.toList());
      System.out.println("expectedInheritAcls: " + expectedInheritAcls);

      // dir should inherit parent DEFAULT acls and itself has DEFAULT scope
      // [user:newUser:rw[DEFAULT], group:newGroup:rwl[DEFAULT]]
      for (int indx = 0; indx < dirs.size(); indx++) {
        String dirName = dirs.get(indx);
        String dbKey;
        // for index=0, parentID is bucketID
        dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
            parentID, dirName);
        OmDirectoryInfo omDirInfo =
            omMetadataManager.getDirectoryTable().get(dbKey);
        List<OzoneAcl> omDirAcls = omDirInfo.getAcls();

        System.out.println(
            "  subdir acls : " + omDirInfo + " ==> " + omDirAcls);
        Assertions.assertEquals(expectedInheritAcls, omDirAcls,
            "Failed to inherit parent DEFAULT acls!");

        parentID = omDirInfo.getObjectID();
        expectedInheritAcls = omDirAcls;

        // file should inherit parent DEFAULT acls and itself has ACCESS scope
        // [user:newUser:rw[ACCESS], group:newGroup:rwl[ACCESS]]
        if (indx == dirs.size() - 1) {
          // verify file acls
          Assertions.assertEquals(omDirInfo.getObjectID(),
              omKeyInfo.getParentObjectID());
          List<OzoneAcl> fileAcls = omDirInfo.getAcls();
          System.out.println("  file acls : " + omKeyInfo + " ==> " + fileAcls);
          Assertions.assertEquals(expectedInheritAcls.stream()
                  .map(acl -> acl.setAclScope(OzoneAcl.AclScope.ACCESS))
                  .collect(Collectors.toList()), fileAcls,
              "Failed to inherit parent DEFAULT acls!");
        }
      }
    } else {
      List<OzoneAcl> keyAcls = omKeyInfo.getAcls();

      List<OzoneAcl> parentDefaultAcl = bucketAcls.stream()
          .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.DEFAULT)
          .collect(Collectors.toList());

      OzoneAcl parentAccessAcl = bucketAcls.stream()
          .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.ACCESS)
          .findAny().orElse(null);

      // Should inherit parent DEFAULT acls
      // [user:newUser:rw[ACCESS], group:newGroup:rwl[ACCESS]]
      Assertions.assertEquals(parentDefaultAcl.stream()
              .map(acl -> acl.setAclScope(OzoneAcl.AclScope.ACCESS))
              .collect(Collectors.toList()), keyAcls,
          "Failed to inherit bucket DEFAULT acls!");
      // Should not inherit parent ACCESS acls
      Assertions.assertFalse(keyAcls.contains(parentAccessAcl));
    }
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

      OMRequest omRequest = createFileRequest(volumeName, bucketName,
          invalidKeyName, HddsProtos.ReplicationFactor.ONE,
          HddsProtos.ReplicationType.RATIS, false, false);

      OMFileCreateRequest omFileCreateRequest =
          getOMFileCreateRequest(omRequest);

      OMException ex = Assertions.assertThrows(OMException.class,
          () -> omFileCreateRequest.preExecute(ozoneManager));

      Assertions.assertTrue(ex.getMessage().contains(expectedErrorMessage));
    }
  }

  protected void testNonRecursivePath(String key,
      boolean overWrite, boolean recursive, boolean fail) throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, key,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        overWrite, recursive);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);

    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    if (fail) {
      OzoneManagerProtocolProtos.Status respStatus =
          omFileCreateResponse.getOMResponse().getStatus();
      Assertions.assertTrue(respStatus == NOT_A_FILE
          || respStatus == FILE_ALREADY_EXISTS
          || respStatus == DIRECTORY_NOT_FOUND);
    } else {
      Assertions.assertTrue(omFileCreateResponse.getOMResponse().getSuccess());
      long id = modifiedOmRequest.getCreateFileRequest().getClientID();

      verifyKeyNameInCreateFileResponse(key, omFileCreateResponse);

      OmKeyInfo omKeyInfo = verifyPathInOpenKeyTable(key, id, true);

      List< OmKeyLocationInfo > omKeyLocationInfoList =
          omKeyInfo.getLatestVersionLocations().getLocationList();
      Assertions.assertEquals(1, omKeyLocationInfoList.size());

      OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

      // Check modification time
      Assertions.assertEquals(modifiedOmRequest.getCreateFileRequest()
          .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

      // Check data of the block
      OzoneManagerProtocolProtos.KeyLocation keyLocation =
          modifiedOmRequest.getCreateFileRequest().getKeyArgs()
              .getKeyLocations(0);

      Assertions.assertEquals(keyLocation.getBlockID().getContainerBlockID()
          .getContainerID(), omKeyLocationInfo.getContainerID());
      Assertions.assertEquals(keyLocation.getBlockID().getContainerBlockID()
          .getLocalID(), omKeyLocationInfo.getLocalID());
    }
  }

  private void verifyKeyNameInCreateFileResponse(String key,
      OMClientResponse omFileCreateResponse) {
    OzoneManagerProtocolProtos.CreateFileResponse createFileResponse =
            omFileCreateResponse.getOMResponse().getCreateFileResponse();
    String actualFileName = createFileResponse.getKeyInfo().getKeyName();
    Assertions.assertEquals(key, actualFileName, "Incorrect keyName");
  }

  /**
   * Create OMRequest which encapsulates OMFileCreateRequest.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param replicationFactor
   * @param replicationType
   * @return OMRequest
   */
  @NotNull
  protected OMRequest createFileRequest(
      String volumeName, String bucketName, String keyName,
      HddsProtos.ReplicationFactor replicationFactor,
      HddsProtos.ReplicationType replicationType, boolean overWrite,
      boolean recursive) {

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName)
            .setKeyName(keyName).setFactor(replicationFactor)
            .setType(replicationType).setDataSize(dataSize);

    CreateFileRequest createFileRequest = CreateFileRequest.newBuilder()
        .setKeyArgs(keyArgs)
        .setIsOverwrite(overWrite)
        .setIsRecursive(recursive).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateKey)
        .setClientId(UUID.randomUUID().toString())
        .setCreateFileRequest(createFileRequest).build();

  }

  /**
   * Gets OMFileCreateRequest reference.
   *
   * @param omRequest om request
   * @return OMFileCreateRequest reference
   */
  @NotNull
  protected OMFileCreateRequest getOMFileCreateRequest(OMRequest omRequest) {
    return new OMFileCreateRequest(omRequest, getBucketLayout());
  }

}
