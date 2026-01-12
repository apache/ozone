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

package org.apache.hadoop.ozone.om.request.file;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addVolumeAndBucketToDB;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.DIRECTORY_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_A_FILE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND;
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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.lock.OzoneLockProvider;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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
    assertNotEquals(omRequest, modifiedOmRequest);

    // Check clientID and modification time is set or not.
    assertTrue(modifiedOmRequest.hasCreateFileRequest());
    assertThat(modifiedOmRequest.getCreateFileRequest().getClientID()).isGreaterThan(0);

    KeyArgs keyArgs = modifiedOmRequest.getCreateFileRequest().getKeyArgs();
    assertNotNull(keyArgs);
    assertThat(keyArgs.getModificationTime()).isGreaterThan(0);

    // As our data size is 100, and scmBlockSize is default to 1000, so we
    // shall have only one block.
    List< OzoneManagerProtocolProtos.KeyLocation> keyLocations =
        keyArgs.getKeyLocationsList();

    // KeyLocation should be set.
    assertEquals(1, keyLocations.size());
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

  @Test
  public void testPreExecuteWithBlankKey() throws Exception {
    OMRequest omRequest = createFileRequest(volumeName, bucketName, "",
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, false);

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);
    assertNotEquals(omRequest, modifiedOmRequest);

    // When KeyName is root, nothing will be set.
    assertTrue(modifiedOmRequest.hasCreateFileRequest());
    assertThat(modifiedOmRequest.getCreateFileRequest().getClientID()).isLessThanOrEqualTo(0);

    KeyArgs keyArgs = modifiedOmRequest.getCreateFileRequest().getKeyArgs();
    assertNotNull(keyArgs);
    assertEquals(0, keyArgs.getModificationTime());
    assertEquals(0, keyArgs.getKeyLocationsList().size());
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
    assertNull(omKeyInfo);

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);

    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omFileCreateResponse.getOMResponse().getStatus());

    // Check open table whether key is added or not.

    omKeyInfo = verifyPathInOpenKeyTable(keyName, id, true);

    List< OmKeyLocationInfo > omKeyLocationInfoList =
        omKeyInfo.getLatestVersionLocations().getLocationList();
    assertEquals(1, omKeyLocationInfoList.size());

    OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

    // Check modification time
    assertEquals(modifiedOmRequest.getCreateFileRequest()
        .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

    assertEquals(omKeyInfo.getModificationTime(), omKeyInfo.getCreationTime());

    // Check data of the block
    OzoneManagerProtocolProtos.KeyLocation keyLocation =
        modifiedOmRequest.getCreateFileRequest().getKeyArgs()
            .getKeyLocations(0);

    assertEquals(keyLocation.getBlockID().getContainerBlockID()
        .getContainerID(), omKeyLocationInfo.getContainerID());
    assertEquals(keyLocation.getBlockID().getContainerBlockID()
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
    assertSame(omFileCreateResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED);
  }

  @Test
  public void testValidateAndUpdateEncryption() throws Exception {
    KeyProviderCryptoExtension.EncryptedKeyVersion eKV =
        KeyProviderCryptoExtension.EncryptedKeyVersion.createForDecryption(
            "key1", "v1", new byte[0], new byte[0]);
    KeyProviderCryptoExtension mockKeyProvider = mock(KeyProviderCryptoExtension.class);
    when(mockKeyProvider.generateEncryptedKey(any())).thenReturn(eKV);

    when(ozoneManager.getKmsProvider()).thenReturn(mockKeyProvider);
    keyName = "test/" + keyName;
    OMRequest omRequest = createFileRequest(volumeName, bucketName, keyName,
        HddsProtos.ReplicationFactor.ONE, HddsProtos.ReplicationType.RATIS,
        false, true);

    // add volume and create bucket with bucket encryption key
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setBucketEncryptionKey(
                new BucketEncryptionKeyInfo.Builder()
                    .setKeyName("key1")
                    .setSuite(mock(CipherSuite.class))
                    .setVersion(mock(CryptoProtocolVersion.class))
                    .build()));

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);
    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    OMFileCreateRequest omFileCreateRequestPreExecuted = getOMFileCreateRequest(modifiedOmRequest);
    OMClientResponse omClientResponse = omFileCreateRequestPreExecuted
        .validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(
        OzoneManagerProtocolProtos.Status.OK, omClientResponse.getOMResponse().getStatus());
    assertTrue(omClientResponse.getOMResponse().getCreateFileResponse().getKeyInfo().hasFileEncryptionInfo());
    when(ozoneManager.getKmsProvider()).thenReturn(null);
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
    assertEquals(VOLUME_NOT_FOUND,
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
    assertEquals(BUCKET_NOT_FOUND,
        omFileCreateResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithNonRecursive() throws Exception {
    testNonRecursivePath(UUID.randomUUID().toString(), false, false, false);
    testNonRecursivePath("a/b", false, false, true);

    ReplicationConfig replicationConfig = ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE);
    // Create some child keys for the path
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/c/d", 0L, replicationConfig, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/c/", 0L, replicationConfig, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/", 0L, replicationConfig, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/", 0L, replicationConfig, omMetadataManager);

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
    ReplicationConfig replicationConfig = ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/d/e/f", 0L, replicationConfig, omMetadataManager);
    testNonRecursivePath("c/d/e/f", true, true, false);
    // Create some child keys for the path
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "a/b/c/d", 0L, replicationConfig, omMetadataManager);
    testNonRecursivePath("a/b/c", false, true, false);
  }

  @Test
  public void testValidateAndUpdateCacheWithRecursiveAndOverWrite()
      throws Exception {

    String key = "c/d/e/f";
    // Should be able to create file even if parent directories does not exist
    testNonRecursivePath(key, false, true, false);

    // 3 parent directory created c/d/e
    assertEquals(omMetadataManager.getBucketTable().get(
            omMetadataManager.getBucketKey(volumeName, bucketName))
        .getUsedNamespace(), 3);

    // Add the key to key table
    ReplicationConfig replicationConfig = ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        key, 0L, replicationConfig, omMetadataManager);

    // Even if key exists, should be able to create file as overwrite is set
    // to true
    testNonRecursivePath(key, true, true, false);
    testNonRecursivePath(key, false, true, true);
  }

  @Test
  public void testValidateAndUpdateCacheWithNonRecursiveAndOverWrite()
      throws Exception {

    String key = "c/d/e/f";
    ReplicationConfig replicationConfig = ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE);
    // Need to add the path which starts with "c/d/e" to keyTable as this is
    // non-recursive parent should exist.
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/", 0L, replicationConfig, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/d/", 0L, replicationConfig, omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        "c/d/e/", 0L, replicationConfig, omMetadataManager);
    testNonRecursivePath(key, false, false, false);

    // Add the key to key table
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        key, 0L, replicationConfig, omMetadataManager);

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
    assertEquals(acls, bucketAclResults);

    // Recursive create file with acls inherited from bucket DEFAULT acls
    OMRequest omRequest = createFileRequest(volumeName, bucketName,
        keyName, HddsProtos.ReplicationFactor.ONE,
        HddsProtos.ReplicationType.RATIS, false, true);

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);
    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);
    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
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
        assertTrue(omDirAcls.containsAll(expectedInheritAcls),
            "Failed to inherit parent DEFAULT acls!");

        parentID = omDirInfo.getObjectID();
        expectedInheritAcls = omDirAcls;

        // file should inherit parent DEFAULT acls and itself has ACCESS scope
        // [user:newUser:rw[ACCESS], group:newGroup:rwl[ACCESS]]
        if (indx == dirs.size() - 1) {
          // verify file acls
          assertEquals(omDirInfo.getObjectID(), omKeyInfo.getParentObjectID());
          List<OzoneAcl> fileAcls = omKeyInfo.getAcls();
          System.out.println("  file acls : " + omKeyInfo + " ==> " + fileAcls);
          assertEquals(expectedInheritAcls.stream()
                  .map(acl -> acl.withScope(OzoneAcl.AclScope.ACCESS))
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
      assertTrue(keyAcls.containsAll(parentDefaultAcl.stream()
              .map(acl -> acl.withScope(OzoneAcl.AclScope.ACCESS))
              .collect(Collectors.toList())),
          "Failed to inherit bucket DEFAULT acls!");
      // Should not inherit parent ACCESS acls
      assertThat(keyAcls).doesNotContain(parentAccessAcl);
    }
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

    // Recursive create file with acls inherited from bucket DEFAULT acls
    OMRequest omRequest = createFileRequest(volumeName, bucketName,
        keyName, HddsProtos.ReplicationFactor.ONE,
        HddsProtos.ReplicationType.RATIS, false, true, aclList);

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);
    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);
    long id = modifiedOmRequest.getCreateFileRequest().getClientID();

    omFileCreateRequest = getOMFileCreateRequest(modifiedOmRequest);
    OMClientResponse omFileCreateResponse =
        omFileCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omFileCreateResponse.getOMResponse().getStatus());

    OmKeyInfo omKeyInfo = verifyPathInOpenKeyTable(keyName, id, true);
    if (ignoreClientACLs) {
      assertFalse(omKeyInfo.getAcls().contains(OzoneAcl.parseAcl(ozoneAll)));
    } else {
      assertTrue(omKeyInfo.getAcls().contains(OzoneAcl.parseAcl(ozoneAll)));
    }
  }

  @ParameterizedTest
  @CsvSource(value = {
      ".snapshot/keyName,Cannot create key under path reserved for snapshot: .snapshot/",
      ".snapshot/a/keyName,Cannot create key under path reserved for snapshot: .snapshot/",
      ".snapshot/a/b/keyName,Cannot create key under path reserved for snapshot: .snapshot/",
      ".snapshot,Cannot create key with reserved name: .snapshot"})
  public void testPreExecuteWithInvalidKeyPrefix(String invalidKeyName,
                                                 String expectedErrorMessage) throws IOException {

    OMRequest omRequest = createFileRequest(volumeName, bucketName,
        invalidKeyName, HddsProtos.ReplicationFactor.ONE,
        HddsProtos.ReplicationType.RATIS, false, false);

    OMFileCreateRequest omFileCreateRequest =
        getOMFileCreateRequest(omRequest);

    OMException ex = assertThrows(OMException.class,
        () -> omFileCreateRequest.preExecute(ozoneManager));
    assertThat(ex.getMessage()).contains(expectedErrorMessage);
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
      assertTrue(respStatus == NOT_A_FILE
          || respStatus == FILE_ALREADY_EXISTS
          || respStatus == DIRECTORY_NOT_FOUND);
    } else {
      assertTrue(omFileCreateResponse.getOMResponse().getSuccess());
      long id = modifiedOmRequest.getCreateFileRequest().getClientID();

      verifyKeyNameInCreateFileResponse(key, omFileCreateResponse);

      OmKeyInfo omKeyInfo = verifyPathInOpenKeyTable(key, id, true);

      List< OmKeyLocationInfo > omKeyLocationInfoList =
          omKeyInfo.getLatestVersionLocations().getLocationList();
      assertEquals(1, omKeyLocationInfoList.size());

      OmKeyLocationInfo omKeyLocationInfo = omKeyLocationInfoList.get(0);

      // Check modification time
      assertEquals(modifiedOmRequest.getCreateFileRequest()
          .getKeyArgs().getModificationTime(), omKeyInfo.getModificationTime());

      // Check data of the block
      OzoneManagerProtocolProtos.KeyLocation keyLocation =
          modifiedOmRequest.getCreateFileRequest().getKeyArgs()
              .getKeyLocations(0);

      assertEquals(keyLocation.getBlockID().getContainerBlockID()
          .getContainerID(), omKeyLocationInfo.getContainerID());
      assertEquals(keyLocation.getBlockID().getContainerBlockID()
          .getLocalID(), omKeyLocationInfo.getLocalID());
    }
  }

  private void verifyKeyNameInCreateFileResponse(String key,
      OMClientResponse omFileCreateResponse) {
    OzoneManagerProtocolProtos.CreateFileResponse createFileResponse =
            omFileCreateResponse.getOMResponse().getCreateFileResponse();
    String actualFileName = createFileResponse.getKeyInfo().getKeyName();
    assertEquals(key, actualFileName, "Incorrect keyName");
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
  @Nonnull
  protected OMRequest createFileRequest(
      String volumeName, String bucketName, String keyName,
      HddsProtos.ReplicationFactor replicationFactor,
      HddsProtos.ReplicationType replicationType, boolean overWrite,
      boolean recursive) {
    return createFileRequest(volumeName, bucketName, keyName, replicationFactor,
        replicationType, overWrite, recursive, null);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  @Nonnull
  protected OMRequest createFileRequest(
      String volumeName, String bucketName, String keyName,
      HddsProtos.ReplicationFactor replicationFactor,
      HddsProtos.ReplicationType replicationType, boolean overWrite,
      boolean recursive, List<OzoneAcl> acls) {

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName)
            .setKeyName(keyName).setFactor(replicationFactor)
            .setType(replicationType).setDataSize(dataSize);
    if (acls != null) {
      for (OzoneAcl acl : acls) {
        keyArgs.addAcls(OzoneAcl.toProtobuf(acl));
      }
    }

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
   * Test that SCM's allocateBlock is still called when creating an empty file.
   */
  @Test
  public void testZeroSizedFileShouldCallAllocateBlock() throws Exception {
    // Reset the mock to clear any previous interactions
    reset(scmBlockLocationProtocol);

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setType(HddsProtos.ReplicationType.RATIS)
        .setDataSize(0);  // Set data size to 0 for empty file

    CreateFileRequest createFileRequest = CreateFileRequest.newBuilder()
        .setKeyArgs(keyArgs)
        .setIsOverwrite(false)
        .setIsRecursive(true).build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateFile)
        .setClientId(UUID.randomUUID().toString())
        .setCreateFileRequest(createFileRequest).build();

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    // Verify that SCM's allocateBlock is always called
    verify(scmBlockLocationProtocol, atLeastOnce())
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class), anyString(),
            any(ExcludeList.class));

    verify(scmBlockLocationProtocol, atLeastOnce())
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class), anyString(),
            any(ExcludeList.class), anyString());

    // Verify key locations are present in the response
    assertTrue(modifiedOmRequest.hasCreateFileRequest());
    CreateFileRequest responseCreateFileRequest =
        modifiedOmRequest.getCreateFileRequest();
    assertTrue(
        responseCreateFileRequest.getKeyArgs().getKeyLocationsCount() > 0,
        "File with zero dataSize should still have key locations allocated");
  }

  /**
   * Test that SCM's allocateBlock is still called when creating a file
   * without explicitly setting dataSize (should use default scmBlockSize).
   */
  @Test
  public void testFileWithoutDataSizeShouldAllocateBlock() throws Exception {
    // Reset the mock to clear any previous interactions
    reset(scmBlockLocationProtocol);

    // Setup mock to return valid blocks
    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
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

    // Create a file request WITHOUT setting dataSize (should default to scmBlockSize)
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setType(HddsProtos.ReplicationType.RATIS);
    // Note: dataSize is not set

    CreateFileRequest createFileRequest = CreateFileRequest.newBuilder()
        .setKeyArgs(keyArgs)
        .setIsOverwrite(false)
        .setIsRecursive(true).build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateFile)
        .setClientId(UUID.randomUUID().toString())
        .setCreateFileRequest(createFileRequest).build();

    OMFileCreateRequest omFileCreateRequest = getOMFileCreateRequest(omRequest);

    OMRequest modifiedOmRequest = omFileCreateRequest.preExecute(ozoneManager);

    // Verify that SCM's allocateBlock was called
    verify(scmBlockLocationProtocol, atLeastOnce())
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class), anyString(),
            any(ExcludeList.class), anyString());

    // Verify key locations are present in the response
    assertTrue(modifiedOmRequest.hasCreateFileRequest());
    CreateFileRequest responseCreateFileRequest =
        modifiedOmRequest.getCreateFileRequest();
    assertTrue(
        responseCreateFileRequest.getKeyArgs().getKeyLocationsCount() > 0,
        "File without explicit dataSize should have key locations allocated");
  }

  /**
   * Gets OMFileCreateRequest reference.
   *
   * @param omRequest om request
   * @return OMFileCreateRequest reference
   */
  @Nonnull
  protected OMFileCreateRequest getOMFileCreateRequest(OMRequest omRequest) throws IOException {
    OMFileCreateRequest request = new OMFileCreateRequest(omRequest, getBucketLayout());
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

}
