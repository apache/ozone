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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for OzoneManager File System operations.
 */
public class TestOzoneManagerFS {
  @TempDir
  private static File dir;
  private static KeyManagerImpl keyManager;
  private static StorageContainerManager scm;
  private static StorageContainerLocationProtocol mockScmContainerClient;
  private static OzoneConfiguration conf;
  private static OMMetadataManager metadataManager;
  private static final String BUCKET_NAME = "bucket1";
  private static final String BUCKET2_NAME = "bucket2";
  private static final String VERSIONED_BUCKET_NAME = "versionedbucket1";
  private static final String VOLUME_NAME = "vol1";
  private static OzoneManagerProtocol writeClient;
  private static OzoneClient rpcClient;
  private static OzoneManager om;

  @BeforeAll
  public static void setUp() throws Exception {
    ExitUtils.disableSystemExit();
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    conf.set(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY, "true");
    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME,
        conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    ScmBlockLocationProtocol mockScmBlockLocationProtocol = mock(ScmBlockLocationProtocol.class);
    MockNodeManager nodeManager = new MockNodeManager(true, 10);
    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager schemaManager = NodeSchemaManager.getInstance();
    schemaManager.init(schemas, false);
    NetworkTopology clusterMap = new NetworkTopologyImpl(schemaManager);
    nodeManager.getAllNodes().forEach(node -> {
      node.setNetworkName(node.getUuidString());
      clusterMap.add(node);
    });
    nodeManager.setNetworkTopology(clusterMap);
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setScmNodeManager(nodeManager);
    configurator.setNetworkTopology(clusterMap);
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    scm = HddsTestUtils.getScm(conf, configurator);
    scm.start();
    scm.exitSafeMode();
    conf.setLong(OZONE_KEY_PREALLOCATION_BLOCKS_MAX, 10);

    mockScmContainerClient =
        mock(StorageContainerLocationProtocol.class);

    OmTestManagers omTestManagers
        = new OmTestManagers(conf, scm.getBlockProtocolServer(),
        mockScmContainerClient);
    om = omTestManagers.getOzoneManager();
    metadataManager = omTestManagers.getMetadataManager();
    keyManager = (KeyManagerImpl)omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    rpcClient = omTestManagers.getRpcClient();

    mockContainerClient();

    when(mockScmBlockLocationProtocol
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class),
            anyString(),
            any(ExcludeList.class),
            anyString())).thenThrow(
                new SCMException("SafeModePrecheck failed for allocateBlock",
            ResultCodes.SAFE_MODE_EXCEPTION));
    createVolume(VOLUME_NAME);
  }

  @AfterAll
  public static void cleanup() throws Exception {
    writeClient.close();
    rpcClient.close();
    scm.stop();
    scm.join();
    om.stop();
  }

  @BeforeEach
  public void init() throws Exception {
    createBucket(VOLUME_NAME, BUCKET_NAME, false);
    createBucket(VOLUME_NAME, BUCKET2_NAME, false);
    createBucket(VOLUME_NAME, VERSIONED_BUCKET_NAME, true);
  }

  @AfterEach
  public void cleanupTest() throws IOException {
    mockContainerClient();
    org.apache.hadoop.fs.Path volumePath = new org.apache.hadoop.fs.Path(OZONE_URI_DELIMITER, VOLUME_NAME);
    try (FileSystem fs = FileSystem.get(conf)) {
      fs.delete(new org.apache.hadoop.fs.Path(volumePath, BUCKET_NAME), true);
      fs.delete(new org.apache.hadoop.fs.Path(volumePath, BUCKET2_NAME), true);
      fs.delete(new org.apache.hadoop.fs.Path(volumePath, VERSIONED_BUCKET_NAME), true);
    }
  }

  private static void mockContainerClient() {
    ScmClient scmClient = new ScmClient(scm.getBlockProtocolServer(),
        mockScmContainerClient, conf);
    HddsWhiteboxTestUtils.setInternalState(keyManager,
        "scmClient", scmClient);
    HddsWhiteboxTestUtils.setInternalState(om,
        "scmClient", scmClient);
  }

  private static void createBucket(String volumeName, String bucketName,
                                   boolean isVersionEnabled)
      throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setIsVersionEnabled(isVersionEnabled)
        .build();

    OMRequestTestUtils.addBucketToOM(metadataManager, bucketInfo);
  }

  private static void createVolume(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("bilbo")
        .setOwnerName("bilbo")
        .build();
    OMRequestTestUtils.addVolumeToOM(metadataManager, volumeArgs);
  }

  @Test
  public void testCreateDirectory() throws IOException {
    // Create directory where the parent directory does not exist
    StringBuilder keyNameBuf = new StringBuilder();
    keyNameBuf.append(RandomStringUtils.secure().nextAlphabetic(5));
    for (int i = 0; i < 5; i++) {
      keyNameBuf.append('/').append(RandomStringUtils.secure().nextAlphabetic(5));
    }
    String keyName = keyNameBuf.toString();
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();

    writeClient.createDirectory(keyArgs);

    Path path = Paths.get(keyName);
    while (path != null) {
      // verify parent directories are created
      assertIsDirectory(BUCKET_NAME, path.toString());
      path = path.getParent();
    }
  }

  @Test
  void cannotCreateDirUnderFile() throws IOException {
    // make sure create directory fails where parent is a file
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    OpenKeySession keySession = writeClient.openKey(keyArgs);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    writeClient.commitKey(keyArgs, keySession.getId());
    OMException e =
        assertThrows(OMException.class, () -> writeClient.createDirectory(keyArgs),
            "Creation should fail for directory.");
    assertEquals(OMException.ResultCodes.FILE_ALREADY_EXISTS, e.getResult());
  }

  @Test
  void createDirUnderRoot() throws IOException {
    // create directory where parent is root
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    writeClient.createDirectory(keyArgs);
    OzoneFileStatus fileStatus = keyManager.getFileStatus(keyArgs);
    assertTrue(fileStatus.isDirectory());
    assertThat(fileStatus.getKeyInfo().getKeyLocationVersions().get(0).getLocationList())
        .isEmpty();
  }

  @Test
  public void testOpenFile() throws IOException {
    // create key
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    OpenKeySession keySession = writeClient.createFile(keyArgs, false, false);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    writeClient.commitKey(keyArgs, keySession.getId());

    // try to open created key with overWrite flag set to false
    OMException ex =
        assertThrows(OMException.class, () -> writeClient.createFile(keyArgs, false, false),
            "Open key should fail for non overwrite create");
    assertEquals(OMException.ResultCodes.FILE_ALREADY_EXISTS, ex.getResult());

    // create file should pass with overwrite flag set to true
    writeClient.createFile(keyArgs, true, false);
  }

  @Test
  void createFileUnderNonexistentParent() throws IOException {
    // try to create a file where parent directories do not exist and
    // recursive flag is set to false
    StringBuilder keyNameBuf = new StringBuilder();
    keyNameBuf.append(RandomStringUtils.secure().nextAlphabetic(5));
    for (int i = 0; i < 5; i++) {
      keyNameBuf.append('/').append(RandomStringUtils.secure().nextAlphabetic(5));
    }
    String keyName = keyNameBuf.toString();
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    OMException ex =
        assertThrows(OMException.class, () -> writeClient.createFile(keyArgs, false, false),
            "Open file should fail for non recursive write");
    assertEquals(OMException.ResultCodes.DIRECTORY_NOT_FOUND, ex.getResult());

    // file create should pass when recursive flag is set to true
    OpenKeySession keySession = writeClient.createFile(keyArgs, false, true);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    writeClient.commitKey(keyArgs, keySession.getId());
    assertTrue(keyManager
        .getFileStatus(keyArgs).isFile());
  }

  @Test
  void cannotOverwriteDirWithFile() throws IOException {
    // try creating a file over a directory
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName("")
        .build();
    OMException ex = assertThrows(OMException.class, () -> writeClient.createFile(keyArgs, true, true),
        "Open file should fail for non recursive write");
    assertEquals(OMException.ResultCodes.NOT_A_FILE, ex.getResult());
  }

  @Test
  public void testCheckAccessForFileKey() throws Exception {
    // GIVEN
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName("testdir/deep/NOTICE.txt")
        .build();
    OpenKeySession keySession = writeClient.createFile(keyArgs, false, true);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    writeClient.commitKey(keyArgs, keySession.getId());

    reset(mockScmContainerClient);
    OzoneObj fileKey = OzoneObjInfo.Builder.fromKeyArgs(keyArgs)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    RequestContext context = currentUserReads();

    // WHEN
    boolean access = keyManager.checkAccess(fileKey, context);

    // THEN
    assertTrue(access);
    verify(mockScmContainerClient, never())
        .getContainerWithPipelineBatch(any());
  }

  @Test
  public void testLookupFile() throws IOException {
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();

    // lookup for a non-existent file
    OMException ex =
        assertThrows(OMException.class, () -> keyManager.lookupFile(keyArgs, null),
            "Lookup file should fail for non existent file");
    assertEquals(OMException.ResultCodes.FILE_NOT_FOUND, ex.getResult());

    // create a file
    OpenKeySession keySession = writeClient.createFile(keyArgs, false, false);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    writeClient.commitKey(keyArgs, keySession.getId());
    assertEquals(keyManager.lookupFile(keyArgs, null).getKeyName(),
        keyName);
  }

  @Test
  void lookupFileFailsForDirectory() throws IOException {
    // lookup for created file
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName("")
        .build();
    OMException ex = assertThrows(OMException.class, () -> keyManager.lookupFile(keyArgs, null),
        "Lookup file should fail for a directory");
    assertEquals(OMException.ResultCodes.NOT_A_FILE, ex.getResult());
  }

  @Test
  public void testGetFileStatus() throws IOException {
    // create a key
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .setLatestVersionLocation(true)
        .build();
    writeClient.createFile(keyArgs, false, false);
    OpenKeySession keySession = writeClient.createFile(keyArgs, true, true);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    writeClient.commitKey(keyArgs, keySession.getId());
    OzoneFileStatus ozoneFileStatus = keyManager.getFileStatus(keyArgs);
    assertEquals(keyName, ozoneFileStatus.getKeyInfo().getFileName());
  }

  @Test
  public void testGetFileStatusWithFakeDir() throws IOException {
    String parentDir = "dir1";
    String fileName = "file1";
    String keyName1 = parentDir + OZONE_URI_DELIMITER + fileName;
    // "dir1.file1" used to confirm that it will not affect
    // the creation of fake directory "dir1"
    String keyName2 = parentDir + "." + fileName;
    OzoneFileStatus ozoneFileStatus;

    // create a key "dir1/key1"
    OmKeyArgs keyArgs = createBuilder().setKeyName(keyName1).build();
    OpenKeySession keySession = writeClient.openKey(keyArgs);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    writeClient.commitKey(keyArgs, keySession.getId());

    // create a key "dir1.key"
    keyArgs = createBuilder().setKeyName(keyName2).build();
    keySession = writeClient.createFile(keyArgs, true, true);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    writeClient.commitKey(keyArgs, keySession.getId());

    // verify key "dir1/key1" and "dir1.key1" can be found in the bucket, and
    // "dir1" can not be found in the bucket
    assertNull(metadataManager.getKeyTable(getDefaultBucketLayout())
        .get(metadataManager.getOzoneKey(VOLUME_NAME, BUCKET_NAME, parentDir)));
    assertNotNull(metadataManager.getKeyTable(getDefaultBucketLayout())
        .get(metadataManager.getOzoneKey(VOLUME_NAME, BUCKET_NAME, keyName1)));
    assertNotNull(metadataManager.getKeyTable(getDefaultBucketLayout())
        .get(metadataManager.getOzoneKey(VOLUME_NAME, BUCKET_NAME, keyName2)));

    // get a non-existing "dir1", since the key is prefixed "dir1/key1",
    // a fake "/dir1" will be returned
    keyArgs = createBuilder().setKeyName(parentDir).build();
    ozoneFileStatus = keyManager.getFileStatus(keyArgs);
    assertEquals(parentDir, ozoneFileStatus.getKeyInfo().getFileName());
    assertTrue(ozoneFileStatus.isDirectory());

    // get a non-existing "dir", since the key is not prefixed "dir1/key1",
    // a `OMException` will be thrown
    keyArgs = createBuilder().setKeyName("dir").build();
    OmKeyArgs finalKeyArgs = keyArgs;
    assertThrows(OMException.class, () -> keyManager.getFileStatus(
        finalKeyArgs));

    // get a file "dir1/key1"
    keyArgs = createBuilder().setKeyName(keyName1).build();
    ozoneFileStatus = keyManager.getFileStatus(keyArgs);
    assertEquals(fileName, ozoneFileStatus.getKeyInfo().getFileName());
    assertTrue(ozoneFileStatus.isFile());
  }

  private static Stream<Arguments> fakeDirScenarios() {
    final String bucket1 = BUCKET_NAME;
    final String bucket2 = BUCKET2_NAME;

    return Stream.of(
        Arguments.of(
            "false positive",
            Stream.of(
                Pair.of(bucket1, "dir1/file1"),
                Pair.of(bucket2, "dir2/file2")
            ),
            // positives
            Stream.of(
                Pair.of(bucket1, "dir1"),
                Pair.of(bucket2, "dir2")
            ),
            // negatives
            Stream.of(
                Pair.of(bucket1, "dir0"),
                // RocksIterator#seek("volume1/bucket1/dir2/") will position
                // at the 2nd dbKey "volume1/bucket2/dir2/file2", which is
                // not belong to bucket1.
                // This might be a false positive, see HDDS-7871.
                Pair.of(bucket1, "dir2"),
                Pair.of(bucket2, "dir0"),
                Pair.of(bucket2, "dir1"),
                Pair.of(bucket2, "dir3")
            )
        ),
        Arguments.of(
            "false negative",
            Stream.of(
                Pair.of(bucket1, "dir1/file1"),
                Pair.of(bucket1, "dir1/file2"),
                Pair.of(bucket1, "dir1/file3"),
                Pair.of(bucket2, "dir1/file1"),
                Pair.of(bucket2, "dir1/file2")
            ),
            // positives
            Stream.of(
                Pair.of(bucket1, "dir1"),
                Pair.of(bucket2, "dir1")
            ),
            // negatives
            Stream.empty()
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("fakeDirScenarios")
  public void testGetFileStatusWithFakeDirs(
      String description,
      Stream<Pair<String, String>> keys,
      Stream<Pair<String, String>> positives,
      Stream<Pair<String, String>> negatives) {
    keys.forEach(f -> createFile(f.getLeft(), f.getRight()));
    positives.forEach(f -> assertIsDirectory(f.getLeft(), f.getRight()));
    negatives.forEach(f -> assertFileNotFound(f.getLeft(), f.getRight()));
  }

  private void createFile(String bucketName, String keyName) {
    try {
      OmKeyArgs keyArgs = createBuilder(bucketName).setKeyName(keyName).build();
      OpenKeySession keySession = writeClient.openKey(keyArgs);
      keyArgs.setLocationInfoList(keySession.getKeyInfo()
          .getLatestVersionLocations().getLocationList());
      writeClient.commitKey(keyArgs, keySession.getId());

      // verify key exist in table
      OmKeyInfo keyInfo = metadataManager.getKeyTable(getDefaultBucketLayout())
          .get(metadataManager.getOzoneKey(VOLUME_NAME, bucketName, keyName));
      assertNotNull(keyInfo);
      assertEquals(VOLUME_NAME, keyInfo.getVolumeName());
      assertEquals(bucketName, keyInfo.getBucketName());
      assertEquals(keyName, keyInfo.getKeyName());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertFileNotFound(String bucketName, String keyName) {
    try {
      OmKeyArgs keyArgs = createBuilder(bucketName).setKeyName(keyName).build();
      OMException ex = assertThrows(OMException.class,
          () -> keyManager.getFileStatus(keyArgs));
      assertEquals(OMException.ResultCodes.FILE_NOT_FOUND, ex.getResult());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertIsDirectory(String bucketName, String keyName) {
    try {
      OmKeyArgs keyArgs = createBuilder(bucketName).setKeyName(keyName).build();
      OzoneFileStatus ozoneFileStatus = keyManager.getFileStatus(keyArgs);
      OmKeyInfo keyInfo = ozoneFileStatus.getKeyInfo();
      assertEquals(VOLUME_NAME, keyInfo.getVolumeName());
      assertEquals(bucketName, keyInfo.getBucketName());
      assertEquals(keyName + '/', keyInfo.getKeyName());
      assertEquals(Paths.get(keyName).getFileName().toString(), keyInfo.getFileName());
      assertTrue(ozoneFileStatus.isDirectory());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private OmKeyArgs.Builder createBuilder() throws IOException {
    return createBuilder(BUCKET_NAME);
  }

  private OmKeyArgs.Builder createBuilder(String bucketName)
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return new OmKeyArgs.Builder()
        .setBucketName(bucketName)
        .setDataSize(0)
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ONE))
        .setAcls(OzoneAclUtil.getAclList(ugi, ALL, ALL))
        .setVolumeName(VOLUME_NAME)
        .setOwnerName(ugi.getShortUserName());
  }

  private RequestContext currentUserReads() throws IOException {
    return RequestContext.newBuilder()
        .setClientUgi(UserGroupInformation.getCurrentUser())
        .setAclRights(ACLType.READ)
        .setAclType(ACLIdentityType.USER)
        .build();
  }

  private static BucketLayout getDefaultBucketLayout() {
    return BucketLayout.DEFAULT;
  }
}
