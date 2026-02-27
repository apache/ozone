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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.SCM_GET_PIPELINE_EXCEPTION;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.db.InMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for @{@link KeyManagerImpl}.
 */
public class TestKeyManagerImpl {
  @TempDir
  private static File dir;
  private static PrefixManager prefixManager;
  private static KeyManagerImpl keyManager;
  private static StorageContainerManager scm;
  private static ScmBlockLocationProtocol mockScmBlockLocationProtocol;
  private static StorageContainerLocationProtocol mockScmContainerClient;
  private static OzoneConfiguration conf;
  private static OMMetadataManager metadataManager;
  private static long scmBlockSize;
  private static final String KEY_NAME = "key1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String BUCKET2_NAME = "bucket2";
  private static final String VERSIONED_BUCKET_NAME = "versionedbucket1";
  private static final String VOLUME_NAME = "vol1";
  private static final ResolvedBucket RESOLVED_BUCKET = new ResolvedBucket(VOLUME_NAME, BUCKET_NAME,
      VOLUME_NAME, BUCKET_NAME, "", BucketLayout.DEFAULT);
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
    mockScmBlockLocationProtocol = mock(ScmBlockLocationProtocol.class);
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
    scmBlockSize = (long) conf
        .getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT,
            StorageUnit.BYTES);
    conf.setLong(OZONE_KEY_PREALLOCATION_BLOCKS_MAX, 10);

    mockScmContainerClient =
        mock(StorageContainerLocationProtocol.class);
    
    OmTestManagers omTestManagers
        = new OmTestManagers(conf, scm.getBlockProtocolServer(),
        mockScmContainerClient);
    om = omTestManagers.getOzoneManager();
    metadataManager = omTestManagers.getMetadataManager();
    keyManager = (KeyManagerImpl)omTestManagers.getKeyManager();
    prefixManager = omTestManagers.getPrefixManager();
    writeClient = omTestManagers.getWriteClient();
    rpcClient = omTestManagers.getRpcClient();

    mockContainerClient();

    when(mockScmBlockLocationProtocol
        .allocateBlock(anyLong(), anyInt(),
            any(ReplicationConfig.class),
            anyString(),
            any(ExcludeList.class),
            anyString(),
            any(StorageType.class))).thenThrow(
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

  private static void mockBlockClient() {
    ScmClient scmClient = new ScmClient(mockScmBlockLocationProtocol, null,
        conf);
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
  public void allocateBlockFailureInSafeMode() throws Exception {
    mockBlockClient();
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(KEY_NAME)
        .build();

    // As now openKey will allocate at least one block, even if the size
    // passed is 0. So adding an entry to openKeyTable manually to test
    // allocateBlock failure.
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationConfig(keyArgs.getReplicationConfig())
        .setFileEncryptionInfo(null).build();
    metadataManager.getOpenKeyTable(getDefaultBucketLayout()).put(
        metadataManager.getOpenKey(VOLUME_NAME, BUCKET_NAME, KEY_NAME, 1L),
        omKeyInfo);
    OMException omException = assertThrows(OMException.class,
         () ->
             writeClient.allocateBlock(keyArgs, 1L, new ExcludeList()));
    assertThat(omException.getMessage())
        .contains("SafeModePrecheck failed for allocateBlock");
  }

  @Test
  public void openKeyFailureInSafeMode() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    mockBlockClient();
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(KEY_NAME)
        .setDataSize(1000)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setAcls(OzoneAclUtil.getAclList(ugi, ALL, ALL))
        .build();
    OMException omException = assertThrows(OMException.class,
        () -> writeClient.openKey(keyArgs));
    assertThat(omException.getMessage())
        .contains("SafeModePrecheck failed for allocateBlock");
  }

  @Test
  public void openKeyWithMultipleBlocks() throws IOException {
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(UUID.randomUUID().toString())
        .setDataSize(scmBlockSize * 10)
        .build();
    OpenKeySession keySession = writeClient.openKey(keyArgs);
    OmKeyInfo keyInfo = keySession.getKeyInfo();
    assertEquals(10,
        keyInfo.getLatestVersionLocations().getLocationList().size());
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
  public void testCheckAccessForNonExistentKey() throws Exception {
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName("testdir/deep/NO_SUCH_FILE.txt")
        .build();
    OzoneObj nonExistentKey = OzoneObjInfo.Builder.fromKeyArgs(keyArgs)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    assertTrue(keyManager.checkAccess(nonExistentKey,
            currentUserReads()));
  }

  @Test
  public void testCheckAccessForDirectoryKey() throws Exception {
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName("some/dir")
        .build();
    writeClient.createDirectory(keyArgs);

    OzoneObj dirKey = OzoneObjInfo.Builder.fromKeyArgs(keyArgs)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    assertTrue(keyManager.checkAccess(dirKey, currentUserReads()));
  }

  @Test
  public void testPrefixAclOps() throws IOException {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String prefix1 = "pf1/";

    OzoneObj ozPrefix1 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix1)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OzoneAcl ozAcl1 = OzoneAcl.of(ACLIdentityType.USER, "user1",
        ACCESS, ACLType.READ);
    writeClient.addAcl(ozPrefix1, ozAcl1);

    List<OzoneAcl> ozAclGet = writeClient.getAcl(ozPrefix1);
    assertEquals(1, ozAclGet.size());
    assertEquals(ozAcl1, ozAclGet.get(0));

    List<OzoneAcl> acls = new ArrayList<>();
    OzoneAcl ozAcl2 = OzoneAcl.of(ACLIdentityType.USER, "admin", ACCESS, ACLType.ALL);

    OzoneAcl ozAcl3 = OzoneAcl.of(ACLIdentityType.GROUP, "dev", ACCESS, READ, WRITE);

    OzoneAcl ozAcl4 = OzoneAcl.of(ACLIdentityType.GROUP, "dev", ACCESS, WRITE);

    OzoneAcl ozAcl5 = OzoneAcl.of(ACLIdentityType.GROUP, "dev", ACCESS, READ);

    acls.add(ozAcl2);
    acls.add(ozAcl3);

    writeClient.setAcl(ozPrefix1, acls);
    ozAclGet = writeClient.getAcl(ozPrefix1);
    assertEquals(2, ozAclGet.size());

    int matchEntries = 0;
    for (OzoneAcl acl : ozAclGet) {
      if (acl.getType() == ACLIdentityType.GROUP) {
        assertEquals(ozAcl3, acl);
        matchEntries++;
      }
      if (acl.getType() == ACLIdentityType.USER) {
        assertEquals(ozAcl2, acl);
        matchEntries++;
      }
    }
    assertEquals(2, matchEntries);

    boolean result = writeClient.removeAcl(ozPrefix1, ozAcl4);
    assertTrue(result);

    ozAclGet = writeClient.getAcl(ozPrefix1);
    assertEquals(2, ozAclGet.size());

    result = writeClient.removeAcl(ozPrefix1, ozAcl3);
    assertTrue(result);
    ozAclGet = writeClient.getAcl(ozPrefix1);
    assertEquals(1, ozAclGet.size());

    assertEquals(ozAcl2, ozAclGet.get(0));

    // add dev:w
    writeClient.addAcl(ozPrefix1, ozAcl4);
    ozAclGet = writeClient.getAcl(ozPrefix1);
    assertEquals(2, ozAclGet.size());

    // add dev:r and validate the acl bitset combined
    writeClient.addAcl(ozPrefix1, ozAcl5);
    ozAclGet = writeClient.getAcl(ozPrefix1);
    assertEquals(2, ozAclGet.size());

    matchEntries = 0;
    for (OzoneAcl acl : ozAclGet) {
      if (acl.getType() == ACLIdentityType.GROUP) {
        assertEquals(ozAcl3, acl);
        matchEntries++;
      }
      if (acl.getType() == ACLIdentityType.USER) {
        assertEquals(ozAcl2, acl);
        matchEntries++;
      }
    }
    assertEquals(2, matchEntries);
    // cleanup
    writeClient.removeAcl(ozPrefix1, ozAcl1);
    writeClient.removeAcl(ozPrefix1, ozAcl2);
    writeClient.removeAcl(ozPrefix1, ozAcl3);
  }

  @Test
  public void testInvalidPrefixAcl() throws IOException {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String prefix1 = "pf1/";

    // Invalid prefix not ending with "/"
    String invalidPrefix = "invalid/pf";
    OzoneAcl ozAcl1 = OzoneAcl.of(ACLIdentityType.USER, "user1",
        ACCESS, ACLType.READ);

    OzoneObj ozInvalidPrefix = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(invalidPrefix)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    // add acl with invalid prefix name
    Exception ex = assertThrows(OMException.class,
        () -> writeClient.addAcl(ozInvalidPrefix, ozAcl1));
    assertThat(ex).hasMessageStartingWith("Missing trailing slash");

    OzoneObj ozPrefix1 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix1)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    writeClient.addAcl(ozPrefix1, ozAcl1);
    List<OzoneAcl> ozAclGet = writeClient.getAcl(ozPrefix1);
    assertEquals(1, ozAclGet.size());
    assertEquals(ozAcl1, ozAclGet.get(0));

    // get acl with invalid prefix name
    ex = assertThrows(OMException.class,
        () -> writeClient.getAcl(ozInvalidPrefix));
    assertThat(ex).hasMessageStartingWith("Missing trailing slash");

    // set acl with invalid prefix name
    List<OzoneAcl> ozoneAcls = new ArrayList<>();
    ozoneAcls.add(ozAcl1);

    ex = assertThrows(OMException.class,
        () -> writeClient.setAcl(ozInvalidPrefix, ozoneAcls));
    assertThat(ex).hasMessageStartingWith("Missing trailing slash");

    // remove acl with invalid prefix name
    ex = assertThrows(OMException.class,
        () -> writeClient.removeAcl(ozInvalidPrefix, ozAcl1));
    assertThat(ex).hasMessageStartingWith("Missing trailing slash");
  }

  @Test
  public void testLongestPrefixPath() throws IOException {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String prefix1 = "pf1/pf11/pf111/pf1111/";
    String file1 = "pf1/pf11/file1";
    String file2 = "pf1/pf11/pf111/pf1111/file2";

    OzoneObj ozPrefix1 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix1)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OzoneAcl ozAcl1 = OzoneAcl.of(ACLIdentityType.USER, "user1",
        ACCESS, ACLType.READ);
    writeClient.addAcl(ozPrefix1, ozAcl1);

    OzoneObj ozFile1 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(file1)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    List<OmPrefixInfo> prefixInfos =
        prefixManager.getLongestPrefixPath(ozFile1.getPath());
    assertEquals(5, prefixInfos.size());

    OzoneObj ozFile2 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(file2)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    prefixInfos =
        prefixManager.getLongestPrefixPath(ozFile2.getPath());
    assertEquals(7, prefixInfos.size());
    // Only the last node has acl on it
    assertEquals(ozAcl1, prefixInfos.get(6).getAcls().get(0));
    // All other nodes don't have acl value associate with it
    for (int i = 0; i < 6; i++) {
      assertNull(prefixInfos.get(i));
    }
    // cleanup
    writeClient.removeAcl(ozPrefix1, ozAcl1);
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

  private OmKeyArgs createKeyArgs(String toKeyName) throws IOException {
    return createBuilder().setKeyName(toKeyName).build();
  }

  @Test
  public void testLookupKeyWithLocation() throws IOException {
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .setSortDatanodesInPipeline(true)
        .build();

    assertDoesNotExist(keyArgs);

    createKeyWithPipeline(keyArgs);

    OmKeyInfo key = keyManager.lookupKey(keyArgs, RESOLVED_BUCKET, null);
    assertEquals(key.getKeyName(), keyName);
    Pipeline keyPipeline =
        key.getLatestVersionLocations().getLocationList().get(0).getPipeline();
    DatanodeDetails leader = keyPipeline.getFirstNode();
    DatanodeDetails follower1 = keyPipeline.getNodes().get(1);
    DatanodeDetails follower2 = keyPipeline.getNodes().get(2);
    assertNotEquals(leader, follower1);
    assertNotEquals(follower1, follower2);

    // lookup key, leader as client
    OmKeyInfo key1 = keyManager.lookupKey(keyArgs, RESOLVED_BUCKET,
        leader.getIpAddress());
    assertEquals(leader, key1.getLatestVersionLocations()
        .getLocationList().get(0).getPipeline().getClosestNode());

    // lookup key, follower1 as client
    OmKeyInfo key2 = keyManager.lookupKey(keyArgs, RESOLVED_BUCKET,
        follower1.getIpAddress());
    assertEquals(follower1, key2.getLatestVersionLocations()
        .getLocationList().get(0).getPipeline().getClosestNode());

    // lookup key, follower2 as client
    OmKeyInfo key3 = keyManager.lookupKey(keyArgs, RESOLVED_BUCKET,
        follower2.getIpAddress());
    assertEquals(follower2, key3.getLatestVersionLocations()
        .getLocationList().get(0).getPipeline().getClosestNode());

    // lookup key, random node as client
    OmKeyInfo key4 = keyManager.lookupKey(keyArgs, RESOLVED_BUCKET,
        "/d=default-drack/127.0.0.1");
    assertThat(keyPipeline.getNodes())
        .containsAll(key4.getLatestVersionLocations()
            .getLocationList().get(0).getPipeline().getNodesInOrder());
  }

  private static void createKeyWithPipeline(OmKeyArgs keyArgs) throws IOException {
    // create a key
    OpenKeySession keySession = writeClient.createFile(keyArgs, false, false);
    // randomly select 3 datanodes
    List<DatanodeDetails> nodeList = new ArrayList<>();
    for (int i = 0; i <= 2; i++) {
      nodeList.add((DatanodeDetails) scm.getClusterMap().getNode(i, null, null, null, null, 0));
    }
    assumeFalse(nodeList.get(0).equals(nodeList.get(1)));
    assumeFalse(nodeList.get(0).equals(nodeList.get(2)));
    // create a pipeline using 3 datanodes
    Pipeline pipeline = scm.getPipelineManager().createPipeline(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE), nodeList);
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    List<OmKeyLocationInfo> locationList =
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList();
    assertEquals(1, locationList.size());
    long containerID = locationList.get(0).getContainerID();
    locationInfoList.add(
        new OmKeyLocationInfo.Builder().setPipeline(pipeline)
            .setBlockID(new BlockID(containerID,
                locationList.get(0).getLocalID())).build());
    keyArgs.setLocationInfoList(locationInfoList);

    writeClient.commitKey(keyArgs, keySession.getId());
    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerID(containerID).setPipelineID(pipeline.getId()).build();
    List<ContainerWithPipeline> containerWithPipelines = Collections.singletonList(
        new ContainerWithPipeline(containerInfo, pipeline));
    when(mockScmContainerClient.getContainerWithPipelineBatch(
        Collections.singletonList(containerID))).thenReturn(containerWithPipelines);
  }

  @Test
  public void testLatestLocationVersion() throws IOException {
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder(VERSIONED_BUCKET_NAME)
        .setKeyName(keyName)
        .build();

    assertDoesNotExist(keyArgs);

    createKeyWithPipeline(keyArgs);
    assertKeyLocations(keyArgs, 1);

    // overwrite
    OpenKeySession keySession = writeClient.createFile(keyArgs, true, true);
    writeClient.commitKey(keyArgs, keySession.getId());

    OmKeyArgs latestVersionOnly = keyArgs.toBuilder()
        .setLatestVersionLocation(true)
        .build();
    assertKeyLocations(latestVersionOnly, 1);

    OmKeyArgs allVersions = keyArgs.toBuilder()
        .setLatestVersionLocation(false)
        .build();
    assertKeyLocations(allVersions, 2);

    // Test ListKeys (latestLocationVersion is always true for ListKeys)
    List<OmKeyInfo> keyInfos =
        keyManager.listKeys(keyArgs.getVolumeName(), keyArgs.getBucketName(),
            "", keyArgs.getKeyName(), 100).getKeys();
    assertEquals(1, keyInfos.size());
    assertEquals(1, keyInfos.get(0).getKeyLocationVersions().size());
  }

  private void assertDoesNotExist(OmKeyArgs keyArgs) {
    // lookup for a non-existent key
    OMException ex = assertThrows(OMException.class, () -> keyManager.lookupKey(keyArgs, RESOLVED_BUCKET, null));
    assertEquals(OMException.ResultCodes.KEY_NOT_FOUND, ex.getResult());
  }

  private void assertKeyLocations(OmKeyArgs keyArgs, int expectedLocations) throws IOException {
    // Test lookupKey
    OmKeyInfo key = keyManager.lookupKey(keyArgs, RESOLVED_BUCKET, null);
    assertEquals(expectedLocations, key.getKeyLocationVersions().size());

    // Test ListStatus
    List<OzoneFileStatus> fileStatuses =
        keyManager.listStatus(keyArgs, false, "", 100);
    assertEquals(1, fileStatuses.size());
    assertEquals(expectedLocations, fileStatuses.get(0).getKeyInfo().getKeyLocationVersions().size());

    // Test GetFileStatus
    OzoneFileStatus ozoneFileStatus = keyManager.getFileStatus(keyArgs, null);
    assertEquals(expectedLocations, ozoneFileStatus.getKeyInfo().getKeyLocationVersions().size());

    // Test LookupFile
    key = keyManager.lookupFile(keyArgs, null);
    assertEquals(expectedLocations, key.getKeyLocationVersions().size());
  }

  @Test
  public void testListStatusWithTableCache() throws Exception {
    // Inspired by TestOmMetadataManager#testListKeys
    String prefixKeyInDB = "key-d";
    String prefixKeyInCache = "key-c";

    // Add a total of 100 key entries to DB and TableCache (50 entries each)
    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {  // Add to DB
        OMRequestTestUtils.addKeyToTable(false,
            VOLUME_NAME, BUCKET_NAME, prefixKeyInDB + i,
            1000L, RatisReplicationConfig.getInstance(ONE), metadataManager);
      } else {  // Add to TableCache
        OMRequestTestUtils.addKeyToTableCache(
            VOLUME_NAME, BUCKET_NAME, prefixKeyInCache + i,
            RatisReplicationConfig.getInstance(ONE),
            metadataManager);
      }
    }

    OmKeyArgs rootDirArgs = createKeyArgs("");
    // Get entries in both TableCache and DB
    List<OzoneFileStatus> fileStatuses =
        keyManager.listStatus(rootDirArgs, true, "", 1000);
    assertEquals(100, fileStatuses.size());

    // Get entries with startKey=prefixKeyInDB
    fileStatuses =
        keyManager.listStatus(rootDirArgs, true, prefixKeyInDB, 1000);
    assertEquals(50, fileStatuses.size());

    // Get entries with startKey=prefixKeyInCache
    fileStatuses =
        keyManager.listStatus(rootDirArgs, true, prefixKeyInCache, 1000);
    assertEquals(100, fileStatuses.size());

    // Clean up cache by marking those keys in cache as deleted
    for (int i = 1; i <= 100; i += 2) {
      String key = metadataManager
          .getOzoneKey(VOLUME_NAME, BUCKET_NAME, prefixKeyInCache + i);
      metadataManager.getKeyTable(getDefaultBucketLayout())
          .addCacheEntry(new CacheKey<>(key),
              CacheValue.get(2L));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testListStatusWithTableCacheRecursive(boolean enablePath) throws Exception {
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, enablePath);
    String keyNameDir1 = "dir1";
    OmKeyArgs keyArgsDir1 =
        createBuilder().setKeyName(keyNameDir1).build();
    addDirectory(keyArgsDir1);

    String keyNameDir1Subdir1 = "dir1" + OZONE_URI_DELIMITER + "subdir1";
    OmKeyArgs keyArgsDir1Subdir1 =
        createBuilder().setKeyName(keyNameDir1Subdir1).build();
    addDirectory(keyArgsDir1Subdir1);

    String keyNameDir2 = "dir2";
    OmKeyArgs keyArgsDir2 =
        createBuilder().setKeyName(keyNameDir2).build();
    addDirectory(keyArgsDir2);

    OmKeyArgs rootDirArgs = createKeyArgs("");
    // Test listStatus with recursive=false, should only have dirs under root
    List<OzoneFileStatus> fileStatuses =
        keyManager.listStatus(rootDirArgs, false, "", 1000);
    assertEquals(2, fileStatuses.size());

    // Test listStatus with recursive=true, should have dirs under root and
    fileStatuses =
        keyManager.listStatus(rootDirArgs, true, "", 1000);
    assertEquals(3, fileStatuses.size());

    // Add a total of 10 key entries to DB and TableCache under dir1
    String prefixKeyInDB = "key-d";
    String prefixKeyInCache = "key-c";
    for (int i = 1; i <= 10; i++) {
      if (i % 2 == 0) {  // Add to DB
        OMRequestTestUtils.addKeyToTable(false,
            VOLUME_NAME, BUCKET_NAME,
            keyNameDir1Subdir1 + OZONE_URI_DELIMITER + prefixKeyInDB + i,
            1000L, RatisReplicationConfig.getInstance(ONE), metadataManager);
      } else {  // Add to TableCache
        OMRequestTestUtils.addKeyToTableCache(
            VOLUME_NAME, BUCKET_NAME,
            keyNameDir1Subdir1 + OZONE_URI_DELIMITER + prefixKeyInCache + i,
            RatisReplicationConfig.getInstance(ONE),
            metadataManager);
      }
    }

    // Test non-recursive, should return the dir under root
    fileStatuses =
        keyManager.listStatus(rootDirArgs, false, "", 1000);
    assertEquals(2, fileStatuses.size());

    // Test recursive, should return the dir and the keys in it
    fileStatuses =
        keyManager.listStatus(rootDirArgs, true, "", 1000);
    assertEquals(10 + 3, fileStatuses.size());

    // Clean up
    for (int i = 1; i <= 10; i += 2) {
      // Mark TableCache entries as deleted
      // Note that DB entry clean up is handled by cleanupTest()
      String key = metadataManager.getOzoneKey(
          VOLUME_NAME, BUCKET_NAME,
          keyNameDir1Subdir1 + OZONE_URI_DELIMITER + prefixKeyInCache + i);
      metadataManager.getKeyTable(getDefaultBucketLayout())
          .addCacheEntry(new CacheKey<>(key),
              CacheValue.get(2L));
    }
  }

  @Test
  public void testListStatusWithDeletedEntriesInCache() throws Exception {
    String prefixKey = "key-";
    TreeSet<String> existKeySet = new TreeSet<>();
    TreeSet<String> deletedKeySet = new TreeSet<>();

    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        OMRequestTestUtils.addKeyToTable(false,
            VOLUME_NAME, BUCKET_NAME, prefixKey + i,
            1000L, RatisReplicationConfig.getInstance(ONE), metadataManager);
        existKeySet.add(prefixKey + i);
      } else {
        OMRequestTestUtils.addKeyToTableCache(
            VOLUME_NAME, BUCKET_NAME, prefixKey + i,
            RatisReplicationConfig.getInstance(ONE),
            metadataManager);

        String key = metadataManager.getOzoneKey(
            VOLUME_NAME, BUCKET_NAME, prefixKey + i);
        // Mark as deleted in cache.
        metadataManager.getKeyTable(getDefaultBucketLayout())
            .addCacheEntry(new CacheKey<>(key),
                CacheValue.get(2L));
        deletedKeySet.add(key);
      }
    }

    OmKeyArgs rootDirArgs = createKeyArgs("");
    List<OzoneFileStatus> fileStatuses =
        keyManager.listStatus(rootDirArgs, true, "", 1000);
    // Should only get entries that are not marked as deleted.
    assertEquals(50, fileStatuses.size());
    // Test startKey
    fileStatuses =
        keyManager.listStatus(rootDirArgs, true, prefixKey, 1000);
    // Should only get entries that are not marked as deleted.
    assertEquals(50, fileStatuses.size());
    // Verify result
    TreeSet<String> expectedKeys = new TreeSet<>();
    for (OzoneFileStatus fileStatus : fileStatuses) {
      String keyName = fileStatus.getKeyInfo().getKeyName();
      expectedKeys.add(keyName);
      assertThat(keyName).startsWith(prefixKey);
    }
    assertEquals(expectedKeys, existKeySet);

    // Sanity check, existKeySet should not intersect with deletedKeySet.
    assertEquals(0,
        Sets.intersection(existKeySet, deletedKeySet).size());

    // Next, mark half of the entries left as deleted
    boolean doDelete = false;
    for (String key : existKeySet) {
      if (doDelete) {
        String ozoneKey =
            metadataManager.getOzoneKey(VOLUME_NAME, BUCKET_NAME, key);
        metadataManager.getKeyTable(getDefaultBucketLayout())
            .addCacheEntry(new CacheKey<>(ozoneKey),
                CacheValue.get(2L));
        deletedKeySet.add(key);
      }
      doDelete = !doDelete;
    }
    // Update existKeySet
    existKeySet.removeAll(deletedKeySet);

    fileStatuses = keyManager.listStatus(
        rootDirArgs, true, "", 1000);
    // Should only get entries that are not marked as deleted.
    assertEquals(50 / 2, fileStatuses.size());

    // Verify result
    expectedKeys.clear();
    for (OzoneFileStatus fileStatus : fileStatuses) {
      String keyName = fileStatus.getKeyInfo().getKeyName();
      expectedKeys.add(keyName);
      assertThat(keyName).startsWith(prefixKey);
    }
    assertEquals(expectedKeys, existKeySet);

    // Test pagination
    final int batchSize = 5;
    String startKey = "";
    expectedKeys.clear();
    do {
      fileStatuses = keyManager.listStatus(
          rootDirArgs, true, startKey, batchSize);
      // Note fileStatuses will never be empty since we are using the last
      // keyName as the startKey of next batch,
      // the startKey itself will show up in the next batch of results.
      // This is fine as we are using a set to store results.
      for (OzoneFileStatus fileStatus : fileStatuses) {
        startKey = fileStatus.getKeyInfo().getKeyName();
        expectedKeys.add(startKey);
        assertThat(startKey).startsWith(prefixKey);
      }
      // fileStatuses.size() == batchSize indicates there might be another batch
      // fileStatuses.size() < batchSize indicates it is the last batch
    } while (fileStatuses.size() == batchSize);
    assertEquals(expectedKeys, existKeySet);

    // Clean up by marking remaining entries as deleted
    for (String key : existKeySet) {
      String ozoneKey =
          metadataManager.getOzoneKey(VOLUME_NAME, BUCKET_NAME, key);
      metadataManager.getKeyTable(getDefaultBucketLayout())
          .addCacheEntry(new CacheKey<>(ozoneKey),
              CacheValue.get(2L));
      deletedKeySet.add(key);
    }
    // Update existKeySet
    existKeySet.removeAll(deletedKeySet);
    assertThat(existKeySet).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testListStatus(boolean enablePath) throws IOException {
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, enablePath);
    String superDir = RandomStringUtils.secure().nextAlphabetic(5);

    int numDirectories = 5;
    int numFiles = 5;
    // set of directory descendants of root
    Set<String> directorySet = new TreeSet<>();
    // set of file descendants of root
    Set<String> fileSet = new TreeSet<>();
    createDepthTwoDirectory(superDir, numDirectories, numFiles, directorySet,
        fileSet);
    // set of all descendants of root
    Set<String> children = new TreeSet<>(directorySet);
    children.addAll(fileSet);
    // number of entries in the filesystem
    int numEntries = directorySet.size() + fileSet.size();

    OmKeyArgs rootDirArgs = createKeyArgs("");
    List<OzoneFileStatus> fileStatuses =
        keyManager.listStatus(rootDirArgs, true, "", 100);
    // verify the number of status returned is same as number of entries
    assertEquals(numEntries, fileStatuses.size());

    fileStatuses = keyManager.listStatus(rootDirArgs, false, "", 100);
    // the number of immediate children of root is 1
    assertEquals(1, fileStatuses.size());

    // if startKey is the first descendant of the root then listStatus should
    // return all the entries.
    String startKey = children.iterator().next();
    fileStatuses = keyManager.listStatus(rootDirArgs, true,
        startKey.substring(0, startKey.length() - 1), 100);
    assertEquals(numEntries, fileStatuses.size());

    for (String directory : directorySet) {
      // verify status list received for each directory with recursive flag set
      // to false
      OmKeyArgs dirArgs = createKeyArgs(directory);
      fileStatuses = keyManager.listStatus(dirArgs, false, "", 100);
      verifyFileStatus(directory, fileStatuses, directorySet, fileSet, false);

      // verify status list received for each directory with recursive flag set
      // to true
      fileStatuses = keyManager.listStatus(dirArgs, true, "", 100);
      verifyFileStatus(directory, fileStatuses, directorySet, fileSet, true);

      // verify list status call with using the startKey parameter and
      // recursive flag set to false. After every call to listStatus use the
      // latest received file status as the startKey until no more entries are
      // left to list.
      List<OzoneFileStatus> tempFileStatus = null;
      Set<OzoneFileStatus> tmpStatusSet = new HashSet<>();
      do {
        tempFileStatus = keyManager.listStatus(dirArgs, false,
            tempFileStatus != null ?
                tempFileStatus.get(tempFileStatus.size() - 1).getKeyInfo()
                    .getKeyName() : null, 2);
        tmpStatusSet.addAll(tempFileStatus);
      } while (tempFileStatus.size() == 2);
      verifyFileStatus(directory, new ArrayList<>(tmpStatusSet), directorySet,
          fileSet, false);

      // verify list status call with using the startKey parameter and
      // recursive flag set to true. After every call to listStatus use the
      // latest received file status as the startKey until no more entries are
      // left to list.
      tempFileStatus = null;
      tmpStatusSet = new HashSet<>();
      do {
        tempFileStatus = keyManager.listStatus(dirArgs, true,
            tempFileStatus != null ?
                tempFileStatus.get(tempFileStatus.size() - 1).getKeyInfo()
                    .getKeyName() : null, 2);
        tmpStatusSet.addAll(tempFileStatus);
      } while (tempFileStatus.size() == 2);
      verifyFileStatus(directory, new ArrayList<>(tmpStatusSet), directorySet,
          fileSet, true);
    }
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

  @Test
  public void testRefreshPipeline() throws Exception {

    OzoneManager ozoneManager = om;

    StorageContainerLocationProtocol sclProtocolMock = mock(
        StorageContainerLocationProtocol.class);
    OMPerformanceMetrics metrics = mock(OMPerformanceMetrics.class);

    List<Long> containerIDs = new ArrayList<>();
    containerIDs.add(100L);
    containerIDs.add(200L);

    List<ContainerWithPipeline> cps = new ArrayList<>();
    for (Long containerID : containerIDs) {
      ContainerWithPipeline containerWithPipelineMock =
          mock(ContainerWithPipeline.class);
      when(containerWithPipelineMock.getPipeline())
          .thenReturn(getRandomPipeline());

      ContainerInfo ci = mock(ContainerInfo.class);
      when(ci.getContainerID()).thenReturn(containerID);
      when(containerWithPipelineMock.getContainerInfo()).thenReturn(ci);

      cps.add(containerWithPipelineMock);
    }

    when(sclProtocolMock.getContainerWithPipelineBatch(containerIDs))
        .thenReturn(cps);

    ScmClient scmClientMock = mock(ScmClient.class);
    when(scmClientMock.getContainerClient()).thenReturn(sclProtocolMock);

    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo("v1",
        "b1", "k1", RatisReplicationConfig.getInstance(THREE)).build();

    // Add block to key.
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    Pipeline pipeline = getRandomPipeline();

    OmKeyLocationInfo omKeyLocationInfo =
        new OmKeyLocationInfo.Builder().setBlockID(
            new BlockID(100L, 1000L))
            .setOffset(0).setLength(100L).setPipeline(pipeline).build();

    omKeyLocationInfoList.add(omKeyLocationInfo);

    OmKeyLocationInfo omKeyLocationInfo2 =
        new OmKeyLocationInfo.Builder().setBlockID(
            new BlockID(200L, 1000L))
            .setOffset(0).setLength(100L).setPipeline(pipeline).build();
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfo omKeyLocationInfo3 =
        new OmKeyLocationInfo.Builder().setBlockID(
            new BlockID(100L, 2000L))
            .setOffset(0).setLength(100L).setPipeline(pipeline).build();
    omKeyLocationInfoList.add(omKeyLocationInfo3);

    omKeyInfo.appendNewBlocks(omKeyLocationInfoList, false);

    KeyManagerImpl keyManagerImpl =
        new KeyManagerImpl(ozoneManager, scmClientMock, conf, metrics);

    keyManagerImpl.refresh(omKeyInfo);

    verify(sclProtocolMock, times(1))
        .getContainerWithPipelineBatch(containerIDs);

  }

  @Test
  public void testRefreshPipelineException() throws Exception {

    OzoneManager ozoneManager = om;

    String errorMessage = "Cannot find container!!";
    StorageContainerLocationProtocol sclProtocolMock = mock(
        StorageContainerLocationProtocol.class);
    doThrow(new IOException(errorMessage)).when(sclProtocolMock)
        .getContainerWithPipelineBatch(any());

    ScmClient scmClientMock = mock(ScmClient.class);
    when(scmClientMock.getContainerClient()).thenReturn(sclProtocolMock);
    OMPerformanceMetrics metrics = mock(OMPerformanceMetrics.class);

    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo("v1",
        "b1", "k1", RatisReplicationConfig.getInstance(THREE)).build();

    // Add block to key.
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    Pipeline pipeline = getRandomPipeline();

    OmKeyLocationInfo omKeyLocationInfo =
        new OmKeyLocationInfo.Builder().setBlockID(
            new BlockID(100L, 1000L))
            .setOffset(0).setLength(100L).setPipeline(pipeline).build();
    omKeyLocationInfoList.add(omKeyLocationInfo);
    omKeyInfo.appendNewBlocks(omKeyLocationInfoList, false);

    KeyManagerImpl keyManagerImpl =
        new KeyManagerImpl(ozoneManager, scmClientMock, conf, metrics);

    OMException omEx = assertThrows(OMException.class,
        () -> keyManagerImpl.refresh(omKeyInfo));
    assertEquals(SCM_GET_PIPELINE_EXCEPTION, omEx.getResult());
    assertEquals(errorMessage, omEx.getMessage());
  }

  @Test
  void testGetAllPartsWhenZeroPartNumber() throws IOException {
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);

    String volume = VOLUME_NAME;

    initKeyTableForMultipartTest(keyName, volume);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volume)
            .setBucketName(BUCKET_NAME)
            .setKeyName(keyName)
            .setMultipartUploadPartNumber(0)
            .build();
    OmKeyInfo omKeyInfo = keyManager.getKeyInfo(keyArgs, RESOLVED_BUCKET, "test");
    assertEquals(keyName, omKeyInfo.getKeyName());
    assertNotNull(omKeyInfo.getLatestVersionLocations());

    List<OmKeyLocationInfo> locationList = omKeyInfo.getLatestVersionLocations().getLocationList();
    assertNotNull(locationList);
    assertEquals(5, locationList.size());
    for (int i = 0; i < 5; i++) {
      assertEquals(i, locationList.get(i).getPartNumber());
    }
  }

  @Test
  void testGetParticularPart() throws IOException {
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);

    String volume = VOLUME_NAME;

    initKeyTableForMultipartTest(keyName, volume);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volume)
            .setBucketName(BUCKET_NAME)
            .setKeyName(keyName)
            .setMultipartUploadPartNumber(3)
            .build();
    OmKeyInfo omKeyInfo = keyManager.getKeyInfo(keyArgs, RESOLVED_BUCKET, "test");
    assertEquals(keyName, omKeyInfo.getKeyName());
    assertNotNull(omKeyInfo.getLatestVersionLocations());

    List<OmKeyLocationInfo> locationList = omKeyInfo.getLatestVersionLocations().getLocationList();
    assertNotNull(locationList);
    assertEquals(1, locationList.size());
    assertEquals(3, locationList.get(0).getPartNumber());
  }

  @Test
  void testGetNotExistedPart() throws IOException {
    String keyName = RandomStringUtils.secure().nextAlphabetic(5);

    String volume = VOLUME_NAME;

    initKeyTableForMultipartTest(keyName, volume);

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volume)
            .setBucketName(BUCKET_NAME)
            .setKeyName(keyName)
            .setMultipartUploadPartNumber(99)
            .build();
    OmKeyInfo omKeyInfo = keyManager.getKeyInfo(keyArgs, RESOLVED_BUCKET, "test");
    assertEquals(keyName, omKeyInfo.getKeyName());
    assertNotNull(omKeyInfo.getLatestVersionLocations());

    List<OmKeyLocationInfo> locationList = omKeyInfo.getLatestVersionLocations().getLocationList();
    assertNotNull(locationList);
    assertEquals(0, locationList.size());
  }

  private OmKeyInfo getMockedOmKeyInfo(OmBucketInfo bucketInfo, long parentId, String key, long objectId) {
    OmKeyInfo omKeyInfo = mock(OmKeyInfo.class);
    if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
      when(omKeyInfo.getFileName()).thenReturn(key);
      when(omKeyInfo.getParentObjectID()).thenReturn(parentId);
    } else {
      when(omKeyInfo.getKeyName()).thenReturn(key);
    }
    when(omKeyInfo.getObjectID()).thenReturn(objectId);
    return omKeyInfo;
  }

  private OmDirectoryInfo getMockedOmDirInfo(long parentId, String key, long objectId) {
    OmDirectoryInfo omKeyInfo = mock(OmDirectoryInfo.class);
    when(omKeyInfo.getName()).thenReturn(key);
    when(omKeyInfo.getParentObjectID()).thenReturn(parentId);
    when(omKeyInfo.getObjectID()).thenReturn(objectId);
    return omKeyInfo;
  }

  private String getDirectoryKey(long volumeId, OmBucketInfo bucketInfo, OmKeyInfo omKeyInfo) {
    if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
      return volumeId + "/" + bucketInfo.getObjectID() + "/" + omKeyInfo.getParentObjectID() + "/" +
          omKeyInfo.getFileName();
    } else {
      return bucketInfo.getVolumeName() + "/" + bucketInfo.getBucketName() + "/" + omKeyInfo.getKeyName();
    }
  }

  private String getDirectoryKey(long volumeId, OmBucketInfo bucketInfo, OmDirectoryInfo omDirInfo) {
    return volumeId + "/" + bucketInfo.getObjectID() + "/" + omDirInfo.getParentObjectID() + "/" +
        omDirInfo.getName();
  }

  private String getRenameKey(String volume, String bucket, long objectId) {
    return volume + "/" + bucket + "/" + objectId;
  }

  @ParameterizedTest
  @EnumSource(value = BucketLayout.class)
  public void testPreviousSnapshotOzoneKeyInfo(BucketLayout bucketLayout) throws IOException {
    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    if (bucketLayout.isFileSystemOptimized()) {
      when(omMetadataManager.getOzonePathKey(anyLong(), anyLong(), anyLong(), anyString()))
          .thenAnswer(i -> Arrays.stream(i.getArguments()).map(Object::toString)
              .collect(Collectors.joining("/")));
    } else {
      when(omMetadataManager.getOzoneKey(anyString(), anyString(), anyString()))
          .thenAnswer(i -> Arrays.stream(i.getArguments()).map(Object::toString)
              .collect(Collectors.joining("/")));
    }
    when(omMetadataManager.getRenameKey(anyString(), anyString(), anyLong())).thenAnswer(
        i -> getRenameKey(i.getArgument(0), i.getArgument(1), i.getArgument(2)));

    OMMetadataManager previousMetadataManager = mock(OMMetadataManager.class);
    OzoneConfiguration configuration = new OzoneConfiguration();
    KeyManagerImpl km = new KeyManagerImpl(null, null, omMetadataManager, configuration, null, null, null);
    KeyManagerImpl prevKM = new KeyManagerImpl(null, null, previousMetadataManager, configuration, null, null, null);
    long volumeId = 1L;
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder().setBucketName(BUCKET_NAME).setVolumeName(VOLUME_NAME)
        .setObjectID(2L).setBucketLayout(bucketLayout).build();
    OmKeyInfo prevKey = getMockedOmKeyInfo(bucketInfo, 5, "key", 1);
    OmKeyInfo prevKey2 = getMockedOmKeyInfo(bucketInfo, 7, "key2", 2);
    OmKeyInfo currentKey = getMockedOmKeyInfo(bucketInfo, 6, "renamedKey", 1);
    OmKeyInfo currentKey2 = getMockedOmKeyInfo(bucketInfo, 7, "key2", 2);
    OmKeyInfo currentKey3 = getMockedOmKeyInfo(bucketInfo, 8, "key3", 3);
    OmKeyInfo currentKey4 = getMockedOmKeyInfo(bucketInfo, 8, "key4", 4);
    Table<String, OmKeyInfo> prevKeyTable =
        new InMemoryTestTable<>(ImmutableMap.of(
            getDirectoryKey(volumeId, bucketInfo, prevKey), prevKey,
            getDirectoryKey(volumeId, bucketInfo, prevKey2), prevKey2));
    Table<String, String> renameTable = new InMemoryTestTable<>(
        ImmutableMap.of(getRenameKey(VOLUME_NAME, BUCKET_NAME, 1), getDirectoryKey(volumeId, bucketInfo, prevKey),
            getRenameKey(VOLUME_NAME, BUCKET_NAME, 3), getDirectoryKey(volumeId, bucketInfo,
                getMockedOmKeyInfo(bucketInfo, 6, "unknownKey", 9))));
    when(previousMetadataManager.getKeyTable(eq(bucketLayout))).thenReturn(prevKeyTable);
    when(omMetadataManager.getSnapshotRenamedTable()).thenReturn(renameTable);
    assertEquals(prevKey, km.getPreviousSnapshotOzoneKeyInfo(volumeId, bucketInfo, currentKey).apply(prevKM));
    assertEquals(prevKey2, km.getPreviousSnapshotOzoneKeyInfo(volumeId, bucketInfo, currentKey2).apply(prevKM));
    assertNull(km.getPreviousSnapshotOzoneKeyInfo(volumeId, bucketInfo, currentKey3).apply(prevKM));
    assertNull(km.getPreviousSnapshotOzoneKeyInfo(volumeId, bucketInfo, currentKey4).apply(prevKM));
  }

  @Test
  public void testPreviousSnapshotOzoneDirInfo() throws IOException {
    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    when(omMetadataManager.getOzonePathKey(anyLong(), anyLong(), anyLong(), anyString()))
        .thenAnswer(i -> Arrays.stream(i.getArguments()).map(Object::toString)
            .collect(Collectors.joining("/")));
    when(omMetadataManager.getRenameKey(anyString(), anyString(), anyLong())).thenAnswer(
        i -> getRenameKey(i.getArgument(0), i.getArgument(1), i.getArgument(2)));

    OMMetadataManager previousMetadataManager = mock(OMMetadataManager.class);
    OzoneConfiguration configuration = new OzoneConfiguration();
    KeyManagerImpl km = new KeyManagerImpl(null, null, omMetadataManager, configuration, null, null, null);
    KeyManagerImpl prevKM = new KeyManagerImpl(null, null, previousMetadataManager, configuration, null, null, null);
    long volumeId = 1L;
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder().setBucketName(BUCKET_NAME).setVolumeName(VOLUME_NAME)
        .setObjectID(2L).setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build();
    OmDirectoryInfo prevKey = getMockedOmDirInfo(5, "key", 1);
    OmDirectoryInfo prevKey2 = getMockedOmDirInfo(7, "key2", 2);
    OmKeyInfo currentKey =  getMockedOmKeyInfo(bucketInfo, 6, "renamedKey", 1);
    OmDirectoryInfo currentKeyDir = getMockedOmDirInfo(6, "renamedKey", 1);
    OmKeyInfo currentKey2 = getMockedOmKeyInfo(bucketInfo, 7, "key2", 2);
    OmDirectoryInfo currentKeyDir2 = getMockedOmDirInfo(7, "key2", 2);
    OmKeyInfo currentKey3 = getMockedOmKeyInfo(bucketInfo, 8, "key3", 3);
    OmDirectoryInfo currentKeyDir3 = getMockedOmDirInfo(8, "key3", 3);
    OmKeyInfo currentKey4 = getMockedOmKeyInfo(bucketInfo, 8, "key4", 4);
    OmDirectoryInfo currentKeyDir4 = getMockedOmDirInfo(8, "key4", 4);
    Table<String, OmDirectoryInfo> prevDirTable = new InMemoryTestTable<>(
        ImmutableMap.of(getDirectoryKey(volumeId, bucketInfo, prevKey), prevKey,
            getDirectoryKey(volumeId, bucketInfo, prevKey2), prevKey2));
    Table<String, String> renameTable = new InMemoryTestTable<>(
        ImmutableMap.of(getRenameKey(VOLUME_NAME, BUCKET_NAME, 1),
            getDirectoryKey(volumeId, bucketInfo, prevKey),
        getRenameKey(VOLUME_NAME, BUCKET_NAME, 3), getDirectoryKey(volumeId, bucketInfo,
            getMockedOmKeyInfo(bucketInfo, 6, "unknownKey", 9))));
    when(previousMetadataManager.getDirectoryTable()).thenReturn(prevDirTable);
    when(omMetadataManager.getSnapshotRenamedTable()).thenReturn(renameTable);
    assertEquals(prevKey, km.getPreviousSnapshotOzoneDirInfo(volumeId, bucketInfo, currentKey).apply(prevKM));
    assertEquals(prevKey2, km.getPreviousSnapshotOzoneDirInfo(volumeId, bucketInfo, currentKey2).apply(prevKM));
    assertNull(km.getPreviousSnapshotOzoneDirInfo(volumeId, bucketInfo, currentKey3).apply(prevKM));
    assertNull(km.getPreviousSnapshotOzoneDirInfo(volumeId, bucketInfo, currentKey4).apply(prevKM));

    assertEquals(prevKey, km.getPreviousSnapshotOzoneDirInfo(volumeId, bucketInfo, currentKeyDir).apply(prevKM));
    assertEquals(prevKey2, km.getPreviousSnapshotOzoneDirInfo(volumeId, bucketInfo, currentKeyDir2).apply(prevKM));
    assertNull(km.getPreviousSnapshotOzoneDirInfo(volumeId, bucketInfo, currentKeyDir3).apply(prevKM));
    assertNull(km.getPreviousSnapshotOzoneDirInfo(volumeId, bucketInfo, currentKeyDir4).apply(prevKM));
  }

  private void initKeyTableForMultipartTest(String keyName, String volume) throws IOException {
    List<OmKeyLocationInfoGroup> locationInfoGroups = new ArrayList<>();
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      OmKeyLocationInfo locationInfo1 = new OmKeyLocationInfo.Builder()
              .setBlockID(new BlockID(i, i))
              .setPartNumber(i)
              .build();
      locationInfoList.add(locationInfo1);
    }

    OmKeyLocationInfoGroup locationInfoGroup = new OmKeyLocationInfoGroup(0, locationInfoList);
    locationInfoGroups.add(locationInfoGroup);
    locationInfoGroup.setMultipartKey(true);

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
            .setKeyName(keyName)
            .setBucketName(BUCKET_NAME)
            .setVolumeName(volume)
            .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
            .setOmKeyLocationInfos(locationInfoGroups)
            .build();

    String key = String.format("/%s/%s/%s", volume, BUCKET_NAME, keyName);
    metadataManager.getKeyTable(BucketLayout.LEGACY).put(key, omKeyInfo);
  }

    /**
   * Get Random pipeline.
   * @return pipeline
   */
  private Pipeline getRandomPipeline() {
    return Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setNodes(new ArrayList<>())
        .build();
  }

  /**
   * Creates a depth two directory.
   *
   * @param superDir       Super directory to create
   * @param numDirectories number of directory children
   * @param numFiles       number of file children
   * @param directorySet   set of descendant directories for the super directory
   * @param fileSet        set of descendant files for the super directory
   */
  private void createDepthTwoDirectory(String superDir, int numDirectories,
      int numFiles, Set<String> directorySet, Set<String> fileSet)
      throws IOException {
    // create super directory
    OmKeyArgs superDirArgs = createKeyArgs(superDir);
    writeClient.createDirectory(superDirArgs);
    directorySet.add(superDir);

    // add directory children to super directory
    Set<String> childDirectories =
        createDirectories(superDir, new HashMap<>(), numDirectories);
    directorySet.addAll(childDirectories);
    // add file to super directory
    fileSet.addAll(createFiles(superDir, new HashMap<>(), numFiles));

    // for each child directory create files and directories
    for (String child : childDirectories) {
      fileSet.addAll(createFiles(child, new HashMap<>(), numFiles));
      directorySet
          .addAll(createDirectories(child, new HashMap<>(), numDirectories));
    }
  }

  private void verifyFileStatus(String directory,
      List<OzoneFileStatus> fileStatuses, Set<String> directorySet,
      Set<String> fileSet, boolean recursive) {

    for (OzoneFileStatus fileStatus : fileStatuses) {
      String normalizedKeyName = fileStatus.getTrimmedName();
      if (!recursive) {
        Path parent = Paths.get(fileStatus.getKeyInfo().getKeyName()).getParent();
        // if recursive is false, verify all the statuses have the input
        // directory as parent
        assertNotNull(parent);
        assertEquals(directory, parent.toString());
      }
      // verify filestatus is present in directory or file set accordingly
      if (fileStatus.isDirectory()) {
        assertThat(directorySet).withFailMessage(directorySet +
            " doesn't contain " + normalizedKeyName).contains(normalizedKeyName);
      } else {
        assertThat(fileSet).withFailMessage(fileSet + " doesn't contain " + normalizedKeyName)
            .contains(normalizedKeyName);
      }
    }

    // count the number of entries which should be present in the directory
    int numEntries = 0;
    Set<String> entrySet = new TreeSet<>(directorySet);
    entrySet.addAll(fileSet);
    for (String entry : entrySet) {
      if (OzoneFSUtils.getParent(entry)
          .startsWith(OzoneFSUtils.addTrailingSlashIfNeeded(directory))) {
        if (recursive) {
          numEntries++;
        } else if (OzoneFSUtils.getParent(entry)
            .equals(OzoneFSUtils.addTrailingSlashIfNeeded(directory))) {
          numEntries++;
        }
      }
    }
    // verify the number of entries match the status list size
    assertEquals(fileStatuses.size(), numEntries);
  }

  private Set<String> createDirectories(String parent,
      Map<String, List<String>> directoryMap, int numDirectories)
      throws IOException {
    Set<String> keyNames = new TreeSet<>();
    for (int i = 0; i < numDirectories; i++) {
      String keyName = parent + "/" + RandomStringUtils.secure().nextAlphabetic(5);
      OmKeyArgs keyArgs = createBuilder().setKeyName(keyName).build();
      writeClient.createDirectory(keyArgs);
      keyNames.add(keyName);
    }
    directoryMap.put(parent, new ArrayList<>(keyNames));
    return keyNames;
  }

  private List<String> createFiles(String parent,
      Map<String, List<String>> fileMap, int numFiles) throws IOException {
    List<String> keyNames = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      String keyName = parent + "/" + RandomStringUtils.secure().nextAlphabetic(5);
      OmKeyArgs keyArgs = createBuilder().setKeyName(keyName).build();
      OpenKeySession keySession = writeClient.createFile(keyArgs, false, false);
      keyArgs.setLocationInfoList(
          keySession.getKeyInfo().getLatestVersionLocations()
              .getLocationList());
      writeClient.commitKey(keyArgs, keySession.getId());
      keyNames.add(keyName);
    }
    fileMap.put(parent, keyNames);
    return keyNames;
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

  private static void addDirectory(OmKeyArgs keyArgs) throws Exception {
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName() + "/")
        .setOmKeyLocationInfos(null)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationConfig(keyArgs.getReplicationConfig())
        .setFileEncryptionInfo(null).build();
    OMRequestTestUtils.addKeyToTable(false, false, omKeyInfo,
        1000L, 0L, metadataManager);
  }
}
