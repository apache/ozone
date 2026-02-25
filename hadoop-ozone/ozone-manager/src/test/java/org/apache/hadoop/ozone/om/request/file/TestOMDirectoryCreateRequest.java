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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test OM directory create request.
 */
public class TestOMDirectoryCreateRequest {

  @TempDir
  private Path folder;

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.getConfig()).thenReturn(ozoneConfiguration.getObject(OmConfig.class));
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    AuditLogger auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    when(ozoneManager.resolveBucketLink(any(KeyArgs.class),
        any(OMClientRequest.class)))
        .thenReturn(new ResolvedBucket("", "",
            "", "", "",
            BucketLayout.DEFAULT));
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(lvm.isAllowed(any(LayoutFeature.class))).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  @ParameterizedTest
  @ValueSource(strings = {"a/b/c", "a/.snapshot/c", "a.snapshot/b/c"})
  public void testPreExecute(String keyName) throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    // As in preExecute, we modify the original request.
    assertNotEquals(omRequest, modifiedOmRequest);
  }

  // Test verifies that .snapshot is not allowed as root dir name.
  @Test
  public void testPreExecuteFailure() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = ".snapshot/a/b/c";

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMException omException = assertThrows(OMException.class,
        () -> omDirectoryCreateRequest.preExecute(ozoneManager));
    assertEquals(
        "Cannot create key under path reserved for snapshot: .snapshot/",
        omException.getMessage());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = genRandomKeyName();

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());
    omDirectoryCreateRequest.setUGI(UserGroupInformation.getCurrentUser());
    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.OK);
    assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(
            omMetadataManager.getOzoneDirKey(volumeName, bucketName, keyName)));

    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));
    assertEquals(OzoneFSUtils.getFileCount(keyName), bucketInfo.getUsedNamespace());
  }

  @Test
  public void testValidateAndUpdateCacheWithNamespaceQuotaExceed()
      throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = "test/" + genRandomKeyName();

    // Add volume and bucket entries to DB with quota
    // create bucket with quota limit 1
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setQuotaInNamespace(1));

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());
    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());
    omDirectoryCreateRequest.setUGI(UserGroupInformation.getCurrentUser());
    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED);
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = genRandomKeyName();

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());

    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(VOLUME_NOT_FOUND, omClientResponse.getOMResponse().getStatus());

    // Key should not exist in DB
    assertNull(omMetadataManager.getKeyTable(getBucketLayout()).
        get(omMetadataManager.getOzoneDirKey(volumeName, bucketName, keyName)));

  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = genRandomKeyName();

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

    // Key should not exist in DB
    assertNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(
            omMetadataManager.getOzoneDirKey(volumeName, bucketName, keyName)));
  }

  @Test
  public void testValidateAndUpdateCacheWithSubDirectoryInPath()
      throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = genRandomKeyName();

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        keyName.substring(0, 12), 1L, RatisReplicationConfig.getInstance(ONE), omMetadataManager);
    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());
    omDirectoryCreateRequest.setUGI(UserGroupInformation.getCurrentUser());
    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.OK);

    // Key should exist in DB and cache.
    assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(
            omMetadataManager.getOzoneDirKey(volumeName, bucketName, keyName)));
    assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
        .getCacheValue(new CacheKey<>(
            omMetadataManager.getOzoneDirKey(volumeName, bucketName,
                keyName))));

  }

  @Test
  public void testValidateAndUpdateCacheWithDirectoryAlreadyExists()
      throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = genRandomKeyName();

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        OzoneFSUtils.addTrailingSlashIfNeeded(keyName), 1L,
        RatisReplicationConfig.getInstance(ONE),
        omMetadataManager);
    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());

    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.DIRECTORY_ALREADY_EXISTS);

    // Key should exist in DB
    assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(
            omMetadataManager.getOzoneDirKey(volumeName, bucketName, keyName)));

    // As it already exists, it should not be in cache.
    assertNull(omMetadataManager.getKeyTable(getBucketLayout())
        .getCacheValue(new CacheKey<>(
            omMetadataManager.getOzoneDirKey(volumeName, bucketName,
                keyName))));

  }

  @Test
  public void testValidateAndUpdateCacheWithFilesInPath() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = genRandomKeyName();

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    // Add a key with first two levels.
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName,
        keyName.substring(0, 11), 1L, RatisReplicationConfig.getInstance(ONE), omMetadataManager);
    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());

    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS);

    // Key should not exist in DB
    assertNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(
            omMetadataManager.getOzoneDirKey(volumeName, bucketName, keyName)));

  }

  @Test
  public void testCreateDirectoryOMMetric()
      throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = genRandomKeyName();

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        OzoneFSUtils.addTrailingSlashIfNeeded(keyName));
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());
    omDirectoryCreateRequest.setUGI(UserGroupInformation.getCurrentUser());

    assertEquals(0L, omMetrics.getNumKeys());
    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(omMetadataManager.getOzoneDirKey(
            volumeName, bucketName, keyName)));

    assertEquals(4L, omMetrics.getNumKeys());
  }

  @Test
  public void testCreateDirectoryInheritParentDefaultAcls() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = genRandomKeyName();

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

    // Create sub dirs
    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest =
        omDirectoryCreateRequest.preExecute(ozoneManager);

    omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());
    omDirectoryCreateRequest.setUGI(UserGroupInformation.getCurrentUser());
    OMClientResponse omClientResponse =
        omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.OK);

    // Verify sub dirs inherit parent DEFAULT acls.
    verifyDirectoriesInheritAcls(volumeName, bucketName, keyName, bucketAcls);

  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testIgnoreClientACL(boolean ignoreClientACLs) throws Exception {
    ozoneManager.getConfig().setIgnoreClientACLs(ignoreClientACLs);

    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = genRandomKeyName();

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    String ozoneAll = "user:ozone:a";
    List<OzoneAcl> aclList = new ArrayList<>();
    aclList.add(OzoneAcl.parseAcl(ozoneAll));
    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        OzoneFSUtils.addTrailingSlashIfNeeded(keyName), aclList);
    OMDirectoryCreateRequest omDirectoryCreateRequest =
        new OMDirectoryCreateRequest(omRequest, getBucketLayout());

    OMRequest modifiedOmRequest = omDirectoryCreateRequest.preExecute(ozoneManager);
    omDirectoryCreateRequest = new OMDirectoryCreateRequest(modifiedOmRequest, getBucketLayout());
    omDirectoryCreateRequest.setUGI(UserGroupInformation.getCurrentUser());

    OMClientResponse omClientResponse = omDirectoryCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omClientResponse.getOMResponse().getStatus());

    OmKeyInfo keyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(
        omMetadataManager.getOzoneDirKey(volumeName, bucketName, keyName));

    if (ignoreClientACLs) {
      assertFalse(keyInfo.getAcls().contains(OzoneAcl.parseAcl(ozoneAll)));
    } else {
      assertTrue(keyInfo.getAcls().contains(OzoneAcl.parseAcl(ozoneAll)));
    }
  }

  private void verifyDirectoriesInheritAcls(String volumeName,
      String bucketName, String keyName, List<OzoneAcl> bucketAcls)
      throws IOException {
    List<String> nodes = Arrays.asList(keyName.split(OZONE_URI_DELIMITER));

    List<OzoneAcl> expectedInheritAcls = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.DEFAULT)
        .collect(Collectors.toList());
    String prefix = "";

    for (String node : nodes) {
      String dirName = prefix + node;
      OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .get(omMetadataManager
              .getOzoneDirKey(volumeName, bucketName, dirName));

      List<OzoneAcl> omKeyAcls = omKeyInfo.getAcls();

      assertTrue(omKeyAcls.containsAll(expectedInheritAcls), "Failed to inherit parent acls!,");

      prefix = dirName + OZONE_URI_DELIMITER;
      expectedInheritAcls = omKeyAcls;
    }
  }

  private OMRequest createDirectoryRequest(String volumeName, String bucketName, String keyName) {
    return createDirectoryRequest(volumeName, bucketName, keyName, null);
  }

  /**
   * Create OMRequest which encapsulates CreateDirectory request.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param acls
   * @return OMRequest
   */
  private OMRequest createDirectoryRequest(String volumeName, String bucketName,
      String keyName, List<OzoneAcl> acls) {
    KeyArgs.Builder builder = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName);
    if (acls != null) {
      for (OzoneAcl acl : acls) {
        builder.addAcls(OzoneAcl.toProtobuf(acl));
      }
    }
    return OMRequest.newBuilder()
        .setCreateDirectoryRequest(CreateDirectoryRequest.newBuilder().setKeyArgs(builder))
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateDirectory)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  private String genRandomKeyName() {
    StringBuilder keyNameBuilder = new StringBuilder();
    keyNameBuilder.append(RandomStringUtils.secure().nextAlphabetic(5));
    for (int i = 0; i < 3; i++) {
      keyNameBuilder.append('/').append(RandomStringUtils.secure().nextAlphabetic(5));
    }
    return keyNameBuilder.toString();
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }
}
