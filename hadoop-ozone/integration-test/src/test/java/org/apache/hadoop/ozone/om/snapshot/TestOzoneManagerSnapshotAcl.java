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

package org.apache.hadoop.ozone.om.snapshot;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.ADMIN;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for Snapshot feature with ACL.
 */
public class TestOzoneManagerSnapshotAcl {

  private static final String ADMIN_USER = "om";
  private static final String ADMIN_GROUP = "ozone";
  private static final UserGroupInformation ADMIN_UGI =
      UserGroupInformation
          .createUserForTesting(ADMIN_USER, new String[] {ADMIN_GROUP});
  private static final String USER1 = "user1";
  private static final String GROUP1 = "group1";
  private static final UserGroupInformation UGI1 =
      UserGroupInformation.createUserForTesting(USER1, new String[] {GROUP1});
  private static final String USER2 = "user2";
  private static final String GROUP2 = "group2";
  private static final UserGroupInformation UGI2 =
      UserGroupInformation.createUserForTesting(USER2, new String[] {GROUP2});
  private static final String USER3 = "user3";
  private static final String GROUP3 = "group3";
  private static final UserGroupInformation UGI3 =
      UserGroupInformation.createUserForTesting(USER3, new String[] {GROUP3});
  private static final OzoneObj.ResourceType RESOURCE_TYPE_KEY =
      OzoneObj.ResourceType.KEY;
  private static MiniOzoneCluster cluster;
  private static ObjectStore objectStore;
  private static OzoneManager ozoneManager;
  private static OzoneClient client;
  private String volumeName;
  private String bucketName;
  private static final String DIR_PREFIX = "dir1/";
  private static final String KEY_PREFIX = DIR_PREFIX + "key-";
  private String keyName;
  private String snapshotKeyPrefix;

  @BeforeAll
  public static void init() throws Exception {
    UserGroupInformation.setLoginUser(ADMIN_UGI);
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);

    final String omServiceId = "om-service-test-1"
        + RandomStringUtils.secure().nextNumeric(32);

    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, omServiceId);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(1)
        .build();
    cluster.waitForClusterToBeReady();

    final String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + omServiceId;
    final OzoneConfiguration clientConf =
        new OzoneConfiguration(cluster.getConf());
    clientConf.set(FS_DEFAULT_NAME_KEY, hostPrefix);

    client = cluster.newClient();
    objectStore = client.getObjectStore();

    ozoneManager = cluster.getOzoneManager();
    final KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");

    // stop the deletion services so that keys can still be read
    keyManager.stop();
    OMStorage.getOmDbDir(cluster.getConf());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testLookupKeyWithAllowedUser(BucketLayout bucketLayout)
      throws Exception {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);

    // WHEN-THEN
    assertDoesNotThrow(() -> ozoneManager.lookupKey(snapshotKeyArgs));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testLookupKeyWithNotAllowedUser(BucketLayout bucketLayout)
      throws Exception {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);
    final OmKeyArgs keyArgs = getOmKeyArgs(false);

    // when reading from snapshot, read disallowed.
    UserGroupInformation.setLoginUser(UGI2);
    final OMException ex = assertThrows(OMException.class,
        () -> ozoneManager.lookupKey(snapshotKeyArgs));

    // THEN
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
        ex.getResult());
    // when same user reads same key from active fs, read allowed.
    assertDoesNotThrow(() -> ozoneManager.lookupKey(keyArgs));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testGeyKeyInfoWithAllowedUser(BucketLayout bucketLayout)
      throws IOException {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);
    final boolean assumeS3Context = false;

    // WHEN-THEN
    assertDoesNotThrow(
        () -> ozoneManager.getKeyInfo(snapshotKeyArgs, assumeS3Context));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testGeyKeyInfoWithNotAllowedUser(BucketLayout bucketLayout)
      throws IOException {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyOmKeyArgs = getOmKeyArgs(true);
    final OmKeyArgs omKeyArgs = getOmKeyArgs(false);
    final boolean assumeS3Context = false;

    // when reading from snapshot, read disallowed.
    UserGroupInformation.setLoginUser(UGI2);
    final OMException ex =
        assertThrows(OMException.class,
            () -> ozoneManager.getKeyInfo(snapshotKeyOmKeyArgs,
                assumeS3Context));

    // THEN
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
        ex.getResult());
    // when same user reads same key from active fs, read allowed.
    assertDoesNotThrow(
        () -> ozoneManager.getKeyInfo(omKeyArgs, assumeS3Context));
  }

  @ParameterizedTest
  @MethodSource("getListStatusArguments")
  public void testListStatusWithAllowedUser(BucketLayout bucketLayout,
      boolean recursive, boolean allowPartialPrefixes)
      throws IOException {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);
    final long numEntries = Long.parseLong(RandomStringUtils.secure().nextNumeric(1));

    // WHEN-THEN
    assertDoesNotThrow(
        () -> ozoneManager.listStatus(snapshotKeyArgs, recursive,
            snapshotKeyArgs.getKeyName(), numEntries,
            allowPartialPrefixes));
  }

  @ParameterizedTest
  @MethodSource("getListStatusArguments")
  public void testListStatusWithNotAllowedUser(BucketLayout bucketLayout,
      boolean recursive, boolean allowPartialPrefixes)
      throws IOException {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);
    final OmKeyArgs keyArgs = getOmKeyArgs(false);
    final long numEntries = Long.parseLong(RandomStringUtils.secure().nextNumeric(1));

    // when reading from snapshot, read disallowed.
    UserGroupInformation.setLoginUser(UGI2);
    final OMException ex =
        assertThrows(OMException.class,
            () -> ozoneManager.listStatus(snapshotKeyArgs, recursive,
                snapshotKeyArgs.getKeyName(), numEntries,
                allowPartialPrefixes));

    // THEN
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
        ex.getResult());
    // when same user reads same key from active fs, read allowed.
    assertDoesNotThrow(() -> ozoneManager.listStatus(keyArgs,
        recursive, keyName, numEntries, allowPartialPrefixes));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testLookupFileWithAllowedUser(BucketLayout bucketLayout)
      throws Exception {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);

    // WHEN-THEN
    assertDoesNotThrow(
        () -> ozoneManager.lookupFile(snapshotKeyArgs));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testLookupFileWithNotAllowedUser(BucketLayout bucketLayout)
      throws Exception {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);
    final OmKeyArgs keyArgs = getOmKeyArgs(false);

    // when reading from snapshot, read disallowed.
    UserGroupInformation.setLoginUser(UGI2);
    final OMException ex = assertThrows(OMException.class,
        () -> ozoneManager.lookupFile(snapshotKeyArgs));

    // THEN
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
        ex.getResult());
    // when same user reads same key from active fs, read allowed.
    assertDoesNotThrow(() -> ozoneManager.lookupFile(keyArgs));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testListKeysWithAllowedUser(BucketLayout bucketLayout)
      throws Exception {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);
    final int maxKeys = Integer.parseInt(RandomStringUtils.secure().nextNumeric(1));

    // WHEN-THEN
    assertDoesNotThrow(() -> ozoneManager.listKeys(volumeName,
        bucketName, snapshotKeyArgs.getKeyName(), snapshotKeyPrefix, maxKeys));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testListKeysWithNotAllowedUser(BucketLayout bucketLayout)
      throws Exception {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);
    final int maxKeys = Integer.parseInt(RandomStringUtils.secure().nextNumeric(1));

    // WHEN
    UserGroupInformation.setLoginUser(UGI2);

    // THEN
    assertDoesNotThrow(
        () -> ozoneManager.listKeys(volumeName, bucketName,
            snapshotKeyArgs.getKeyName(), snapshotKeyPrefix, maxKeys));
    assertDoesNotThrow(() -> ozoneManager.listKeys(volumeName,
        bucketName, keyName, KEY_PREFIX, maxKeys));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testGetAclWithAllowedUser(BucketLayout bucketLayout)
      throws Exception {
    // GIVEN
    setup(bucketLayout);
    final OmKeyArgs snapshotKeyOmKeyArgs = getOmKeyArgs(true);
    final OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(RESOURCE_TYPE_KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(snapshotKeyOmKeyArgs.getKeyName())
        .build();

    // WHEN-THEN
    assertDoesNotThrow(() -> ozoneManager.getAcl(ozoneObj));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testGetAclWithNotAllowedUser(BucketLayout bucketLayout)
      throws Exception {
    // GIVEN
    setup(bucketLayout);
    final OzoneObj snapshotObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(RESOURCE_TYPE_KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(snapshotKeyPrefix + keyName)
        .build();
    final OzoneObj keyObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(RESOURCE_TYPE_KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();

    // when reading from snapshot, read disallowed.
    UserGroupInformation.setLoginUser(UGI2);
    final OMException ex = assertThrows(OMException.class,
        () -> ozoneManager.getAcl(snapshotObj));

    // THEN
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
        ex.getResult());
    // when same user reads same key from active fs, read allowed.
    assertDoesNotThrow(() -> ozoneManager.getAcl(keyObj));
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testLookupKeyWithAllowedUserForPrefixAcl(BucketLayout bucketLayout) throws Exception {
    UserGroupInformation.setLoginUser(UGI1);

    createVolume();

    final OzoneVolume volume = objectStore.getVolume(volumeName);
    createBucket(bucketLayout, volume);

    final OzoneBucket bucket = volume.getBucket(bucketName);

    setDefaultPrefixAcls();

    createKey(bucket);

    setDefaultVolumeAcls();
    setDefaultBucketAcls();

    createSnapshot();

    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);
    assertDoesNotThrow(() -> ozoneManager.lookupKey(snapshotKeyArgs));
  }

  @Flaky("HDDS-11354")
  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testLookupKeyWithNotAllowedUserForPrefixAcl(BucketLayout bucketLayout) throws Exception {
    UserGroupInformation.setLoginUser(UGI1);

    createVolume();

    final OzoneVolume volume = objectStore.getVolume(volumeName);
    createBucket(bucketLayout, volume);

    final OzoneBucket bucket = volume.getBucket(bucketName);

    setDefaultPrefixAcls();

    createKey(bucket);

    setDefaultVolumeAcls();
    setDefaultBucketAcls();

    createSnapshot();

    final OmKeyArgs snapshotKeyArgs = getOmKeyArgs(true);

    // Add user2 to bucket and prefix ACL
    setBucketAcl();
    setPrefixAcls();

    createKey(bucket);
    final OmKeyArgs keyArgs = getOmKeyArgs(false);

    UserGroupInformation.setLoginUser(UGI2);
    final OMException ex = assertThrows(OMException.class, () -> ozoneManager.lookupKey(snapshotKeyArgs));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());

    assertDoesNotThrow(() -> ozoneManager.lookupKey(keyArgs));
  }

  /**
   * Verifies that bucket owner can: create, rename and delete snapshots.
   * Verifies that non bucket owner can not create, rename and delete.
   * Verifies that user with bucket read permissions can: list snapshots, get snapshot info,
   * snapshot diff, list snapshot diff jobs and cancel snapshot diff jobs.
   * Verifies that user with no permissions cannot do any of the above.
   *
   * @param bucketLayout
   * @throws Exception
   */
  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testSnapshotPermissions(BucketLayout bucketLayout) throws Exception {
    createVolume();

    // create a bucket whose owner is user1
    final OzoneVolume volume = objectStore.getVolume(volumeName);
    createBucket(bucketLayout, volume, USER1);
    // allow user2 volume read, and bucket read and list permissions.
    setDefaultVolumeAcls();
    setBucketAcl();

    ObjectStore objectStore1 = UGI1.doAs(
        (PrivilegedExceptionAction<ObjectStore>)() -> cluster.newClient().getObjectStore());
    String snapshot1 = "snapshot-" + RandomStringUtils.secure().nextNumeric(32);
    objectStore1.createSnapshot(volumeName, bucketName, snapshot1);
    String snapshot2 = "snapshot-" + RandomStringUtils.secure().nextNumeric(32);
    objectStore1.createSnapshot(volumeName, bucketName, snapshot2);
    String snapshot3 = "snapshot-" + RandomStringUtils.secure().nextNumeric(32);
    objectStore1.createSnapshot(volumeName, bucketName, snapshot3);
    String snapshot4 = "snapshot-" + RandomStringUtils.secure().nextNumeric(32);

    objectStore1.renameSnapshot(volumeName, bucketName, snapshot3, snapshot4);
    objectStore1.deleteSnapshot(volumeName, bucketName, snapshot4);

    objectStore1.listSnapshot(volumeName, bucketName, null, null);
    objectStore1.getSnapshotInfo(volumeName, bucketName, snapshot1);
    objectStore1.snapshotDiff(volumeName, bucketName,
        snapshot1, snapshot2, null, 0, false, false);
    objectStore1.listSnapshotDiffJobs(volumeName, bucketName, "", true, null);
    objectStore1.cancelSnapshotDiff(volumeName, bucketName, snapshot1, snapshot2);

    ObjectStore objectStore2 = UGI2.doAs(
        (PrivilegedExceptionAction<ObjectStore>)() -> cluster.newClient().getObjectStore());
    // user2 should not be able to create a snapshot in bucket1,
    // should not be able to rename in it, delete in it.
    OMException ex = assertThrows(OMException.class,
        () -> objectStore2.createSnapshot(volumeName, bucketName, snapshot1));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());

    ex = assertThrows(OMException.class,
        () -> objectStore2.renameSnapshot(volumeName, bucketName, snapshot1, snapshot2));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());

    ex = assertThrows(OMException.class,
        () -> objectStore2.deleteSnapshot(volumeName, bucketName, snapshot1));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());

    // user2 should be able to list the snapshots, get snapshot info,
    // snapshot diff, list snapshot diff jobs and cancel snapshot diff jobs
    objectStore2.listSnapshot(volumeName, bucketName, null, null);
    objectStore2.getSnapshotInfo(volumeName, bucketName, snapshot1);
    objectStore2.snapshotDiff(volumeName, bucketName,
              snapshot1, snapshot2, null, 0, false, false);
    objectStore2.listSnapshotDiffJobs(volumeName, bucketName, "", true, null);
    objectStore2.cancelSnapshotDiff(volumeName, bucketName, snapshot1, snapshot2);

    // user3 has no ACL permissions, should not be able to list the snapshots, get snapshot info,
    // snapshot diff, list snapshot diff jobs and cancel snapshot diff jobs.
    ObjectStore objectStore3 = UGI3.doAs(
        (PrivilegedExceptionAction<ObjectStore>)() -> cluster.newClient().getObjectStore());
    ex = assertThrows(OMException.class,
        () -> objectStore3.listSnapshot(volumeName, bucketName, null, null));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());

    ex = assertThrows(OMException.class,
        () -> objectStore3.getSnapshotInfo(volumeName, bucketName, snapshot1));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());

    ex = assertThrows(OMException.class,
        () -> objectStore3.snapshotDiff(volumeName, bucketName, snapshot1, snapshot2,
            null, 0, false, false));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());

    ex = assertThrows(OMException.class,
        () -> objectStore3.listSnapshotDiffJobs(volumeName, bucketName, "", true, null));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());

    ex = assertThrows(OMException.class,
        () -> objectStore3.cancelSnapshotDiff(volumeName, bucketName, snapshot1, snapshot2));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());

  }

  private void setup(BucketLayout bucketLayout)
      throws IOException {
    UserGroupInformation.setLoginUser(UGI1);

    createVolume();

    final OzoneVolume volume = objectStore.getVolume(volumeName);
    createBucket(bucketLayout, volume);

    final OzoneBucket bucket = volume.getBucket(bucketName);
    createKey(bucket);

    setDefaultAcls();

    createSnapshot();

    setKeyAcl();
    setBucketAcl();
  }

  private void setDefaultAcls() throws IOException {
    setDefaultVolumeAcls();
    setDefaultBucketAcls();
    setDefaultKeyAcls();
  }

  private void setDefaultVolumeAcls() throws IOException {
    final OzoneObj volumeObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .build();
    objectStore.setAcl(volumeObj, OzoneAcl.parseAcls(
        "user:" + USER1 + ":r," +
            "user:" + USER2 + ":r"));
  }

  private void setDefaultBucketAcls() throws IOException {
    final OzoneObj bucketObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    objectStore.setAcl(bucketObj, OzoneAcl.parseAcls(
        "user:" + USER1 + ":r," +
            "user:" + USER1 + ":l"));
  }

  private void setDefaultKeyAcls() throws IOException {
    final OzoneObj keyObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(RESOURCE_TYPE_KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    objectStore.setAcl(keyObj, OzoneAcl.parseAcls(
        "user:" + USER1 + ":r," +
            "user:" + USER1 + ":x"));
  }

  private void setDefaultPrefixAcls() throws IOException {
    final OzoneObj prefixObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(DIR_PREFIX)
        .build();

    objectStore.setAcl(prefixObj, OzoneAcl.parseAcls(
        "user:" + USER1 + ":r[DEFAULT]," +
            "user:" + USER1 + ":x[DEFAULT]"));
  }

  private void setBucketAcl() throws IOException {
    OzoneObj bucketObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();

    objectStore.setAcl(bucketObj, OzoneAcl.parseAcls(
        "user:" + USER1 + ":r," +
            "user:" + USER1 + ":l," +
            "user:" + USER2 + ":r," +
            "user:" + USER2 + ":l"));
  }

  private void setKeyAcl() throws IOException {
    final OzoneObj keyObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(RESOURCE_TYPE_KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    objectStore.setAcl(keyObj, OzoneAcl.parseAcls(
        "user:" + USER1 + ":r," +
            "user:" + USER1 + ":x," +
            "user:" + USER2 + ":r," +
            "user:" + USER2 + ":x"));
  }

  private void setPrefixAcls() throws IOException {
    final OzoneObj prefixObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(DIR_PREFIX)
        .build();

    objectStore.setAcl(prefixObj, OzoneAcl.parseAcls(
        "user:" + USER1 + ":r[DEFAULT]," +
            "user:" + USER1 + ":x[DEFAULT]," +
            "user:" + USER2 + ":r[DEFAULT]," +
            "user:" + USER2 + ":x[DEFAULT]"));
  }

  private void createKey(OzoneBucket bucket) throws IOException {
    keyName = KEY_PREFIX + RandomStringUtils.secure().nextNumeric(32);
    byte[] data = RandomStringUtils.secure().nextAscii(1).getBytes(UTF_8);
    final OzoneOutputStream fileKey = bucket.createKey(keyName, data.length);
    fileKey.write(data);
    fileKey.close();
  }

  private void createSnapshot()
      throws IOException {
    final String snapshotPrefix = "snapshot-";
    final String snapshotName =
        snapshotPrefix + RandomStringUtils.secure().nextNumeric(32);
    objectStore.createSnapshot(volumeName, bucketName, snapshotName);
    snapshotKeyPrefix = OmSnapshotManager
        .getSnapshotPrefix(snapshotName);
    final SnapshotInfo snapshotInfo = ozoneManager
        .getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName));
    // Allow the snapshot to be written to disk
    String fileName =
        getSnapshotPath(ozoneManager.getConfiguration(), snapshotInfo, 0);
    File snapshotDir = new File(fileName);
    if (!RDBCheckpointUtils
        .waitForCheckpointDirectoryExist(snapshotDir)) {
      throw new IOException("snapshot directory doesn't exist");
    }
  }

  private static Stream<Arguments> getListStatusArguments() {
    return Stream.of(
        arguments(BucketLayout.OBJECT_STORE, false, false),
        arguments(BucketLayout.FILE_SYSTEM_OPTIMIZED, false, false),
        arguments(BucketLayout.LEGACY, false, false),
        arguments(BucketLayout.OBJECT_STORE, true, false),
        arguments(BucketLayout.LEGACY, true, false),
        arguments(BucketLayout.OBJECT_STORE, false, true),
        arguments(BucketLayout.FILE_SYSTEM_OPTIMIZED, false, true),
        arguments(BucketLayout.LEGACY, false, true),
        arguments(BucketLayout.OBJECT_STORE, true, true),
        arguments(BucketLayout.LEGACY, true, true));
  }

  private OmKeyArgs getOmKeyArgs(boolean isSnapshot) {
    return new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(isSnapshot ? snapshotKeyPrefix + keyName : keyName)
        .build();
  }

  private void createBucket(BucketLayout bucketLayout,
      OzoneVolume volume) throws IOException {
    createBucket(bucketLayout, volume, ADMIN);
  }

  private void createBucket(BucketLayout bucketLayout,
      OzoneVolume volume, String owner) throws IOException {
    final String bucketPrefix = "bucket-";
    bucketName = bucketPrefix + RandomStringUtils.secure().nextNumeric(32);
    final BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setOwner(owner)
        .setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName, bucketArgs);
  }

  private void createVolume() throws IOException {
    final String volumePrefix = "volume-";
    volumeName = volumePrefix + RandomStringUtils.secure().nextNumeric(32);
    final VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setAdmin(ADMIN)
        .setOwner(ADMIN)
        .build();
    objectStore.createVolume(volumeName, volumeArgs);
  }

}
