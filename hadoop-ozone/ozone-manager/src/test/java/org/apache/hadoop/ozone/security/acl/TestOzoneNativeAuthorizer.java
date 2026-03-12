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

package org.apache.hadoop.ozone.security.acl;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.ANONYMOUS;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.WORLD;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.PREFIX;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.VolumeManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests Ozone Native Authorizer.
 */
@ExtendWith(MockitoExtension.class)
public class TestOzoneNativeAuthorizer {

  private static final List<String> ADMIN_USERNAMES = singletonList("om");
  @TempDir
  private static File testDir;
  private String vol;
  private String buck;
  private String key;
  private String prefix;
  private ACLType parentDirUserAcl;
  private ACLType parentDirGroupAcl;
  private boolean expectedAclResult;

  private static OzoneManagerProtocol writeClient;
  private static OMMetadataManager metadataManager;
  private static OzoneNativeAuthorizer nativeAuthorizer;
  private static UserGroupInformation adminUgi;
  private static UserGroupInformation testUgi;

  private OzoneObj volObj;
  private OzoneObj buckObj;
  private OzoneObj keyObj;

  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {"key", "dir1/", ALL, ALL, true},
        {"file1", "2019/june/01/", ALL, ALL, true},
        {"file2", "", ALL, ALL, true},
        {"dir1/dir2/dir4/", "", ALL, ALL, true},
        {"key", "dir1/", NONE, NONE, false},
        {"file1", "2019/june/01/", NONE, NONE, false},
        {"file2", "", NONE, NONE, false},
        {"dir1/dir2/dir4/", "", NONE, NONE, false}
    });
  }

  public void createAll(
      String keyName, String prefixName, ACLType userRight,
      ACLType groupRight, boolean expectedResult) throws IOException {
    int randomInt = RandomUtils.secure().randomInt();
    this.vol = "vol" + randomInt;
    this.buck = "bucket" + randomInt;
    this.key = keyName + randomInt;
    this.prefix = prefixName + randomInt + OZONE_URI_DELIMITER;
    this.parentDirUserAcl = userRight;
    this.parentDirGroupAcl = groupRight;
    this.expectedAclResult = expectedResult;

    createVolume(this.vol);
    createBucket(this.vol, this.buck);
    createKey(this.vol, this.buck, this.key);
  }

  @BeforeAll
  public static void setup() throws Exception {
    OzoneConfiguration ozConfig = new OzoneConfiguration();
    ozConfig.set(OZONE_ACL_AUTHORIZER_CLASS,
        OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    ozConfig.set(OZONE_METADATA_DIRS, testDir.toString());
    ozConfig.set(OZONE_ADMINISTRATORS, "om");

    OmTestManagers omTestManagers =
        new OmTestManagers(ozConfig);
    metadataManager = omTestManagers.getMetadataManager();
    VolumeManager volumeManager = omTestManagers.getVolumeManager();
    BucketManager bucketManager = omTestManagers.getBucketManager();
    PrefixManager prefixManager = omTestManagers.getPrefixManager();
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    nativeAuthorizer = new OzoneNativeAuthorizer(volumeManager, bucketManager,
        keyManager, prefixManager, new OzoneAdmins(ADMIN_USERNAMES));
    adminUgi = UserGroupInformation.createUserForTesting("om",
        new String[]{"ozone"});
    testUgi = UserGroupInformation.createUserForTesting("testuser",
        new String[]{"test"});
  }

  private void createKey(String volume,
                         String bucket, String keyName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(keyName)
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(0)
        .setAcls(OzoneAclUtil.getAclList(testUgi, ALL, ALL))
        .setOwnerName(UserGroupInformation.getCurrentUser().getShortUserName())
        .build();

    if (keyName.split(OZONE_URI_DELIMITER).length > 1) {
      writeClient.createDirectory(keyArgs);
      key = key + OZONE_URI_DELIMITER;
    } else {
      OpenKeySession keySession = writeClient.createFile(keyArgs, true, false);
      keyArgs.setLocationInfoList(
          keySession.getKeyInfo().getLatestVersionLocations()
              .getLocationList());
      writeClient.commitKey(keyArgs, keySession.getId());
    }

    keyObj = new OzoneObjInfo.Builder()
        .setVolumeName(vol)
        .setBucketName(buck)
        .setKeyName(key)
        .setResType(KEY)
        .setStoreType(OZONE)
        .build();
  }

  private void createBucket(String volumeName, String bucketName)
      throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    OMRequestTestUtils.addBucketToOM(metadataManager, bucketInfo);
    buckObj = new OzoneObjInfo.Builder()
        .setVolumeName(vol)
        .setBucketName(buck)
        .setResType(BUCKET)
        .setStoreType(OZONE)
        .build();
  }

  private void createVolume(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName(adminUgi.getUserName())
        .setOwnerName(testUgi.getUserName())
        .build();
    OMRequestTestUtils.addVolumeToOM(metadataManager, volumeArgs);
    volObj = new OzoneObjInfo.Builder()
        .setVolumeName(vol)
        .setResType(VOLUME)
        .setStoreType(OZONE)
        .build();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCheckAccessForVolume(
      String keyName, String prefixName, ACLType userRight,
      ACLType groupRight, boolean expectedResult) throws Exception {
    createAll(keyName, prefixName, userRight, groupRight, expectedResult);
    expectedAclResult = true;
    resetAclsAndValidateAccess(volObj, USER, writeClient);
    resetAclsAndValidateAccess(volObj, GROUP, writeClient);
    resetAclsAndValidateAccess(volObj, WORLD, writeClient);
    resetAclsAndValidateAccess(volObj, ANONYMOUS, writeClient);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCheckAccessForBucket(
      String keyName, String prefixName, ACLType userRight,
      ACLType groupRight, boolean expectedResult) throws Exception {
    createAll(keyName, prefixName, userRight, groupRight, expectedResult);
    OzoneAcl userAcl = OzoneAcl.of(USER, testUgi.getUserName(),
        ACCESS, parentDirUserAcl);
    OzoneAcl groupAcl = OzoneAcl.of(GROUP, !testUgi.getGroups().isEmpty() ?
        testUgi.getGroups().get(0) : "", ACCESS, parentDirGroupAcl);
    // Set access for volume.
    // We should directly add to table because old API's update to DB.

    setVolumeAcl(Arrays.asList(userAcl, groupAcl));


    resetAclsAndValidateAccess(buckObj, USER, writeClient);
    resetAclsAndValidateAccess(buckObj, GROUP, writeClient);
    resetAclsAndValidateAccess(buckObj, WORLD, writeClient);
    resetAclsAndValidateAccess(buckObj, ANONYMOUS, writeClient);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCheckAccessForKey(
      String keyName, String prefixName, ACLType userRight,
      ACLType groupRight, boolean expectedResult) throws Exception {
    createAll(keyName, prefixName, userRight, groupRight, expectedResult);
    OzoneAcl userAcl = OzoneAcl.of(USER, testUgi.getUserName(),
        ACCESS, parentDirUserAcl);
    OzoneAcl groupAcl = OzoneAcl.of(GROUP, !testUgi.getGroups().isEmpty() ?
        testUgi.getGroups().get(0) : "", ACCESS, parentDirGroupAcl);
    // Set access for volume & bucket. We should directly add to table
    // because old API's update to DB.

    setVolumeAcl(Arrays.asList(userAcl, groupAcl));
    setBucketAcl(Arrays.asList(userAcl, groupAcl));

    resetAclsAndValidateAccess(keyObj, USER, writeClient);
    resetAclsAndValidateAccess(keyObj, GROUP, writeClient);
    resetAclsAndValidateAccess(keyObj, WORLD, writeClient);
    resetAclsAndValidateAccess(keyObj, ANONYMOUS, writeClient);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCheckAccessForPrefix(
      String keyName, String prefixName, ACLType userRight,
      ACLType groupRight, boolean expectedResult) throws Exception {
    createAll(keyName, prefixName, userRight, groupRight, expectedResult);
    OzoneObj prefixObj = new OzoneObjInfo.Builder()
        .setVolumeName(vol)
        .setBucketName(buck)
        .setPrefixName(this.prefix)
        .setResType(PREFIX)
        .setStoreType(OZONE)
        .build();

    OzoneAcl userAcl = OzoneAcl.of(USER, testUgi.getUserName(),
        ACCESS, parentDirUserAcl);
    OzoneAcl groupAcl = OzoneAcl.of(GROUP, !testUgi.getGroups().isEmpty() ?
        testUgi.getGroups().get(0) : "", ACCESS, parentDirGroupAcl);
    // Set access for volume & bucket. We should directly add to table
    // because old API's update to DB.

    setVolumeAcl(Arrays.asList(userAcl, groupAcl));

    setBucketAcl(Arrays.asList(userAcl, groupAcl));

    resetAclsAndValidateAccess(prefixObj, USER, writeClient);
    resetAclsAndValidateAccess(prefixObj, GROUP, writeClient);
    resetAclsAndValidateAccess(prefixObj, WORLD, writeClient);
    resetAclsAndValidateAccess(prefixObj, ANONYMOUS, writeClient);
  }

  private void setVolumeAcl(List<OzoneAcl> ozoneAcls) throws IOException {
    OzoneNativeAclTestUtil.setVolumeAcl(metadataManager, vol, ozoneAcls);
  }

  private void setBucketAcl(List<OzoneAcl> ozoneAcls) throws IOException {
    OzoneNativeAclTestUtil.setBucketAcl(metadataManager, vol, buck, ozoneAcls);
  }

  private void addVolumeAcl(OzoneAcl ozoneAcl) throws IOException {
    OzoneNativeAclTestUtil.addVolumeAcl(metadataManager, vol, ozoneAcl);
  }

  private void addBucketAcl(OzoneAcl ozoneAcl) throws IOException {
    OzoneNativeAclTestUtil.addBucketAcl(metadataManager, vol, buck, ozoneAcl);
  }

  private void resetAclsAndValidateAccess(
      OzoneObj obj, ACLIdentityType accessType,
      OzoneManagerProtocol aclImplementor)
      throws IOException {
    List<OzoneAcl> acls;
    String user = testUgi.getUserName();
    String group = (!testUgi.getGroups().isEmpty()) ?
        testUgi.getGroups().get(0) : "";

    RequestContext.Builder builder = RequestContext.newBuilder()
        .setClientUgi(testUgi)
        .setAclType(accessType);

    // Get all acls.
    List<ACLType> allAcls = Arrays.stream(ACLType.values()).
        collect(Collectors.toList());

    /*
     * 1. Reset default acls to an acl.
     * 2. Test if user/group has access only to it.
     * 3. Add remaining acls one by one and then test
     *    if user/group has access to them.
     */
    for (ACLType a1 : allAcls) {
      OzoneAcl newAcl = OzoneAcl.of(accessType, getAclName(accessType), ACCESS, a1
      );

      // Reset acls to only one right.
      if (obj.getResourceType() == VOLUME) {
        setVolumeAcl(singletonList(newAcl));
      } else if (obj.getResourceType() == BUCKET) {
        setBucketAcl(singletonList(newAcl));
      } else {
        aclImplementor.setAcl(obj, singletonList(newAcl));
      }


      // Fetch current acls and validate.
      acls = aclImplementor.getAcl(obj);
      assertEquals(1, acls.size());
      assertThat(acls).contains(newAcl);

      // Special handling for ALL.
      if (a1.equals(ALL)) {
        validateAll(obj, builder);
        continue;
      }

      // Special handling for NONE.
      if (a1.equals(NONE)) {
        validateNone(obj, builder);
        continue;
      }

      String msg = "Acl to check:" + a1 + " accessType:" +
          accessType + " path:" + obj.getPath();
      RequestContext context = builder.setAclRights(a1).build();
      boolean expectedResult =
          a1.equals(CREATE) && obj.getResourceType().equals(VOLUME)
              ? ADMIN_USERNAMES.contains(user)
              : expectedAclResult;
      assertEquals(expectedResult,
          nativeAuthorizer.checkAccess(obj, context), msg);

      List<ACLType> aclsToBeValidated =
          Arrays.stream(ACLType.values()).collect(Collectors.toList());
      List<ACLType> aclsToBeAdded =
          Arrays.stream(ACLType.values()).collect(Collectors.toList());
      aclsToBeValidated.remove(NONE);
      // Do not validate "WRITE" since write acl type requires object to be
      // present in OpenKeyTable.
      aclsToBeValidated.remove(WRITE);
      aclsToBeValidated.remove(a1);

      aclsToBeAdded.remove(NONE);
      aclsToBeAdded.remove(ALL);
      // AclType "CREATE" is skipped from access check on objects
      // since the object will not exist during access check.
      aclsToBeAdded.remove(CREATE);
      // AclType "WRITE" is removed from being tested here,
      // because object must always be present in OpenKeyTable for write
      // acl requests. But, here the objects are already committed
      // and will move to keyTable.
      aclsToBeAdded.remove(WRITE);

      // Fetch acls again.
      for (ACLType a2 : aclsToBeAdded) {
        if (!a2.equals(a1)) {

          acls = aclImplementor.getAcl(obj);
          List<List<ACLType>> right = acls.stream()
              .map(OzoneAcl::getAclList)
              .collect(Collectors.toList());
          assertFalse(nativeAuthorizer.checkAccess(obj,
              builder.setAclRights(a2).build()), "Did not expect client " +
              "to have " + a2 + " acl. " +
              "Current acls found:" + right + ". Type:" + accessType + ","
              + " name:" + (accessType == USER ? user : group));

          // Randomize next type.
          int type = RandomUtils.secure().randomInt(0, 3);
          ACLIdentityType identityType = ACLIdentityType.values()[type];
          // Add remaining acls one by one and then check access.
          OzoneAcl addAcl = OzoneAcl.of(identityType,
              getAclName(identityType), ACCESS, a2);

          // For volume and bucket update to cache. As Old API's update to
          // only DB not cache.
          if (obj.getResourceType() == VOLUME) {
            addVolumeAcl(addAcl);
          } else if (obj.getResourceType() == BUCKET) {
            addBucketAcl(addAcl);
          } else {
            aclImplementor.addAcl(obj, addAcl);
          }

          // Fetch acls again.
          acls = aclImplementor.getAcl(obj);
          boolean a2AclFound = false;
          boolean a1AclFound = false;
          for (OzoneAcl acl : acls) {
            if (acl.getAclList().contains(a2)) {
              a2AclFound = true;
            }
            if (acl.getAclList().contains(a1)) {
              a1AclFound = true;
            }
          }

          assertTrue(a2AclFound, "Current acls :" + acls + ". " +
              "Type:" + accessType + ", name:" + (accessType == USER ? user
              : group) + " acl:" + a2);
          assertTrue(a1AclFound, "Expected client to have " + a1 + " acl. " +
              "Current acls found:" + acls + ". Type:" + accessType +
              ", name:" + (accessType == USER ? user : group));
          assertEquals(expectedAclResult, nativeAuthorizer.checkAccess(obj,
                  builder.setAclRights(a2).build()),
              "Current acls " + acls + ". Expect acl:" + a2 +
                  " to be set? " + expectedAclResult + " accessType:"
                  + accessType);
          aclsToBeValidated.remove(a2);
          for (ACLType a3 : aclsToBeValidated) {
            if (!a3.equals(a1) && !a3.equals(a2) && !a3.equals(CREATE)) {
              assertFalse(nativeAuthorizer.checkAccess(obj,
                      builder.setAclRights(a3).build()),
                  "User shouldn't have right " + a3 + ". " +
                      "Current acl rights for user:" + a1 + "," + a2);
            }
          }
        }
      }
    }
  }

  private String getAclName(ACLIdentityType identityType) {
    switch (identityType) {
    case USER:
      return testUgi.getUserName();
    case GROUP:
      if (!testUgi.getGroups().isEmpty()) {
        return testUgi.getGroups().get(0);
      }
    default:
      return "";
    }
  }

  /**
   * Helper function to test acl rights with user/group had ALL acl bit set.
   */
  private void validateAll(OzoneObj obj, RequestContext.Builder
      builder) throws OMException {
    List<ACLType> allAcls = new ArrayList<>(Arrays.asList(ACLType.values()));
    allAcls.remove(ALL);
    allAcls.remove(NONE);
    RequestContext ctx = builder.build();
    String userName = ctx.getClientUgi().getUserName();
    boolean expectedResult = expectedAclResult
        || ADMIN_USERNAMES.contains(userName);
    for (ACLType a : allAcls) {
      assertEquals(expectedResult, nativeAuthorizer.checkAccess(obj, ctx),
          "User " + userName + " should have right " + a + ".");
    }
  }

  /**
   * Helper function to test acl rights with user/group had NONE acl bit set.
   */
  private void validateNone(OzoneObj obj, RequestContext.Builder
      builder) throws OMException {
    List<ACLType> allAcls = new ArrayList<>(Arrays.asList(ACLType.values()));
    allAcls.remove(NONE);
    // Removing CREATE, WRITE since they need special handling.
    allAcls.remove(CREATE);
    allAcls.remove(WRITE);
    for (ACLType a : allAcls) {
      assertFalse(nativeAuthorizer.checkAccess(obj,
              builder.setAclRights(a).build()),
          "User shouldn't have right " + a + ".");
    }
  }
}
