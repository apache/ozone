/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

import com.google.common.base.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
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
import org.apache.ozone.test.GenericTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.PREFIX;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link OzoneNativeAuthorizer}.
 */
@RunWith(Parameterized.class)
public class TestOzoneNativeAuthorizer {

  private static OzoneConfiguration ozConfig;
  private String vol;
  private String buck;
  private String key;
  private String prefix;
  private ACLType parentDirUserAcl;
  private ACLType parentDirGroupAcl;
  private boolean expectedAclResult;

  private static OzoneManagerProtocol writeClient;
  private static KeyManager keyManager;
  private static VolumeManager volumeManager;
  private static BucketManager bucketManager;
  private static PrefixManager prefixManager;
  private static OMMetadataManager metadataManager;
  private static OzoneNativeAuthorizer nativeAuthorizer;
  private static UserGroupInformation adminUgi;
  private static UserGroupInformation testUgi;

  private OzoneObj volObj;
  private OzoneObj buckObj;
  private OzoneObj keyObj;
  private OzoneObj prefixObj;

  @Parameterized.Parameters
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

  public TestOzoneNativeAuthorizer(String keyName, String prefixName,
      ACLType userRight,
      ACLType groupRight, boolean expectedResult) throws IOException {
    int randomInt = RandomUtils.nextInt();
    vol = "vol" + randomInt;
    buck = "bucket" + randomInt;
    key = keyName + randomInt;
    prefix = prefixName + randomInt + OZONE_URI_DELIMITER;
    parentDirUserAcl = userRight;
    parentDirGroupAcl = groupRight;
    expectedAclResult = expectedResult;

    createVolume(vol);
    createBucket(vol, buck);
    createKey(vol, buck, key);
  }

  @BeforeClass
  public static void setup() throws Exception {
    ozConfig = new OzoneConfiguration();
    ozConfig.set(OZONE_ACL_AUTHORIZER_CLASS,
        OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    File dir = GenericTestUtils.getRandomizedTestDir();
    ozConfig.set(OZONE_METADATA_DIRS, dir.toString());
    ozConfig.set(OZONE_ADMINISTRATORS, "om");

    OmTestManagers omTestManagers =
        new OmTestManagers(ozConfig);
    metadataManager = omTestManagers.getMetadataManager();
    volumeManager = omTestManagers.getVolumeManager();
    bucketManager = omTestManagers.getBucketManager();
    prefixManager = omTestManagers.getPrefixManager();
    keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    nativeAuthorizer = new OzoneNativeAuthorizer(volumeManager, bucketManager,
        keyManager, prefixManager,
        new OzoneAdmins(Collections.singletonList("om")));
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
        .setAcls(OzoneAclUtil.getAclList(testUgi.getUserName(),
            testUgi.getGroupNames(), ALL, ALL))
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

  @Test
  public void testCheckAccessForVolume() throws Exception {
    expectedAclResult = true;
    resetAclsAndValidateAccess(volObj, USER, writeClient);
    resetAclsAndValidateAccess(volObj, GROUP, writeClient);
    resetAclsAndValidateAccess(volObj, WORLD, writeClient);
    resetAclsAndValidateAccess(volObj, ANONYMOUS, writeClient);
  }

  @Test
  public void testCheckAccessForBucket() throws Exception {

    OzoneAcl userAcl = new OzoneAcl(USER, testUgi.getUserName(),
        parentDirUserAcl, ACCESS);
    OzoneAcl groupAcl = new OzoneAcl(GROUP, testUgi.getGroups().size() > 0 ?
        testUgi.getGroups().get(0) : "", parentDirGroupAcl, ACCESS);
    // Set access for volume.
    // We should directly add to table because old API's update to DB.

    setVolumeAcl(Arrays.asList(userAcl, groupAcl));


    resetAclsAndValidateAccess(buckObj, USER, writeClient);
    resetAclsAndValidateAccess(buckObj, GROUP, writeClient);
    resetAclsAndValidateAccess(buckObj, WORLD, writeClient);
    resetAclsAndValidateAccess(buckObj, ANONYMOUS, writeClient);
  }

  @Test
  public void testCheckAccessForKey() throws Exception {
    OzoneAcl userAcl = new OzoneAcl(USER, testUgi.getUserName(),
        parentDirUserAcl, ACCESS);
    OzoneAcl groupAcl = new OzoneAcl(GROUP, testUgi.getGroups().size() > 0 ?
        testUgi.getGroups().get(0) : "", parentDirGroupAcl, ACCESS);
    // Set access for volume & bucket. We should directly add to table
    // because old API's update to DB.

    setVolumeAcl(Arrays.asList(userAcl, groupAcl));
    setBucketAcl(Arrays.asList(userAcl, groupAcl));

    resetAclsAndValidateAccess(keyObj, USER, writeClient);
    resetAclsAndValidateAccess(keyObj, GROUP, writeClient);
    resetAclsAndValidateAccess(keyObj, WORLD, writeClient);
    resetAclsAndValidateAccess(keyObj, ANONYMOUS, writeClient);
  }

  @Test
  public void testCheckAccessForPrefix() throws Exception {
    prefixObj = new OzoneObjInfo.Builder()
        .setVolumeName(vol)
        .setBucketName(buck)
        .setPrefixName(prefix)
        .setResType(PREFIX)
        .setStoreType(OZONE)
        .build();

    OzoneAcl userAcl = new OzoneAcl(USER, testUgi.getUserName(),
        parentDirUserAcl, ACCESS);
    OzoneAcl groupAcl = new OzoneAcl(GROUP, testUgi.getGroups().size() > 0 ?
        testUgi.getGroups().get(0) : "", parentDirGroupAcl, ACCESS);
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
    String volumeKey = metadataManager.getVolumeKey(volObj.getVolumeName());
    OmVolumeArgs omVolumeArgs =
        metadataManager.getVolumeTable().get(volumeKey);

    omVolumeArgs.setAcls(ozoneAcls);

    metadataManager.getVolumeTable().addCacheEntry(new CacheKey<>(volumeKey),
        new CacheValue<>(Optional.of(omVolumeArgs), 1L));
  }

  private void setBucketAcl(List<OzoneAcl> ozoneAcls) throws IOException {
    String bucketKey = metadataManager.getBucketKey(vol, buck);
    OmBucketInfo omBucketInfo = metadataManager.getBucketTable().get(bucketKey);

    omBucketInfo.setAcls(ozoneAcls);

    metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
  }

  private void addVolumeAcl(OzoneAcl ozoneAcl) throws IOException {
    String volumeKey = metadataManager.getVolumeKey(volObj.getVolumeName());
    OmVolumeArgs omVolumeArgs =
        metadataManager.getVolumeTable().get(volumeKey);

    omVolumeArgs.addAcl(ozoneAcl);

    metadataManager.getVolumeTable().addCacheEntry(new CacheKey<>(volumeKey),
        new CacheValue<>(Optional.of(omVolumeArgs), 1L));
  }

  private void addBucketAcl(OzoneAcl ozoneAcl) throws IOException {
    String bucketKey = metadataManager.getBucketKey(vol, buck);
    OmBucketInfo omBucketInfo = metadataManager.getBucketTable().get(bucketKey);

    omBucketInfo.addAcl(ozoneAcl);

    metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
  }

  private void resetAclsAndValidateAccess(OzoneObj obj,
      ACLIdentityType accessType, OzoneManagerProtocol aclImplementor)
      throws IOException {
    List<OzoneAcl> acls;
    String user = testUgi.getUserName();
    String group = (testUgi.getGroups().size() > 0) ?
        testUgi.getGroups().get(0) : "";

    RequestContext.Builder builder = new RequestContext.Builder()
        .setClientUgi(testUgi)
        .setAclType(accessType);

    // Get all acls.
    List<ACLType> allAcls = Arrays.stream(ACLType.values()).
        collect(Collectors.toList());

    /**
     * 1. Reset default acls to an acl.
     * 2. Test if user/group has access only to it.
     * 3. Add remaining acls one by one and then test
     *    if user/group has access to them.
     * */
    for (ACLType a1 : allAcls) {
      OzoneAcl newAcl = new OzoneAcl(accessType, getAclName(accessType), a1,
          ACCESS);

      // Reset acls to only one right.
      if (obj.getResourceType() == VOLUME) {
        setVolumeAcl(Collections.singletonList(newAcl));
      } else if (obj.getResourceType() == BUCKET) {
        setBucketAcl(Collections.singletonList(newAcl));
      } else {
        aclImplementor.setAcl(obj, Collections.singletonList(newAcl));
      }


      // Fetch current acls and validate.
      acls = aclImplementor.getAcl(obj);
      assertTrue(acls.size() == 1);
      assertTrue(acls.contains(newAcl));

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
      if (a1.equals(CREATE) && obj.getResourceType().equals(VOLUME)) {
        assertEquals(msg, nativeAuthorizer.getOzoneAdmins()
                         .getAdminUsernames().contains(user),
            nativeAuthorizer.checkAccess(obj,
                builder.setAclRights(a1).build()));
      } else {
        assertEquals(msg, expectedAclResult, nativeAuthorizer.checkAccess(obj,
            builder.setAclRights(a1).build()));
      }
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
          List right = acls.stream().map(a -> a.getAclList()).collect(
              Collectors.toList());
          assertFalse("Did not expect client to have " + a2 + " acl. " +
                  "Current acls found:" + right + ". Type:" + accessType + ","
                  + " name:" + (accessType == USER ? user : group),
              nativeAuthorizer.checkAccess(obj,
                  builder.setAclRights(a2).build()));

          // Randomize next type.
          int type = RandomUtils.nextInt(0, 3);
          ACLIdentityType identityType = ACLIdentityType.values()[type];
          // Add remaining acls one by one and then check access.
          OzoneAcl addAcl = new OzoneAcl(identityType, 
              getAclName(identityType), a2, ACCESS);

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

          assertTrue("Current acls :" + acls + ". " +
              "Type:" + accessType + ", name:" + (accessType == USER ? user
              : group) + " acl:" + a2, a2AclFound);
          assertTrue("Expected client to have " + a1 + " acl. Current acls " +
              "found:" + acls + ". Type:" + accessType +
              ", name:" + (accessType == USER ? user : group), a1AclFound);
          assertEquals("Current acls " + acls + ". Expect acl:" + a2 +
                  " to be set? " + expectedAclResult + " accessType:"
                  + accessType, expectedAclResult,
              nativeAuthorizer.checkAccess(obj,
                  builder.setAclRights(a2).build()));
          aclsToBeValidated.remove(a2);
          for (ACLType a3 : aclsToBeValidated) {
            if (!a3.equals(a1) && !a3.equals(a2) && !a3.equals(CREATE)) {
              assertFalse("User shouldn't have right " + a3 + ". " +
                      "Current acl rights for user:" + a1 + "," + a2,
                  nativeAuthorizer.checkAccess(obj,
                      builder.setAclRights(a3).build()));
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
      if (testUgi.getGroups().size() > 0) {
        return testUgi.getGroups().get(0);
      }
    default:
      return "";
    }
  }

  /**
   * Helper function to test acl rights with user/group had ALL acl bit set.
   * @param obj
   * @param builder
   */
  private void validateAll(OzoneObj obj, RequestContext.Builder
      builder) throws OMException {
    List<ACLType> allAcls = new ArrayList<>(Arrays.asList(ACLType.values()));
    allAcls.remove(ALL);
    allAcls.remove(NONE);
    RequestContext ctx = builder.build();
    boolean expectedResult = expectedAclResult;
    if (nativeAuthorizer.getOzoneAdmins().getAdminUsernames().contains(
        ctx.getClientUgi().getUserName())) {
      expectedResult = true;
    }
    for (ACLType a : allAcls) {
      assertEquals("User should have right " + a + ".",
          expectedResult, nativeAuthorizer.checkAccess(obj, ctx));
    }
  }

  /**
   * Helper function to test acl rights with user/group had NONE acl bit set.
   * @param obj
   * @param builder
   */
  private void validateNone(OzoneObj obj, RequestContext.Builder
      builder) throws OMException {
    List<ACLType> allAcls = new ArrayList<>(Arrays.asList(ACLType.values()));
    allAcls.remove(NONE);
    // Removing CREATE, WRITE since they need special handling.
    allAcls.remove(CREATE);
    allAcls.remove(WRITE);
    for (ACLType a : allAcls) {
      assertFalse("User shouldn't have right " + a + ".", 
          nativeAuthorizer.checkAccess(obj, builder.setAclRights(a).build()));
    }
  }
}