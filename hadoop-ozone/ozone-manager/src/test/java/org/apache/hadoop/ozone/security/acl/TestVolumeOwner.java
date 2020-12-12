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

package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.om.BucketManagerImpl;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.VolumeManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
import static org.mockito.Mockito.mock;


/**
 * Test Ozone owner check from OzoneNativeAuthorizer.
 */
public class TestVolumeOwner {

  private static OzoneConfiguration ozoneConfig;
  private static OzoneNativeAuthorizer nativeAuthorizer;
  private static KeyManagerImpl keyManager;
  private static VolumeManagerImpl volumeManager;
  private static BucketManagerImpl bucketManager;
  private static PrefixManager prefixManager;
  private static OMMetadataManager metadataManager;
  private static UserGroupInformation testUgi;

  @BeforeClass
  public static void setup() throws IOException {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set(OZONE_ACL_AUTHORIZER_CLASS,
        OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    File dir = GenericTestUtils.getRandomizedTestDir();
    ozoneConfig.set(OZONE_METADATA_DIRS, dir.toString());

    metadataManager = new OmMetadataManagerImpl(ozoneConfig);
    volumeManager = new VolumeManagerImpl(metadataManager, ozoneConfig);
    bucketManager = new BucketManagerImpl(metadataManager);
    keyManager = new KeyManagerImpl(mock(ScmBlockLocationProtocol.class),
        metadataManager, ozoneConfig, "om1", null);
    prefixManager = new PrefixManagerImpl(metadataManager, false);

    nativeAuthorizer = new OzoneNativeAuthorizer(volumeManager, bucketManager,
        keyManager, prefixManager,
        Collections.singletonList("om"));

    testUgi = UserGroupInformation.createUserForTesting("testuser",
        new String[]{"test"});

    prepareTestVols();
    prepareTestBuckets();
    prepareTestKeys();
  }

  // create 2 volumes
  private static void prepareTestVols() throws IOException {
    for (int i = 0; i < 2; i++) {
      OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
          .setVolume(getTestVolumeName(i))
          .setAdminName("om")
          .setOwnerName(getTestVolOwnerName(i))
          .build();
      TestOMRequestUtils.addVolumeToOM(metadataManager, volumeArgs);
    }
  }

  // create 2 buckets under each volume
  private static void prepareTestBuckets() throws IOException {
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
            .setVolumeName(getTestVolumeName(i))
            .setBucketName(getTestBucketName(j))
            .build();
        TestOMRequestUtils.addBucketToOM(metadataManager, bucketInfo);
      }
    }
  }

  // create 2 keys under each test buckets
  private static void prepareTestKeys() throws IOException {
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        for (int k = 0; k < 2; k++) {
          OmKeyArgs.Builder keyArgsBuilder = new OmKeyArgs.Builder()
              .setVolumeName(getTestVolumeName(i))
              .setBucketName(getTestBucketName(j))
              .setKeyName(getTestKeyName(k))
              .setFactor(HddsProtos.ReplicationFactor.ONE)
              .setDataSize(0)
              .setType(HddsProtos.ReplicationType.STAND_ALONE);
          if (k == 0) {
            keyArgsBuilder.setAcls(OzoneAclUtil.getAclList(
                testUgi.getUserName(), testUgi.getGroupNames(), ALL, ALL));
          } else {
            keyArgsBuilder.setAcls(OzoneAclUtil.getAclList(
                testUgi.getUserName(), testUgi.getGroupNames(), NONE, NONE));
          }
          OmKeyArgs keyArgs = keyArgsBuilder.build();
          OpenKeySession keySession = keyManager.createFile(keyArgs, true,
              false);
          keyArgs.setLocationInfoList(
              keySession.getKeyInfo().getLatestVersionLocations()
                  .getLocationList());
          keyManager.commitKey(keyArgs, keySession.getId());
        }
      }
    }
  }

  @Test
  public void testVolumeOps() throws Exception {
    OzoneObj vol0 = getTestVolumeobj(0);

    // admin = true, owner = false, ownerName = testvolumeOwner
    RequestContext nonOwnerContext = getUserRequestContext("om",
        IAccessAuthorizer.ACLType.CREATE, false, getTestVolOwnerName(0));
    Assert.assertTrue("matching admins are allowed to perform admin " +
        "operations", nativeAuthorizer.checkAccess(vol0, nonOwnerContext));

    // admin = true, owner = false, ownerName = null
    Assert.assertTrue("matching admins are allowed to perform admin " +
        "operations", nativeAuthorizer.checkAccess(vol0, nonOwnerContext));

    // admin = false, owner = false, ownerName = testvolumeOwner
    RequestContext nonAdminNonOwnerContext = getUserRequestContext("testuser",
        IAccessAuthorizer.ACLType.CREATE, false, getTestVolOwnerName(0));
    Assert.assertFalse("mismatching admins are not allowed to perform admin " +
        "operations", nativeAuthorizer.checkAccess(vol0,
        nonAdminNonOwnerContext));

    // admin = false, owner = true
    RequestContext nonAdminOwnerContext = getUserRequestContext(
        getTestVolOwnerName(0), IAccessAuthorizer.ACLType.CREATE,
        true, getTestVolOwnerName(0));
    Assert.assertFalse("mismatching admins are not allowed to perform admin " +
        "operations even for owner", nativeAuthorizer.checkAccess(vol0,
        nonAdminOwnerContext));

    List<IAccessAuthorizer.ACLType> aclsToTest =
        Arrays.stream(IAccessAuthorizer.ACLType.values()).filter(
            (type)-> type != NONE && type != CREATE)
            .collect(Collectors.toList());
    for (IAccessAuthorizer.ACLType type: aclsToTest) {
      nonAdminOwnerContext = getUserRequestContext(getTestVolOwnerName(0),
          type, true, getTestVolOwnerName(0));
      Assert.assertTrue("Owner is allowed to perform all non-admin " +
          "operations", nativeAuthorizer.checkAccess(vol0,
          nonAdminOwnerContext));
    }
  }

  @Test
  public void testBucketOps() throws Exception {
    OzoneObj obj = getTestBucketobj(1, 1);
    List<IAccessAuthorizer.ACLType> aclsToTest = getAclsToTest();

    // admin = false, owner = true
    for (IAccessAuthorizer.ACLType type: aclsToTest) {
      RequestContext nonAdminOwnerContext = getUserRequestContext(
          getTestVolOwnerName(1), type, true, getTestVolOwnerName(1));
      Assert.assertTrue("non admin volume owner without acls are allowed" +
          " to do " + type + " on bucket",
          nativeAuthorizer.checkAccess(obj, nonAdminOwnerContext));
    }

    // admin = false, owner = false
    for (IAccessAuthorizer.ACLType type: aclsToTest) {
      RequestContext nonAdminOwnerContext = getUserRequestContext(
          getTestVolOwnerName(1), type, false, getTestVolOwnerName(0));
      Assert.assertFalse("non admin non volume owner without acls" +
          " are not allowed to do " + type + " on bucket",
          nativeAuthorizer.checkAccess(obj, nonAdminOwnerContext));
    }
  }

  @Test
  public void testKeyOps() throws Exception {
    OzoneObj obj = getTestKeyobj(0, 0, 1);
    List<IAccessAuthorizer.ACLType> aclsToTest = getAclsToTest();

    // admin = false, owner = true
    for (IAccessAuthorizer.ACLType type: aclsToTest) {
      RequestContext nonAdminOwnerContext = getUserRequestContext(
          getTestVolOwnerName(0), type, true, getTestVolOwnerName(0));
      Assert.assertTrue("non admin volume owner without acls are allowed to " +
              "access key",
          nativeAuthorizer.checkAccess(obj, nonAdminOwnerContext));
    }

    // admin = false, owner = false
    for (IAccessAuthorizer.ACLType type: aclsToTest) {
      RequestContext nonAdminOwnerContext = getUserRequestContext(
          getTestVolOwnerName(0), type, false, getTestVolOwnerName(1));
      Assert.assertFalse("non admin volume owner without acls are" +
              " not allowed to access key",
          nativeAuthorizer.checkAccess(obj, nonAdminOwnerContext));
    }
  }

  private RequestContext getUserRequestContext(String username,
      IAccessAuthorizer.ACLType type, boolean isOwner, String ownerName) {
    return RequestContext.getBuilder(
        UserGroupInformation.createRemoteUser(username), null, null,
        type, ownerName).build();
  }

  private static String getTestVolumeName(int index) {
    return "vol" + index;
  }

  private static String getTestVolOwnerName(int index) {
    return "owner" + index;
  }

  private static String getTestBucketName(int index) {
    return "bucket" + index;
  }

  private static String getTestKeyName(int index) {
    return "key" + index;
  }

  private OzoneObj getTestVolumeobj(int index) {
    return OzoneObjInfo.Builder.getBuilder(OzoneObj.ResourceType.VOLUME,
        OzoneObj.StoreType.OZONE,
        getTestVolumeName(index), null, null).build();
  }

  private OzoneObj getTestBucketobj(int volIndex, int bucketIndex) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(getTestVolumeName(volIndex))
        .setBucketName(getTestBucketName(bucketIndex)).build();
  }

  private OzoneObj getTestKeyobj(int volIndex, int bucketIndex,
      int keyIndex) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(getTestVolumeName(volIndex))
        .setBucketName(getTestBucketName(bucketIndex))
        .setKeyName(getTestKeyName(keyIndex))
        .build();
  }

  List<IAccessAuthorizer.ACLType> getAclsToTest() {
    return Arrays.stream(IAccessAuthorizer.ACLType.values()).filter(
        (type)-> type != NONE).collect(Collectors.toList());
  }
}
