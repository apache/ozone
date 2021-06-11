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

import com.google.common.base.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.BucketManagerImpl;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.VolumeManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.DELETE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE_ACL;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.mockito.Mockito.mock;

/**
 * Test parent acl requirements when accessing children with native authorizer.
 */
public class TestParentAcl {
  private static OzoneConfiguration ozConfig;
  private static KeyManagerImpl keyManager;
  private static VolumeManagerImpl volumeManager;
  private static BucketManagerImpl bucketManager;
  private static PrefixManager prefixManager;
  private static OMMetadataManager metadataManager;
  private static OzoneNativeAuthorizer nativeAuthorizer;
  private static UserGroupInformation adminUgi;
  private static UserGroupInformation testUgi, testUgi1;

  @BeforeClass
  public static void setup() throws IOException {
    ozConfig = new OzoneConfiguration();
    ozConfig.set(OZONE_ACL_AUTHORIZER_CLASS,
        OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    File dir = GenericTestUtils.getRandomizedTestDir();
    ozConfig.set(OZONE_METADATA_DIRS, dir.toString());
    ozConfig.set(OZONE_ADMINISTRATORS, "om");

    metadataManager = new OmMetadataManagerImpl(ozConfig);
    volumeManager = new VolumeManagerImpl(metadataManager, ozConfig);
    bucketManager = new BucketManagerImpl(metadataManager);
    prefixManager = new PrefixManagerImpl(metadataManager, false);

    keyManager = new KeyManagerImpl(mock(ScmBlockLocationProtocol.class),
        metadataManager, ozConfig, "om1", null);

    nativeAuthorizer = new OzoneNativeAuthorizer(volumeManager, bucketManager,
        keyManager, prefixManager,
        Collections.singletonList("om"));
    adminUgi = UserGroupInformation.createUserForTesting("om",
        new String[]{"ozone"});
    testUgi = UserGroupInformation.createUserForTesting("testuser",
        new String[]{"test"});
    testUgi1 = UserGroupInformation.createUserForTesting("testuser1",
        new String[]{"test1"});
  }

  // Refined the parent context
  // OP         |CHILD     |PARENT

  // CREATE      NONE         WRITE
  // DELETE      DELETE       WRITE
  // WRITE       WRITE        WRITE
  // WRITE_ACL   WRITE_ACL    WRITE     (V1 WRITE_ACL=>WRITE)

  // READ        READ         READ
  // LIST        LIST         READ      (V1 LIST=>READ)
  // READ_ACL    READ_ACL     READ      (V1 READ_ACL=>READ)
  @Test
  public void testKeyAcl()
      throws IOException {
    OzoneObj keyObj;
    int randomInt = RandomUtils.nextInt();
    String vol = "vol" + randomInt;
    String buck = "bucket" + randomInt;
    String key = "key" + randomInt;

    createVolume(vol);
    createBucket(vol, buck);
    keyObj = createKey(vol, buck, key);

    List<OzoneAcl> originalVolAcls = getVolumeAcls(vol);
    List<OzoneAcl> originalBuckAcls = getBucketAcls(vol, buck);
    List<OzoneAcl> originalKeyAcls = getBucketAcls(vol, buck);

    testParentChild(keyObj, WRITE, WRITE_ACL);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls,
        key, originalKeyAcls);

    testParentChild(keyObj, WRITE, DELETE);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls,
        key, originalKeyAcls);

    testParentChild(keyObj, READ, READ_ACL);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls,
        key, originalKeyAcls);

    testParentChild(keyObj, READ, LIST);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls,
        key, originalKeyAcls);

    testParentChild(keyObj, WRITE, CREATE);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls,
        key, originalKeyAcls);

    testParentChild(keyObj, WRITE, WRITE);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls,
        key, originalKeyAcls);

    testParentChild(keyObj, READ, READ);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls,
        key, originalKeyAcls);
  }

  @Test
  public void testBucketAcl()
      throws IOException {
    OzoneObj bucketObj;
    int randomInt = RandomUtils.nextInt();
    String vol = "vol" + randomInt;
    String buck = "bucket" + randomInt;

    createVolume(vol);
    bucketObj = createBucket(vol, buck);

    List<OzoneAcl> originalVolAcls = getVolumeAcls(vol);
    List<OzoneAcl> originalBuckAcls = getBucketAcls(vol, buck);
    testParentChild(bucketObj, WRITE, WRITE_ACL);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls, null, null);

    testParentChild(bucketObj, WRITE, DELETE);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls, null, null);

    testParentChild(bucketObj, READ, READ_ACL);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls, null, null);

    testParentChild(bucketObj, READ, LIST);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls, null, null);

    testParentChild(bucketObj, WRITE, CREATE);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls, null, null);

    testParentChild(bucketObj, READ, READ);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls, null, null);

    testParentChild(bucketObj, WRITE, WRITE);
    resetAcl(vol, originalVolAcls, buck, originalBuckAcls, null, null);
  }

  private void resetAcl(String vol, List<OzoneAcl> volAcls,
      String buck, List<OzoneAcl> buckAcls,
      String key, List<OzoneAcl> keyAcls) throws IOException {
    if (volAcls != null) {
      setVolumeAcl(vol, volAcls);
    }

    if (buckAcls != null) {
      setBucketAcl(vol, buck, buckAcls);
    }

    if (keyAcls != null) {
      setKeyAcl(vol, buck, key, keyAcls);
    }
  }

  private void testParentChild(OzoneObj child,
      ACLType parentAclType, ACLType childAclType) throws IOException {

    RequestContext requestContext = new RequestContext.Builder()
        .setClientUgi(testUgi1)
        .setAclType(USER)
        .setAclRights(childAclType).build();

    OzoneAcl childAcl = new OzoneAcl(USER,
        testUgi1.getUserName(), childAclType, ACCESS);

    OzoneAcl parentAcl = new OzoneAcl(USER,
        testUgi1.getUserName(), parentAclType, ACCESS);

    Assert.assertFalse(nativeAuthorizer.checkAccess(child, requestContext));
    if (child.getResourceType() == BUCKET) {
      // add the bucket acl
      addBucketAcl(child.getVolumeName(), child.getBucketName(), childAcl);
      Assert.assertFalse(nativeAuthorizer.checkAccess(child, requestContext));

      // add the volume acl (parent), now bucket access is allowed.
      addVolumeAcl(child.getVolumeName(), parentAcl);
      Assert.assertTrue(nativeAuthorizer.checkAccess(child, requestContext));

    } else if (child.getResourceType() == KEY) {
      // add key acl is not enough
      addKeyAcl(child.getVolumeName(), child.getBucketName(),
          child.getKeyName(), childAcl);
      Assert.assertFalse(nativeAuthorizer.checkAccess(child, requestContext));

      // add the bucket acl is not enough (parent)
      addBucketAcl(child.getVolumeName(), child.getBucketName(), parentAcl);
      Assert.assertFalse(nativeAuthorizer.checkAccess(child, requestContext));

      // add the volume acl (grand-parent), now key access is allowed.
      addVolumeAcl(child.getVolumeName(), parentAcl);
      Assert.assertTrue(nativeAuthorizer.checkAccess(child, requestContext));
    }
  }

  private void addVolumeAcl(String vol, OzoneAcl ozoneAcl) throws IOException {
    String volumeKey = metadataManager.getVolumeKey(vol);
    OmVolumeArgs omVolumeArgs =
        metadataManager.getVolumeTable().get(volumeKey);

    omVolumeArgs.addAcl(ozoneAcl);

    metadataManager.getVolumeTable().addCacheEntry(new CacheKey<>(volumeKey),
        new CacheValue<>(Optional.of(omVolumeArgs), 1L));
  }

  private List<OzoneAcl> getVolumeAcls(String vol) throws IOException {
    String volumeKey = metadataManager.getVolumeKey(vol);
    OmVolumeArgs omVolumeArgs =
        metadataManager.getVolumeTable().get(volumeKey);

    return omVolumeArgs.getAcls();
  }

  private void setVolumeAcl(String vol, List<OzoneAcl> ozoneAcls)
      throws IOException {
    String volumeKey = metadataManager.getVolumeKey(vol);
    OmVolumeArgs omVolumeArgs = metadataManager.getVolumeTable().get(volumeKey);

    omVolumeArgs.setAcls(ozoneAcls);

    metadataManager.getVolumeTable().addCacheEntry(new CacheKey<>(volumeKey),
        new CacheValue<>(Optional.of(omVolumeArgs), 1L));
  }

  private void addKeyAcl(String vol, String buck, String key,
      OzoneAcl ozoneAcl) throws IOException {
    String objKey = metadataManager.getOzoneKey(vol, buck, key);
    OmKeyInfo omKeyInfo = metadataManager.getKeyTable().get(objKey);

    omKeyInfo.addAcl(ozoneAcl);

    metadataManager.getKeyTable().addCacheEntry(new CacheKey<>(objKey),
        new CacheValue<>(Optional.of(omKeyInfo), 1L));
  }

  private void setKeyAcl(String vol, String buck, String key,
                         List<OzoneAcl> ozoneAcls) throws IOException {
    String objKey = metadataManager.getOzoneKey(vol, buck, key);
    OmKeyInfo omKeyInfo = metadataManager.getKeyTable().get(objKey);
    omKeyInfo.setAcls(ozoneAcls);

    metadataManager.getKeyTable().addCacheEntry(new CacheKey<>(objKey),
        new CacheValue<>(Optional.of(omKeyInfo), 1L));
  }

  private void addBucketAcl(String vol, String buck, OzoneAcl ozoneAcl)
      throws IOException {
    String bucketKey = metadataManager.getBucketKey(vol, buck);
    OmBucketInfo omBucketInfo = metadataManager.getBucketTable().get(bucketKey);

    omBucketInfo.addAcl(ozoneAcl);

    metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
  }

  private List<OzoneAcl> getBucketAcls(String vol, String buck)
      throws IOException {
    String bucketKey = metadataManager.getBucketKey(vol, buck);
    OmBucketInfo omBucketInfo = metadataManager.getBucketTable().get(bucketKey);

    return omBucketInfo.getAcls();
  }

  private void setBucketAcl(String vol, String buck,
      List<OzoneAcl> ozoneAcls) throws IOException {
    String bucketKey = metadataManager.getBucketKey(vol, buck);
    OmBucketInfo omBucketInfo = metadataManager.getBucketTable().get(bucketKey);

    omBucketInfo.setAcls(ozoneAcls);

    metadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
  }

  private static OzoneObjInfo createVolume(String volumeName)
      throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName(adminUgi.getUserName())
        .setOwnerName(testUgi.getUserName())
        .build();
    TestOMRequestUtils.addVolumeToOM(metadataManager, volumeArgs);
    return new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(VOLUME)
        .setStoreType(OZONE)
        .build();
  }

  private static OzoneObjInfo createBucket(String volumeName, String bucketName)
      throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    TestOMRequestUtils.addBucketToOM(metadataManager, bucketInfo);
    return new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setResType(BUCKET)
        .setStoreType(OZONE)
        .build();
  }

  private OzoneObjInfo createKey(String volume, String bucket, String keyName)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(keyName)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setDataSize(0)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        // here we give test ugi full access
        .setAcls(OzoneAclUtil.getAclList(testUgi.getUserName(),
            testUgi.getGroupNames(), ALL, ALL))
        .build();


    if (keyName.split(OZONE_URI_DELIMITER).length > 1) {
      keyManager.createDirectory(keyArgs);
    } else {
      OpenKeySession keySession = keyManager.createFile(keyArgs, true, false);
      keyArgs.setLocationInfoList(
          keySession.getKeyInfo().getLatestVersionLocations()
              .getLocationList());
      keyManager.commitKey(keyArgs, keySession.getId());
    }

    return new OzoneObjInfo.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(keyName)
        .setResType(KEY)
        .setStoreType(OZONE)
        .build();
  }
}
