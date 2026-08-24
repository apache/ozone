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

import static org.apache.hadoop.ozone.TestDataUtil.createKey;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.AclTests;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.Test;

/**
 * Test recursive acl checks for delete and rename for FSO Buckets.
 */
public abstract class TestRecursiveAclWithFSO implements NonHATests.TestCase {

  private static final String UNIQUE = UUID.randomUUID().toString();
  private static final String VOLUME_NAME = "vol-" + UNIQUE;

  private final UserGroupInformation user1 = UserGroupInformation
      .createUserForTesting("user1-" + UNIQUE, new String[] {"test1"});
  private final UserGroupInformation user2 = UserGroupInformation
      .createUserForTesting("user2-" + UNIQUE, new String[] {"test2"});
  private final UserGroupInformation user3 = UserGroupInformation
      .createUserForTesting("user3-" + UNIQUE, new String[] {"test3, test4"});

  @Test
  public void testKeyDeleteAndRenameWithoutPermission() throws Exception {
    /* r = READ, w = WRITE, c = CREATE, d = DELETE
       l = LIST, a = ALL, n = NONE, x = READ_ACL, y = WRITE_ACL */
    String aclWorldAll = "world::a";
    List<String> keys = new ArrayList<>();
    // Create volumes with user1
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      createVolumeWithOwnerAndAcl(objectStore, VOLUME_NAME, user1.getShortUserName(), aclWorldAll);
    }

    // Login as user1, create directories and keys
    UserGroupInformation.setLoginUser(user1);
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
      BucketArgs omBucketArgs =
          BucketArgs.newBuilder().setStorageType(StorageType.DISK).build();

      // create bucket with user1
      volume.createBucket("bucket1", omBucketArgs);
      setBucketAcl(objectStore, volume.getName(), "bucket1", aclWorldAll);
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");

      /*
       *                       buck-1
       *                        |
       *                        a
       *                        |
       *          ------------------------------------
       *         |           |              |        |
       *         b1          b2             b3      file1
       *       -----       ------           -----
       *       |    |      |    |          |    |
       *      c1   c2     d1   d2          e1   e2
       *       |    |      |    |           |    |
       *       f1   f2     f3  --------     f5   f6
       *                      |        |
       *                    d21        file2
       *                     |
       *                     f4
       *
       *     Test Case 1 :
       *     Remove delete acl from file File2
       *     Try deleting b2
       *
       *     Test case 2:
       *     Remove delete acl from dir c2
       *     Try deleting b1
       *
       *     Test case 3
       *     try deleting b3
       */

      String keyf1 = "a/b1/c1/f1";
      String keyf2 = "a/b1/c2/f2";
      String keyf3 = "a/b2/d1/f3";
      String keyf4 = "a/b2/d2/d21/f4";
      String keyf5 = "/a/b3/e1/f5";
      String keyf6 = "/a/b3/e2/f6";
      String file1 = "a/" + "file" + RandomStringUtils.secure().nextNumeric(5);
      String file2 = "a/b2/d2/" + "file" + RandomStringUtils.secure().nextNumeric(5);

      keys.add(keyf1);
      keys.add(keyf2);
      keys.add(keyf3);
      keys.add(keyf4);
      keys.add(keyf5);
      keys.add(keyf6);
      keys.add(file1);
      keys.add(file2);
      createKeys(objectStore, ozoneBucket, keys);

      // Test case 1
      // Remove acls from file2
      // Delete/Rename on directory a/b2 should throw permission denied
      // (since file2 is a child)
      removeAclsFromKey(objectStore, ozoneBucket, file2);
    }

    UserGroupInformation.setLoginUser(user2);
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");

      // perform  delete
      OMException e =
          assertThrows(OMException.class, () -> ozoneBucket.deleteDirectory("a/b2", true));
      // expect permission error
      assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
          e.getResult(), "Permission check failed");

      // perform rename
      e = assertThrows(OMException.class, () -> ozoneBucket.renameKey("a/b2", "a/b2_renamed"));
      // expect permission error
      assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
          e.getResult(), "Permission check failed");

      // Test case 2
      // Remove acl from directory c2, delete/rename a/b1 should throw
      // permission denied since c2 is a subdirectory
      user1.doAs((PrivilegedExceptionAction<Void>) () -> {
        try (OzoneClient c = cluster().newClient()) {
          ObjectStore o = c.getObjectStore();
          OzoneBucket b = o.getVolume(VOLUME_NAME).getBucket("bucket1");
          removeAclsFromKey(o, b, "a/b1/c2");
        }
        return null;
      });

      UserGroupInformation.setLoginUser(user2);
      // perform  delete
      e = assertThrows(OMException.class, () -> ozoneBucket.deleteDirectory("a/b1", true));
      // expect permission error
      assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
          e.getResult(), "Permission check failed");

      // perform rename
      e = assertThrows(OMException.class, () -> ozoneBucket.renameKey("a/b1", "a/b1_renamed"));
      // expect permission error
      assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
          e.getResult(), "Permission check failed");

      // Test case 3
      // delete b3 and this should throw exception because user2 has no acls
      e = assertThrows(OMException.class, () -> ozoneBucket.deleteDirectory("a/b3", true));
      // expect permission error
      assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
          e.getResult(), "Permission check failed");
    }
  }

  @Test
  public void testKeyDefaultACL() throws Exception {
    String volumeName = "vol1";
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      objectStore.createVolume(volumeName);
      addVolumeAcl(objectStore, volumeName, "world::a");

      // verify volume ACLs. This volume will have 2 default ACLs, plus above one added
      OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volumeName)
          .setResType(OzoneObj.ResourceType.VOLUME)
          .setStoreType(OZONE).build();
      List<OzoneAcl> acls = objectStore.getAcl(obj);
      assertEquals(3, acls.size());
      assertEquals(AclTests.ADMIN_UGI.getShortUserName(), acls.get(0).getName());
      OmConfig omConfig = cluster().getOzoneManager().getConfig();
      assertEquals(omConfig.getUserDefaultRights(), acls.get(0).getAclSet());
      assertEquals(AclTests.ADMIN_UGI.getPrimaryGroupName(), acls.get(1).getName());
      assertEquals(omConfig.getGroupDefaultRights(), acls.get(1).getAclSet());
      assertEquals("WORLD", acls.get(2).getName());
      assertEquals(omConfig.getUserDefaultRights(), acls.get(2).getAclSet());
    }

    // set LoginUser as user3
    UserGroupInformation.setLoginUser(user3);
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      OzoneVolume volume = objectStore.getVolume(volumeName);
      BucketArgs omBucketArgs =
          BucketArgs.newBuilder().setStorageType(StorageType.DISK).build();
      String bucketName = "bucket";
      volume.createBucket(bucketName, omBucketArgs);
      OzoneBucket ozoneBucket = volume.getBucket(bucketName);

      // verify bucket default ACLs
      OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volume.getName())
          .setBucketName(ozoneBucket.getName()).setResType(OzoneObj.ResourceType.BUCKET)
          .setStoreType(OZONE).build();
      List<OzoneAcl> acls = objectStore.getAcl(obj);
      assertEquals(2, acls.size());
      assertEquals(user3.getShortUserName(), acls.get(0).getName());
      OmConfig omConfig = cluster().getOzoneManager().getConfig();
      assertEquals(omConfig.getUserDefaultRights(), acls.get(0).getAclSet());
      assertEquals(user3.getPrimaryGroupName(), acls.get(1).getName());
      assertEquals(omConfig.getGroupDefaultRights(), acls.get(1).getAclSet());

      // verify key default ACLs
      int length = 10;
      byte[] input = new byte[length];
      Arrays.fill(input, (byte) 96);
      String keyName = UUID.randomUUID().toString();
      createKey(ozoneBucket, keyName, input);
      obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volume.getName())
          .setBucketName(ozoneBucket.getName()).setKeyName(keyName)
          .setResType(OzoneObj.ResourceType.KEY).setStoreType(OZONE).build();
      acls = objectStore.getAcl(obj);
      assertEquals(2, acls.size());
      assertEquals(user3.getShortUserName(), acls.get(0).getName());
      assertEquals(omConfig.getUserDefaultRights(), acls.get(0).getAclSet());
      assertEquals(user3.getPrimaryGroupName(), acls.get(1).getName());
      assertEquals(omConfig.getGroupDefaultRights(), acls.get(1).getAclSet());
    }
  }

  private void removeAclsFromKey(ObjectStore objectStore,
      OzoneBucket ozoneBucket, String key) throws IOException {
    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder().setKeyName(key)
        .setBucketName(ozoneBucket.getName())
        .setVolumeName(ozoneBucket.getVolumeName())
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setResType(OzoneObj.ResourceType.KEY).build();
    List<OzoneAcl> aclList1 = objectStore.getAcl(ozoneObj);
    for (OzoneAcl acl : aclList1) {
      objectStore.removeAcl(ozoneObj, acl);
    }
  }

  private void createVolumeWithOwnerAndAcl(ObjectStore objectStore,
      String volumeName, String ownerName, String aclString)
      throws IOException {
    ClientProtocol proxy = objectStore.getClientProxy();
    objectStore.createVolume(volumeName);
    proxy.setVolumeOwner(volumeName, ownerName);
    setVolumeAcl(objectStore, volumeName, aclString);
  }

  /**
   * Helper function to set volume ACL.
   */
  private void setVolumeAcl(ObjectStore objectStore, String volumeName,
      String aclString) throws IOException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME).setStoreType(OZONE).build();
    assertTrue(objectStore.setAcl(obj, OzoneAcl.parseAcls(aclString)));
  }

  /**
   * Helper function to add volume ACL.
   */
  private void addVolumeAcl(ObjectStore objectStore, String volumeName,
      String aclString) throws IOException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME).setStoreType(OZONE).build();
    assertTrue(objectStore.addAcl(obj, OzoneAcl.parseAcl(aclString)));
  }

  /**
   * Helper function to set bucket ACL.
   */
  private void setBucketAcl(ObjectStore objectStore, String volumeName,
      String bucket, String aclString) throws IOException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volumeName)
        .setBucketName(bucket).setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OZONE).build();
    assertTrue(objectStore.setAcl(obj, OzoneAcl.parseAcls(aclString)));
  }

  /**
   * Helper function to set key ACL.
   */
  private void setKeyAcl(ObjectStore objectStore, String volumeName,
      String bucket, String key, String aclString) throws IOException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volumeName)
        .setBucketName(bucket).setKeyName(key)
        .setResType(OzoneObj.ResourceType.KEY).setStoreType(OZONE).build();
    assertTrue(objectStore.setAcl(obj, OzoneAcl.parseAcls(aclString)));
  }

  private void createKeys(ObjectStore objectStore, OzoneBucket ozoneBucket,
      List<String> keys) throws Exception {

    String aclWorldAll = "world::a";

    for (String key : keys) {
      TestDataUtil.createStringKey(ozoneBucket, key, 10);
      setKeyAcl(objectStore, ozoneBucket.getVolumeName(), ozoneBucket.getName(),
          key, aclWorldAll);
    }
  }
}

