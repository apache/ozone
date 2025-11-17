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

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.TestDataUtil.createKey;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.AclTests;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test for Ozone Bucket Owner.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestBucketOwner implements NonHATests.TestCase {

  private static final String UNIQUE = UUID.randomUUID().toString();
  private static final String VOLUME_NAME = "vol-" + UNIQUE;
  private static  UserGroupInformation user1 = UserGroupInformation
      .createUserForTesting("user-" + UNIQUE + 1, new String[] {"test1"});
  private static UserGroupInformation user2 = UserGroupInformation
      .createUserForTesting("user-" + UNIQUE + 2, new String[] {"test2"});
  private static UserGroupInformation user3 = UserGroupInformation
      .createUserForTesting("user-" + UNIQUE + 3, new String[] {"test3"});

  @BeforeAll
  void init() throws Exception {
    UserGroupInformation.setLoginUser(AclTests.ADMIN_UGI);
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore objectStore = client.getObjectStore();
    /* r = READ, w = WRITE, c = CREATE, d = DELETE
       l = LIST, a = ALL, n = NONE, x = READ_ACL, y = WRITE_ACL */
      String aclWorldAll = "world::a";
      createVolumeWithOwnerAndAcl(objectStore, VOLUME_NAME, user2.getShortUserName(), aclWorldAll);
    }
    UserGroupInformation.setLoginUser(user1);
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
      BucketArgs omBucketArgs = BucketArgs.newBuilder()
          .setStorageType(StorageType.DISK).setOwner(user1.getShortUserName()).build();
      volume.createBucket("bucket1", omBucketArgs);
      volume.createBucket("bucket2", omBucketArgs);
      volume.createBucket("bucket3", omBucketArgs);
    }
  }

  @Test
  public void testBucketOwner() throws Exception {
    // Test Key Operations as Bucket Owner,  Non-Volume Owner
    UserGroupInformation.setLoginUser(user1);
    try (OzoneClient client = cluster().newClient()) {
      OzoneVolume volume = client.getObjectStore()
          .getVolume(VOLUME_NAME);
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      //Key Create
      createKey(ozoneBucket, "key1", new byte[10]);
      createKey(ozoneBucket, "key2", new byte[10]);
      //Key Delete
      ozoneBucket.deleteKey("key1");
      //Bucket Delete
      volume.deleteBucket("bucket3");
      //List Keys
      ozoneBucket.listKeys("key");
      //Get Acls
      ozoneBucket.getAcls();
      //Add Acls
      OzoneAcl acl = OzoneAcl.of(USER, "testuser",
          DEFAULT, IAccessAuthorizer.ACLType.ALL);
      ozoneBucket.addAcl(acl);
    }
  }

  @Test
  public void testNonBucketNonVolumeOwner() throws Exception {
    // Test Key Operations Non-Bucket Owner, Non-Volume Owner
    //Key Create
    UserGroupInformation.setLoginUser(user3);
    try (OzoneClient client = cluster().newClient()) {
      assertThrows(Exception.class, () -> {
        OzoneVolume volume = client.getObjectStore().getVolume(VOLUME_NAME);
        OzoneBucket ozoneBucket = volume.getBucket("bucket1");
        createKey(ozoneBucket, "key3", new byte[10]);
      }, "Create key as non-volume and non-bucket owner should fail");
    }
    //Key Delete - should fail
    try (OzoneClient client = cluster().newClient()) {
      assertThrows(Exception.class, () -> {
        OzoneVolume volume = client.getObjectStore().getVolume(VOLUME_NAME);
        OzoneBucket ozoneBucket = volume.getBucket("bucket1");
        ozoneBucket.deleteKey("key2");
      }, "Delete key as non-volume and non-bucket owner should fail");
    }
    //Key Rename - should fail
    try (OzoneClient client = cluster().newClient()) {
      assertThrows(Exception.class, () -> {
        OzoneVolume volume = client.getObjectStore().getVolume(VOLUME_NAME);
        OzoneBucket ozoneBucket = volume.getBucket("bucket1");
        ozoneBucket.renameKey("key2", "key4");
      }, "Rename key as non-volume and non-bucket owner should fail");
    }
    //List Keys - should fail
    try (OzoneClient client = cluster().newClient()) {
      assertThrows(Exception.class, () -> {
        OzoneVolume volume = client.getObjectStore().getVolume(VOLUME_NAME);
        OzoneBucket ozoneBucket = volume.getBucket("bucket1");
        ozoneBucket.listKeys("key");
      }, "List keys as non-volume and non-bucket owner should fail");
    }
    //Get Acls - should fail
    try (OzoneClient client = cluster().newClient()) {
      assertThrows(Exception.class, () -> {
        OzoneVolume volume = client.getObjectStore().getVolume(VOLUME_NAME);
        OzoneBucket ozoneBucket = volume.getBucket("bucket1");
        ozoneBucket.getAcls();
      }, "Get Acls as non-volume and non-bucket owner should fail");
    }

    //Add Acls - should fail
    try (OzoneClient client = cluster().newClient()) {
      assertThrows(Exception.class, () -> {
        OzoneVolume volume = client.getObjectStore().getVolume(VOLUME_NAME);
        OzoneBucket ozoneBucket = volume.getBucket("bucket1");
        OzoneAcl acl = OzoneAcl.of(USER, "testuser1",
            DEFAULT, IAccessAuthorizer.ACLType.ALL);
        ozoneBucket.addAcl(acl);
      }, "Add Acls as non-volume and non-bucket owner should fail");
    }
  }

  @Test
  public void testVolumeOwner() throws Exception {
    //Test Key Operations for Volume Owner
    UserGroupInformation.setLoginUser(user2);
    try (OzoneClient client = cluster().newClient()) {
      OzoneVolume volume = client.getObjectStore().getVolume(VOLUME_NAME);
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      createKey(ozoneBucket, "key2", new byte[10]);
      //Key Delete
      ozoneBucket.deleteKey("key2");
      //List Keys
      ozoneBucket.listKeys("key");
      //Get Acls
      ozoneBucket.getAcls();
      //Add Acls
      OzoneAcl acl = OzoneAcl.of(USER, "testuser2",
          DEFAULT, IAccessAuthorizer.ACLType.ALL);
      ozoneBucket.addAcl(acl);
      //Bucket Delete
      volume.deleteBucket("bucket2");
    }
  }

  private static void createVolumeWithOwnerAndAcl(ObjectStore store,
      String volumeName, String ownerName, String aclString)
      throws IOException {
    ClientProtocol proxy = store.getClientProxy();
    store.createVolume(volumeName);
    proxy.setVolumeOwner(volumeName, ownerName);
    setVolumeAcl(store, volumeName, aclString);
  }

  /**
   * Helper function to set volume ACL.
   */
  private static void setVolumeAcl(ObjectStore store, String volumeName,
      String aclString) throws IOException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder().setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME).setStoreType(OZONE).build();
    assertTrue(store.setAcl(obj, OzoneAcl.parseAcls(aclString)));
  }
}
