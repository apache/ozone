/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for Ozone Bucket Owner.
 */
@Timeout(120)
public class TestBucketOwner {

  private static MiniOzoneCluster cluster;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestBucketOwner.class);
  private static  UserGroupInformation adminUser =
      UserGroupInformation.createUserForTesting("om", new String[]{"ozone"});
  private static  UserGroupInformation user1 = UserGroupInformation
      .createUserForTesting("user1", new String[] {"test1"});
  private static UserGroupInformation user2 = UserGroupInformation
      .createUserForTesting("user2", new String[] {"test2"});
  private static UserGroupInformation user3 = UserGroupInformation
      .createUserForTesting("user3", new String[] {"test3"});

  @BeforeAll
  public static void init() throws Exception {
    // loginUser is the user running this test.
    UserGroupInformation.setLoginUser(adminUser);
    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
            .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore objectStore = client.getObjectStore();
    /* r = READ, w = WRITE, c = CREATE, d = DELETE
       l = LIST, a = ALL, n = NONE, x = READ_ACL, y = WRITE_ACL */
      String aclWorldAll = "world::a";
      createVolumeWithOwnerAndAcl(objectStore, "volume1", "user2", aclWorldAll);
    }
    UserGroupInformation.setLoginUser(user1);
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      OzoneVolume volume = objectStore.getVolume("volume1");
      BucketArgs omBucketArgs = BucketArgs.newBuilder()
          .setStorageType(StorageType.DISK).setOwner("user1").build();
      volume.createBucket("bucket1", omBucketArgs);
      volume.createBucket("bucket2", omBucketArgs);
      volume.createBucket("bucket3", omBucketArgs);
    }
  }

  @AfterAll
  public static void stopCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBucketOwner() throws Exception {
    // Test Key Operations as Bucket Owner,  Non-Volume Owner
    UserGroupInformation.setLoginUser(user1);
    try (OzoneClient client = cluster.newClient()) {
      OzoneVolume volume = client.getObjectStore()
          .getVolume("volume1");
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      //Key Create
      createKey(ozoneBucket, "key1", 10, new byte[10]);
      createKey(ozoneBucket, "key2", 10, new byte[10]);
      //Key Delete
      ozoneBucket.deleteKey("key1");
      //Bucket Delete
      volume.deleteBucket("bucket3");
      //List Keys
      ozoneBucket.listKeys("key");
      //Get Acls
      ozoneBucket.getAcls();
      //Add Acls
      OzoneAcl acl = new OzoneAcl(USER, "testuser",
          IAccessAuthorizer.ACLType.ALL, DEFAULT);
      ozoneBucket.addAcl(acl);
    }
  }

  @Test
  public void testNonBucketNonVolumeOwner() throws Exception {
    // Test Key Operations Non-Bucket Owner, Non-Volume Owner
    //Key Create
    UserGroupInformation.setLoginUser(user3);
    try (OzoneClient client = cluster.newClient()) {
      OzoneVolume volume = client.getObjectStore()
              .getVolume("volume1");
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      createKey(ozoneBucket, "key3", 10, new byte[10]);
      fail("Create key as non-volume and non-bucket owner should fail");
    } catch (Exception ex) {
      LOG.info(ex.getMessage());
    }
    //Key Delete - should fail
    try (OzoneClient client = cluster.newClient()) {
      OzoneVolume volume = client.getObjectStore()
              .getVolume("volume1");
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      ozoneBucket.deleteKey("key2");
      fail("Delete key as non-volume and non-bucket owner should fail");
    } catch (Exception ex) {
      LOG.info(ex.getMessage());
    }
    //Key Rename - should fail
    try (OzoneClient client = cluster.newClient()) {
      OzoneVolume volume = client.getObjectStore()
              .getVolume("volume1");
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      ozoneBucket.renameKey("key2", "key4");
      fail("Rename key as non-volume and non-bucket owner should fail");
    } catch (Exception ex) {
      LOG.info(ex.getMessage());
    }
    //List Keys - should fail
    try (OzoneClient client = cluster.newClient()) {
      OzoneVolume volume = client.getObjectStore()
              .getVolume("volume1");
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      ozoneBucket.listKeys("key");
      fail("List keys as non-volume and non-bucket owner should fail");
    } catch (Exception ex) {
      LOG.info(ex.getMessage());
    }
    //Get Acls - should fail
    try (OzoneClient client = cluster.newClient()) {
      OzoneVolume volume = client.getObjectStore()
              .getVolume("volume1");
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      ozoneBucket.getAcls();
      fail("Get Acls as non-volume and non-bucket owner should fail");
    } catch (Exception ex) {
      LOG.info(ex.getMessage());
    }
    //Add Acls - should fail
    try (OzoneClient client = cluster.newClient()) {
      OzoneVolume volume = client.getObjectStore()
              .getVolume("volume1");
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      OzoneAcl acl = new OzoneAcl(USER, "testuser1",
              IAccessAuthorizer.ACLType.ALL, DEFAULT);
      ozoneBucket.addAcl(acl);
      fail("Add Acls as non-volume and non-bucket owner should fail");
    } catch (Exception ex) {
      LOG.info(ex.getMessage());
    }
  }

  @Test
  public void testVolumeOwner() throws Exception {
    //Test Key Operations for Volume Owner
    UserGroupInformation.setLoginUser(user2);
    try (OzoneClient client = cluster.newClient()) {
      OzoneVolume volume = client.getObjectStore().getVolume("volume1");
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");
      //Key Create
      createKey(ozoneBucket, "key2", 10, new byte[10]);
      //Key Delete
      ozoneBucket.deleteKey("key2");
      //List Keys
      ozoneBucket.listKeys("key");
      //Get Acls
      ozoneBucket.getAcls();
      //Add Acls
      OzoneAcl acl = new OzoneAcl(USER, "testuser2",
          IAccessAuthorizer.ACLType.ALL, DEFAULT);
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

  private void createKey(OzoneBucket ozoneBucket, String key, int length,
       byte[] input) throws Exception {
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key, length);
    ozoneOutputStream.write(input);
    ozoneOutputStream.write(input, 0, 10);
    ozoneOutputStream.close();
  }
}
