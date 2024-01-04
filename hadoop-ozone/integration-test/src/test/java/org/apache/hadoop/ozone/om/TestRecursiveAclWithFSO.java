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

package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
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
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test recursive acl checks for delete and rename for FSO Buckets.
 */
@Timeout(120)
public class TestRecursiveAclWithFSO {

  private MiniOzoneCluster cluster;

  private final UserGroupInformation adminUser =
      UserGroupInformation.createUserForTesting("om", new String[] {"ozone"});
  private final UserGroupInformation user1 = UserGroupInformation
      .createUserForTesting("user1", new String[] {"test1"});
  private final UserGroupInformation user2 = UserGroupInformation
      .createUserForTesting("user2", new String[] {"test2"});

  @BeforeEach
  public void init() throws Exception {
    // loginUser is the user running this test.
    // Implication: loginUser is automatically added to the OM admin list.
    UserGroupInformation.setLoginUser(adminUser);
    // ozone.acl.enabled = true
    // start a cluster
    startCluster();
  }

  @Test
  public void testKeyDeleteAndRenameWithoutPermission() throws Exception {
    /* r = READ, w = WRITE, c = CREATE, d = DELETE
       l = LIST, a = ALL, n = NONE, x = READ_ACL, y = WRITE_ACL */
    String aclWorldAll = "world::a";
    List<String> keys = new ArrayList<>();
    // Create volumes with user1
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      createVolumeWithOwnerAndAcl(objectStore, "volume1", "user1", aclWorldAll);
    }

    // Login as user1, create directories and keys
    UserGroupInformation.setLoginUser(user1);
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      OzoneVolume volume = objectStore.getVolume("volume1");
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
      String file1 = "a/" + "file" + RandomStringUtils.randomNumeric(5);
      String file2 = "a/b2/d2/" + "file" + RandomStringUtils.randomNumeric(5);

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
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      OzoneVolume volume = objectStore.getVolume("volume1");
      OzoneBucket ozoneBucket = volume.getBucket("bucket1");

      // perform  delete
      try {
        ozoneBucket.deleteDirectory("a/b2", true);
        fail("Should throw permission denied !");
      } catch (OMException ome) {
        // expect permission error
        assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
            ome.getResult(), "Permission check failed");
      }
      // perform rename
      try {
        ozoneBucket.renameKey("a/b2", "a/b2_renamed");
        fail("Should throw permission denied !");
      } catch (OMException ome) {
        // expect permission error
        assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
            ome.getResult(), "Permission check failed");
      }

      // Test case 2
      // Remove acl from directory c2, delete/rename a/b1 should throw
      // permission denied since c2 is a subdirectory
      user1.doAs((PrivilegedExceptionAction<Void>) () -> {
        try (OzoneClient c = cluster.newClient()) {
          ObjectStore o = c.getObjectStore();
          OzoneBucket b = o.getVolume("volume1").getBucket("bucket1");
          removeAclsFromKey(o, b, "a/b1/c2");
        }
        return null;
      });

      UserGroupInformation.setLoginUser(user2);
      // perform  delete
      try {
        ozoneBucket.deleteDirectory("a/b1", true);
        fail("Should throw permission denied !");
      } catch (OMException ome) {
        // expect permission error
        assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
            ome.getResult(), "Permission check failed");
      }

      // perform rename
      try {
        ozoneBucket.renameKey("a/b1", "a/b1_renamed");
        fail("Should throw permission denied !");
      } catch (OMException ome) {
        // expect permission error
        assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
            ome.getResult(), "Permission check failed");
      }

      // Test case 3
      // delete b3 and this should throw exception because user2 has no acls
      try {
        ozoneBucket.deleteDirectory("a/b3", true);
        fail("Should throw permission denied !");
      } catch (OMException ome) {
        // expect permission error
        assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
            ome.getResult(), "Permission check failed");
      }
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

  /**
   * Create a MiniOzoneCluster for testing.
   */
  private void startCluster() throws Exception {

    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();

    // Use native impl here, default impl doesn't do actual checks
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    // Note: OM doesn't support live config reloading
    conf.setBoolean(OZONE_ACL_ENABLED, true);

    OMRequestTestUtils.configureFSOptimizedPaths(conf, true);

    cluster =
        MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
            .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();

  }

  @AfterEach
  public void stopCluster() {
    if (cluster != null) {
      cluster.shutdown();
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
    int length = 10;
    String aclWorldAll = "world::a";
    byte[] input = new byte[length];
    Arrays.fill(input, (byte) 96);
    for (String key : keys) {
      createKey(ozoneBucket, key, 10, input);
      setKeyAcl(objectStore, ozoneBucket.getVolumeName(), ozoneBucket.getName(),
          key, aclWorldAll);
    }
  }

  private void createKey(OzoneBucket ozoneBucket, String key, int length,
      byte[] input) throws Exception {
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key, length);
    ozoneOutputStream.write(input);
    ozoneOutputStream.write(input, 0, 10);
    ozoneOutputStream.close();
  }

}

