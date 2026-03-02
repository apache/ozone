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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration test for OM HA leader-specific ACL enforcement.
 * Demonstrates that ACL check responsibility depends entirely on the current leader,
 * with no expectation that all leaders are synchronized. Each leader enforces
 * ACLs based on its own configuration independently.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOMHALeaderSpecificACLEnforcement {

  private static final String OM_SERVICE_ID = "om-service-test-admin";
  private static final int NUM_OF_OMS = 3;
  private static final String TEST_USER = "testuser-" + 
      RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
  private static final String TEST_VOLUME = "testvol-" + 
      RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
  private static final String ADMIN_VOLUME = "adminvol-" + 
      RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
  private static final String TEST_BUCKET = "testbucket-" + 
      RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

  private MiniOzoneHAClusterImpl cluster;
  private OzoneClient client;
  private UserGroupInformation testUserUgi;
  private UserGroupInformation adminUserUgi;
  private OzoneManager theLeaderOM;

  @BeforeAll
  public void init() throws Exception {
    // Create test user
    testUserUgi = UserGroupInformation.createUserForTesting(TEST_USER, new String[]{"testgroup"});
    adminUserUgi = UserGroupInformation.getCurrentUser();
    
    // Set up and start the cluster
    setupCluster();
    
    // Create admin volume that will be used for bucket permission testing
    theLeaderOM = cluster.getOMLeader();
    createAdminVolume();
  }

  @AfterAll
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @BeforeEach
  public void restoreLeadership() throws IOException, InterruptedException, TimeoutException {
    OzoneManager currentLeader = cluster.getOMLeader();
    if (!currentLeader.getOMNodeId().equals(theLeaderOM.getOMNodeId())) {
      currentLeader.transferLeadership(theLeaderOM.getOMNodeId());
      GenericTestUtils.waitFor(() -> {
        try {
          OzoneManager currentLeaderCheck = cluster.getOMLeader();
          return !currentLeaderCheck.getOMNodeId().equals(currentLeader.getOMNodeId());
        } catch (Exception e) {
          return false;
        }
      }, 1000, 30000);
    }
  }

  /**
   * Main test method that validates leader-specific ACL enforcement in OM HA.
   * 1. Creates a mini cluster with OM HA
   * 2. Adds test user as admin to only the current leader OM node
   * 3. Validates user can perform admin operations when leader has the config
   * 4. Transfers leadership to another node (with independent configuration)
   * 5. Demonstrates that ACL enforcement depends entirely on new leader's config
   */
  @Test
  public void testOMHAAdminPrivilegesAfterLeadershipChange() throws Exception {
    // Step 1: Get the current leader OM
    OzoneManager currentLeader = cluster.getOMLeader();
    String leaderNodeId = currentLeader.getOMNodeId();
    
    // Step 2: Add test user as admin only to the current leader OM
    addAdminToSpecificOM(currentLeader, TEST_USER);
    
    // Verify admin was added
    assertTrue(currentLeader.getOmAdminUsernames().contains(TEST_USER),
        "Test user should be admin on leader OM");
    
    // Step 3: Test volume and bucket creation as test user (should succeed)
    testVolumeAndBucketCreationAsUser(true);
    
    // Step 4: Force leadership transfer to another OM node
    OzoneManager newLeader = cluster.transferOMLeadershipToAnotherNode(currentLeader);
    assertNotEquals(leaderNodeId, newLeader.getOMNodeId(), 
        "Leadership should have transferred to a different node");
    
    // Step 5: Verify test user is NOT admin on new leader
    assertTrue(!newLeader.getOmAdminUsernames().contains(TEST_USER),
        "Test user should NOT be admin on new leader OM");
    
    // Step 6: Test volume and bucket creation as test user (should fail)
    testVolumeAndBucketCreationAsUser(false);
  }

  /**
   * Sets up the OM HA cluster with node-specific admin configurations.
   */
  private void setupCluster() throws Exception {
    OzoneConfiguration conf = createBaseConfiguration();
    conf.setClass(OZONE_ACL_AUTHORIZER_CLASS, OzoneNativeAuthorizer.class,
        IAccessAuthorizer.class);
    
    // Build HA cluster
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(NUM_OF_OMS)
        .setNumDatanodes(3);
    
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
    
    // Create client
    client = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf);
  }

  /**
   * Creates base configuration for the cluster.
   */
  private OzoneConfiguration createBaseConfiguration() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    
    // Enable ACL for proper permission testing
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    
    // Set current user as initial admin (needed for cluster setup)
    String currentUser = adminUserUgi.getShortUserName();
    conf.set(OZONE_ADMINISTRATORS, currentUser);
    
    return conf;
  }

  /**
   * Creates an admin volume that will be used for testing bucket creation permissions.
   * This volume is created by the admin user, so non-admin users should not be able
   * to create buckets in it.
   */
  private void createAdminVolume() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    
    // Create volume as admin user
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    
    adminObjectStore.createVolume(ADMIN_VOLUME, volumeArgs);
  }

  /**
   * Adds a user as admin to a specific OM instance.
   * This uses reconfiguration to add the admin user.
   */
  private void addAdminToSpecificOM(OzoneManager om, String username) throws Exception {
    // Get current admin users
    String currentAdmins = String.join(",", om.getOmAdminUsernames());
    
    // Add the new user to admin list
    String newAdmins = currentAdmins + "," + username;
    
    // Reconfigure the OM to add the new admin
    om.getReconfigurationHandler().reconfigureProperty(OZONE_ADMINISTRATORS, newAdmins);
  }

  /**
   * Tests volume and bucket creation as the test user.
   * 
   * @param shouldSucceed true if operations should succeed, false if they should fail
   */
  private void testVolumeAndBucketCreationAsUser(boolean shouldSucceed) throws Exception {
    // Switch to test user context
    UserGroupInformation.setLoginUser(testUserUgi);
    
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      
      if (shouldSucceed) {
        // Test volume creation (should succeed)
        VolumeArgs volumeArgs = VolumeArgs.newBuilder()
            .setOwner(TEST_USER)
            .build();
        
        userObjectStore.createVolume(TEST_VOLUME, volumeArgs);
        OzoneVolume volume = userObjectStore.getVolume(TEST_VOLUME);
        assertNotNull(volume, "Volume should be created successfully");
        assertEquals(TEST_VOLUME, volume.getName());
        
        // Test bucket creation (should succeed)
        BucketArgs bucketArgs = BucketArgs.newBuilder()
            .build();
        
        volume.createBucket(TEST_BUCKET, bucketArgs);
        OzoneBucket bucket = volume.getBucket(TEST_BUCKET);
        assertNotNull(bucket, "Bucket should be created successfully");
        assertEquals(TEST_BUCKET, bucket.getName());
        
      } else {
        // Test volume creation (should fail)
        VolumeArgs volumeArgs = VolumeArgs.newBuilder()
            .setOwner(TEST_USER)
            .build();
        
        String newVolumeName = "failtest-" + RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
        OMException volumeException = assertThrows(OMException.class, () -> {
          userObjectStore.createVolume(newVolumeName, volumeArgs);
        }, "Volume creation should fail for non-admin user");
        assertEquals(PERMISSION_DENIED, volumeException.getResult());
        
        // Test bucket creation (should fail) - use admin-created volume
        if (volumeExists(userObjectStore, ADMIN_VOLUME)) {
          OzoneVolume adminVolume = userObjectStore.getVolume(ADMIN_VOLUME);
          BucketArgs bucketArgs = BucketArgs.newBuilder().build();
          String newBucketName = "failtest-" + RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
          
          OMException bucketException = assertThrows(OMException.class, () -> {
            adminVolume.createBucket(newBucketName, bucketArgs);
          }, "Bucket creation should fail for non-admin user in admin-owned volume");
          assertEquals(PERMISSION_DENIED, bucketException.getResult());
        }
      }
    } finally {
      // Reset to original user
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Tests that setTimes ACL check is enforced in preExecute and is leader-specific.
   * 1. Creates a key with admin user
   * 2. Adds test user as admin on the current leader
   * 3. Verifies that test user (as admin) can setTimes on key owned by someone else
   * 4. Transfers leadership to another node
   * 5. Verifies that setTimes fails with PERMISSION_DENIED when test user is no longer admin
   */
  @Test
  public void testKeySetTimesAclEnforcementAfterLeadershipChange() throws Exception {
    // Step 1: Create a volume, bucket, and key as the admin user
    ObjectStore adminObjectStore = client.getObjectStore();
    String keyTestVolume = "keyvol-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String keyTestBucket = "keybucket-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String keyName = "testkey-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    String adminUser = adminUserUgi.getShortUserName();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUser)
        .build();
    adminObjectStore.createVolume(keyTestVolume, volumeArgs);
    OzoneVolume adminVolume = adminObjectStore.getVolume(keyTestVolume);

    BucketArgs bucketArgs = BucketArgs.newBuilder().build();
    adminVolume.createBucket(keyTestBucket, bucketArgs);
    OzoneBucket adminBucket = adminVolume.getBucket(keyTestBucket);

    // Create a key as admin (so test user is NOT the owner)
    try (OzoneOutputStream out = adminBucket.createKey(keyName, 0)) {
      out.write("test data".getBytes(UTF_8));
    }

    OzoneKey key = adminBucket.getKey(keyName);
    assertNotNull(key, "Key should be created successfully");
    long originalMtime = key.getModificationTime().toEpochMilli();

    // Step 2: Get the current leader and add test user as admin
    OzoneManager currentLeader = cluster.getOMLeader();
    String leaderNodeId = currentLeader.getOMNodeId();
    addAdminToSpecificOM(currentLeader, TEST_USER);

    // Verify admin was added
    assertTrue(currentLeader.getOmAdminUsernames().contains(TEST_USER),
        "Test user should be admin on leader OM");

    // Switch to test user and try setTimes as admin (should succeed)
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      OzoneVolume userVolume = userObjectStore.getVolume(keyTestVolume);
      OzoneBucket userBucket = userVolume.getBucket(keyTestBucket);

      long newMtime = System.currentTimeMillis();
      userBucket.setTimes(keyName, newMtime, -1);

      // Verify the modification time was updated
      key = userBucket.getKey(keyName);
      assertEquals(newMtime, key.getModificationTime().toEpochMilli(),
          "Modification time should be updated by admin user");
      assertNotEquals(originalMtime, key.getModificationTime().toEpochMilli(),
          "Modification time should have changed");

      OzoneManager newLeader = cluster.transferOMLeadershipToAnotherNode(currentLeader);
      assertNotEquals(leaderNodeId, newLeader.getOMNodeId(),
          "Leadership should have transferred to a different node");
      assertFalse(newLeader.getOmAdminUsernames().contains(TEST_USER),
          "Test user should NOT be admin on new leader OM");

      long anotherMtime = System.currentTimeMillis() + 10000;
      OMException exception = assertThrows(OMException.class, () -> {
        userBucket.setTimes(keyName, anotherMtime, -1);
      }, "setTimes should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, exception.getResult(),
          "Should get PERMISSION_DENIED when ACL check fails in preExecute");
    } finally {
      // Reset to original user
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Helper method to check if volume exists.
   */
  private boolean volumeExists(ObjectStore store, String volumeName) {
    try {
      store.getVolume(volumeName);
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
