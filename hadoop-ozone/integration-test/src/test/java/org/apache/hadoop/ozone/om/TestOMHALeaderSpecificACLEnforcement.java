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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneAcl;
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
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
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
      BooleanSupplier leadershipCheck = () -> {
        try {
          return !cluster.getOMLeader().getOMNodeId().equals(currentLeader.getOMNodeId());
        } catch (Exception e) {
          return false;
        }
      };
      GenericTestUtils.waitFor(leadershipCheck, 1000, 30000);
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
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Step 3: Test volume and bucket creation as test user (should succeed)
    testVolumeAndBucketCreationAsUser(true);

    // Step 4: Force leadership transfer to another OM node
    OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
    assertNotEquals(leaderNodeId, newLeader.getOMNodeId(),
        "Leadership should have transferred to a different node");

    // Step 5: Verify test user is NOT admin on new leader
    assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

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
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

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

      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertNotEquals(leaderNodeId, newLeader.getOMNodeId(),
          "Leadership should have transferred to a different node");
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

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
   * Tests that setQuota ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testVolumeSetQuotaAclEnforcementAfterLeadershipChange() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    String testVolume = "quotavol-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    // Create volume as admin
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    adminObjectStore.createVolume(testVolume, volumeArgs);

    // Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Test user should be able to set quota as admin
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      OzoneVolume userVolume = userObjectStore.getVolume(testVolume);

      OzoneQuota quota1 = OzoneQuota.getOzoneQuota(100L * 1024 * 1024 * 1024, 1000);
      userVolume.setQuota(quota1); // Set quota to 100GB

      // Transfer leadership
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

      // Should fail on new leader
      OzoneQuota quota2 = OzoneQuota.getOzoneQuota(200L * 1024 * 1024 * 1024, 2000);
      OMException exception = assertThrows(OMException.class, () -> {
        userVolume.setQuota(quota2);
      }, "setQuota should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, exception.getResult());
    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Tests that setOwner ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testVolumeSetOwnerAclEnforcementAfterLeadershipChange() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    String testVolume = "ownervol-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    // Create volume as admin
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    adminObjectStore.createVolume(testVolume, volumeArgs);

    // Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Test user should be able to change owner as admin
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      OzoneVolume userVolume = userObjectStore.getVolume(testVolume);

      userVolume.setOwner("newowner");

      // Transfer leadership
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

      // Should fail on new leader
      OMException exception = assertThrows(OMException.class, () -> {
        userVolume.setOwner("anothernewowner");
      }, "setOwner should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, exception.getResult());
    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Tests that deleteVolume ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testVolumeDeleteAclEnforcementAfterLeadershipChange() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    String testVolume1 = "delvol1-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String testVolume2 = "delvol2-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    // Create volumes as admin
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    adminObjectStore.createVolume(testVolume1, volumeArgs);
    adminObjectStore.createVolume(testVolume2, volumeArgs);

    // Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Test user should be able to delete volume as admin
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();

      userObjectStore.deleteVolume(testVolume1);

      // Transfer leadership
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

      // Should fail on new leader
      OMException exception = assertThrows(OMException.class, () -> {
        userObjectStore.deleteVolume(testVolume2);
      }, "deleteVolume should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, exception.getResult());
    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Tests that setBucketProperty ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testBucketSetPropertyAclEnforcementAfterLeadershipChange() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    String testVolume = "bucketpropvol-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String testBucket = "bucketprop-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    // Create volume and bucket as admin
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    adminObjectStore.createVolume(testVolume, volumeArgs);
    OzoneVolume adminVolume = adminObjectStore.getVolume(testVolume);

    BucketArgs bucketArgs = BucketArgs.newBuilder().build();
    adminVolume.createBucket(testBucket, bucketArgs);

    // Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Test user should be able to set bucket properties as admin
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      OzoneVolume userVolume = userObjectStore.getVolume(testVolume);
      OzoneBucket userBucket = userVolume.getBucket(testBucket);

      // Set versioning
      userBucket.setVersioning(true);

      // Transfer leadership
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

      // Should fail on new leader
      OMException exception = assertThrows(OMException.class, () -> {
        userBucket.setVersioning(false);
      }, "setBucketProperty should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, exception.getResult());
    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Tests that setBucketOwner ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testBucketSetOwnerAclEnforcementAfterLeadershipChange() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    String testVolume = "bucketownervol-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String testBucket = "bucketowner-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    // Create volume and bucket as admin
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    adminObjectStore.createVolume(testVolume, volumeArgs);
    OzoneVolume adminVolume = adminObjectStore.getVolume(testVolume);

    BucketArgs bucketArgs = BucketArgs.newBuilder().build();
    adminVolume.createBucket(testBucket, bucketArgs);

    // Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Test user should be able to set bucket owner as admin
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      OzoneVolume userVolume = userObjectStore.getVolume(testVolume);
      OzoneBucket userBucket = userVolume.getBucket(testBucket);

      // Set new owner
      userBucket.setOwner("newowner");

      // Transfer leadership
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

      // Should fail on new leader
      OMException exception = assertThrows(OMException.class, () -> {
        userBucket.setOwner("anothernewowner");
      }, "setBucketOwner should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, exception.getResult());
    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Tests that deleteBucket ACL check is enforced in preExecute and is leader-specific.
   */
  @Test
  public void testBucketDeleteAclEnforcementAfterLeadershipChange() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    String testVolume = "bucketdelvol-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String testBucket1 = "bucketdel1-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String testBucket2 = "bucketdel2-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    // Create volume and buckets as admin
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    adminObjectStore.createVolume(testVolume, volumeArgs);
    OzoneVolume adminVolume = adminObjectStore.getVolume(testVolume);

    BucketArgs bucketArgs = BucketArgs.newBuilder().build();
    adminVolume.createBucket(testBucket1, bucketArgs);
    adminVolume.createBucket(testBucket2, bucketArgs);

    // Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Test user should be able to delete bucket as admin
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      OzoneVolume userVolume = userObjectStore.getVolume(testVolume);

      userVolume.deleteBucket(testBucket1);

      // Transfer leadership
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

      // Should fail on new leader
      OMException exception = assertThrows(OMException.class, () -> {
        userVolume.deleteBucket(testBucket2);
      }, "deleteBucket should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, exception.getResult());
    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }
  }

  /**
   * Tests that deleteKeys (bulk) ACL check is enforced in preExecute and is leader-specific.
   *
   * <p>This test verifies that when using the bulk deleteKeys API:
   * <ul>
   *   <li>Keys that pass ACL checks are deleted successfully</li>
   *   <li>Keys that fail ACL checks are silently filtered out (no exception thrown)</li>
   *   <li>ACL enforcement is based on the current leader's configuration</li>
   * </ul>
   *
   * <p>The test flow:
   * <ol>
   *   <li>Create test volume, bucket, and multiple keys as admin</li>
   *   <li>Add test user as admin on current leader</li>
   *   <li>Test user successfully deletes first key using bulk API</li>
   *   <li>Transfer leadership to node where test user is NOT admin</li>
   *   <li>Test user attempts to delete second key - operation succeeds but key is not deleted</li>
   *   <li>Verify the key still exists (was filtered out due to ACL failure)</li>
   * </ol>
   */
  @Test
  public void testKeysDeleteAclEnforcementAfterLeadershipChange() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    String testVolume = "keysdelvol-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String testBucket = "keysdel-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String keyName1 = "key1-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String keyName2 = "key2-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    // Step 1: Create volume, bucket, and keys as admin
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    adminObjectStore.createVolume(testVolume, volumeArgs);
    OzoneVolume adminVolume = adminObjectStore.getVolume(testVolume);

    BucketArgs bucketArgs = BucketArgs.newBuilder().build();
    adminVolume.createBucket(testBucket, bucketArgs);
    OzoneBucket adminBucket = adminVolume.getBucket(testBucket);

    // Create test keys
    try (OzoneOutputStream out = adminBucket.createKey(keyName1, 0)) {
      out.write("test data 1".getBytes(UTF_8));
    }
    try (OzoneOutputStream out = adminBucket.createKey(keyName2, 0)) {
      out.write("test data 2".getBytes(UTF_8));
    }

    // Step 2: Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    String originalLeaderNodeId = currentLeader.getOMNodeId();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Step 3: Test user deletes first key successfully using bulk API
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      OzoneVolume userVolume = userObjectStore.getVolume(testVolume);
      OzoneBucket userBucket = userVolume.getBucket(testBucket);

      // Use bulk deleteKeys API (this supports ACL filtering)
      userBucket.deleteKeys(Collections.singletonList(keyName1));

      // Verify key1 was deleted
      OMException ex1 = assertThrows(OMException.class, () -> userBucket.getKey(keyName1),
          "Key1 should be deleted");
      assertEquals(OMException.ResultCodes.KEY_NOT_FOUND, ex1.getResult());

      // Step 4: Transfer leadership to another node where test user is NOT admin
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertNotEquals(originalLeaderNodeId, newLeader.getOMNodeId(),
          "Leadership should have transferred to a different node");
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

      // Step 5: Attempt to delete second key - should not throw but key should not be deleted
      // The bulk deleteKeys API filters out keys that fail ACL check in preExecute
      userBucket.deleteKeys(Collections.singletonList(keyName2));
    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }

    // Step 6: Verify key2 still exists (was filtered out due to ACL failure)
    OzoneKey key2 = adminBucket.getKey(keyName2);
    assertNotNull(key2, "Key2 should still exist after being filtered out by ACL check");
    assertEquals(keyName2, key2.getName());
  }

  /**
   * Tests that renameKeys (bulk) ACL check is enforced in preExecute and is leader-specific.
   *
   * <p>This test verifies that when using the bulk renameKeys API:
   * <ul>
   *   <li>Key pairs that pass ACL checks are renamed successfully</li>
   *   <li>Key pairs that fail ACL checks are silently filtered out (no exception thrown)</li>
   *   <li>ACL enforcement is based on the current leader's configuration</li>
   * </ul>
   *
   * <p>The test flow:
   * <ol>
   *   <li>Create test volume, bucket, and multiple keys as admin with LEGACY bucket layout</li>
   *   <li>Add test user as admin on current leader</li>
   *   <li>Test user successfully renames first key using bulk API</li>
   *   <li>Transfer leadership to node where test user is NOT admin</li>
   *   <li>Test user attempts to rename second key - operation succeeds but key is not renamed</li>
   *   <li>Verify the key still has original name (was filtered out due to ACL failure)</li>
   * </ol>
   *
   * <p>Note: This test uses LEGACY bucket layout because the bulk renameKeys API is deprecated
   * and not supported for FILE_SYSTEM_OPTIMIZED layouts.
   */
  @Test
  public void testKeysRenameAclEnforcementAfterLeadershipChange() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    String testVolume = "keysrenamevol-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String testBucket = "keysrename-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String keyName1 = "key1-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String keyName2 = "key2-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String newKeyName1 = "newkey1-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String newKeyName2 = "newkey2-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    // Step 1: Create volume, bucket with LEGACY layout, and keys as admin
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    adminObjectStore.createVolume(testVolume, volumeArgs);
    OzoneVolume adminVolume = adminObjectStore.getVolume(testVolume);

    // Use LEGACY bucket layout since bulk renameKeys is not supported for FSO
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.LEGACY)
        .build();
    adminVolume.createBucket(testBucket, bucketArgs);
    OzoneBucket adminBucket = adminVolume.getBucket(testBucket);

    // Create test keys
    try (OzoneOutputStream out = adminBucket.createKey(keyName1, 0)) {
      out.write("test data 1".getBytes(UTF_8));
    }
    try (OzoneOutputStream out = adminBucket.createKey(keyName2, 0)) {
      out.write("test data 2".getBytes(UTF_8));
    }

    // Step 2: Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    String originalLeaderNodeId = currentLeader.getOMNodeId();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Step 3: Test user renames first key successfully using bulk API
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();
      OzoneVolume userVolume = userObjectStore.getVolume(testVolume);
      OzoneBucket userBucket = userVolume.getBucket(testBucket);

      // Use bulk renameKeys API (this supports ACL filtering)
      Map<String, String> renameMap1 = new HashMap<>();
      renameMap1.put(keyName1, newKeyName1);
      userBucket.renameKeys(renameMap1);

      // Verify key1 was renamed
      OMException ex1 = assertThrows(OMException.class, () -> userBucket.getKey(keyName1),
          "Original key1 should not exist after rename");
      assertEquals(OMException.ResultCodes.KEY_NOT_FOUND, ex1.getResult());
      assertNotNull(userBucket.getKey(newKeyName1), "Renamed key1 should exist");

      // Step 4: Transfer leadership to another node where test user is NOT admin
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertNotEquals(originalLeaderNodeId, newLeader.getOMNodeId(),
          "Leadership should have transferred to a different node");
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

      // Step 5: Attempt to rename second key - should not throw but key should not be renamed
      // The bulk renameKeys API filters out key pairs that fail ACL check in preExecute
      Map<String, String> renameMap2 = new HashMap<>();
      renameMap2.put(keyName2, newKeyName2);
      userBucket.renameKeys(renameMap2);
    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }

    // Step 6: Verify key2 still has original name (was filtered out due to ACL failure)
    OzoneKey key2 = adminBucket.getKey(keyName2);
    assertNotNull(key2, "Original key2 should still exist after being filtered out by ACL check");
    assertEquals(keyName2, key2.getName());

    // Verify new key name doesn't exist
    OMException ex2 = assertThrows(OMException.class, () -> adminBucket.getKey(newKeyName2),
        "New key name should not exist after ACL filtering");
    assertEquals(OMException.ResultCodes.KEY_NOT_FOUND, ex2.getResult());
  }

  /**
   * Tests that key ACL operations (addAcl, removeAcl, setAcl) are enforced in preExecute
   * and are leader-specific. Uses FILE_SYSTEM_OPTIMIZED bucket layout.
   *
   * <p>This test verifies that ACL operations on keys work correctly across leadership changes:
   * <ul>
   *   <li>Admin user can modify ACLs on any key when they're admin on the current leader</li>
   *   <li>Admin user cannot modify ACLs after leadership transfer to a node where they're not admin</li>
   *   <li>ACL checks happen in preExecute phase before any transaction execution</li>
   * </ul>
   */
  @Test
  public void testKeyAclOperationsEnforcementAfterLeadershipChangeWithFSO() throws Exception {
    ObjectStore adminObjectStore = client.getObjectStore();
    String testVolume = "keyaclvol-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String testBucket = "keyaclbucket-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);
    String keyName = "keyacl-" +
        RandomStringUtils.secure().nextAlphabetic(5).toLowerCase(Locale.ROOT);

    // Step 1: Create volume, bucket with FSO layout, and key as admin
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setOwner(adminUserUgi.getShortUserName())
        .build();
    adminObjectStore.createVolume(testVolume, volumeArgs);
    OzoneVolume adminVolume = adminObjectStore.getVolume(testVolume);

    // Use FILE_SYSTEM_OPTIMIZED bucket layout
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();
    adminVolume.createBucket(testBucket, bucketArgs);
    OzoneBucket adminBucket = adminVolume.getBucket(testBucket);

    // Create a key as admin (so test user is NOT the owner)
    try (OzoneOutputStream out = adminBucket.createKey(keyName, 0)) {
      out.write("test data for ACL operations".getBytes(UTF_8));
    }

    OzoneKey key = adminBucket.getKey(keyName);
    assertNotNull(key, "Key should be created successfully");

    // Create OzoneObj for ACL operations
    OzoneObj keyObj = OzoneObjInfo.Builder.newBuilder()
        .setVolumeName(testVolume)
        .setBucketName(testBucket)
        .setKeyName(keyName)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    int originalAclCount = adminObjectStore.getAcl(keyObj).size();

    // Step 2: Add test user as admin on current leader
    OzoneManager currentLeader = cluster.getOMLeader();
    String leaderNodeId = currentLeader.getOMNodeId();
    addAdminToSpecificOM(currentLeader, TEST_USER);
    assertThat(currentLeader.getOmAdminUsernames()).contains(TEST_USER);

    // Step 3: Test user performs ACL operations as admin (should succeed)
    UserGroupInformation.setLoginUser(testUserUgi);
    try (OzoneClient userClient = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, cluster.getConf())) {
      ObjectStore userObjectStore = userClient.getObjectStore();

      // Add ACL - should succeed
      OzoneAcl addAcl = OzoneAcl.parseAcl("user:anotheruser:rw[ACCESS]");
      boolean addResult = userObjectStore.addAcl(keyObj, addAcl);
      assertThat(addResult).isTrue();

      // Verify ACL was added
      List<OzoneAcl> acls = userObjectStore.getAcl(keyObj);
      assertThat(acls).hasSize(originalAclCount + 1);
      assertThat(acls).contains(addAcl);

      // Set ACL - should succeed
      OzoneAcl setAcl = OzoneAcl.parseAcl("user:setuser:rwx[ACCESS]");
      boolean setResult = userObjectStore.setAcl(keyObj, Collections.singletonList(setAcl));
      assertThat(setResult).isTrue();

      // Verify ACL was set (replaced all previous ACLs)
      acls = userObjectStore.getAcl(keyObj);
      assertThat(acls).hasSize(1);
      assertThat(acls).contains(setAcl);

      // Step 4: Transfer leadership to another node where test user is NOT admin
      OzoneManager newLeader = transferLeadershipToAnotherNode(currentLeader);
      assertNotEquals(leaderNodeId, newLeader.getOMNodeId(),
          "Leadership should have transferred to a different node");
      assertThat(newLeader.getOmAdminUsernames()).doesNotContain(TEST_USER);

      // Step 5: Try ACL operations on new leader - should fail with PERMISSION_DENIED
      OzoneAcl anotherAcl = OzoneAcl.parseAcl("user:yetanotheruser:r[ACCESS]");

      // Add ACL should fail
      OMException addException = assertThrows(OMException.class, () -> {
        userObjectStore.addAcl(keyObj, anotherAcl);
      }, "addAcl should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, addException.getResult(),
          "Should get PERMISSION_DENIED when ACL check fails in preExecute");

      // Remove ACL should fail
      OMException removeException = assertThrows(OMException.class, () -> {
        userObjectStore.removeAcl(keyObj, setAcl);
      }, "removeAcl should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, removeException.getResult(),
          "Should get PERMISSION_DENIED when ACL check fails in preExecute");

      // Set ACL should fail
      OzoneAcl newSetAcl = OzoneAcl.parseAcl("user:failuser:w[ACCESS]");
      OMException setException = assertThrows(OMException.class, () -> {
        userObjectStore.setAcl(keyObj, Collections.singletonList(newSetAcl));
      }, "setAcl should fail for non-admin user on new leader");
      assertEquals(PERMISSION_DENIED, setException.getResult(),
          "Should get PERMISSION_DENIED when ACL check fails in preExecute");

    } finally {
      UserGroupInformation.setLoginUser(adminUserUgi);
    }

    // Step 6: Verify the ACLs remain unchanged (operations were rejected in preExecute)
    List<OzoneAcl> finalAcls = adminObjectStore.getAcl(keyObj);
    assertThat(finalAcls).hasSize(1);
    assertThat(finalAcls).contains(OzoneAcl.parseAcl("user:setuser:rwx[ACCESS]"));
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

  /**
   * Transfers leadership from current leader to another OM node.
   *
   * @param currentLeader the current leader OM
   * @return the new leader OM after transfer
   */
  private OzoneManager transferLeadershipToAnotherNode(OzoneManager currentLeader) throws Exception {
    // Get list of all OMs
    List<OzoneManager> omList = new ArrayList<>(cluster.getOzoneManagersList());

    // Remove current leader from list
    omList.remove(currentLeader);

    // Select the first alternative OM as target
    OzoneManager targetOM = omList.get(0);
    String targetNodeId = targetOM.getOMNodeId();

    // Transfer leadership
    currentLeader.transferLeadership(targetNodeId);

    // Wait for leadership transfer to complete
    BooleanSupplier leadershipTransferCheck = () -> {
      try {
        return !cluster.getOMLeader().getOMNodeId().equals(currentLeader.getOMNodeId());
      } catch (Exception e) {
        return false;
      }
    };
    GenericTestUtils.waitFor(leadershipTransferCheck, 1000, 30000);

    // Verify leadership change
    cluster.waitForLeaderOM();
    OzoneManager newLeader = cluster.getOMLeader();

    assertEquals(targetNodeId, newLeader.getOMNodeId(),
        "Leadership should have transferred to target OM");

    return newLeader;
  }
}
