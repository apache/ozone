/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Integration tests for the open key cleanup service on OM.
 */
@RunWith(Parameterized.class)
public class TestOpenKeyCleanupService {
  // Increase service interval of open key cleanup so we can trigger the
  // service manually between setting up the DB and checking the results.
  // Increase service interval of key deleting service to ensure it does not
  // run during the tests, interfering with results.
  private static final TimeDuration TESTING_SERVICE_INTERVAL =
      TimeDuration.valueOf(24, TimeUnit.HOURS);
  // High expiration time used so keys without modified creation time will not
  // expire during the test.
  private static final TimeDuration TESTING_EXPIRE_THRESHOLD =
      TimeDuration.valueOf(24, TimeUnit.HOURS);
  // Maximum number of keys to be cleaned up per run of the service.
  private static final int TESTING_TASK_LIMIT = 10;
  // Volume and bucket created and added to the DB that will hold open keys
  // created by this test unless tests specify otherwise.
  private static final String DEFAULT_VOLUME = "volume";
  private static final String DEFAULT_BUCKET = "bucket";
  // Time in milliseconds to wait for followers in the cluster to apply
  // transactions.
  private static final int FOLLOWER_WAIT_TIMEOUT = 10000;
  // Time in milliseconds between checks that followers have applied
  // transactions.
  private static final int FOLLOWER_CHECK_INTERVAL = 1000;

  private MiniOzoneCluster cluster;
  private boolean isOMHA;
  private List<OzoneManager> ozoneManagers;

  // Parameterized to test open key cleanup in both OM HA and non-HA.
  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    List<Object[]> params = new ArrayList<>();
    params.add(new Boolean[]{true});
    params.add(new Boolean[]{false});
    return params;
  }

  public TestOpenKeyCleanupService(boolean isOMHA) {
    this.isOMHA = isOMHA;
  }

  private void setupCluster() throws Exception {
    setupCluster(TESTING_SERVICE_INTERVAL, TESTING_EXPIRE_THRESHOLD);
  }

  private void setupCluster(TimeDuration openKeyCleanupServiceInterval,
      TimeDuration openKeyExpireThreshold) throws Exception {

    OzoneConfiguration conf = new OzoneConfiguration();

    // Make sure key deletion does not run during the tests.
    conf.setTimeDuration(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        TESTING_SERVICE_INTERVAL.getDuration(),
        TESTING_SERVICE_INTERVAL.getUnit());
    // Set open key cleanup configurations.
    conf.setTimeDuration(OMConfigKeys.OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
        openKeyCleanupServiceInterval.getDuration(),
        openKeyCleanupServiceInterval.getUnit());
    conf.setTimeDuration(OMConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD,
        openKeyExpireThreshold.getDuration(), openKeyExpireThreshold.getUnit());
    conf.setInt(OMConfigKeys.OZONE_OPEN_KEY_CLEANUP_LIMIT_PER_TASK,
        TESTING_TASK_LIMIT);

    if (isOMHA) {
      cluster = MiniOzoneCluster.newHABuilder(conf)
          .setOMServiceId("om-service-id")
          .setNumOfOzoneManagers(3)
          .build();
      ozoneManagers = ((MiniOzoneHAClusterImpl) cluster).getOzoneManagersList();
    } else {
      cluster = MiniOzoneCluster.newBuilder(conf)
          .build();
      ozoneManagers = Collections.singletonList(cluster.getOzoneManager());
    }

    cluster.waitForClusterToBeReady();

    ObjectStore store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
    store.createVolume(DEFAULT_VOLUME);
    store.getVolume(DEFAULT_VOLUME).createBucket(DEFAULT_BUCKET);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Uses a short service interval and expiration threshold to test
   * the open key cleanup service. This allows all open keys to expire without
   * artificially modifying their creation time, and allows the service to be
   * triggered at its service interval, instead of being manually triggered.
   * <p>
   * This approach does not allow manually creating separate expired and
   * unexpired open keys, and does not provide a way to put an upper bound on
   * the number of service runs. For this reason, all other unit tests use
   * manually modified key creation times to differentiate expired and
   * unexpired open keys, and trigger their service runs manually.
   */
  @Test
  public void testWithServiceInterval() throws Exception {
    TimeDuration serviceInterval = TimeDuration.valueOf(100,
        TimeUnit.MILLISECONDS);
    TimeDuration expireThreshold = TimeDuration.valueOf(50,
        TimeUnit.MILLISECONDS);

    setupCluster(serviceInterval, expireThreshold);

    final int numBlocks = 3;
    final int minRuns = 2;
    final int numKeys = TESTING_TASK_LIMIT * minRuns;

    // Setup test and verify the setup.
    Set<String> originalOpenKeys = createExpiredOpenKeys(numKeys, numBlocks);
    Assert.assertEquals(numKeys, originalOpenKeys.size());
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(originalOpenKeys, getAllOpenKeys(om));
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
    }

    // Wait for all open keys to become expired and be deleted on all OMs when
    // the service interval is triggered.
    for (OzoneManager om: ozoneManagers) {
      LambdaTestUtils.await(FOLLOWER_WAIT_TIMEOUT, FOLLOWER_CHECK_INTERVAL,
          () -> getAllOpenKeys(om).size() == 0);

      Assert.assertEquals(originalOpenKeys, getAllPendingDeleteKeys(om));
    }

    OpenKeyCleanupService service = getService();
    // The service may run more than this number of times, but it should have
    // taken at least this many runs to delete all the open keys.
    Assert.assertTrue(service.getRunCount().get() >= minRuns);
  }

  /**
   * Tests cleanup of expired open keys that do not have block data, meaning
   * they should be removed from the open key table, but not added to the
   * delete table.
   */
  @Test
  public void testOpenKeysWithoutBlockData() throws Exception {
    setupCluster();

    // Setup test and verify the setup.
    Set<String> originalOpenKeys = createOpenKeys(TESTING_TASK_LIMIT);
    Assert.assertEquals(TESTING_TASK_LIMIT, originalOpenKeys.size());
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(originalOpenKeys, getAllOpenKeys(om));
    }

    Set<String> originalExpiredOpenKeys =
        createExpiredOpenKeys(TESTING_TASK_LIMIT);
    Assert.assertEquals(TESTING_TASK_LIMIT, originalExpiredOpenKeys.size());
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(originalExpiredOpenKeys, getAllExpiredOpenKeys(om));
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
    }

    runService();

    // Expired open keys with no block data should be removed from open key
    // table without being put in the delete table.
    for (OzoneManager om: ozoneManagers) {
      LambdaTestUtils.await(FOLLOWER_WAIT_TIMEOUT, FOLLOWER_CHECK_INTERVAL,
          () -> originalOpenKeys.equals(getAllOpenKeys(om)));
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
    }
  }

  /**
   * Tests cleanup of expired open keys that do have block data, meaning
   * they should be removed from the open key table, and added to the delete
   * table.
   */
  @Test
  public void testOpenKeysWithBlockData() throws Exception {
    setupCluster();

    final int numBlocks = 3;

    // Setup test and verify the setup.
    Set<String> originalOpenKeys = createOpenKeys(TESTING_TASK_LIMIT);
    Assert.assertEquals(TESTING_TASK_LIMIT, originalOpenKeys.size());
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(originalOpenKeys, getAllOpenKeys(om));
    }

    Set<String> originalExpiredKeys = createExpiredOpenKeys(TESTING_TASK_LIMIT,
        numBlocks);
    Assert.assertEquals(TESTING_TASK_LIMIT, originalExpiredKeys.size());
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(originalExpiredKeys, getAllExpiredOpenKeys(om));
    }

    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
    }

    runService();

    for (OzoneManager om: ozoneManagers) {
      LambdaTestUtils.await(FOLLOWER_WAIT_TIMEOUT, FOLLOWER_CHECK_INTERVAL,
          () -> originalExpiredKeys.equals(getAllPendingDeleteKeys(om)));
      Assert.assertEquals(originalOpenKeys, getAllOpenKeys(om));
    }
  }

  @Test
  public void testWithNoExpiredOpenKeys() throws Exception {
    setupCluster();

    Set<String> originalOpenKeys = createOpenKeys(TESTING_TASK_LIMIT);

    // Verify test setup.
    Assert.assertEquals(TESTING_TASK_LIMIT, originalOpenKeys.size());

    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(getAllOpenKeys(om), originalOpenKeys);
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
      Assert.assertEquals(0, getAllExpiredOpenKeys(om).size());
    }

    runService();

    // Tables should be unchanged since no keys are expired.
    for (OzoneManager om: ozoneManagers) {
      LambdaTestUtils.await(FOLLOWER_WAIT_TIMEOUT, FOLLOWER_CHECK_INTERVAL,
          () -> originalOpenKeys.equals(getAllOpenKeys(om)));
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
    }
  }

  @Test
  public void testWithNoOpenKeys() throws Exception {
    setupCluster();

    // Verify test setup.
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(0, getAllOpenKeys(om).size());
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
    }

    // Make sure service runs without errors.
    runService();

    // Tables should be unchanged since no keys are expired.
    for (OzoneManager om: ozoneManagers) {
      LambdaTestUtils.await(FOLLOWER_WAIT_TIMEOUT, FOLLOWER_CHECK_INTERVAL,
          () -> getAllOpenKeys(om).size() == 0);
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
    }
  }

  /**
   * Creates more expired open keys than can be deleted by the service after
   * a fixed number of runs, and checks that the service does not exceed its
   * task limit by deleting the extra keys.
   */
  @Test
  public void testTaskLimitWithMultipleRuns() throws Exception {
    setupCluster();

    final int numServiceRuns = 2;
    final int numBlocks = 3;
    final int numKeysToDelete = TESTING_TASK_LIMIT * numServiceRuns;
    // Create more keys than the service will clean up in the allowed number
    // of runs.
    final int numKeys = numKeysToDelete + TESTING_TASK_LIMIT;

    Set<String> originalExpiredKeys = createExpiredOpenKeys(numKeys, numBlocks);

    // Verify test setup.
    Assert.assertEquals(numKeys, originalExpiredKeys.size());
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(getAllExpiredOpenKeys(om), originalExpiredKeys);
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
    }

    // After each service run, wait for the service to finish so runs do not
    // pick up the same keys to delete.
    for (int run = 1; run <= numServiceRuns; run++) {
      runService();
      int runNum = run;
      for (OzoneManager om: ozoneManagers) {
        LambdaTestUtils.await(FOLLOWER_WAIT_TIMEOUT, FOLLOWER_CHECK_INTERVAL,
            () -> getAllPendingDeleteKeys(om).size() ==
                TESTING_TASK_LIMIT * runNum);
      }
    }

    // Order that the service deletes keys is not defined.
    // So for multiple runs that will not delete all keys, we can only
    // check that the correct key counts were deleted, and that the deleted
    // keys are a subset of the originally created keys.
    for (OzoneManager om: ozoneManagers) {
      Set<String> pendingDeleteKeys = getAllPendingDeleteKeys(om);
      Set<String> expiredKeys = getAllExpiredOpenKeys(om);

      Assert.assertTrue(originalExpiredKeys.containsAll(pendingDeleteKeys));
      Assert.assertTrue(originalExpiredKeys.containsAll(expiredKeys));

      // Service runs should have reached but not exceeded their task limit.
      Assert.assertEquals(numKeysToDelete, pendingDeleteKeys.size());
      // All remaining keys should still be present in the open key table.
      Assert.assertEquals(numKeys - numKeysToDelete,
          expiredKeys.size());
    }
  }

  /**
   * Tests cleanup of open keys whose volume and bucket does not exist in the
   * DB. This simulates the condition where open keys are deleted after the
   * volume or bucket they were supposed to belong to if committed.
   */
  @Test
  public void testWithMissingVolumeAndBucket() throws Exception  {
    setupCluster();

    int numBlocks = 3;

    // Open keys created from a non-existent volume and bucket.
    Set<String> originalExpiredKeys = createExpiredOpenKeys(DEFAULT_VOLUME +
            "2", DEFAULT_BUCKET + "2", TESTING_TASK_LIMIT, numBlocks);

    // Verify test setup.
    Assert.assertEquals(TESTING_TASK_LIMIT, originalExpiredKeys.size());
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(getAllExpiredOpenKeys(om), originalExpiredKeys);
      Assert.assertEquals(0, getAllPendingDeleteKeys(om).size());
    }

    runService();

    // All keys should have been cleaned up.
    for (OzoneManager om: ozoneManagers) {
      LambdaTestUtils.await(FOLLOWER_WAIT_TIMEOUT, FOLLOWER_CHECK_INTERVAL,
          () -> getAllExpiredOpenKeys(om).size() == 0);

      Assert.assertEquals(originalExpiredKeys, getAllPendingDeleteKeys(om));
    }
  }

  /**
   * Tests cleanup of expired open keys across multiple volumes and buckets,
   * some of which exist and some of which do not.
   */
  @Test
  public void testWithMultipleVolumesAndBuckets() throws Exception {
    setupCluster();

    int numKeysPerBucket = TESTING_TASK_LIMIT;
    int numBlocks = 3;
    int numServiceRuns = 3;

    Set<String> allCreatedKeys = new HashSet<>();

    // Open keys created from the default volume and bucket.
    allCreatedKeys.addAll(
        createExpiredOpenKeys(numKeysPerBucket, numBlocks));

    // Open keys created from the default volume and a non-existent bucket.
    allCreatedKeys.addAll(
        createExpiredOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET + "2",
        numKeysPerBucket, numBlocks));

    // Open keys created from a non-existent volume and bucket.
    allCreatedKeys.addAll(
        createExpiredOpenKeys(DEFAULT_VOLUME + "2", DEFAULT_BUCKET + "2",
        numKeysPerBucket, numBlocks));

    // Verify test setup.
    Assert.assertEquals(numKeysPerBucket * 3, allCreatedKeys.size());
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(getAllOpenKeys(om), allCreatedKeys);
    }

    // After each service run, wait for the service to finish so runs do not
    // pick up the same keys to delete.
    for (int run = 1; run <= numServiceRuns; run++) {
      runService();
      int runNum = run;
      for (OzoneManager om: ozoneManagers) {
        LambdaTestUtils.await(FOLLOWER_WAIT_TIMEOUT, FOLLOWER_CHECK_INTERVAL,
            () -> getAllPendingDeleteKeys(om).size() ==
                TESTING_TASK_LIMIT * runNum);
      }
    }

    // All keys should have been cleaned up.
    for (OzoneManager om: ozoneManagers) {
      Assert.assertEquals(allCreatedKeys, getAllPendingDeleteKeys(om));
      Assert.assertEquals(0, getAllExpiredOpenKeys(om).size());
    }
  }

  private Set<String> getAllExpiredOpenKeys(OzoneManager om) throws Exception {
    return new HashSet<>(om.getKeyManager()
        .getExpiredOpenKeys(TESTING_EXPIRE_THRESHOLD, Integer.MAX_VALUE));
  }

  private Set<String> getAllOpenKeys(OzoneManager om) throws Exception {
    Set<String> keys = new HashSet<>();
    List<? extends Table.KeyValue<String, OmKeyInfo>> keyPairs =
        om.getMetadataManager().getOpenKeyTable()
            .getRangeKVs(null, Integer.MAX_VALUE);

    for (Table.KeyValue<String, OmKeyInfo> keyPair: keyPairs) {
      keys.add(keyPair.getKey());
    }

    return keys;
  }

  private Set<String> getAllPendingDeleteKeys(OzoneManager om)
      throws Exception {
    List<BlockGroup> blocks =
        om.getKeyManager().getPendingDeletionKeys(Integer.MAX_VALUE);

    Set<String> keyNames = new HashSet<>();
    for (BlockGroup block: blocks) {
      keyNames.add(block.getGroupID());
    }

    return keyNames;
  }

  /**
   * Runs the key deleting service on the OM leader,
   * but does not wait for OMs to apply results of the
   * run to their DBs before returning.
   *
   * Callers should wait on their desired state for each OM after the service
   * runs before running assertions about OM state.
   */
  private void runService() throws Exception {
    getService().getTasks().poll().call();
  }

  private OpenKeyCleanupService getService() {
    return (OpenKeyCleanupService) getLeader().getKeyManager()
        .getOpenKeyCleanupService();
  }

  private OzoneManager getLeader() {
    if (cluster instanceof MiniOzoneHAClusterImpl) {
      return ((MiniOzoneHAClusterImpl) cluster).getOMLeader();
    } else {
      return cluster.getOzoneManager();
    }
  }

  private Set<String> createExpiredOpenKeys(int numKeys) throws Exception {
    return createOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET, numKeys, 0, true);
  }

  private Set<String> createExpiredOpenKeys(int numKeys, int numBlocks)
      throws Exception {
    return createOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET, numKeys, numBlocks,
        true);
  }

  private Set<String> createExpiredOpenKeys(String volume, String bucket,
      int numKeys, int numBlocks) throws Exception {
    return createOpenKeys(volume, bucket, numKeys, numBlocks, true);
  }

  private Set<String> createOpenKeys(int numKeys) throws Exception {
    return createOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET, numKeys, 0, false);
  }

  /**
   * Adds open keys to the open key table of every OM in the cluster.
   * Keys are manually inserted into each OM's DB so that creation time can
   * be artificially set to simulate expiration.
   */
  private Set<String> createOpenKeys(String volume, String bucket, int numKeys,
      int numBlocks, boolean expired) throws Exception {
    Set<String> openKeys = new HashSet<>();
    long creationTime = Instant.now().toEpochMilli();

    // Simulate expired keys by creating them with age twice that of the
    // expiration age.
    if (expired) {
      long ageMillis = TESTING_EXPIRE_THRESHOLD
          .add(TESTING_EXPIRE_THRESHOLD)
          .toLong(TimeUnit.MILLISECONDS);
      creationTime -= ageMillis;
    }

    for (int i = 0; i < numKeys; i++) {
      String key = null;
      if (i == 0) {
        // Add one messy key with lots of separators for testing.
        key = OM_KEY_PREFIX +
            UUID.randomUUID().toString() +
            OM_KEY_PREFIX +
            OM_KEY_PREFIX +
            UUID.randomUUID().toString() +
            OM_KEY_PREFIX +
            OM_KEY_PREFIX;
      } else {
        key = UUID.randomUUID().toString();
      }

      long clientID = new Random().nextLong();

      OmKeyInfo keyInfo = TestOMRequestUtils.createOmKeyInfo(volume,
          bucket, key, HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE, 0L, creationTime);

      if (numBlocks > 0) {
        TestOMRequestUtils.addKeyLocationInfo(keyInfo, 0, numBlocks);
      }

      // Insert keys into every ozone manager's DB.
      for (OzoneManager om: ozoneManagers) {
        TestOMRequestUtils.addKeyToTable(true, false,
            keyInfo, clientID, 0L, om.getMetadataManager());

        String fullKeyName = om.getMetadataManager().getOpenKey(volume, bucket,
            keyInfo.getKeyName(), clientID);
        openKeys.add(fullKeyName);
      }
    }

    return openKeys;
  }
}
