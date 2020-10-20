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
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestOpenKeyCleanupService {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  // Increase service interval of open key cleanup so we can trigger the
  // service manually between setting up the DB and checking the results.
  // Increase service interval of key deleting service to ensure it does not
  // run during the tests, interfering with results.
  private static final TimeDuration DEFAULT_SERVICE_INTERVAL = TimeDuration.valueOf(24,
      TimeUnit.HOURS);
  // High expiration time used so keys without modified creation time will not
  // expire during the test.
  private static final TimeDuration DEFAULT_EXPIRE_THRESHOLD = TimeDuration.valueOf(24,
      TimeUnit.HOURS);
  // Maximum number of keys to be cleaned up per run of the service.
  private static final int DEFAULT_TASK_LIMIT = 10;

  // Volume and bucket created and added to the DB that will hold open keys
  // created by this test unless tests specify otherwise.
  private static final String DEFAULT_VOLUME = "volume";
  private static final String DEFAULT_BUCKET = "bucket";

  private OzoneConfiguration conf;
  private KeyManager keyManager;
  private OpenKeyCleanupService service;
  private  OMMetadataManager metadataManager;
  private MiniOzoneCluster cluster;

  private void setupCluster() throws Exception {
    setupCluster(DEFAULT_SERVICE_INTERVAL, DEFAULT_EXPIRE_THRESHOLD);
  }

  private void setupCluster(TimeDuration openKeyCleanupServiceInterval,
      TimeDuration openKeyExpireThreshold) throws Exception {

    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, folder.getRoot().getAbsolutePath());

    // Make sure key deletion does not run during the tests.
    conf.setTimeDuration(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        DEFAULT_SERVICE_INTERVAL.getDuration(), DEFAULT_SERVICE_INTERVAL.getUnit());
    // Set open key cleanup configurations.
    conf.setTimeDuration(OMConfigKeys.OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
        openKeyCleanupServiceInterval.getDuration(), openKeyCleanupServiceInterval.getUnit());
    conf.setTimeDuration(OMConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD,
        openKeyExpireThreshold.getDuration(), openKeyExpireThreshold.getUnit());
    conf.setInt(OMConfigKeys.OZONE_OPEN_KEY_CLEANUP_LIMIT_PER_TASK, DEFAULT_TASK_LIMIT);

    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();

    OzoneManager omLeader = cluster.getOzoneManager();
    keyManager = omLeader.getKeyManager();
    service = (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();
    metadataManager = omLeader.getMetadataManager();

    TestOMRequestUtils.addVolumeToDB(DEFAULT_VOLUME, metadataManager);
    TestOMRequestUtils.addBucketToDB(DEFAULT_VOLUME, DEFAULT_BUCKET, metadataManager);
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testServiceInterval() throws Exception {
    TimeDuration serviceInterval = TimeDuration.valueOf(100,
        TimeUnit.MILLISECONDS);
    TimeDuration expireThreshold = TimeDuration.valueOf(50,
        TimeUnit.MILLISECONDS);

    setupCluster(serviceInterval, expireThreshold);

    final int numBlocks = 3;
    final int numRuns = 2;
    final int numKeys = DEFAULT_TASK_LIMIT * numRuns;

    // Setup test and verify the setup.
    Set<String> originalOpenKeys = createExpiredOpenKeys(numKeys, numBlocks);
    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
    Assert.assertEquals(numKeys, originalOpenKeys.size());

    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    int serviceIntervalMillis =
        serviceInterval.toIntExact(TimeUnit.MILLISECONDS);

    // Wait for all open keys to become expired and be deleted.
    GenericTestUtils.waitFor(
        () -> service.getSubmittedOpenKeyCount().get() >= numKeys,
        serviceIntervalMillis,
        serviceIntervalMillis * 10);

    Assert.assertTrue(service.getRunCount().get() >= numRuns);
    Assert.assertEquals(originalOpenKeys, getAllPendingDeleteKeys());
    Assert.assertEquals(0, getAllOpenKeys().size());
  }

  @Test
  public void testOpenKeysWithoutBlockData() throws Exception {
    setupCluster();

    // Setup test and verify the setup.
    Set<String> originalOpenKeys = createOpenKeys(DEFAULT_TASK_LIMIT);
    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
    Assert.assertEquals(DEFAULT_TASK_LIMIT, originalOpenKeys.size());

    Set<String> originalExpiredKeys = createExpiredOpenKeys(DEFAULT_TASK_LIMIT);
    Assert.assertEquals(originalExpiredKeys, getAllExpiredOpenKeys());
    Assert.assertEquals(DEFAULT_TASK_LIMIT, originalExpiredKeys.size());

    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService();

    // Keys with no block data should be removed from open key table without
    // being put in the delete table.
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    Assert.assertEquals(DEFAULT_TASK_LIMIT, getAllOpenKeys().size());
    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
  }

  @Test
  public void testOpenKeysWithBlockData() throws Exception {
    setupCluster();

    final int numBlocks = 3;

    // Setup test and verify the setup.
    Set<String> originalOpenKeys = createOpenKeys(DEFAULT_TASK_LIMIT);
    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
    Assert.assertEquals(DEFAULT_TASK_LIMIT, originalOpenKeys.size());

    Set<String> originalExpiredKeys = createExpiredOpenKeys(DEFAULT_TASK_LIMIT,
        numBlocks);
    Assert.assertEquals(originalExpiredKeys, getAllExpiredOpenKeys());
    Assert.assertEquals(DEFAULT_TASK_LIMIT, originalExpiredKeys.size());

    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService();

    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
    Assert.assertEquals(originalExpiredKeys, getAllPendingDeleteKeys());
  }

  @Test
  public void testWithNoExpiredOpenKeys() throws Exception {
    setupCluster();

    Set<String> originalOpenKeys = createOpenKeys(DEFAULT_TASK_LIMIT);

    // Verify test setup.
    Assert.assertEquals(DEFAULT_TASK_LIMIT, originalOpenKeys.size());
    Assert.assertEquals(getAllOpenKeys(), originalOpenKeys);
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService();

    // Tables should be unchanged since no keys are expired.
    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testWithNoOpenKeys() throws Exception {
    setupCluster();

    // Verify test setup.
    Assert.assertEquals(0, getAllOpenKeys().size());
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    // Make sure service runs without errors.
    runService();

    // Tables should be unchanged since no keys are expired.
    Assert.assertEquals(0, getAllOpenKeys().size());
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testTaskLimitWithMultipleRuns() throws Exception {
    setupCluster();

    final int numServiceRuns = 2;
    final int numBlocks = 3;
    // Create more keys than the service will clean up in the allowed number
    // of runs.
    final int numKeys = DEFAULT_TASK_LIMIT * (numServiceRuns + 1);

    Set<String> originalExpiredKeys = createExpiredOpenKeys(numKeys, numBlocks);

    // Verify test setup.
    Assert.assertEquals(numKeys, originalExpiredKeys.size());
    Assert.assertEquals(getAllExpiredOpenKeys(), originalExpiredKeys);
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService(numServiceRuns);

    // Order that the service deletes keys is not defined.
    // So for multiple runs that will not delete all keys, we can only
    // check that the correct key counts were deleted, and that the deleted
    // keys are a subset of the originally created keys.
    Set<String> pendingDeleteKeys = getAllPendingDeleteKeys();
    Set<String> expiredKeys = getAllExpiredOpenKeys();

    Assert.assertTrue(originalExpiredKeys.containsAll(pendingDeleteKeys));
    Assert.assertTrue(originalExpiredKeys.containsAll(expiredKeys));

    // Two service runs should have reached their task limit.
    Assert.assertEquals(DEFAULT_TASK_LIMIT * numServiceRuns,
        pendingDeleteKeys.size());
    // All remaining keys should still be present in the open key table.
    Assert.assertEquals(DEFAULT_TASK_LIMIT, expiredKeys.size());
  }

  @Test
  public void testWithMissingVolumeAndBucket() throws Exception  {
    setupCluster();

    int numBlocks = 3;

    // Open keys created from a non-existent volume and bucket.
    Set<String> originalExpiredKeys = createExpiredOpenKeys(DEFAULT_VOLUME +
            "2", DEFAULT_BUCKET + "2", DEFAULT_TASK_LIMIT, numBlocks);

    // Verify test setup.
    Assert.assertEquals(DEFAULT_TASK_LIMIT, originalExpiredKeys.size());
    Assert.assertEquals(getAllExpiredOpenKeys(), originalExpiredKeys);
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService();

    // All keys should have been cleaned up.
    Assert.assertEquals(originalExpiredKeys, getAllPendingDeleteKeys());
    Assert.assertEquals(0, getAllExpiredOpenKeys().size());
  }

  @Test
  public void testWithMultipleVolumesAndBuckets() throws Exception {
    setupCluster();

    int numKeysPerBucket = DEFAULT_TASK_LIMIT;
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
    Assert.assertEquals(getAllOpenKeys(), allCreatedKeys);

    runService(numServiceRuns);

    // All keys should have been cleaned up.
    Assert.assertEquals(allCreatedKeys, getAllPendingDeleteKeys());
    Assert.assertEquals(0, getAllExpiredOpenKeys().size());
  }

  private Set<String> getAllExpiredOpenKeys() throws Exception {
    return new HashSet<>(keyManager.getExpiredOpenKeys(DEFAULT_EXPIRE_THRESHOLD,
        Integer.MAX_VALUE));
  }

  private Set<String> getAllOpenKeys() throws Exception {
    Set<String> keys = new HashSet<>();
    List<? extends Table.KeyValue<String, OmKeyInfo>> keyPairs =
        metadataManager.getOpenKeyTable().getRangeKVs(null,
        Integer.MAX_VALUE);

    for (Table.KeyValue<String, OmKeyInfo> keyPair : keyPairs) {
      keys.add(keyPair.getKey());
    }

    return keys;
  }

  private Set<String> getAllPendingDeleteKeys() throws Exception {
    List<BlockGroup> blocks =
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE);

    Set<String> keyNames = new HashSet<>();
    for (BlockGroup block : blocks) {
      keyNames.add(block.getGroupID());
    }

    return keyNames;
  }

  private void runService() throws Exception {
    runService(1);
  }

  private void runService(int numRuns) throws Exception {
    int serviceIntervalMillis =
        DEFAULT_SERVICE_INTERVAL.toIntExact(TimeUnit.MILLISECONDS);

    for (int i = 0; i < numRuns; i++) {
      service.getTasks().poll().call();
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

  private Set<String> createOpenKeys(String volume, String bucket, int numKeys,
      int numBlocks, boolean expired) throws Exception {

    Set<String> openKeys = new HashSet<>();
    long creationTime = Instant.now().toEpochMilli();

    // Simulate expired keys by creating them with age twice that of the
    // expiration age.
    if (expired) {
      long ageMillis =
          DEFAULT_EXPIRE_THRESHOLD.add(DEFAULT_EXPIRE_THRESHOLD).toLong(TimeUnit.MILLISECONDS);
      creationTime -= ageMillis;
    }

    for (int i = 0; i < numKeys; i++) {
      String key = UUID.randomUUID().toString();
      long clientID = new Random().nextLong();

      OmKeyInfo keyInfo = TestOMRequestUtils.createOmKeyInfo(volume,
          bucket, key, HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE, 0L, creationTime);

      if (numBlocks > 0) {
        TestOMRequestUtils.addKeyLocationInfo(keyInfo, 0, numBlocks);
      }

      TestOMRequestUtils.addKeyToTable(true, false,
          keyInfo, clientID, 0L, metadataManager);

      // For returning created keys.
      String groupID = metadataManager.getOpenKey(volume, bucket,
          keyInfo.getKeyName(), clientID);
      openKeys.add(groupID);
    }

    return openKeys;
  }
}
