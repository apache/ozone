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

import java.security.cert.CollectionCertStoreParameters;
import java.time.Instant;
import java.util.Collection;
import java.util.Random;

import com.google.common.collect.Lists;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestOpenKeyCleanupService {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  // Lower service interval to speed up testing.
  private static final TimeDuration serviceInterval = TimeDuration.valueOf(100,
      TimeUnit.MILLISECONDS);
  // High expiration time used so keys without modified creation time will not
  // expire during the test.
  private static final TimeDuration expireThreshold = TimeDuration.valueOf(24,
      TimeUnit.HOURS);
  // Maximum number of keys to be cleaned up per run of the service.
  private static final int taskLimit = 10;

  // Volume and bucket created and added to the DB that will hold open keys
  // created by this test unless tests specify otherwise.
  private static final String DEFAULT_VOLUME = "volume";
  private static final String DEFAULT_BUCKET = "bucket";

  private OzoneConfiguration conf;
  private KeyManager keyManager;
  private OpenKeyCleanupService service;
  private  OMMetadataManager metadataManager;
  private MiniOzoneCluster cluster;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, folder.getRoot().getAbsolutePath());

    // Set custom configurations for testing.
    conf.setTimeDuration(OMConfigKeys.OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
        serviceInterval.getDuration(), serviceInterval.getUnit());
    conf.setTimeDuration(OMConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD,
        expireThreshold.getDuration(), expireThreshold.getUnit());

    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();

    OzoneManager omLeader = cluster.getOMLeader();
    keyManager = omLeader.getKeyManager();
    service = (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();
    metadataManager = omLeader.getMetadataManager();

    // Stop all OMs so no services are run until we trigger them.
    cluster.stop();

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
  public void testOpenKeysWithoutBlockData() throws Exception {
    // Setup test and verify the setup.
    List<String> originalOpenKeys = createOpenKeys(taskLimit);
    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
    Assert.assertEquals(taskLimit, originalOpenKeys.size());

    List<String> originalExpiredKeys = createExpiredOpenKeys(taskLimit);
    Assert.assertEquals(originalExpiredKeys, getAllExpiredOpenKeys());
    Assert.assertEquals(taskLimit, originalExpiredKeys.size());

    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService();

    // Keys with no block data should be removed from open key table without
    // being put in the delete table.
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    Assert.assertEquals(taskLimit, getAllOpenKeys().size());
    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
  }

  @Test
  public void testOpenKeysWithBlockData() throws Exception {
    final int numBlocks = 3;

    // Setup test and verify the setup.
    List<String> originalOpenKeys = createOpenKeys(taskLimit);
    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
    Assert.assertEquals(taskLimit, originalOpenKeys.size());

    List<String> originalExpiredKeys = createExpiredOpenKeys(taskLimit,
        numBlocks);
    Assert.assertEquals(originalExpiredKeys, getAllExpiredOpenKeys());
    Assert.assertEquals(taskLimit, originalExpiredKeys.size());

    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService();

    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
    Assert.assertEquals(originalExpiredKeys, getAllPendingDeleteKeys());
  }

  @Test
  public void testWithNoExpiredOpenKeys() throws Exception {
    List<String> originalOpenKeys = createOpenKeys(taskLimit);

    // Verify test setup.
    Assert.assertEquals(taskLimit, originalOpenKeys.size());
    Assert.assertEquals(getAllOpenKeys(), originalOpenKeys);
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService();

    // Tables should be unchanged since no keys are expired.
    Assert.assertEquals(originalOpenKeys, getAllOpenKeys());
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testWithNoOpenKeys() throws Exception {
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
    final int numServiceRuns = 2;
    final int numBlocks = 3;
    // Create more keys than the service will clean up in the allowed number
    // of runs.
    final int numKeys = taskLimit * (numServiceRuns + 1);

    List<String> originalExpiredKeys = createExpiredOpenKeys(numKeys, numBlocks);

    // Verify test setup.
    Assert.assertEquals(numKeys, originalExpiredKeys.size());
    Assert.assertEquals(getAllExpiredOpenKeys(), originalExpiredKeys);
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService(numServiceRuns);

    // Order that the service deletes keys is not defined.
    // So for multiple runs that will not delete all keys, we can only
    // check that the correct key counts were deleted, and that the deleted
    // keys are a subset of the originally created keys.
    List<String> pendingDeleteKeys = getAllPendingDeleteKeys();
    List<String> expiredKeys = getAllExpiredOpenKeys();

    Assert.assertTrue(originalExpiredKeys.containsAll(pendingDeleteKeys));
    Assert.assertTrue(originalExpiredKeys.containsAll(expiredKeys));

    // Two service runs should have reached their task limit.
    Assert.assertEquals(taskLimit * numServiceRuns,
        getAllPendingDeleteKeys().size());
    // All remaining keys should still be present in the open key table.
    Assert.assertEquals(taskLimit, getAllExpiredOpenKeys().size());
  }

  @Test
  public void testWithMissingVolumeAndBucket() throws Exception  {
    int numBlocks = 3;

    // Open keys created from a non-existent volume and bucket.
    List<String> originalExpiredKeys = createExpiredOpenKeys(DEFAULT_VOLUME + "2",
        DEFAULT_BUCKET + "2", taskLimit, numBlocks);

    // Verify test setup.
    Assert.assertEquals(taskLimit, originalExpiredKeys.size());
    Assert.assertEquals(getAllExpiredOpenKeys(), originalExpiredKeys);
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    runService();

    // All keys should have been cleaned up.
    Assert.assertEquals(originalExpiredKeys, getAllPendingDeleteKeys());
    Assert.assertEquals(0, getAllExpiredOpenKeys().size());
  }

  @Test
  public void testWithMultipleVolumesAndBuckets() throws Exception {
    int numKeysPerBucket = taskLimit;
    int numBlocks = 3;
    int numServiceRuns = 3;

    List<String> allCreatedKeys = new ArrayList<>();

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

  private List<String> getAllExpiredOpenKeys() throws Exception {
    return keyManager.getExpiredOpenKeys(expireThreshold, Integer.MAX_VALUE);
  }

  private List<String> getAllOpenKeys() throws Exception {
    List<String> keys = new ArrayList<>();
    List<? extends Table.KeyValue<String, OmKeyInfo>> keyPairs =
        metadataManager.getOpenKeyTable().getRangeKVs(null,
        Integer.MAX_VALUE);

    for (Table.KeyValue<String, OmKeyInfo> keyPair : keyPairs) {
      keys.add(keyPair.getKey());
    }

    return keys;
  }

  private List<String> getAllPendingDeleteKeys() throws Exception {
    List<BlockGroup> blocks =
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE);

    List<String> keyNames = new ArrayList<>();
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
        serviceInterval.toIntExact(TimeUnit.MILLISECONDS);

    cluster.restartOzoneManager();

    for (int i = 0; i < numRuns; i++) {
      service.getTasks().poll().call();
    }
//    GenericTestUtils.waitFor(
//        () -> service.getRunCount().get() >= numRuns,
//        serviceIntervalMillis, serviceIntervalMillis * 10);

    cluster.stop();
  }

  private List<String> createExpiredOpenKeys(int numKeys) throws Exception {
    return createOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET, numKeys, 0, true);
  }

  private List<String> createExpiredOpenKeys(int numKeys, int numBlocks)
      throws Exception {
    return createOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET, numKeys, numBlocks,
        true);
  }

  private List<String> createExpiredOpenKeys(String volume, String bucket,
      int numKeys, int numBlocks) throws Exception {
    return createOpenKeys(volume, bucket, numKeys, numBlocks, true);
  }

  private List<String> createOpenKeys(int numKeys) throws Exception {
    return createOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET, numKeys, 0, false);
  }

  private List<String> createOpenKeys(String volume, String bucket, int numKeys,
      int numBlocks, boolean expired) throws Exception {

    List<String> openKeys = new ArrayList<>();
    long creationTime = Instant.now().toEpochMilli();

    // Simulate expired keys by creating them with age twice that of the
    // expiration age.
    if (expired) {
      long ageMillis =
          expireThreshold.add(expireThreshold).toLong(TimeUnit.MILLISECONDS);
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
