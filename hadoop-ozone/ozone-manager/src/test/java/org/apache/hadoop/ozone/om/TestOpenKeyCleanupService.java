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
import java.util.Random;
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
    createOpenKeys(taskLimit);
    createExpiredOpenKeys(taskLimit);

    Assert.assertEquals(taskLimit * 2, getAllOpenKeys().size());
    Assert.assertEquals(taskLimit, getAllExpiredOpenKeys().size());

    runService();

    // Keys with no block data should be removed from open key table without
    // being put in the delete table.
    Assert.assertEquals(taskLimit, getAllOpenKeys().size());
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testOpenKeysWithBlockData() throws Exception {
    final int numBlocks = 3;

    createOpenKeys(taskLimit);
    createExpiredOpenKeys(taskLimit, numBlocks);
    Assert.assertEquals(taskLimit * 2, getAllOpenKeys().size());

    runService();

    Assert.assertEquals(taskLimit, getAllOpenKeys().size());
    Assert.assertEquals(taskLimit, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testWithNoExpiredOpenKeys() throws Exception {
    createOpenKeys(taskLimit);
    Assert.assertEquals(taskLimit, getAllOpenKeys().size());
    runService();

    // Tables should be unchanged since no keys are expired.
    Assert.assertEquals(taskLimit, getAllOpenKeys().size());
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testWithNoOpenKeys() throws Exception {
    Assert.assertEquals(0, getAllOpenKeys().size());
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

    createExpiredOpenKeys(numKeys, numBlocks);
    Assert.assertEquals(numKeys, getAllOpenKeys().size());

    runService(numServiceRuns);

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
    createExpiredOpenKeys(DEFAULT_VOLUME + "2", DEFAULT_BUCKET + "2",
        taskLimit, numBlocks);
    Assert.assertEquals(taskLimit, getAllOpenKeys().size());

    runService();

    // All keys should have been cleaned up.
    Assert.assertEquals(taskLimit, getAllPendingDeleteKeys().size());
    Assert.assertEquals(0, getAllExpiredOpenKeys().size());
  }

  @Test
  public void testWithMultipleVolumesAndBuckets() throws Exception {
    int numKeysPerBucket = taskLimit;
    int numBlocks = 3;
    int numServiceRuns = 3;

    // Open keys created from the default volume and bucket.
    createExpiredOpenKeys(numKeysPerBucket, numBlocks);
    // Open keys created from the default volume and a non-existent bucket.
    createExpiredOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET + "2",
        numKeysPerBucket, numBlocks);
    // Open keys created from a non-existent volume and bucket.
    createExpiredOpenKeys(DEFAULT_VOLUME + "2", DEFAULT_BUCKET + "2",
        numKeysPerBucket, numBlocks);
    Assert.assertEquals(numKeysPerBucket * 3, getAllOpenKeys().size());

    runService(numServiceRuns);

    // All keys should have been cleaned up.
    Assert.assertEquals(taskLimit * numServiceRuns,
        getAllPendingDeleteKeys().size());
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

    GenericTestUtils.waitFor(
        () -> service.getRunCount().get() >= numRuns,
        serviceIntervalMillis, serviceIntervalMillis * 10);

    cluster.stop();
  }

  private void createExpiredOpenKeys(int numKeys) throws Exception {
    createOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET, numKeys, 0, true);
  }

  private void createExpiredOpenKeys(int numKeys, int numBlocks)
      throws Exception {
    createOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET, numKeys, numBlocks, true);
  }

  private void createExpiredOpenKeys(String volume, String bucket, int numKeys,
      int numBlocks) throws Exception {
    createOpenKeys(volume, bucket, numKeys, numBlocks, true);
  }

  private void createOpenKeys(int numKeys) throws Exception {
    createOpenKeys(DEFAULT_VOLUME, DEFAULT_BUCKET, numKeys, 0, false);
  }

  private void createOpenKeys(String volume, String bucket, int numKeys,
      int numBlocks, boolean expired) throws Exception {

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
//      String groupID = metadataManager.getOpenKey(volumeName, bucketName,
//          keyInfo.getKeyName(), clientID);
//      expiredKeys.add(groupID);
    }
  }
}
