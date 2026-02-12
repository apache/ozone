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

package org.apache.hadoop.ozone.om.snapshot;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.apache.ozone.test.LambdaTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests snapshot in OM HA setup.
 */
public class TestOzoneManagerHASnapshot {
  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient client;
  private static String volumeName;
  private static String bucketName;
  private static ObjectStore store;
  private static OzoneBucket ozoneBucket;

  @BeforeAll
  public static void staticInit() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.setTimeDuration(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service-test")
        .setNumOfOzoneManagers(3)
        .build();

    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();
    ozoneBucket = TestDataUtil.createVolumeAndBucket(client);
    volumeName = ozoneBucket.getVolumeName();
    bucketName = ozoneBucket.getName();
  }

  @AfterAll
  public static void cleanUp() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  // Test snapshot diff when OM restarts in HA OM env.
  @Test
  public void testSnapshotDiffWhenOmLeaderRestart()
      throws Exception {
    String snapshot1 = "snap-" + RandomStringUtils.secure().nextNumeric(10);
    String snapshot2 = "snap-" + RandomStringUtils.secure().nextNumeric(10);

    createFileKey(ozoneBucket, "key-" + RandomStringUtils.secure().nextNumeric(10));
    store.createSnapshot(volumeName, bucketName, snapshot1);

    for (int i = 0; i < 100; i++) {
      createFileKey(ozoneBucket, "key-" + RandomStringUtils.secure().nextNumeric(10));
    }

    store.createSnapshot(volumeName, bucketName, snapshot2);

    SnapshotDiffResponse response =
        store.snapshotDiff(volumeName, bucketName,
            snapshot1, snapshot2, null, 0, false, false);

    assertEquals(IN_PROGRESS, response.getJobStatus());

    String oldLeader = cluster.getOMLeader().getOMNodeId();

    OzoneManager omLeader = cluster.getOMLeader();
    cluster.shutdownOzoneManager(omLeader);
    cluster.restartOzoneManager(omLeader, true);

    cluster.waitForLeaderOM();

    String newLeader = cluster.getOMLeader().getOMNodeId();

    if (Objects.equals(oldLeader, newLeader)) {
      // If old leader becomes leader again. Job should be done by this time.
      response = store.snapshotDiff(volumeName, bucketName,
          snapshot1, snapshot2, null, 0, false, false);
      assertEquals(DONE, response.getJobStatus());
      assertEquals(100, response.getSnapshotDiffReport().getDiffList().size());
    } else {
      // If new leader is different from old leader. SnapDiff request will be
      // new to OM, and job status should be IN_PROGRESS.
      response = store.snapshotDiff(volumeName, bucketName, snapshot1,
          snapshot2, null, 0, false, false);
      assertEquals(IN_PROGRESS, response.getJobStatus());
      while (true) {
        response = store.snapshotDiff(volumeName, bucketName, snapshot1,
                snapshot2, null, 0, false, false);
        if (DONE == response.getJobStatus()) {
          assertEquals(100,
              response.getSnapshotDiffReport().getDiffList().size());
          break;
        }
        Thread.sleep(response.getWaitTimeInMs());
      }
    }
  }

  @Test
  public void testSnapshotIdConsistency() throws Exception {
    createFileKey(ozoneBucket, "key-" + RandomStringUtils.secure().nextNumeric(10));

    String snapshotName = "snap-" + RandomStringUtils.secure().nextNumeric(10);

    store.createSnapshot(volumeName, bucketName, snapshotName);
    List<OzoneManager> ozoneManagers = cluster.getOzoneManagersList();
    List<UUID> snapshotIds = new ArrayList<>();

    for (OzoneManager ozoneManager : ozoneManagers) {
      await(120_000, 100, () -> {
        SnapshotInfo snapshotInfo;
        try {
          snapshotInfo = ozoneManager.getMetadataManager()
              .getSnapshotInfoTable()
              .get(SnapshotInfo.getTableKey(volumeName,
                  bucketName,
                  snapshotName));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        if (snapshotInfo != null) {
          snapshotIds.add(snapshotInfo.getSnapshotId());
        }
        return snapshotInfo != null;
      });
    }

    assertEquals(1, snapshotIds.stream().distinct().count());
  }

  /**
   * Test snapshotNames are unique among OM nodes when snapshotName is not
   * passed or empty.
   */
  @Test
  public void testSnapshotNameConsistency() throws Exception {
    store.createSnapshot(volumeName, bucketName, "");
    List<OzoneManager> ozoneManagers = cluster.getOzoneManagersList();
    List<String> snapshotNames = new ArrayList<>();

    for (OzoneManager ozoneManager : ozoneManagers) {
      await(120_000, 100, () -> {
        String snapshotPrefix = OM_KEY_PREFIX + volumeName +
            OM_KEY_PREFIX + bucketName;
        SnapshotInfo snapshotInfo = null;
        try (Table.KeyValueIterator<String, SnapshotInfo>
                 iterator = ozoneManager.getMetadataManager()
            .getSnapshotInfoTable().iterator(snapshotPrefix)) {
          while (iterator.hasNext()) {
            snapshotInfo = iterator.next().getValue();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        if (snapshotInfo != null) {
          snapshotNames.add(snapshotInfo.getName());
        }
        return snapshotInfo != null;
      });
    }

    assertEquals(1, snapshotNames.stream().distinct().count());
    assertNotNull(snapshotNames.get(0));
    assertTrue(snapshotNames.get(0).startsWith("s"));
  }

  @Test
  public void testSnapshotChainManagerRestore() throws Exception {
    List<OzoneBucket> ozoneBuckets = new ArrayList<>();
    List<String> volumeNames = new ArrayList<>();
    List<String> bucketNames = new ArrayList<>();
    List<List<String>> snapshotNamesList = new ArrayList<>();

    // Create 10 buckets and initialize snapshot name lists.
    for (int i = 0; i < 10; i++) {
      OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);
      ozoneBuckets.add(bucket);
      volumeNames.add(bucket.getVolumeName());
      bucketNames.add(bucket.getName());
      snapshotNamesList.add(new ArrayList<>());
    }

    // Create multiple snapshots for each bucket.
    // Here we create 5 snapshots per bucket.
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 10; j++) {
        OzoneBucket bucket = ozoneBuckets.get(j);
        // Create a new key to generate state change.
        createFileKey(bucket, "key-" + RandomStringUtils.secure().nextNumeric(10));
        String snapshotName = "snapshot-" + RandomStringUtils.secure().nextNumeric(10);
        store.createSnapshot(volumeNames.get(j), bucketNames.get(j), snapshotName);
        snapshotNamesList.get(j).add(snapshotName);
      }
    }

    // Restart leader OM.
    OzoneManager omLeader = cluster.getOMLeader();
    cluster.shutdownOzoneManager(omLeader);
    cluster.restartOzoneManager(omLeader, true);

    cluster.waitForLeaderOM();
    assertNotNull(cluster.getOMLeader());

    // Now delete one snapshot from each bucket to simulate snapshot deletion.
    for (int j = 0; j < 10; j++) {
      // Choose the third snapshot (index 2) if it exists.
      if (snapshotNamesList.get(j).size() > 2) {
        String snapshotToDelete = snapshotNamesList.get(j).get(2);
        store.deleteSnapshot(volumeNames.get(j), bucketNames.get(j), snapshotToDelete);
      }
    }

    // Restart leader OM.
    omLeader = cluster.getOMLeader();
    cluster.shutdownOzoneManager(omLeader);
    cluster.restartOzoneManager(omLeader, true);

    cluster.waitForLeaderOM();
    assertNotNull(cluster.getOMLeader());

    // wait until the snapshots complete deletion
    for (int j = 0; j < 10; j++) {
      String snapshotToDelete = snapshotNamesList.get(j).get(2);
      String tableKey = SnapshotInfo.getTableKey(volumeNames.get(j), bucketNames.get(j), snapshotToDelete);
      GenericTestUtils.waitFor(() -> {
        try {
          return cluster.getOMLeader().getMetadataManager().getSnapshotInfoTable().get(tableKey) == null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, 1000, 60000);
    }

    OmMetadataManagerImpl metadataManager = (OmMetadataManagerImpl) cluster.getOMLeader().getMetadataManager();
    // Verify that the snapshot chain is not corrupted even after deletions.
    assertFalse(metadataManager.getSnapshotChainManager().isSnapshotChainCorrupted());
  }

  private void createFileKey(OzoneBucket bucket, String keyName)
      throws IOException {
    byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
    try (OzoneOutputStream fileKey = bucket.createKey(keyName, value.length)) {
      fileKey.write(value);
    }
  }

  /**
   * This is to simulate HDDS-11152 scenario. In which a follower's doubleBuffer is lagging and accumulates purgeKey
   * and purgeSnapshot in same batch.
   */
  @Test
  public void testKeyAndSnapshotDeletionService()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    OzoneManager omLeader = cluster.getOMLeader();
    OzoneManager omFollower;

    if (omLeader != cluster.getOzoneManager(0)) {
      omFollower = cluster.getOzoneManager(0);
    } else {
      omFollower = cluster.getOzoneManager(1);
    }

    int numKeys = 5;
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      String keyName = "key-" + RandomStringUtils.secure().nextNumeric(10);
      createFileKey(ozoneBucket, keyName);
      keys.add(keyName);
    }

    // Stop the key deletion service so that deleted keys get trapped in the snapshots.
    omLeader.getKeyManager().getDeletingService().suspend();
    // Stop the snapshot deletion service so that deleted keys get trapped in the snapshots.
    omLeader.getKeyManager().getSnapshotDeletingService().suspend();

    // Delete half of the keys
    for (int i = 0; i < numKeys / 2; i++) {
      ozoneBucket.deleteKey(keys.get(i));
    }

    String snapshotName = "snap-" + RandomStringUtils.secure().nextNumeric(10);
    createSnapshot(volumeName, bucketName, snapshotName);

    // Wait for double buffer flush on follower to ensure that
    // the key deletion and snapshot creation are flushed to the DB.
    OzoneManagerDoubleBuffer omFollowerDoubleBuffer =
        omFollower.getOmRatisServer().getOmStateMachine().getOzoneManagerDoubleBuffer();
    omFollowerDoubleBuffer.awaitFlush();
    // Pause double buffer on follower node to accumulate all the key purge, snapshot delete and purge transactions.
    omFollowerDoubleBuffer.stopDaemon();

    store.deleteSnapshot(volumeName, bucketName, snapshotName);

    long keyDeleteServiceCount = omLeader.getKeyManager().getDeletingService().getRunCount().get();
    omLeader.getKeyManager().getDeletingService().resume();

    GenericTestUtils.waitFor(
        () -> omLeader.getKeyManager().getDeletingService().getRunCount().get() > keyDeleteServiceCount,
        1000, 60000);

    long snapshotDeleteServiceCount = omLeader.getKeyManager().getSnapshotDeletingService().getRunCount().get();
    omLeader.getKeyManager().getSnapshotDeletingService().resume();

    GenericTestUtils.waitFor(
        () -> omLeader.getKeyManager().getSnapshotDeletingService().getRunCount().get() > snapshotDeleteServiceCount,
        1000, 60000);

    String tableKey = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName);
    checkSnapshotIsPurgedFromDB(omLeader, tableKey);

    // Resume the DoubleBuffer on the follower and flush the pending transactions.
    omFollowerDoubleBuffer.resume();
    CompletableFuture.supplyAsync(() -> {
      omFollowerDoubleBuffer.flushTransactions();
      return null;
    });
    omFollowerDoubleBuffer.awaitFlush();
    checkSnapshotIsPurgedFromDB(omFollower, tableKey);
  }

  @Test
  public void testSnapshotInFlightCount() throws Exception {
    // snapshot inflight count should be reset to 0 when leader changes

    // first do some snapshot creations
    String snapshotName1 = UUID.randomUUID().toString();
    store.createSnapshot(volumeName, bucketName, snapshotName1);

    // then shutdown the leader
    OzoneManager omLeader = cluster.getOMLeader();
    cluster.shutdownOzoneManager(omLeader);

    // wait for the new leader to be elected
    cluster.waitForLeaderOM();

    // check the inflight count on the new leader is 0
    OzoneManager newLeader = cluster.getOMLeader();
    assertEquals(0, newLeader.getOmSnapshotManager().getInFlightSnapshotCount());

    // restart the previous shutdowned node
    cluster.restartOzoneManager(omLeader, true);
  }

  private void createSnapshot(String volName, String buckName, String snapName) throws IOException {
    store.createSnapshot(volName, buckName, snapName);

    String tableKey = SnapshotInfo.getTableKey(volName, buckName, snapName);
    SnapshotInfo snapshotInfo = SnapshotUtils.getSnapshotInfo(cluster.getOMLeader(), tableKey);
    String fileName = getSnapshotPath(cluster.getOMLeader().getConfiguration(), snapshotInfo, 0);
    File snapshotDir = new File(fileName);
    if (!RDBCheckpointUtils.waitForCheckpointDirectoryExist(snapshotDir)) {
      throw new IOException("Snapshot directory doesn't exist");
    }
  }

  private void checkSnapshotIsPurgedFromDB(OzoneManager ozoneManager, String snapshotTableKey)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      try {
        return ozoneManager.getMetadataManager().getSnapshotInfoTable().get(snapshotTableKey) == null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 60000);
  }
}
