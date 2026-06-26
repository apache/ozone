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

package org.apache.hadoop.hdds.scm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that a follower SCM correctly rebuilds container replica locations
 * for containers that were <em>created</em> while it was offline. After the
 * follower restarts, catches up its Ratis log, and is promoted to leader, all
 * such containers must still have the expected replica count and all keys must
 * be readable.
 *
 * <p>This is the create-while-down counterpart of
 * {@code TestSCMFollowerCatchupWithContainerClose} (HDDS-14989). It exercises
 * the deferred datanode-server start: the restarted follower must finish Raft
 * log replay <em>before</em> accepting datanode container reports, otherwise a
 * report for a not-yet-replayed container is dropped with CONTAINER_NOT_FOUND
 * and the replica location is lost until the next full container report.
 *
 * <p>The container report interval is set high so that, without the fix, the
 * dropped replicas are not re-reported within the test window and the
 * assertions fail; with the fix the datanode server is deferred until catch-up,
 * datanodes (re)register against the up-to-date state, and replicas are
 * recorded immediately.
 */
@Timeout(300)
public class TestSCMFollowerCatchupWithContainerReport {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSCMFollowerCatchupWithContainerReport.class);

  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final String SCM_SERVICE_ID = "scm-service-test1";
  private static final int NUM_OF_SCMS = 3;
  private static final int NUM_OF_DNS = 3;
  private static final int NUM_KEYS = 5;
  private static final String VOLUME = "testvol";
  private static final String BUCKET = "testbucket";

  private MiniOzoneHAClusterImpl cluster;

  @BeforeEach
  void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Keep the full container report interval long so a replica dropped during
    // catch-up is not silently re-reported within the test window. This makes
    // the regression deterministic: only the deferred-start path can repopulate
    // replicas in time.
    conf.setTimeDuration("hdds.container.report.interval", 5, TimeUnit.MINUTES);
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setSCMServiceId(SCM_SERVICE_ID)
        .setNumOfOzoneManagers(1)
        .setNumOfStorageContainerManagers(NUM_OF_SCMS)
        .setNumOfActiveSCMs(NUM_OF_SCMS)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterEach
  void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * HDDS-14989 scenario: containers are closed while a follower SCM is offline.
   * After the follower restarts and is promoted to leader, each container must
   * be CLOSED with a full replica set and all keys must remain readable.
   */
  @Test
  void testFollowerCatchupAfterContainerClose() throws Exception {
    byte[] keyData = "value-of-key".getBytes(UTF_8);
    Set<Long> containerIds = createKeys(keyData);
    assertFalse(containerIds.isEmpty(), "Should have created containers");

    StorageContainerManager followerScm = null;
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      if (!scm.checkLeader() && followerScm == null) {
        followerScm = scm;
      }
    }
    assertFalse(followerScm == null, "Expected to find a follower SCM");

    cluster.shutdownStorageContainerManager(followerScm);
    followerScm.join();

    for (long cid : containerIds) {
      cluster.getStorageContainerLocationClient().closeContainer(cid);
    }
    for (long cid : containerIds) {
      waitForContainerState(cluster.getActiveSCM(), ContainerID.valueOf(cid), CLOSED);
    }

    StorageContainerManager newFollower =
        cluster.restartStorageContainerManager(followerScm, false);
    GenericTestUtils.waitFor(() -> !newFollower.isInSafeMode(), 1000, 120_000);

    cluster.getStorageContainerLocationClient()
        .transferLeadership(newFollower.getScmId());
    GenericTestUtils.waitFor(newFollower::checkLeader, 1000, 60_000);

    for (long cid : containerIds) {
      ContainerID id = ContainerID.valueOf(cid);
      assertEquals(CLOSED,
          newFollower.getContainerManager().getContainer(id).getState(),
          "Container " + cid + " should be CLOSED");
      waitForReplicaCount(newFollower, id, NUM_OF_DNS);
      assertEquals(NUM_OF_DNS,
          newFollower.getContainerManager().getContainerReplicas(id).size(),
          "Container " + cid + " should have " + NUM_OF_DNS + " replicas");
    }
    assertKeysReadable(keyData);
  }

  /**
   * Reproduces the production failure: containers are created while a follower
   * SCM is offline. After the follower restarts and is promoted to leader, the
   * containers must have full replica sets (not an empty replica list).
   */
  @Test
  void testFollowerCatchupAfterContainerCreate() throws Exception {
    // ---- Step 1: pick a leader and a follower ----
    StorageContainerManager followerScm = null;
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      if (!scm.checkLeader() && followerScm == null) {
        followerScm = scm;
      }
    }
    assertFalse(followerScm == null, "Expected to find a follower SCM");

    // ---- Step 2: stop the follower BEFORE creating containers, so it misses
    //              the container-create transactions entirely ----
    cluster.shutdownStorageContainerManager(followerScm);
    followerScm.join();

    // ---- Step 3: create keys -> new containers created while follower offline.
    byte[] keyData = "value-of-key".getBytes(UTF_8);
    Set<Long> containerIds = createKeys(keyData);
    assertFalse(containerIds.isEmpty(), "Should have created containers");

    // ---- Step 4: restart the follower and wait for safe-mode exit ----
    StorageContainerManager newFollower =
        cluster.restartStorageContainerManager(followerScm, false);
    BooleanSupplier safeModeExited = () -> !newFollower.isInSafeMode();
    GenericTestUtils.waitFor(safeModeExited, 1000, 120_000);

    // ---- Step 5: transfer leadership to the restarted follower ----
    cluster.getStorageContainerLocationClient()
        .transferLeadership(newFollower.getScmId());
    GenericTestUtils.waitFor(newFollower::checkLeader, 1000, 60_000);
    LOG.info("Leadership transferred to {}", newFollower.getScmId());

    // ---- Step 6: every container must have a full replica set on the new
    //              leader (the bug shows replicas == 0) ----
    for (long cid : containerIds) {
      ContainerID id = ContainerID.valueOf(cid);
      waitForReplicaCount(newFollower, id, NUM_OF_DNS);
      assertEquals(NUM_OF_DNS,
          newFollower.getContainerManager().getContainerReplicas(id).size(),
          "Container " + cid + " should have " + NUM_OF_DNS + " replicas");
    }

    // ---- Step 7: every key must still be readable ----
    assertKeysReadable(keyData);
  }

  /**
   * Edge case for removing the background polling loop: on an otherwise idle
   * cluster a restarted follower must still start its datanode server (exit safe
   * mode) and serve replicas after promotion, driven by Ratis heartbeats /
   * notifyLeaderChanged rather than a steady stream of new transactions.
   */
  @Test
  void testFollowerCatchupOnIdleCluster() throws Exception {
    byte[] keyData = "value-of-key".getBytes(UTF_8);
    Set<Long> containerIds = createKeys(keyData);
    assertFalse(containerIds.isEmpty(), "Should have created containers");

    StorageContainerManager followerScm = null;
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      if (!scm.checkLeader() && followerScm == null) {
        followerScm = scm;
      }
    }
    assertFalse(followerScm == null, "Expected to find a follower SCM");

    // Stop the follower, then do NO further writes (idle cluster).
    cluster.shutdownStorageContainerManager(followerScm);
    followerScm.join();

    StorageContainerManager newFollower =
        cluster.restartStorageContainerManager(followerScm, false);
    // Must still exit safe mode (i.e. the datanode server started) without any
    // new transactions to apply.
    GenericTestUtils.waitFor(() -> !newFollower.isInSafeMode(), 1000, 120_000);

    cluster.getStorageContainerLocationClient()
        .transferLeadership(newFollower.getScmId());
    GenericTestUtils.waitFor(newFollower::checkLeader, 1000, 60_000);

    for (long cid : containerIds) {
      ContainerID id = ContainerID.valueOf(cid);
      waitForReplicaCount(newFollower, id, NUM_OF_DNS);
      assertEquals(NUM_OF_DNS,
          newFollower.getContainerManager().getContainerReplicas(id).size(),
          "Container " + cid + " should have " + NUM_OF_DNS + " replicas");
    }
    assertKeysReadable(keyData);
  }

  private Set<Long> createKeys(byte[] keyData) throws IOException {
    Set<Long> containerIds = new LinkedHashSet<>();
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore store = client.getObjectStore();
      store.createVolume(VOLUME);
      OzoneVolume volume = store.getVolume(VOLUME);
      volume.createBucket(BUCKET);
      OzoneBucket bucket = volume.getBucket(BUCKET);

      for (int i = 0; i < NUM_KEYS; i++) {
        String keyName = "key-" + i;
        TestDataUtil.createKey(bucket, keyName,
            RatisReplicationConfig.getInstance(THREE), keyData);
        OzoneKeyDetails keyDetails = bucket.getKey(keyName);
        keyDetails.getOzoneKeyLocations()
            .forEach(loc -> containerIds.add(loc.getContainerID()));
      }
    }
    return containerIds;
  }

  private void assertKeysReadable(byte[] keyData) throws IOException {
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore store = client.getObjectStore();
      OzoneBucket bucket = store.getVolume(VOLUME).getBucket(BUCKET);
      for (int i = 0; i < NUM_KEYS; i++) {
        String keyName = "key-" + i;
        try (OzoneInputStream is = bucket.readKey(keyName)) {
          byte[] readData = new byte[keyData.length];
          int bytesRead = is.read(readData);
          assertEquals(keyData.length, bytesRead);
          assertArrayEquals(keyData, readData);
        }
      }
    }
  }

  private static void waitForContainerState(
      StorageContainerManager scm, ContainerID id, LifeCycleState expectedState)
      throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        return scm.getContainerManager().getContainer(id).getState()
            == expectedState;
      } catch (Exception e) {
        return false;
      }
    }, 1000, 120_000);
  }

  private static void waitForReplicaCount(
      StorageContainerManager scm, ContainerID id, int expectedCount)
      throws Exception {
    BooleanSupplier check = () -> {
      try {
        return scm.getContainerManager().getContainerReplicas(id).size()
            == expectedCount;
      } catch (Exception e) {
        return false;
      }
    };
    GenericTestUtils.waitFor(check, 1000, 120_000);
  }
}
