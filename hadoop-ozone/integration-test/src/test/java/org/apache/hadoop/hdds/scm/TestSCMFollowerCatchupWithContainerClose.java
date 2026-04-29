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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
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
 * Verifies that a follower SCM correctly replays container state changes
 * (OPEN -> CLOSED) that were committed while it was offline. After the
 * follower restarts, catches up, and is promoted to leader, all containers
 * must still have the expected replica count and all keys must be readable.
 *
 * <p>This exercises the deferred DN-server start introduced by HDDS-14989:
 * the restarted follower must finish Raft log replay <em>before</em>
 * accepting datanode heartbeats, so that container reports are processed
 * against the up-to-date state.
 */
@Timeout(300)
public class TestSCMFollowerCatchupWithContainerClose {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSCMFollowerCatchupWithContainerClose.class);

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

  private Set<Long> createrKeys(byte[] keyData) throws IOException {
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

  @Test
  void testFollowerCatchupAfterContainerClose() throws Exception {
    // ---- Step 1: create volume / bucket / keys with THREE replicas ----
    byte[] keyData = ("value-of-key").getBytes(UTF_8);
    Set<Long> containerIds = createrKeys(keyData);
    assertFalse(containerIds.isEmpty(), "Should have created containers");

    // ---- Step 2: stop the follower so it misses container-close txns ----
    StorageContainerManager followerScm = null;
    StorageContainerManager leaderScm = null;
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      if (scm.checkLeader()) {
        leaderScm = scm;
      } else if (followerScm == null) {
        followerScm = scm;
      }
    }
    cluster.shutdownStorageContainerManager(followerScm);
    followerScm.join();

    // ---- Step 3: close every container while the follower is offline ----
    for (long cid : containerIds) {
      cluster.getStorageContainerLocationClient().closeContainer(cid);
    }
    StorageContainerManager leader = leaderScm;
    for (long cid : containerIds) {
      ContainerID id = ContainerID.valueOf(cid);
      waitForContainerState(leader, id, LifeCycleState.CLOSED);
    }

    // ---- Step 5: restart the follower and wait for safe-mode exit ----
    StorageContainerManager newFollower =
        cluster.restartStorageContainerManager(followerScm, false);
    BooleanSupplier safeModeExited = () -> !newFollower.isInSafeMode();
    GenericTestUtils.waitFor(safeModeExited, 1000, 120_000);

    // ---- Step 6: transfer leadership to the restarted follower ----
    cluster.getStorageContainerLocationClient()
        .transferLeadership(newFollower.getScmId());
    GenericTestUtils.waitFor(newFollower::checkLeader, 1000, 60_000);
    LOG.info("Leadership transferred to {}", newFollower.getScmId());

    // ---- Step 7: verify container state and replica count = 3 ----
    for (long cid : containerIds) {
      ContainerID id = ContainerID.valueOf(cid);

      assertEquals(LifeCycleState.CLOSED,
          newFollower.getContainerManager().getContainer(id).getState(),
          "Container " + cid + " should be CLOSED on new leader");

      waitForReplicaCount(newFollower, id, NUM_OF_DNS);

      assertEquals(NUM_OF_DNS,
          newFollower.getContainerManager()
              .getContainerReplicas(id).size(),
          "Container " + cid + " should have " + NUM_OF_DNS + " replicas");
    }
    LOG.info("All containers verified CLOSED with {} replicas", NUM_OF_DNS);

    // ---- Step 8: verify every key is still readable ----
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
      StorageContainerManager scm, ContainerID id,
      LifeCycleState expected) throws Exception {
    ContainerManager cm = scm.getContainerManager();
    BooleanSupplier check = () -> {
      try {
        return cm.getContainer(id).getState() == expected;
      } catch (Exception e) {
        return false;
      }
    };
    GenericTestUtils.waitFor(check, 1000, 120_000);
  }

  private static void waitForReplicaCount(
      StorageContainerManager scm, ContainerID id,
      int expectedCount) throws Exception {
    ContainerManager cm = scm.getContainerManager();
    BooleanSupplier check = () -> {
      try {
        return cm.getContainerReplicas(id).size() == expectedCount;
      } catch (Exception e) {
        return false;
      }
    };
    GenericTestUtils.waitFor(check, 1000, 120_000);
  }
}
