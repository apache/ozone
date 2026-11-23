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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_SCM_FOLLOWER_LAG_THROTTLE_THRESHOLD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_SCM_LAG_THROTTLED_LIMIT_PER_TASK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.invoker.ScmInvoker;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.HddsWhiteboxTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Verifies that OM throttles key deletion when any SCM Ratis follower is behind in <em>applying</em>
 * committed Raft log entries (HDDS-17075).
 *
 * <p>Apply is stalled on selected followers by replacing the block-deletion invoker of their
 * {@code SCMStateMachine} with one that blocks until released. Raft log replication is not affected, so the
 * leader keeps committing while the stalled followers fall behind in applying, which the leader cannot see from
 * append-entries replies and has to learn by polling the followers.
 */
@Timeout(300)
public class TestScmFollowerApplyLagThrottle {

  private static final String OM_SERVICE_ID = "om-service-apply-lag";
  private static final String SCM_SERVICE_ID = "scm-service-apply-lag";
  private static final int NUM_OF_SCMS = 3;
  private static final int NUM_OF_DNS = 3;
  private static final int NUM_KEYS = 5;
  // Above the few entries a healthy follower is normally behind while applying.
  private static final int THROTTLE_THRESHOLD = 20;
  private static final byte[] KEY_DATA = "value-of-key".getBytes(UTF_8);

  private static MiniOzoneHAClusterImpl cluster;
  private static int keyCounter;

  @BeforeAll
  static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration("hdds.heartbeat.interval", 1, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HA_FOLLOWER_APPLY_LAG_POLL_INTERVAL, 200, TimeUnit.MILLISECONDS);
    // Throttle down to a single key per task.
    conf.setInt(OZONE_KEY_DELETING_SCM_FOLLOWER_LAG_THROTTLE_THRESHOLD, THROTTLE_THRESHOLD);
    conf.setInt(OZONE_KEY_DELETING_LIMIT_PER_TASK, 50);
    conf.setInt(OZONE_KEY_DELETING_SCM_LAG_THROTTLED_LIMIT_PER_TASK, 1);
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setSCMServiceId(SCM_SERVICE_ID)
        .setNumOfOzoneManagers(1)
        .setNumOfStorageContainerManagers(NUM_OF_SCMS)
        .setNumOfActiveSCMs(NUM_OF_SCMS);
    builder.setNumDatanodes(NUM_OF_DNS);
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * One slow follower raises the lag even though the leader and the other follower are caught up.
   */
  @Test
  void oneSlowFollowerThrottlesOmUntilCaughtUp() throws Exception {
    waitForNoApplyLag();
    final StorageContainerManager leader = cluster.getScmLeader();
    assertThrottlesUntilCaughtUp(leader, getFollowers(leader).subList(0, 1));
  }

  @Test
  void allFollowersBehindThrottlesOmUntilCaughtUp() throws Exception {
    waitForNoApplyLag();
    final StorageContainerManager leader = cluster.getScmLeader();
    assertThrottlesUntilCaughtUp(leader, getFollowers(leader));
  }

  /**
   * Stalls apply on {@code stalled}, then checks that the leader reports apply lag, OM throttles, and OM goes back
   * to the full limit once the followers have caught up.
   */
  private static void assertThrottlesUntilCaughtUp(StorageContainerManager leader,
      List<StorageContainerManager> stalled) throws Exception {
    final OzoneManager om = cluster.getOzoneManager();
    final long throttledBefore = om.getDeletionMetrics().getNumKeyDeletionThrottledBySCMLag();

    final CountDownLatch release = new CountDownLatch(1);
    final List<Map<RequestType, ScmInvoker<?>>> allInvokers = stalled.stream()
        .map(TestScmFollowerApplyLagThrottle::getInvokers).collect(Collectors.toList());
    final List<ScmInvoker<?>> originals = allInvokers.stream()
        .map(invokers -> stallBlockApply(invokers, release)).collect(Collectors.toList());
    try {
      // Every new Raft entry queues up behind the stalled one, so keep writing until the lag passes the threshold.
      GenericTestUtils.waitFor(() -> {
        try {
          createAndDeleteKeys();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return getRatisServer(leader).getFollowerApplyLag() >= THROTTLE_THRESHOLD;
      }, 100, 60_000);

      // OM learns the lag from the next deleteKeyBlocks response, then throttles its following runs.
      createAndDeleteKeys();
      GenericTestUtils.waitFor(
          () -> om.getScmClient().getBlockClient().getLastKnownFollowerApplyLag() >= THROTTLE_THRESHOLD,
          100, 60_000);
      GenericTestUtils.waitFor(
          () -> om.getDeletionMetrics().getNumKeyDeletionThrottledBySCMLag() > throttledBefore, 100, 60_000);
    } finally {
      release.countDown();
      for (int i = 0; i < stalled.size(); i++) {
        allInvokers.get(i).put(RequestType.BLOCK, originals.get(i));
      }
    }

    waitForNoApplyLag();
    // OM only refreshes its view on a deleteKeyBlocks response, so send one more deletion.
    createAndDeleteKeys();
    GenericTestUtils.waitFor(
        () -> om.getScmClient().getBlockClient().getLastKnownFollowerApplyLag() < THROTTLE_THRESHOLD, 100, 60_000);
    final long throttledAfterRecovery = om.getDeletionMetrics().getNumKeyDeletionThrottledBySCMLag();
    Thread.sleep(3000);
    assertEquals(throttledAfterRecovery, om.getDeletionMetrics().getNumKeyDeletionThrottledBySCMLag(),
        "OM must stop throttling once the SCM followers have caught up");
  }

  /**
   * Replaces the block-deletion invoker so that applying a block-deletion entry waits for {@code release}.
   *
   * @return the original invoker, to be put back by the caller
   */
  private static ScmInvoker<?> stallBlockApply(Map<RequestType, ScmInvoker<?>> invokers, CountDownLatch release) {
    final ScmInvoker<?> original = invokers.get(RequestType.BLOCK);
    final ScmInvoker<?> stalled = spy(original);
    try {
      doAnswer(invocation -> {
        // Bounded so that a failing test cannot leave the apply thread stuck forever.
        release.await(120, TimeUnit.SECONDS);
        return invocation.callRealMethod();
      }).when(stalled).invokeLocal(any(), any());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    invokers.put(RequestType.BLOCK, stalled);
    return original;
  }

  @SuppressWarnings("unchecked")
  private static Map<RequestType, ScmInvoker<?>> getInvokers(StorageContainerManager scm) {
    return (Map<RequestType, ScmInvoker<?>>) HddsWhiteboxTestUtils.getInternalState(
        getRatisServer(scm).getSCMStateMachine(), "invokers");
  }

  private static SCMRatisServer getRatisServer(StorageContainerManager scm) {
    return scm.getScmHAManager().getRatisServer();
  }

  private static List<StorageContainerManager> getFollowers(StorageContainerManager leader) {
    final List<StorageContainerManager> followers = cluster.getStorageContainerManagersList().stream()
        .filter(scm -> scm != leader).collect(Collectors.toList());
    assertEquals(NUM_OF_SCMS - 1, followers.size());
    return followers;
  }

  /** Waits until the leader reports no apply lag, so tests do not affect each other. */
  private static void waitForNoApplyLag() throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        return getRatisServer(cluster.getScmLeader()).getFollowerApplyLag() == 0;
      } catch (Exception e) {
        return false;
      }
    }, 100, 120_000);
  }

  /** Creates keys with data in their own bucket and deletes them, so that OM sends their blocks to SCM. */
  private static void createAndDeleteKeys() throws IOException {
    final String volumeName = "vol-" + (++keyCounter);
    final String bucketName = "bucket-" + keyCounter;
    try (OzoneClient client = cluster.newClient()) {
      final ObjectStore store = client.getObjectStore();
      store.createVolume(volumeName);
      store.getVolume(volumeName).createBucket(bucketName);
      final OzoneBucket bucket = store.getVolume(volumeName).getBucket(bucketName);
      for (int i = 0; i < NUM_KEYS; i++) {
        DataTestUtil.createKey(bucket, "key-" + i, RatisReplicationConfig.getInstance(THREE), KEY_DATA);
      }
      for (int i = 0; i < NUM_KEYS; i++) {
        bucket.deleteKey("key-" + i);
      }
      assertFalse(bucket.listKeys("key-").hasNext());
    }
  }
}
