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

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.RatisBlockOutputStream;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that client write operations
 * fail within acceptable time bounds when pipelines/datanodes are down.
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestClientRetryTimeout {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestClientRetryTimeout.class);

  // Small chunk/flush/block sizes so we can trigger flushes quickly
  private static final int CHUNK_SIZE = 1024;
  private static final int FLUSH_SIZE = 2 * CHUNK_SIZE;
  private static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  private static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;

  /**
   * Maximum acceptable duration for a SINGLE retry cycle (write + watch)
   * when the pipeline is completely dead.
   * <p>
   * With the proposed config fixes:
   * Write: RPC(60s) + retry1(1s+60s) + retry2(2s+60s) ≈ 183s
   * Watch: RPC(30s)
   * Total single cycle: ~213s
   * <p>
   * We use 4 minutes (240s) as the upper bound with some margin.
   * The OLD defaults would take ~8 minutes per cycle.
   */
  private static final Duration MAX_SINGLE_CYCLE_DURATION =
      Duration.ofMinutes(4);

  /**
   * Maximum acceptable duration for the watch-for-commit operation alone.
   * <p>
   * With the proposed fix: watch RPC timeout is 30s, but the write timeout
   * (70s) may also be hit if majority is lost. We use 120s to accommodate
   * write(70s) + watch(30s) + overhead.
   * The OLD default would take 180s+ (3 minutes watch alone).
   */
  private static final Duration MAX_WATCH_DURATION = Duration.ofSeconds(120);

  /**
   * Maximum acceptable duration for the end-to-end write failure with
   * Ozone-level retries (ozone.client.max.retries = 5).
   * <p>
   * With proposed fixes: 5 × ~93s ≈ 465s ≈ 8 min.
   * The OLD defaults would take ~40 minutes.
   */
  private static final Duration MAX_TOTAL_WRITE_DURATION =
      Duration.ofMinutes(10);

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;

  @BeforeAll
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // Use small buffer sizes so we can trigger flushes with small writes
    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .applyTo(conf);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    // Fast leader election so new leader can be chosen quickly
    conf.setTimeDuration(
        OzoneConfigKeys.HDDS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        1, TimeUnit.SECONDS);

    // Prevent SCM from removing dead datanodes during the test
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 300, TimeUnit.SECONDS);

    // Allow multiple pipelines per datanode to accommodate all tests
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 5);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 20);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(7)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE,
        180000);

    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "retrytest-" + UUID.randomUUID().toString().substring(0, 8);
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterAll
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test 1: Write to a pipeline where ALL datanodes are dead.
   * <p>
   * Demonstrates the problem:
   * - Client sends WriteChunk to dead leader
   * - TimeoutIOException triggers exponential backoff
   * - OLD config: retries with unlimited max retries for up to 5 minutes
   * - FIXED config: retries at most 2 times, fails in ~63s
   * <p>
   * This test asserts that the write failure is detected within
   * MAX_SINGLE_CYCLE_DURATION (4 minutes), which would fail with the old
   * 5-minute write timeout + 3-minute watch timeout = 8 minutes.
   */
  @Test
  @Order(1)
  public void testWriteToDeadPipelineFailsFast() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName);

    // Write initial data to establish the pipeline connection
    byte[] data = generateData(FLUSH_SIZE);
    key.write(data);
    key.flush();

    // Get the pipeline for this key
    KeyOutputStream keyOutputStream =
        assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    RatisBlockOutputStream blockOutputStream =
        assertInstanceOf(RatisBlockOutputStream.class, stream);
    XceiverClientRatis ratisClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Pipeline pipeline = ratisClient.getPipeline();
    List<DatanodeDetails> nodes = pipeline.getNodes();

    LOG.info("Shutting down ALL datanodes in pipeline: {}", pipeline.getId());
    // Shut down ALL datanodes in the pipeline
    for (DatanodeDetails dn : nodes) {
      cluster.shutdownHddsDatanode(dn);
    }

    // Now write more data. This should eventually fail because the entire
    // pipeline is dead. The question is: HOW LONG does it take?
    long startNanos = System.nanoTime();
    try {
      // Write enough data to trigger a flush (which will try to commit)
      byte[] moreData = generateData(MAX_FLUSH_SIZE + CHUNK_SIZE);
      key.write(moreData);
      key.flush();
      key.close();
      // If we get here without exception, the write succeeded via retry
      // on a different pipeline (which is fine — it means Ozone-level
      // retry worked). Check the duration.
    } catch (IOException e) {
      // Expected: the write should fail after retries are exhausted
      LOG.info("Write failed as expected with: {}", e.getMessage());
    }
    Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

    LOG.info("Write to dead pipeline took: {} seconds", elapsed.getSeconds());
    assertThat(elapsed)
        .as("Write to dead pipeline should fail within %s but took %s. "
                + "This indicates the retry/timeout defaults are too aggressive.",
            MAX_SINGLE_CYCLE_DURATION, elapsed)
        .isLessThan(MAX_SINGLE_CYCLE_DURATION);

    // Restart the datanodes for subsequent tests
    for (DatanodeDetails dn : nodes) {
      cluster.restartHddsDatanode(dn, false);
    }
    cluster.waitForClusterToBeReady();
  }

  /**
   * Test 2: Watch-for-commit when follower datanodes are dead.
   * <p>
   * Demonstrates the problem:
   * - Client writes data, leader commits but followers are dead
   * - Watch-ALL_COMMITTED is issued
   * - OLD config: watch RPC timeout is 180s, client waits 3 minutes
   * - FIXED config: watch RPC timeout is 30s, matches server timeout
   * <p>
   * This test asserts that the watch failure is detected within
   * MAX_WATCH_DURATION (60s), which would fail with the old 180s timeout.
   */
  @Test
  @Order(2)
  public void testWatchForCommitWithDeadFollowersFailsFast() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName);

    // Write initial data to establish the pipeline
    byte[] data = generateData(FLUSH_SIZE);
    key.write(data);
    key.flush();

    // Get the pipeline and identify leader vs followers
    KeyOutputStream keyOutputStream =
        assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    RatisBlockOutputStream blockOutputStream =
        assertInstanceOf(RatisBlockOutputStream.class, stream);
    XceiverClientRatis ratisClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Pipeline pipeline = ratisClient.getPipeline();

    // Find and shut down exactly ONE follower (keep leader + 1 follower
    // alive so majority exists for write, but ALL_COMMITTED will fail)
    List<DatanodeDetails> nodesInPipeline = pipeline.getNodes();
    DatanodeDetails shutdownFollower = null;
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      if (nodesInPipeline.contains(dn.getDatanodeDetails())
          && RatisTestHelper.isRatisFollower(dn, pipeline)) {
        LOG.info("Shutting down follower: {}",
            dn.getDatanodeDetails().getUuidString());
        cluster.shutdownHddsDatanode(dn.getDatanodeDetails());
        shutdownFollower = dn.getDatanodeDetails();
        break;  // Only shut down one follower
      }
    }
    LOG.info("Shut down 1 follower, leader + 1 follower still alive");
    assertTrue(shutdownFollower != null,
        "Should have shut down at least 1 follower");

    // Now write more data. The leader can accept the write, but
    // Watch-ALL_COMMITTED will fail because followers are dead.
    // The key question: how long does the watch take to fail?
    long startNanos = System.nanoTime();
    try {
      byte[] moreData = generateData(MAX_FLUSH_SIZE + CHUNK_SIZE);
      key.write(moreData);
      key.flush();
      key.close();
      // If close succeeds, it means MAJORITY_COMMITTED fallback worked
      LOG.info("Write succeeded (majority committed fallback)");
    } catch (IOException e) {
      LOG.info("Write failed with: {}", e.getMessage());
    }
    Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

    LOG.info("Watch with dead followers took: {} seconds", elapsed.getSeconds());
    assertThat(elapsed)
        .as("Watch-for-commit with dead followers should complete within %s "
                + "but took %s. This indicates the watch RPC timeout (180s) "
                + "is not aligned with the server watch timeout (30s).",
            MAX_WATCH_DURATION, elapsed)
        .isLessThan(MAX_WATCH_DURATION);

    // Restart the follower we shut down
    try {
      cluster.restartHddsDatanode(shutdownFollower, false);
    } catch (Exception e) {
      // May already be running
    }
    cluster.waitForClusterToBeReady();
  }

  /**
   * Test 3: Write failure when the Raft leader is specifically killed.
   * <p>
   * Demonstrates the problem:
   * - Client is writing to a pipeline
   * - The Raft leader is killed mid-write
   * - RaftClient still holds stale connection to dead leader
   * - OLD config: exponential backoff retries indefinitely for 5 min
   * - FIXED config: limited to 2 retries, fails in ~63s
   * <p>
   * This test asserts the entire operation (write + close) completes
   * within MAX_SINGLE_CYCLE_DURATION.
   */
  @Test
  @Order(3)
  public void testWriteWithLeaderFailureFailsFast() throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName);

    // Write initial data
    byte[] data = generateData(FLUSH_SIZE);
    key.write(data);
    key.flush();

    // Get the pipeline and find the leader
    KeyOutputStream keyOutputStream =
        assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    OutputStream stream = keyOutputStream.getStreamEntries().get(0)
        .getOutputStream();
    RatisBlockOutputStream blockOutputStream =
        assertInstanceOf(RatisBlockOutputStream.class, stream);
    XceiverClientRatis ratisClient =
        (XceiverClientRatis) blockOutputStream.getXceiverClient();
    Pipeline pipeline = ratisClient.getPipeline();

    // Find and kill the leader
    HddsDatanodeService leader = null;
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      if (pipeline.getNodes().contains(dn.getDatanodeDetails())
          && RatisTestHelper.isRatisLeader(dn, pipeline)) {
        leader = dn;
        break;
      }
    }
    assertThat(leader).as("Should find leader in pipeline").isNotNull();

    LOG.info("Shutting down leader: {}",
        leader.getDatanodeDetails().getUuidString());
    cluster.shutdownHddsDatanode(leader.getDatanodeDetails());

    // Write more data. The RaftClient will try to send to the dead leader.
    long startNanos = System.nanoTime();
    try {
      byte[] moreData = generateData(MAX_FLUSH_SIZE + CHUNK_SIZE);
      key.write(moreData);
      key.flush();
      key.close();
      LOG.info("Write completed (new leader elected or pipeline retry)");
    } catch (IOException e) {
      LOG.info("Write failed with: {}", e.getMessage());
    }
    Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

    LOG.info("Write with dead leader took: {} seconds", elapsed.getSeconds());
    assertThat(elapsed)
        .as("Write with dead leader should fail/recover within %s but took %s. "
                + "This indicates exponential backoff max retries "
                + "(Integer.MAX_VALUE) or the write timeout (5m) is too high.",
            MAX_SINGLE_CYCLE_DURATION, elapsed)
        .isLessThan(MAX_SINGLE_CYCLE_DURATION);

    // Restart the leader
    cluster.restartHddsDatanode(leader.getDatanodeDetails(), false);
    cluster.waitForClusterToBeReady();
  }

  /**
   * Test 4: End-to-end write with ALL datanodes killed, verifying the
   * total time including Ozone-level retries.
   * <p>
   * Demonstrates the compound problem:
   * - Ozone retries up to 5 times (ozone.client.max.retries)
   * - Each retry allocates a new pipeline, but ALL datanodes are dead
   * - OLD config: 5 × (5min write + 3min watch) = 40 minutes
   * - FIXED config: 5 × (~63s write + 30s watch) ≈ 7.7 minutes
   * <p>
   * This test asserts the TOTAL time is within MAX_TOTAL_WRITE_DURATION
   * (10 minutes), which would fail with the old 40-minute worst case.
   */
  @Test
  @Order(4)
  public void testEndToEndWriteWithAllDatanodesDownFailsFast()
      throws Exception {
    String keyName = getKeyName();
    OzoneOutputStream key = createKey(keyName);

    // Write initial data to establish a pipeline
    byte[] data = generateData(FLUSH_SIZE);
    key.write(data);
    key.flush();

    LOG.info("Shutting down ALL datanodes in the cluster");
    // Copy the list to avoid ConcurrentModificationException since
    // cluster.getHddsDatanodes() returns the live internal list
    List<HddsDatanodeService> allDatanodes =
        new ArrayList<>(cluster.getHddsDatanodes());
    for (HddsDatanodeService dn : allDatanodes) {
      cluster.shutdownHddsDatanode(dn.getDatanodeDetails());
    }

    // Now try to write + close. Every pipeline allocation will fail.
    // The client should exhaust all ozone-level retries and throw.
    long startNanos = System.nanoTime();
    try {
      byte[] moreData = generateData(MAX_FLUSH_SIZE + CHUNK_SIZE);
      key.write(moreData);
      key.flush();
      key.close();
      // Should not succeed — all datanodes are down
      LOG.warn("Write unexpectedly succeeded with all datanodes down");
    } catch (IOException e) {
      LOG.info("Write failed as expected: {}", e.getMessage());
    }
    Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

    LOG.info("End-to-end write with all datanodes down took: {} seconds",
        elapsed.getSeconds());
    assertThat(elapsed)
        .as("End-to-end write failure should complete within %s but took %s. "
                + "This indicates the compound retry/timeout configuration "
                + "is causing the client to hang.",
            MAX_TOTAL_WRITE_DURATION, elapsed)
        .isLessThan(MAX_TOTAL_WRITE_DURATION);

    // Restart all datanodes
    for (HddsDatanodeService dn : allDatanodes) {
      cluster.restartHddsDatanode(dn.getDatanodeDetails(), false);
    }
    cluster.waitForClusterToBeReady();
  }

  private String getKeyName() {
    return UUID.randomUUID().toString();
  }

  private OzoneOutputStream createKey(String keyName) throws Exception {
    return TestHelper.createKey(keyName, ReplicationType.RATIS, 0,
        objectStore, volumeName, bucketName);
  }

  private byte[] generateData(int length) {
    StringBuilder sb = new StringBuilder(length);
    while (sb.length() < length) {
      sb.append(UUID.randomUUID());
    }
    return sb.substring(0, length).getBytes(UTF_8);
  }
}
