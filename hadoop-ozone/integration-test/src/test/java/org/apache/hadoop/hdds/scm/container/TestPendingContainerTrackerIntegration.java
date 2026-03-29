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

package org.apache.hadoop.hdds.scm.container;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.metrics.SCMContainerManagerMetrics;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for PendingContainerTracker.
 */
@Timeout(300)
public class TestPendingContainerTrackerIntegration {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestPendingContainerTrackerIntegration.class);

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private StorageContainerManager scm;
  private OzoneClient client;
  private ContainerManager containerManager;
  private PendingContainerTracker pendingTracker;
  private SCMContainerManagerMetrics metrics;
  private OzoneBucket bucket;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();

    conf.set(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, "60s");
    
    // Reduce heartbeat interval for faster container reports
    conf.set(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, "10s");
    
    conf.set("ozone.scm.container.size", "100MB");
    conf.set("ozone.scm.pipeline.owner.container.count", "1");
    conf.set("ozone.scm.pipeline.per.metadata.disk", "1");
    conf.set("ozone.scm.datanode.pipeline.limit", "1");
    
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();
    
    scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    client = cluster.newClient();
    
    // Create bucket for testing
    bucket = TestDataUtil.createVolumeAndBucket(client);
    
    // Get the pending tracker
    if (containerManager instanceof ContainerManagerImpl) {
      pendingTracker = ((ContainerManagerImpl) containerManager)
          .getPendingContainerTracker();
      assertNotNull(pendingTracker, "PendingContainerTracker should be initialized");
    }
    metrics = pendingTracker.getMetrics();
    // metrics = SCMContainerManagerMetrics.create();
    
    LOG.info("Test setup complete - ICR interval: 5s, Heartbeat interval: 1s");
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (metrics != null) {
      metrics.unRegister();
    }
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test: Write key → Container allocation → Pending tracked → ICR → Pending removed.
   */
  @Test
  public void testKeyWriteRecordsPendingAndICRRemovesIt() throws Exception {
    long initialAdded = metrics.getNumPendingContainersAdded();
    long initialRemoved = metrics.getNumPendingContainersRemoved();

    // Allocate a container directly
    ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        "omServiceIdDefault");

    // Find the container that was allocated
    ContainerInfo containerInfo = scm.getContainerManager().getContainers().get(0);
    ContainerWithPipeline containerWithPipeline =
        scm.getClientProtocolServer().getContainerWithPipeline(
            containerInfo.getContainerID());

    Pipeline pipeline = containerWithPipeline.getPipeline();

    // Verify pending containers are tracked for all nodes in pipeline
    List<DatanodeDetails> nodesWithPending = new ArrayList<>();
    for (DatanodeDetails dn : pipeline.getNodes()) {
      long pendingSize = pendingTracker.getPendingAllocationSize(dn);
      if (pendingSize > 0) {
        nodesWithPending.add(dn);
        LOG.info("DataNode {} has {} bytes pending", dn.getUuidString(), pendingSize);

        Set<ContainerID> pendingContainers = pendingTracker.getPendingContainers(dn);
        assertThat(pendingContainers.contains(container.containerID()));
      }
    }

    assertThat(nodesWithPending.size() > 0);

    // Verify metrics increased
    long afterAdded = metrics.getNumPendingContainersAdded();
    assertThat(afterAdded > initialAdded);

    LOG.info("Pending tracked successfully. Waiting for ICR to remove pending...");

    // Write a key
    String keyName = "testKey1";
    byte[] data = "Hello Ozone - Testing Pending Container Tracker".getBytes(UTF_8);

    LOG.info("Writing key: {}", keyName);
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length,
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        new java.util.HashMap<>())) {
      out.write(data);
    }
    LOG.info("Key written successfully");

    // Wait for ICRs to be sent
    GenericTestUtils.waitFor(() -> {
      for (DatanodeDetails dn : nodesWithPending) {
        Set<ContainerID> pendingContainers = pendingTracker.getPendingContainers(dn);
        if (pendingContainers.contains(container.containerID())) {
          LOG.info("Still waiting for ICR from DN {}", dn.getUuidString());
          return false;
        }
      }

      LOG.info("All pending containers removed via ICR!");
      return true;
    }, 100, 5000);

    // Verify all pending containers removed
    for (DatanodeDetails dn : nodesWithPending) {
      Set<ContainerID> pendingContainers = pendingTracker.getPendingContainers(dn);
      assertThat(!pendingContainers.contains(container.containerID()));
    }

    // Verify remove metrics increased
    long afterRemoved = metrics.getNumPendingContainersRemoved();
    assertThat(afterRemoved > initialRemoved);

    LOG.info("After added = " + afterAdded);

  }

  /**
   * Test: Verify idempotency - container reported multiple times.
   */
  @Test
  public void testIdempotentPendingTracking() throws Exception {
    // Allocate a container directly
    ContainerInfo container = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        "omServiceIdDefault");
    
    Pipeline pipeline = scm.getPipelineManager().getPipeline(container.getPipelineID());
    DatanodeDetails firstNode = pipeline.getFirstNode();
    
    // Record initial state
    long initialSize = pendingTracker.getPendingAllocationSize(firstNode);
    int initialCount = pendingTracker.getPendingContainers(firstNode).size();
    
    LOG.info("Initial pending state: size={}, count={}", initialSize, initialCount);
    
    // Try adding the same container again (simulates retry or duplicate allocation)
    pendingTracker.recordPendingAllocationForDatanode(firstNode, container.containerID());
    
    long afterSize = pendingTracker.getPendingAllocationSize(firstNode);
    int afterCount = pendingTracker.getPendingContainers(firstNode).size();
    
    // Size and count should remain the same (idempotent)
    assertEquals(initialSize, afterSize,
        "Pending size should not change when adding duplicate container");
    assertEquals(initialCount, afterCount,
        "Pending count should not change when adding duplicate container");

  }

  /**
   * Test: Verify metrics are updated correctly.
   */
  @Test
  public void testMetricsUpdateThroughLifecycle() throws Exception {
    long initialAdded = metrics.getNumPendingContainersAdded();
    long initialRemoved = metrics.getNumPendingContainersRemoved();
    
    LOG.info("Initial metrics: added={}, removed={}", initialAdded, initialRemoved);
    
    // Write multiple keys
    for (int i = 0; i < 3; i++) {
      String keyName = "metricsTestKey" + i;
      byte[] data = ("Metrics test " + i).getBytes(UTF_8);
      
      try (OzoneOutputStream out = bucket.createKey(keyName, data.length,
          RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
          new java.util.HashMap<>())) {
        out.write(data);
      }
    }

    // addedMetrics should increase as containers are allocated
    GenericTestUtils.waitFor(() -> {
      long afterAdded = metrics.getNumPendingContainersAdded();
      return afterAdded > initialAdded;
    }, 100, 5000);

    // Removed metric should increase after icr process
    GenericTestUtils.waitFor(() -> {
      long afterRemoved = metrics.getNumPendingContainersRemoved();
      return initialRemoved < afterRemoved;
    }, 100, 5000);
  }
}
