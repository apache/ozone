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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.function.BooleanSupplier;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.node.PendingContainerTracker;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeMetrics;
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
  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ContainerManager containerManager;
  private SCMNodeMetrics metrics;
  private OzoneBucket bucket;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

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
    
    StorageContainerManager scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    client = cluster.newClient();

    // Create bucket for testing
    bucket = TestDataUtil.createVolumeAndBucket(client);

    SCMNodeManager nodeManager = (SCMNodeManager) scm.getScmNodeManager();
    assertNotNull(nodeManager);
    PendingContainerTracker pendingTracker = nodeManager.getPendingContainerTracker();
    assertNotNull(pendingTracker, "PendingContainerTracker should be initialized");
    metrics = pendingTracker.getMetrics();
    
    LOG.info("Test setup complete - ICR interval: 5s, Heartbeat interval: 1s");
  }

  @AfterEach
  public void cleanup() throws Exception {
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
    containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        "omServiceIdDefault");

    // Verify the added metric increased, meaning pending was recorded
    GenericTestUtils.waitFor(
        (BooleanSupplier) () -> metrics.getNumPendingContainersAdded() > initialAdded,
        100, 5000);

    long afterAdded = metrics.getNumPendingContainersAdded();
    assertThat(afterAdded).isGreaterThan(initialAdded);

    LOG.info("Pending tracked successfully. Waiting for ICR to remove pending...");

    // Write a key so datanodes send ICRs
    String keyName = "testKey1";
    byte[] data = "Testing Pending Container Tracker".getBytes(UTF_8);

    LOG.info("Writing key: {}", keyName);
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length,
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        new java.util.HashMap<>())) {
      out.write(data);
    }
    LOG.info("Key written successfully");

    // Wait for ICRs to be processed and removed metric to increase
    GenericTestUtils.waitFor(
        (BooleanSupplier) () -> metrics.getNumPendingContainersRemoved() > initialRemoved,
        100, 5000);

    long afterRemoved = metrics.getNumPendingContainersRemoved();
    assertThat(afterRemoved).isGreaterThan(initialRemoved);

    LOG.info("After added={}, removed={}", afterAdded, afterRemoved);
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
    GenericTestUtils.waitFor(
        (BooleanSupplier) () -> metrics.getNumPendingContainersAdded() > initialAdded,
        100, 5000);

    // Removed metric should increase after ICR processing
    GenericTestUtils.waitFor(
        (BooleanSupplier) () -> metrics.getNumPendingContainersRemoved() > initialRemoved,
        100, 5000);
  }
}
