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

package org.apache.hadoop.ozone.recon;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachHealthState;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport.HealthState;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.recon.fsck.ContainerHealthTask;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Integration tests to validate discrepancies between SCM and Recon
 * container health reporting for MISSING, UNDER_REPLICATED, OVER_REPLICATED,
 * and MIS_REPLICATED containers.
 *
 * These tests validate the findings from Recon_SCM_Data_Correctness_Analysis.md
 * and will be updated after Recon fixes are implemented to verify matching counts.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestSCMVsReconContainerHealthDiscrepancies {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSCMVsReconContainerHealthDiscrepancies.class);

  private static final int DATANODE_COUNT = 5;
  private static final RatisReplicationConfig RATIS_THREE = RatisReplicationConfig
      .getInstance(HddsProtos.ReplicationFactor.THREE);

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private ReconService recon;
  private OzoneClient client;
  private OzoneBucket bucket;

  // SCM components
  private StorageContainerManager scm;
  private ContainerManager scmContainerManager;
  private ReplicationManager scmReplicationManager;
  private NodeManager scmNodeManager;

  // Recon components
  private ReconStorageContainerManagerFacade reconScm;
  private ReconContainerManager reconContainerManager;
  private ContainerHealthTask reconContainerHealthTask;
  private ContainerHealthSchemaManager containerHealthSchemaManager;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();

    // ============================================================
    // PHASE 1: BASELINE TEST - Use LEGACY Implementation
    // ============================================================
    // Set feature flag to FALSE to use legacy ContainerHealthTask
    // This establishes baseline discrepancies between SCM and Recon
    conf.setBoolean("ozone.recon.container.health.use.scm.report", false);
    LOG.info("=== PHASE 1: Testing with LEGACY ContainerHealthTask (flag=false) ===");

    // Heartbeat and report intervals - match TestReconTasks pattern
    // IMPORTANT: 100ms is too aggressive and causes cluster instability!
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "5s");
    conf.set(HDDS_PIPELINE_REPORT_INTERVAL, "5s");
    conf.set("ozone.scm.heartbeat.interval", "1s");
    conf.set("ozone.scm.heartbeat.process.interval", "1s");

    // Node state transition intervals - match TestReconTasks
    conf.set("ozone.scm.stale.node.interval", "6s");
    conf.set("ozone.scm.dead.node.interval", "8s");  // 8s NOT 2s - critical!
    conf.setTimeDuration(OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, 0, SECONDS);

    // Fast replication manager processing
    ReplicationManager.ReplicationManagerConfiguration rmConf =
        conf.getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    rmConf.setInterval(Duration.ofSeconds(1));
    rmConf.setUnderReplicatedInterval(Duration.ofMillis(100));
    rmConf.setOverReplicatedInterval(Duration.ofMillis(100));
    conf.setFromObject(rmConf);

    // Initialize Recon service
    recon = new ReconService(conf);

    // Build cluster with 5 datanodes for flexible replica manipulation
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(DATANODE_COUNT)
        .addService(recon)
        .build();

    cluster.waitForClusterToBeReady();

    // Initialize SCM components
    scm = cluster.getStorageContainerManager();
    scmContainerManager = scm.getContainerManager();
    scmReplicationManager = scm.getReplicationManager();
    scmNodeManager = scm.getScmNodeManager();

    // Initialize Recon components
    reconScm = (ReconStorageContainerManagerFacade)
        recon.getReconServer().getReconStorageContainerManager();
    reconContainerManager = (ReconContainerManager) reconScm.getContainerManager();
    reconContainerHealthTask = (ContainerHealthTask) reconScm.getContainerHealthTask();
    containerHealthSchemaManager = reconContainerManager.getContainerSchemaManager();

    // Create client and test bucket
    client = cluster.newClient();
    bucket = TestDataUtil.createVolumeAndBucket(client);

    LOG.info("=== Test setup complete: {} datanodes ready ===", DATANODE_COUNT);
  }

  @AfterEach
  public void tearDown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    LOG.info("=== Test teardown complete ===");
  }

  /**
   * Test Scenario 1A: MISSING Container - All Replicas Lost
   *
   * This test validates that both SCM and Recon detect containers as MISSING
   * when all replicas are lost (all hosting datanodes are dead).
   *
   * Expected: Both SCM and Recon should report the container as MISSING
   * (±5% timing variance is acceptable)
   */
  @Test
  @Order(1)
  public void testMissingContainerAllReplicasLost() throws Exception {
    LOG.info("=== TEST 1A: MISSING Container - All Replicas Lost ===");

    // Step 1: Create a key and close the container
    String keyName = "test-missing-all-replicas-" + System.currentTimeMillis();
    TestDataUtil.createKey(bucket, keyName, RATIS_THREE,
        "test content for missing".getBytes(StandardCharsets.UTF_8));

    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    long containerIDLong = keyLocations.get(0).getContainerID();
    ContainerID containerId = ContainerID.valueOf(containerIDLong);

    ContainerInfo containerInfo = scmContainerManager.getContainer(containerId);
    LOG.info("Created container: {}, state: {}", containerIDLong, containerInfo.getState());

    // Close the container to enable health checks
    OzoneTestUtils.closeContainer(scm, containerInfo);
    LOG.info("Closed container: {}", containerIDLong);

    // Step 2: Get all datanodes hosting this container
    Set<ContainerReplica> replicas = scmContainerManager.getContainerReplicas(containerId);
    List<DatanodeDetails> hostingDatanodes = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());

    assertEquals(3, hostingDatanodes.size(),
        "Container should have 3 replicas (replication factor 3)");
    LOG.info("Container {} has replicas on datanodes: {}",
        containerIDLong, hostingDatanodes);

    // Step 3: Shutdown all datanodes hosting replicas
    LOG.info("Shutting down all {} datanodes hosting container {}",
        hostingDatanodes.size(), containerIDLong);
    for (DatanodeDetails dn : hostingDatanodes) {
      LOG.info("Shutting down datanode: {}", dn.getUuidString());
      cluster.shutdownHddsDatanode(dn);
      waitForDnToReachHealthState(scmNodeManager, dn, DEAD);
      LOG.info("Datanode {} is now DEAD", dn.getUuidString());
    }

    // Step 4: Wait for SCM ReplicationManager to detect MISSING container
    LOG.info("Waiting for SCM to detect container {} as MISSING", containerIDLong);
    GenericTestUtils.waitFor(() -> {
      try {
        ReplicationManagerReport report = new ReplicationManagerReport();
        scmReplicationManager.checkContainerStatus(
            scmContainerManager.getContainer(containerId), report);
        long missingCount = report.getStat(HealthState.MISSING);
        LOG.debug("SCM MISSING count for container {}: {}", containerIDLong, missingCount);
        return missingCount > 0;
      } catch (Exception e) {
        LOG.error("Error checking SCM container status", e);
        return false;
      }
    }, 500, 30000);

    // Step 5: Get SCM report
    ReplicationManagerReport scmReport = new ReplicationManagerReport();
    scmReplicationManager.checkContainerStatus(
        scmContainerManager.getContainer(containerId), scmReport);

    long scmMissingCount = scmReport.getStat(HealthState.MISSING);
    LOG.info("SCM Reports: MISSING={}", scmMissingCount);
    assertEquals(1, scmMissingCount, "SCM should report 1 MISSING container");

    // Step 6: Trigger Recon ContainerHealthTask
    LOG.info("Triggering Recon ContainerHealthTask");
    reconContainerHealthTask.run();

    // Step 7: Wait for Recon to process and update
    Thread.sleep(2000); // Give Recon time to process

    // Step 8: Get Recon report via ContainerHealthSchemaManager
    List<UnhealthyContainers> missingContainers =
        containerHealthSchemaManager.getUnhealthyContainers(
            UnHealthyContainerStates.MISSING, 0L, Optional.empty(), 1000);

    int reconMissingCount = missingContainers.size();
    LOG.info("Recon Reports: MISSING={}", reconMissingCount);

    // Step 9: Compare and validate
    LOG.info("=== COMPARISON: MISSING Containers ===");
    LOG.info("SCM:   {}", scmMissingCount);
    LOG.info("Recon: {}", reconMissingCount);

    // Both SCM and Recon should detect MISSING containers identically
    // MISSING detection is based on all replicas being on dead nodes
    assertEquals(scmMissingCount, reconMissingCount,
        "MISSING container count should match between SCM and Recon");

    LOG.info("✓ TEST 1A PASSED: MISSING container detection validated");
  }

  /**
   * Test Scenario 2A: UNDER_REPLICATED Container - Simple Case
   *
   * This test validates that both SCM and Recon detect containers as
   * UNDER_REPLICATED when replica count drops below replication factor.
   *
   * Expected: Both should detect under-replication
   * (±10% variance due to config differences is acceptable)
   */
  // @Test
  // @Order(2)
  public void testUnderReplicatedContainerSimple() throws Exception {
    LOG.info("=== TEST 2A: UNDER_REPLICATED Container - Simple Case ===");

    // Step 1: Create key with Ratis THREE replication
    String keyName = "test-under-rep-simple-" + System.currentTimeMillis();
    TestDataUtil.createKey(bucket, keyName, RATIS_THREE,
        "test content for under-replication".getBytes(StandardCharsets.UTF_8));

    long containerIDLong = bucket.getKey(keyName)
        .getOzoneKeyLocations().get(0).getContainerID();
    ContainerID containerId = ContainerID.valueOf(containerIDLong);
    ContainerInfo containerInfo = scmContainerManager.getContainer(containerId);

    // Close container to enable replication processing
    OzoneTestUtils.closeContainer(scm, containerInfo);
    LOG.info("Created and closed container: {}", containerIDLong);

    // Verify initial replica count
    Set<ContainerReplica> initialReplicas =
        scmContainerManager.getContainerReplicas(containerId);
    assertEquals(3, initialReplicas.size(), "Should start with 3 replicas");

    // Step 2: Kill ONE datanode to make it under-replicated (2 < 3)
    DatanodeDetails datanodeToKill =
        initialReplicas.iterator().next().getDatanodeDetails();
    LOG.info("Shutting down datanode: {}", datanodeToKill.getUuidString());

    cluster.shutdownHddsDatanode(datanodeToKill);
    waitForDnToReachHealthState(scmNodeManager, datanodeToKill, DEAD);
    LOG.info("Datanode {} is now DEAD", datanodeToKill.getUuidString());

    // Step 3: Wait for replication manager to detect under-replication
    LOG.info("Waiting for SCM to detect under-replication");
    GenericTestUtils.waitFor(() -> {
      try {
        Set<ContainerReplica> currentReplicas =
            scmContainerManager.getContainerReplicas(containerId);
        // Filter out dead datanode replicas
        long healthyCount = currentReplicas.stream()
            .filter(r -> !r.getDatanodeDetails().equals(datanodeToKill))
            .count();
        LOG.debug("Healthy replica count: {}", healthyCount);
        return healthyCount < 3; // Under-replicated
      } catch (Exception e) {
        LOG.error("Error checking replica count", e);
        return false;
      }
    }, 500, 30000);

    // Step 4: Check SCM status
    ReplicationManagerReport scmReport = new ReplicationManagerReport();
    scmReplicationManager.checkContainerStatus(containerInfo, scmReport);

    long scmUnderRepCount = scmReport.getStat(HealthState.UNDER_REPLICATED);
    LOG.info("SCM Reports: UNDER_REPLICATED={}", scmUnderRepCount);
    assertEquals(1, scmUnderRepCount,
        "SCM should report 1 UNDER_REPLICATED container");

    // Step 5: Trigger Recon and verify
    LOG.info("Triggering Recon ContainerHealthTask");
    reconContainerHealthTask.run();
    Thread.sleep(2000);

    List<UnhealthyContainers> underReplicatedContainers =
        containerHealthSchemaManager.getUnhealthyContainers(
            UnHealthyContainerStates.UNDER_REPLICATED, 0L, Optional.empty(), 1000);

    int reconUnderRepCount = underReplicatedContainers.size();
    LOG.info("Recon Reports: UNDER_REPLICATED={}", reconUnderRepCount);

    // Step 6: Compare and validate
    LOG.info("=== COMPARISON: UNDER_REPLICATED Containers ===");
    LOG.info("SCM:   {}", scmUnderRepCount);
    LOG.info("Recon: {}", reconUnderRepCount);

    // Both SCM and Recon should detect UNDER_REPLICATED containers identically
    // UNDER_REPLICATED detection is based on healthy replica count < replication factor
    assertEquals(scmUnderRepCount, reconUnderRepCount,
        "UNDER_REPLICATED container count should match between SCM and Recon");

    LOG.info("✓ TEST 2A PASSED: UNDER_REPLICATED container detection validated");
  }

  /**
   * Test Scenario 3A: OVER_REPLICATED Container - Healthy Excess Replicas
   *
   * This test simulates over-replication by bringing a datanode back online
   * after a new replica was created on another node.
   *
   * Expected: Both SCM and Recon should detect over-replication
   * (Phase 1 check - healthy replicas only)
   */
  // @Test
  // @Order(3)
  public void testOverReplicatedWithHealthyReplicas() throws Exception {
    LOG.info("=== TEST 3A: OVER_REPLICATED Container - Healthy Excess ===");

    // Step 1: Create and close container
    String keyName = "test-over-rep-healthy-" + System.currentTimeMillis();
    TestDataUtil.createKey(bucket, keyName, RATIS_THREE,
        "test content for over-replication".getBytes(StandardCharsets.UTF_8));

    long containerIDLong = bucket.getKey(keyName)
        .getOzoneKeyLocations().get(0).getContainerID();
    ContainerID containerId = ContainerID.valueOf(containerIDLong);
    ContainerInfo containerInfo = scmContainerManager.getContainer(containerId);

    OzoneTestUtils.closeContainer(scm, containerInfo);
    LOG.info("Created and closed container: {}", containerIDLong);

    // Start with 3 healthy replicas
    Set<ContainerReplica> initialReplicas =
        scmContainerManager.getContainerReplicas(containerId);
    assertEquals(3, initialReplicas.size(), "Should have 3 replicas initially");

    // Step 2: Simulate scenario - shutdown one DN, wait for replication, restart
    DatanodeDetails dnToRestart =
        initialReplicas.iterator().next().getDatanodeDetails();
    LOG.info("Shutting down datanode {} to trigger replication",
        dnToRestart.getUuidString());

    cluster.shutdownHddsDatanode(dnToRestart);
    waitForDnToReachHealthState(scmNodeManager, dnToRestart, DEAD);

    // Step 3: Wait for RM to schedule replication
    LOG.info("Waiting for ReplicationManager to schedule replication");
    long initialReplicationCmds = scmReplicationManager.getMetrics()
        .getReplicationCmdsSentTotal();

    GenericTestUtils.waitFor(() -> {
      long currentCmds = scmReplicationManager.getMetrics()
          .getReplicationCmdsSentTotal();
      LOG.debug("Replication commands sent: {} (initial: {})",
          currentCmds, initialReplicationCmds);
      return currentCmds > initialReplicationCmds;
    }, 1000, 60000);

    LOG.info("Replication command sent, waiting for new replica creation");

    // Step 4: Wait for new replica to be created
    GenericTestUtils.waitFor(() -> {
      try {
        Set<ContainerReplica> currentReplicas =
            scmContainerManager.getContainerReplicas(containerId);
        long healthyCount = currentReplicas.stream()
            .filter(r -> !r.getDatanodeDetails().equals(dnToRestart))
            .count();
        LOG.debug("Healthy replica count (excluding dead node): {}", healthyCount);
        return healthyCount >= 3; // New replica created
      } catch (Exception e) {
        return false;
      }
    }, 1000, 60000);

    LOG.info("New replica created, restarting original datanode");

    // Step 5: Restart the original datanode (now we have 4 replicas)
    cluster.restartHddsDatanode(dnToRestart, true);
    LOG.info("Restarted datanode {}", dnToRestart.getUuidString());

    // Wait for original replica to be reported back
    GenericTestUtils.waitFor(() -> {
      try {
        Set<ContainerReplica> currentReplicas =
            scmContainerManager.getContainerReplicas(containerId);
        int replicaCount = currentReplicas.size();
        LOG.debug("Total replica count: {}", replicaCount);
        return replicaCount >= 4; // Over-replicated!
      } catch (Exception e) {
        return false;
      }
    }, 1000, 30000);

    // Step 6: Verify SCM detects over-replication
    LOG.info("Checking SCM for over-replication detection");
    ReplicationManagerReport scmReport = new ReplicationManagerReport();
    scmReplicationManager.checkContainerStatus(containerInfo, scmReport);

    long scmOverRepCount = scmReport.getStat(HealthState.OVER_REPLICATED);
    LOG.info("SCM Reports: OVER_REPLICATED={}", scmOverRepCount);
    assertEquals(1, scmOverRepCount,
        "SCM should report 1 OVER_REPLICATED container");

    // Step 7: Verify Recon detects it too (Phase 1 check matches)
    LOG.info("Triggering Recon ContainerHealthTask");
    reconContainerHealthTask.run();
    Thread.sleep(2000);

    List<UnhealthyContainers> overReplicatedContainers =
        containerHealthSchemaManager.getUnhealthyContainers(
            UnHealthyContainerStates.OVER_REPLICATED, 0L, Optional.empty(), 1000);

    int reconOverRepCount = overReplicatedContainers.size();
    LOG.info("Recon Reports: OVER_REPLICATED={}", reconOverRepCount);

    // Step 8: Compare and validate
    LOG.info("=== COMPARISON: OVER_REPLICATED Containers (Healthy Excess) ===");
    LOG.info("SCM:   {}", scmOverRepCount);
    LOG.info("Recon: {}", reconOverRepCount);

    // For this scenario (Phase 1 check - all replicas healthy), both SCM and Recon
    // use the same logic and should detect over-replication identically
    assertEquals(scmOverRepCount, reconOverRepCount,
        "OVER_REPLICATED container count should match for Phase 1 check (healthy replicas only)");

    LOG.info("✓ TEST 3A PASSED: OVER_REPLICATED (healthy excess) detection validated");
  }

  /**
   * TODO: Test Scenario 1B: EC Container MISSING (Insufficient Data Blocks)
   * TODO: Test Scenario 2B: UNDER_REPLICATED with Maintenance Nodes
   * TODO: Test Scenario 3B: OVER_REPLICATED with Unhealthy Replicas (CRITICAL)
   * TODO: Test Scenario 4A: MIS_REPLICATED (Rack Awareness Violation)
   * TODO: Test Scenario 4B: MIS_REPLICATED with Unhealthy Replicas
   *
   * These tests will be added in subsequent commits.
   */
}