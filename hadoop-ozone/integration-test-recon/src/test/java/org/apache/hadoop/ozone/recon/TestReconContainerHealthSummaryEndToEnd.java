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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType.KeyValueContainer;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.UnhealthyContainerRecord;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comprehensive end-to-end integration test validating that:
 * <ol>
 *   <li><b>Container State Summary</b> — per lifecycle-state counts (OPEN, CLOSING,
 *       QUASI_CLOSED, CLOSED) are identical between SCM and Recon after a full sync.</li>
 *   <li><b>Container Health Summary</b> — UNHEALTHY_CONTAINERS derby table counts in
 *       Recon match exactly the health states classified by SCM's ReplicationManager
 *       after both process the same container replica state.</li>
 * </ol>
 *
 * <p><b>Health states covered:</b>
 * <ul>
 *   <li>{@code UNDER_REPLICATED} — RF3 CLOSED container with 1 replica removed from
 *       both SCM and Recon → 2 of 3 replicas present.</li>
 *   <li>{@code OVER_REPLICATED} — RF1 CLOSED container with a phantom replica injected
 *       into both SCM and Recon → 2 replicas for an RF1 container.</li>
 *   <li>{@code MISSING} — RF1 CLOSED container with all replicas removed from both,
 *       {@code numberOfKeys=1} → SCM RM: {@code MISSING} (via
 *       {@code RatisReplicationCheckHandler}), Recon: {@code MISSING}.</li>
 *   <li>{@code EMPTY_MISSING} — RF1 <b>CLOSING</b> container with all replicas removed
 *       from both, {@code numberOfKeys=0} (default). SCM RM emits both:
 *       {@code getStat(MISSING)} (via {@code ClosingContainerHandler}) for these containers
 *       AND {@code getStat(EMPTY)} (via {@code EmptyContainerHandler} case 3) for the
 *       CLOSED contrast group below. When the <em>same</em> container is both MISSING
 *       (no replicas → health=MISSING in SCM) and EMPTY (no keys → numberOfKeys=0),
 *       Recon stores it as {@code EMPTY_MISSING}.</li>
 *   <li>{@code EMPTY} (contrast to {@code EMPTY_MISSING}) — RF1 <b>CLOSED</b> container
 *       with 0 replicas and {@code numberOfKeys=0}, never created on any datanode.
 *       SCM RM: {@code EMPTY} (via {@code EmptyContainerHandler} case 3, which fires
 *       <em>before</em> {@code RatisReplicationCheckHandler} and stops the chain).
 *       Recon: also {@code EMPTY} — NOT stored in {@code UNHEALTHY_CONTAINERS}. This
 *       shows that the same content properties (0 keys + 0 replicas) produce a different
 *       classification depending on lifecycle state: CLOSING → MISSING/EMPTY_MISSING,
 *       CLOSED → EMPTY/not-stored.</li>
 *   <li>{@code MIS_REPLICATED} — NOT COVERED: requires a rack-aware placement policy
 *       configured with a specific multi-rack DN topology, not available in mini-cluster
 *       integration tests. Expected count = 0 in both SCM and Recon.</li>
 * </ul>
 *
 * <p><b>Key design notes on EMPTY, MISSING, and EMPTY_MISSING:</b>
 * <ul>
 *   <li>A container is stored as {@code EMPTY_MISSING} in Recon when it is
 *       classified as {@code MISSING} by SCM's RM (no replicas → health=MISSING)
 *       AND the container is empty (no OM-tracked keys → numberOfKeys=0).
 *       SCM's RM emits {@code getStat(MISSING)} for such containers, while Recon
 *       refines this to {@code EMPTY_MISSING} in {@code handleMissingContainer()}.
 *   </li>
 *   <li><b>MISSING</b> path: CLOSED + 0 replicas + {@code numberOfKeys > 0} →
 *       {@code EmptyContainerHandler} case 3 does NOT fire (numberOfKeys≠0) →
 *       {@code RatisReplicationCheckHandler} fires → SCM: {@code MISSING},
 *       Recon: {@code MISSING}.</li>
 *   <li><b>EMPTY_MISSING</b> path: CLOSING + 0 replicas + {@code numberOfKeys == 0} →
 *       {@code ClosingContainerHandler} fires → SCM: {@code MISSING} (getStat(MISSING)++),
 *       Recon: {@code EMPTY_MISSING}. The container is simultaneously MISSING (no replicas,
 *       health=MISSING) and EMPTY (no keys, numberOfKeys=0).</li>
 *   <li><b>EMPTY (not EMPTY_MISSING)</b> path: CLOSED + 0 replicas +
 *       {@code numberOfKeys == 0} → {@code EmptyContainerHandler} case 3 fires
 *       <em>first</em> (CLOSED state, before {@code RatisReplicationCheckHandler}) →
 *       SCM: {@code EMPTY} (getStat(EMPTY)++). Even though this container also has 0
 *       replicas, the chain stops at EMPTY and never reaches MISSING classification.
 *       Recon also classifies it as EMPTY and does NOT store it in
 *       {@code UNHEALTHY_CONTAINERS}. This is the critical boundary.</li>
 * </ul>
 */
public class TestReconContainerHealthSummaryEndToEnd {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReconContainerHealthSummaryEndToEnd.class);

  // Timeouts
  private static final int PIPELINE_READY_TIMEOUT_MS = 30_000;
  private static final int POLL_INTERVAL_MS = 500;
  // Upper bound for waiting on replica ICRs to propagate after container creation.
  // RF3 Ratis containers require all 3 DataNodes to commit via Ratis consensus and
  // then each DN sends a separate ICR to Recon.  In slower CI environments this can
  // take longer than a simple RF1 allocation; 60 seconds gives enough headroom.
  private static final int REPLICA_SYNC_TIMEOUT_MS = 60_000;

  // Upper bound for UNHEALTHY_CONTAINERS query pagination (no paging needed for tests)
  private static final int MAX_RESULT = 100_000;

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private ReconService recon;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    // Use a 10-minute full container report (FCR) interval so that datanodes do
    // NOT send periodic full reports during the test (<3 min).  Incremental
    // container reports (ICRs) are still sent immediately on container creation,
    // which is what we rely on to populate replica state.  The long FCR window
    // prevents a removed replica from being re-added by a background DN report
    // before processAll() runs.
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "10m");
    conf.set(HDDS_PIPELINE_REPORT_INTERVAL, "1s");

    // Delay Recon's background SCM-sync schedulers well beyond any test duration
    // so they cannot interfere with the test's manual syncWithSCMContainerInfo()
    // calls.  Without this, the snapshot scheduler fires at ~1 minute (its default
    // initial delay), acquires the isSyncDataFromSCMRunning flag, and — before the
    // flag-leak fix — never releases it, causing all subsequent
    // syncWithSCMContainerInfo() calls to silently return false, leaving
    // containers absent from Recon and causing ContainerNotFoundException.
    conf.set(OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY, "1h");
    conf.set(OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INITIAL_DELAY, "1h");

    ReconTaskConfig taskConfig = conf.getObject(ReconTaskConfig.class);
    taskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));
    conf.setFromObject(taskConfig);

    // Keep SCM's remediation processors idle during tests so injected unhealthy
    // states are not healed before assertions run. 5 minutes is well beyond any
    // test's duration.
    conf.set("hdds.scm.replication.under.replicated.interval", "5m");
    conf.set("hdds.scm.replication.over.replicated.interval", "5m");

    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder().build())
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ONE, PIPELINE_READY_TIMEOUT_MS);
    cluster.waitForPipelineTobeReady(
        HddsProtos.ReplicationFactor.THREE, PIPELINE_READY_TIMEOUT_MS);

    // Wait until Recon's pipeline manager has synced from SCM so RF3 containers
    // can be allocated and reach Recon's replica bookkeeping.
    ReconStorageContainerManagerFacade reconScm = getReconScm();
    LambdaTestUtils.await(PIPELINE_READY_TIMEOUT_MS, POLL_INTERVAL_MS,
        () -> !reconScm.getPipelineManager().getPipelines().isEmpty());
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  // ---------------------------------------------------------------------------
  // Test 1 — Container State Summary
  // ---------------------------------------------------------------------------

  /**
   * Validates that per lifecycle-state container counts match exactly between
   * SCM and Recon for all four induciable lifecycle states.
   *
   * <p>After allocating containers in SCM and transitioning them to OPEN,
   * CLOSING, QUASI_CLOSED and CLOSED states, a full
   * {@code syncWithSCMContainerInfo()} is executed. The test then asserts:
   * <pre>
   *   scmCm.getContainers(state).size() == reconCm.getContainers(state).size()
   * </pre>
   * for every {@link HddsProtos.LifeCycleState} value.
   *
   * <p>Note on DELETING and DELETED: transitioning to these states requires
   * additional SCM-internal bookkeeping (block deletion flows) that goes
   * beyond direct ContainerManager API calls. These states are not induced
   * here but their expected count (0) is still validated.
   */
  @Test
  public void testContainerStateSummaryMatchesBetweenSCMAndRecon()
      throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManager scmCm = scm.getContainerManager();
    ReconStorageContainerManagerFacade reconScm = getReconScm();
    ReconContainerManager reconCm =
        (ReconContainerManager) reconScm.getContainerManager();

    // Allocate all containers as OPEN in SCM first. syncWithSCMContainerInfo()
    // (Pass 2) adds OPEN containers from SCM to Recon. We then transition each
    // group to its target state in BOTH SCM and Recon so the counts always match.
    //
    // CLOSING containers must follow this allocate-then-sync-then-FINALIZE pattern
    // because the four-pass sync does NOT cover the CLOSING lifecycle state — it
    // only syncs OPEN, CLOSED, and QUASI_CLOSED containers.

    // OPEN — 3 RF1 containers; no state transition needed.
    List<ContainerID> openIds = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      openIds.add(scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test").containerID());
    }

    // Allocate CLOSING, QUASI_CLOSED, and CLOSED candidates as OPEN in SCM.
    List<ContainerID> closingIds = new ArrayList<>();
    List<ContainerID> quasiClosedIds = new ArrayList<>();
    List<ContainerID> closedIds = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      closingIds.add(scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test").containerID());
    }
    for (int i = 0; i < 3; i++) {
      quasiClosedIds.add(scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test").containerID());
    }
    for (int i = 0; i < 3; i++) {
      closedIds.add(scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test").containerID());
    }

    // Sync Recon: Pass 2 adds all OPEN containers (all 12 allocated above) to Recon.
    // After this sync every container is in OPEN state in both SCM and Recon.
    syncAndWaitForReconContainers(reconScm, reconCm,
        combineContainerIds(openIds, closingIds, quasiClosedIds, closedIds));

    // Transition each group to its target state in BOTH SCM and Recon simultaneously.
    // CLOSING — FINALIZE: OPEN → CLOSING.
    for (ContainerID cid : closingIds) {
      scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
      reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
    }
    // QUASI_CLOSED — FINALIZE then QUASI_CLOSE.
    for (ContainerID cid : quasiClosedIds) {
      scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
      scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.QUASI_CLOSE);
      reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
      reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.QUASI_CLOSE);
    }
    // CLOSED — FINALIZE then CLOSE.
    for (ContainerID cid : closedIds) {
      scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
      scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.CLOSE);
      reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
      reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.CLOSE);
    }

    // Assert per-state counts match between SCM and Recon for every state.
    logStateSummaryHeader();
    Map<HddsProtos.LifeCycleState, Integer> mismatches =
        validateAndLogStateSummary(scmCm, reconCm);

    assertTrue(mismatches.isEmpty(),
        "Container State Summary counts diverge between SCM and Recon for states: "
            + mismatches);
  }

  // ---------------------------------------------------------------------------
  // Test 2 — Container Health Summary
  // ---------------------------------------------------------------------------

  /**
   * Validates that Container Health Summary counts match exactly between SCM's
   * {@link ReplicationManagerReport} and Recon's UNHEALTHY_CONTAINERS derby
   * table after both process the same injected container states.
   *
   * <p>The test also explicitly validates the lifecycle-state boundary that
   * determines when Recon emits {@code EMPTY_MISSING}: a container is stored
   * as {@code EMPTY_MISSING} when SCM's RM emits {@code getStat(MISSING)}
   * for it (no replicas → health=MISSING) AND the container has no keys
   * (numberOfKeys=0, the "EMPTY" property). The contrast group ({@code EMPTY_ONLY})
   * shows that CLOSED containers with the same 0-key+0-replica content are
   * classified as {@code EMPTY} by SCM — not {@code MISSING} — and are NOT
   * stored in Recon's {@code UNHEALTHY_CONTAINERS}.
   *
   * <p>Setup per health state:
   * <table border="1">
   *   <tr><th>State</th><th>RF</th><th>Lifecycle</th><th>Replicas</th><th>keys</th>
   *       <th>Expected in SCM (getStat)</th><th>Expected in Recon</th></tr>
   *   <tr><td>UNDER_REPLICATED</td><td>RF3</td><td>CLOSED</td><td>2</td><td>0</td>
   *       <td>UNDER_REPLICATED=2</td><td>UNDER_REPLICATED (count=2)</td></tr>
   *   <tr><td>OVER_REPLICATED</td><td>RF1</td><td>CLOSED</td><td>2 (phantom)</td><td>0</td>
   *       <td>OVER_REPLICATED=2</td><td>OVER_REPLICATED (count=2)</td></tr>
   *   <tr><td>MISSING</td><td>RF1</td><td>CLOSED</td><td>0</td><td>1</td>
   *       <td>MISSING=2</td><td>MISSING (count=2)</td></tr>
   *   <tr><td>EMPTY_MISSING</td><td>RF1</td><td>CLOSING</td><td>0</td><td>0</td>
   *       <td>MISSING=+2 (same stat as MISSING; total MISSING = missingIds+emptyMissingIds)</td>
   *       <td>EMPTY_MISSING (count=2)</td></tr>
   *   <tr><td>EMPTY (contrast)</td><td>RF1</td><td>CLOSED</td><td>0</td><td>0</td>
   *       <td>EMPTY=2 (EmptyContainerHandler case 3 fires, NOT MISSING)</td>
   *       <td>NOT stored (EMPTY not mapped to UNHEALTHY_CONTAINERS)</td></tr>
   *   <tr><td>MIS_REPLICATED</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>
   *       <td>0</td><td>0</td></tr>
   * </table>
   */
  @Test
  public void testContainerHealthSummaryMatchesBetweenSCMAndRecon()
      throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManager scmCm = scm.getContainerManager();
    ReconStorageContainerManagerFacade reconScm = getReconScm();
    ReconContainerManager reconCm =
        (ReconContainerManager) reconScm.getContainerManager();
    HealthSummarySetup setup =
        setupHealthSummaryScenario(scmCm, reconScm, reconCm, 2);

    // Run SCM RM (updates ContainerInfo.healthState on every container in SCM).
    // Remediation intervals are 5m so no commands will be dispatched to DNs.
    scm.getReplicationManager().processAll();
    ReplicationManagerReport scmReport =
        scm.getReplicationManager().getContainerReport();

    // Run Recon RM (writes to UNHEALTHY_CONTAINERS derby table).
    reconScm.getReplicationManager().processAll();
    ReconHealthRecords records = loadReconHealthRecords(reconCm);

    // Log Container Health Summary in the user-facing format.
    logHealthSummary(scmReport, records.underRep, records.overRep,
        records.missing, records.emptyMissing, records.misRep);
    assertHealthSummaryMatches(scmCm, scmReport, setup, records);
  }

  // ---------------------------------------------------------------------------
  // Test 3 — Comprehensive Summary Report (State Summary + Health Summary)
  // ---------------------------------------------------------------------------

  /**
   * Comprehensive end-to-end test that validates both Container State Summary
   * and Container Health Summary in a single scenario. After setup and both
   * RM runs, logs a formatted report matching the Container Summary Report
   * output format requested by the user.
   *
   * <p>Expected output pattern:
   * <pre>
   * Container Summary Report
   * ==========================================================
   *
   * Container State Summary (SCM vs Recon — counts must match)
   * =======================
   * OPEN:         SCM=N, Recon=N
   * CLOSING:      SCM=N, Recon=N
   * QUASI_CLOSED: SCM=N, Recon=N
   * CLOSED:       SCM=N, Recon=N
   * DELETING:     SCM=0, Recon=0
   * DELETED:      SCM=0, Recon=0
   * RECOVERING:   SCM=0, Recon=0
   *
   * Container Health Summary (SCM RM Report vs Recon UNHEALTHY_CONTAINERS)
   * ========================
   * HEALTHY:             SCM=N  (not stored in UNHEALTHY_CONTAINERS)
   * UNDER_REPLICATED:    SCM=N, Recon=N
   * MIS_REPLICATED:      SCM=0, Recon=0  (not induced — rack-aware topology required)
   * OVER_REPLICATED:     SCM=N, Recon=N
   * MISSING:             SCM=N, Recon MISSING=N + EMPTY_MISSING=N
   * ...
   * </pre>
   */
  @Test
  public void testComprehensiveSummaryReport() throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManager scmCm = scm.getContainerManager();
    ReconStorageContainerManagerFacade reconScm = getReconScm();
    ReconContainerManager reconCm =
        (ReconContainerManager) reconScm.getContainerManager();
    setupStateSummaryScenario(scmCm, reconScm, reconCm);
    HealthSummarySetup setup =
        setupHealthSummaryScenario(scmCm, reconScm, reconCm, 1);

    // Run both RMs.
    scm.getReplicationManager().processAll();
    ReplicationManagerReport scmReport =
        scm.getReplicationManager().getContainerReport();
    reconScm.getReplicationManager().processAll();
    ReconHealthRecords records = loadReconHealthRecords(reconCm);
    logContainerSummaryReport(scmCm, reconCm, scmReport, records);
    assertStateSummaryMatches(scmCm, reconCm);
    assertHealthSummaryMatches(scmCm, scmReport, setup, records);
  }

  private void setupStateSummaryScenario(
      ContainerManager scmCm,
      ReconStorageContainerManagerFacade reconScm,
      ReconContainerManager reconCm) throws Exception {
    List<ContainerID> closingStateCandidates = new ArrayList<>();
    List<ContainerID> quasiClosedStateCandidates = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      closingStateCandidates.add(scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test").containerID());
      quasiClosedStateCandidates.add(scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test").containerID());
    }
    syncAndWaitForReconContainers(reconScm, reconCm,
        combineContainerIds(closingStateCandidates, quasiClosedStateCandidates));
    for (ContainerID cid : closingStateCandidates) {
      scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
      reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
    }
    for (ContainerID cid : quasiClosedStateCandidates) {
      scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
      scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.QUASI_CLOSE);
      reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
      reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.QUASI_CLOSE);
    }
  }

  private HealthSummarySetup setupHealthSummaryScenario(
      ContainerManager scmCm,
      ReconStorageContainerManagerFacade reconScm,
      ReconContainerManager reconCm,
      int count) throws Exception {
    HealthSummarySetup setup = new HealthSummarySetup();
    setup.underReplicatedIds =
        setupUnderReplicatedContainers(scmCm, reconScm, reconCm, count);
    setup.overReplicatedIds =
        setupOverReplicatedContainers(scmCm, reconScm, reconCm, count);
    setup.missingIds =
        setupMissingContainers(scmCm, reconScm, reconCm, count);
    setup.emptyMissingIds =
        setupEmptyMissingContainers(scmCm, reconScm, reconCm, count);
    setup.emptyOnlyIds = setupEmptyOnlyContainers(scmCm, count);
    syncAndWaitForReconContainers(reconScm, reconCm, setup.emptyOnlyIds.stream()
        .map(ContainerID::valueOf)
        .collect(Collectors.toList()));
    return setup;
  }

  // ===========================================================================
  // Setup helpers
  // ===========================================================================

  /**
   * Creates RF3 CLOSED containers with exactly 2 of 3 required replicas injected
   * synthetically into both SCM and Recon. Both RMs will classify these as
   * {@code UNDER_REPLICATED}.
   *
   * <p>Containers are never created on actual datanodes — synthetic replicas are
   * injected directly into the in-memory replica metadata. This avoids the race
   * condition where the datanode (which holds the real container) re-reports its
   * replica within the 1-second container-report interval, re-adding the removed
   * replica before {@code processAll()} can classify the container as UNDER_REPLICATED.
   *
   * <p>Classification path:
   * <ol>
   *   <li>Container is CLOSED (FINALIZE + CLOSE) with 2 synthetic replicas (keyCount=1).</li>
   *   <li>{@code EmptyContainerHandler}: replicas not empty (keyCount=1) → does NOT fire.</li>
   *   <li>{@code RatisReplicationCheckHandler}: 2 replicas for RF3 → {@code UNDER_REPLICATED}.</li>
   * </ol>
   */
  private List<Long> setupUnderReplicatedContainers(
      ContainerManager scmCm,
      ReconStorageContainerManagerFacade reconScm,
      ReconContainerManager reconCm,
      int count) throws Exception {

    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ContainerInfo c = scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
          "test");
      createContainerOnPipeline(c);
      long cid = c.getContainerID();
      ContainerID containerID = ContainerID.valueOf(cid);
      ids.add(cid);

      syncAndWaitForReconContainers(reconScm, reconCm,
          Arrays.asList(containerID));

      // The explicit createContainerOnPipeline() above ensures the physical
      // container exists on the RF3 pipeline, so both SCM and Recon should
      // learn the initial 3 replicas via the normal create-time report path.
      LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
        try {
          return scmCm.getContainerReplicas(containerID).size() >= 3
              && reconCm.getContainerReplicas(containerID).size() >= 3;
        } catch (Exception e) {
          return false;
        }
      });
      drainScmAndReconEventQueues();

      // Transition the container to CLOSED in both SCM and Recon metadata.
      // ContainerManagerImpl.updateContainerState() does NOT dispatch CLOSE
      // commands to the DNs (those are dispatched by the ReplicationManager
      // and CloseContainerEventHandler, both of which are idle during tests
      // due to the 5m interval settings).  Therefore no further ICRs are
      // triggered by this metadata-only state change.
      closeInBoth(scmCm, reconCm, containerID);

      // Remove exactly 1 physical replica from a real DN and let heartbeat /
      // report processing update SCM and Recon through the normal path.
      ContainerReplica toRemove = scmCm.getContainerReplicas(containerID)
          .iterator().next();
      deleteContainerReplica(cluster, toRemove.getDatanodeDetails(), cid);
      LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
        try {
          return scmCm.getContainerReplicas(containerID).size() == 2
              && reconCm.getContainerReplicas(containerID).size() == 2;
        } catch (Exception e) {
          return false;
        }
      });
    }
    return ids;
  }

  /**
   * Creates RF1 CLOSED containers with 2 replicas in both SCM and Recon:
   * 1 real replica (registered via ICR when the DN creates the container) plus
   * 1 phantom replica injected on a different DN.
   * Both RMs will classify these as {@code OVER_REPLICATED}
   * (2 replicas for an RF1 container that expects only 1).
   *
   * <p>Classification path:
   * <ol>
   *   <li>Container is RF1, CLOSED.  1 DN has the container (real replica).
   *       A phantom replica is injected for a second DN that never had it.</li>
   *   <li>{@code EmptyContainerHandler}: replicas not empty → does NOT fire.</li>
   *   <li>{@code RatisReplicationCheckHandler}: 2 replicas for RF1 →
   *       {@code OVER_REPLICATED}.</li>
   * </ol>
   */
  private List<Long> setupOverReplicatedContainers(
      ContainerManager scmCm,
      ReconStorageContainerManagerFacade reconScm,
      ReconContainerManager reconCm,
      int count) throws Exception {

    List<DatanodeDetails> allDatanodes = cluster.getHddsDatanodes().stream()
        .map(HddsDatanodeService::getDatanodeDetails)
        .collect(Collectors.toList());

    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ContainerInfo c = scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test");
      createContainerOnPipeline(c);
      long cid = c.getContainerID();
      ContainerID containerID = ContainerID.valueOf(cid);
      ids.add(cid);

      syncAndWaitForReconContainers(reconScm, reconCm,
          Arrays.asList(containerID));

      LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
        try {
          return !scmCm.getContainerReplicas(containerID).isEmpty()
              && !reconCm.getContainerReplicas(containerID).isEmpty();
        } catch (Exception e) {
          return false;
        }
      });
      drainScmAndReconEventQueues();

      // Transition to CLOSED in both SCM and Recon metadata (no CLOSE command
      // dispatched to the DN; see UNDER_REPLICATED setup for the full rationale).
      closeInBoth(scmCm, reconCm, containerID);

      // Inject a phantom replica on a DN that does NOT already hold the container.
      // That DN will never send an ICR for this container (it doesn't have it),
      // so the phantom persists for the duration of the test.
      // With 10m FCR, the real DN won't send a full report that changes replica counts.
      // Result: 2 replicas for RF1 → OVER_REPLICATED.
      Set<UUID> existingUuids = scmCm.getContainerReplicas(containerID)
          .stream()
          .map(r -> r.getDatanodeDetails().getUuid())
          .collect(Collectors.toSet());
      DatanodeDetails phantomDN = allDatanodes.stream()
          .filter(d -> !existingUuids.contains(d.getUuid()))
          .findFirst()
          .orElseThrow(() -> new AssertionError(
              "No spare DN available to inject phantom replica for " + containerID));

      ContainerReplica phantom = ContainerReplica.newBuilder()
          .setContainerID(containerID)
          .setContainerState(ContainerReplicaProto.State.CLOSED)
          .setDatanodeDetails(phantomDN)
          .setKeyCount(1)
          .setBytesUsed(100)
          .setSequenceId(1)
          .build();
      scmCm.updateContainerReplica(containerID, phantom);
      reconCm.updateContainerReplica(containerID, phantom);
    }
    return ids;
  }

  /**
   * Creates RF1 CLOSED containers with 0 replicas and {@code numberOfKeys=1}.
   * Both SCM RM and Recon classify these as {@code MISSING}.
   *
   * <p>Containers are never created on actual datanodes, eliminating any
   * datanode-report race condition where a re-reporting datanode re-adds the
   * replica before {@code processAll()} runs.
   *
   * <p>Classification path:
   * <ol>
   *   <li>Container is CLOSED (FINALIZE + CLOSE) with 0 replicas and numberOfKeys=1.</li>
   *   <li>{@code EmptyContainerHandler} case 3 requires {@code numberOfKeys == 0} →
   *       does NOT fire (numberOfKeys=1).</li>
   *   <li>{@code RatisReplicationCheckHandler}: 0 replicas for RF1 → {@code MISSING}.</li>
   *   <li>Recon {@code handleMissingContainer()}: {@code numberOfKeys=1 > 0} →
   *       stored as {@code MISSING} (not EMPTY_MISSING).</li>
   * </ol>
   */
  /**
   * Creates RF1 CLOSED containers with 0 replicas and {@code numberOfKeys=1}.
   * Both SCM RM and Recon classify these as {@code MISSING}.
   *
   * <p>Classification path:
   * <ol>
   *   <li>Container is RF1, CLOSED, numberOfKeys=1, 0 replicas.</li>
   *   <li>{@code EmptyContainerHandler} case 3 requires {@code numberOfKeys == 0}
   *       → does NOT fire (numberOfKeys=1).</li>
   *   <li>{@code RatisReplicationCheckHandler}: 0 replicas for RF1 →
   *       {@code MISSING}.</li>
   *   <li>Recon {@code handleMissingContainer()}: {@code numberOfKeys=1 > 0}
   *       → stored as {@code MISSING} (not EMPTY_MISSING).</li>
   * </ol>
   */
  private List<Long> setupMissingContainers(
      ContainerManager scmCm,
      ReconStorageContainerManagerFacade reconScm,
      ReconContainerManager reconCm,
      int count) throws Exception {

    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ContainerInfo c = scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test");
      createContainerOnPipeline(c);
      long cid = c.getContainerID();
      ContainerID containerID = ContainerID.valueOf(cid);
      ids.add(cid);

      syncAndWaitForReconContainers(reconScm, reconCm,
          Arrays.asList(containerID));

      LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
        try {
          return !scmCm.getContainerReplicas(containerID).isEmpty()
              && !reconCm.getContainerReplicas(containerID).isEmpty();
        } catch (Exception e) {
          return false;
        }
      });
      drainScmAndReconEventQueues();

      // Transition to CLOSED in both SCM and Recon metadata.
      closeInBoth(scmCm, reconCm, containerID);

      // Set numberOfKeys=1 so EmptyContainerHandler case 3
      // (CLOSED + 0 keys + 0 replicas → EMPTY) does NOT fire.
      scmCm.getContainer(containerID).setNumberOfKeys(1);
      reconCm.getContainer(containerID).setNumberOfKeys(1);

      // Remove the single physical replica and wait for SCM / Recon to observe
      // the absence through the normal report path.
      ContainerReplica toRemove = scmCm.getContainerReplicas(containerID)
          .iterator().next();
      deleteContainerReplica(cluster, toRemove.getDatanodeDetails(), cid);
      LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
        try {
          return scmCm.getContainerReplicas(containerID).isEmpty()
              && reconCm.getContainerReplicas(containerID).isEmpty();
        } catch (Exception e) {
          return false;
        }
      });
    }
    return ids;
  }

  /**
   * Creates RF1 <b>CLOSING</b> containers with 0 replicas and {@code numberOfKeys=0}.
   * SCM RM classifies these as {@code MISSING}; Recon stores them as {@code EMPTY_MISSING}.
   *
   * <p>Containers are first allocated as OPEN in SCM, synced to Recon as OPEN
   * (Pass 2), then FINALIZED in both SCM and Recon simultaneously. This ensures
   * the CLOSING state is present in both systems without requiring datanode creation
   * (which would introduce datanode-report race conditions).
   *
   * <p>Classification path (the correct path for EMPTY_MISSING):
   * <ol>
   *   <li>Container is in CLOSING state (FINALIZE only, NOT CLOSE) with 0 replicas
   *       and numberOfKeys=0.</li>
   *   <li>{@code ClosingContainerHandler}: CLOSING state + 0 replicas →
   *       {@code report.incrementAndSample(MISSING)} → {@code MISSING} health state,
   *       chain stops.</li>
   *   <li>Recon {@code handleMissingContainer()}: {@code numberOfKeys=0} →
   *       {@code isEmptyMissing() = true} → stored as {@code EMPTY_MISSING}.</li>
   * </ol>
   *
   * <p><b>Why CLOSING (not CLOSED) is required:</b>
   * For a CLOSED container with {@code numberOfKeys=0} and 0 replicas,
   * {@code EmptyContainerHandler} case 3 fires first and classifies the container as
   * {@code EMPTY} — stopping the chain. Using CLOSING state bypasses this because
   * {@code EmptyContainerHandler} only handles CLOSED and QUASI_CLOSED containers.
   */
  private List<Long> setupEmptyMissingContainers(
      ContainerManager scmCm,
      ReconStorageContainerManagerFacade reconScm,
      ReconContainerManager reconCm,
      int count) throws Exception {

    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ContainerInfo c = scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test");
      ids.add(c.getContainerID());
    }

    // Sync adds OPEN containers from SCM to Recon (Pass 2). After this sync
    // every container exists in both SCM and Recon in OPEN state.
    syncAndWaitForReconContainers(reconScm, reconCm, ids.stream()
        .map(ContainerID::valueOf)
        .collect(Collectors.toList()));

    for (long cid : ids) {
      ContainerID containerID = ContainerID.valueOf(cid);

      // Transition OPEN → CLOSING in BOTH SCM and Recon simultaneously.
      // numberOfKeys stays 0 (default). 0 replicas (never on any datanode).
      scmCm.updateContainerState(containerID, HddsProtos.LifeCycleEvent.FINALIZE);
      reconCm.updateContainerState(containerID, HddsProtos.LifeCycleEvent.FINALIZE);
    }
    return ids;
  }

  /**
   * Creates RF1 <b>CLOSED</b> containers with 0 replicas and {@code numberOfKeys=0},
   * never created on any datanode. Serves as the contrast group to
   * {@code setupEmptyMissingContainers}: same content properties (0 keys + 0 replicas)
   * but CLOSED lifecycle state instead of CLOSING.
   *
   * <p>Classification path:
   * <ol>
   *   <li>Container is CLOSED (FINALIZE + CLOSE) with 0 replicas and numberOfKeys=0
   *       (default). The container was never created on any datanode.</li>
   *   <li>{@code EmptyContainerHandler} case 3: CLOSED + numberOfKeys==0 +
   *       replicas.isEmpty() → {@code report.incrementAndSample(EMPTY)} →
   *       {@code containerInfo.setHealthState(EMPTY)}, chain stops.</li>
   *   <li>The container WOULD be MISSING (0 replicas for RF1) if not for
   *       {@code EmptyContainerHandler} case 3 firing first for CLOSED containers.</li>
   *   <li>Recon: also classifies as EMPTY → {@code storeHealthStatesToDatabase()} skips
   *       EMPTY (not mapped to any {@code UnHealthyContainerStates}) → NOT stored in
   *       Recon's {@code UNHEALTHY_CONTAINERS} table.</li>
   * </ol>
   *
   * <p>After calling this method, the caller must invoke
   * {@code reconScm.syncWithSCMContainerInfo()} to make these containers visible to
   * Recon's container manager (Pass 1 of the sync discovers CLOSED containers in SCM
   * that are absent from Recon and adds them with their current replica set, which is
   * empty for these containers).
   */
  private List<Long> setupEmptyOnlyContainers(
      ContainerManager scmCm,
      int count) throws Exception {

    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ContainerInfo c = scmCm.allocateContainer(
          RatisReplicationConfig.getInstance(ONE), "test");
      long cid = c.getContainerID();
      ContainerID containerID = ContainerID.valueOf(cid);

      // Transition to CLOSED immediately without creating the container on any datanode.
      // The result is a CLOSED container with 0 replicas and numberOfKeys=0.
      scmCm.updateContainerState(containerID, HddsProtos.LifeCycleEvent.FINALIZE);
      scmCm.updateContainerState(containerID, HddsProtos.LifeCycleEvent.CLOSE);

      ids.add(cid);
    }
    return ids;
  }

  // ===========================================================================
  // Assertion helpers
  // ===========================================================================

  private void assertStateSummaryMatches(
      ContainerManager scmCm,
      ReconContainerManager reconCm) {
    logStateSummaryHeader();
    Map<HddsProtos.LifeCycleState, Integer> stateMismatches =
        validateAndLogStateSummary(scmCm, reconCm);
    assertTrue(stateMismatches.isEmpty(),
        "Container State Summary counts diverge between SCM and Recon: "
            + stateMismatches);
  }

  private void assertHealthSummaryMatches(
      ContainerManager scmCm,
      ReplicationManagerReport scmReport,
      HealthSummarySetup setup,
      ReconHealthRecords records) throws Exception {
    assertStateMatch(scmCm, setup.underReplicatedIds, records.underRep,
        ContainerHealthState.UNDER_REPLICATED, "UNDER_REPLICATED",
        "UNDER_REPLICATED count must match between SCM RM report and Recon "
            + "UNHEALTHY_CONTAINERS");
    assertStateMatch(scmCm, setup.overReplicatedIds, records.overRep,
        ContainerHealthState.OVER_REPLICATED, "OVER_REPLICATED",
        "OVER_REPLICATED count must match between SCM RM report and Recon "
            + "UNHEALTHY_CONTAINERS");
    assertStateMatch(scmCm, setup.missingIds, records.missing,
        ContainerHealthState.MISSING, "MISSING",
        "MISSING count must match between SCM RM report and Recon "
            + "UNHEALTHY_CONTAINERS");

    assertAllClassifiedBySCM(scmCm, setup.emptyOnlyIds, ContainerHealthState.EMPTY,
        "EMPTY");
    assertNoneInRecon(records.emptyMissing, setup.emptyOnlyIds,
        "CLOSED containers with 0 keys and 0 replicas must NOT be stored as "
            + "EMPTY_MISSING");
    assertEquals(setup.emptyOnlyIds.size(),
        countMatchingHealthState(scmCm, setup.emptyOnlyIds, ContainerHealthState.EMPTY),
        "SCM must classify every CLOSED + 0-key + 0-replica emptyOnly "
            + "container as EMPTY");

    assertAllClassifiedBySCM(scmCm, setup.emptyMissingIds,
        ContainerHealthState.MISSING,
        "MISSING (CLOSING + 0 replicas → SCM RM emits getStat(MISSING)++)");
    assertAllEmptyContent(scmCm, setup.emptyMissingIds);
    assertAllClassifiedByRecon(records.emptyMissing, setup.emptyMissingIds,
        "EMPTY_MISSING");
    assertEquals(setup.emptyMissingIds.size(),
        countMatchingReconRecords(records.emptyMissing, setup.emptyMissingIds),
        "EMPTY_MISSING: CLOSING containers that are both MISSING (no "
            + "replicas, getStat(MISSING)++ in SCM) and EMPTY "
            + "(numberOfKeys=0) must be stored as EMPTY_MISSING in Recon");
    assertEquals((long) (setup.missingIds.size() + setup.emptyMissingIds.size()),
        countMatchingHealthState(scmCm, setup.missingIds, ContainerHealthState.MISSING)
            + countMatchingHealthState(scmCm, setup.emptyMissingIds,
            ContainerHealthState.MISSING),
        "SCM getStat(MISSING) must equal the combined MISSING + "
            + "EMPTY_MISSING count");

    assertEquals(0L, scmReport.getStat(ContainerHealthState.MIS_REPLICATED),
        "MIS_REPLICATED SCM RM count should be 0 when not induced");
    assertEquals(0, records.misRep.size(),
        "MIS_REPLICATED Recon count should be 0 when not induced");
  }

  private void assertStateMatch(
      ContainerManager scmCm,
      List<Long> ids,
      List<UnhealthyContainerRecord> records,
      ContainerHealthState expected,
      String label,
      String message) throws Exception {
    assertAllClassifiedBySCM(scmCm, ids, expected, label);
    assertAllClassifiedByRecon(records, ids, label);
    assertEquals(countMatchingHealthState(scmCm, ids, expected),
        countMatchingReconRecords(records, ids), message);
  }

  /**
   * Asserts that every container ID in {@code ids} has the expected
   * {@link ContainerHealthState} set on SCM's {@link ContainerInfo} object
   * after SCM's {@code ReplicationManager.processAll()} has run.
   */
  private void assertAllClassifiedBySCM(
      ContainerManager scmCm,
      List<Long> ids,
      ContainerHealthState expected,
      String label) throws Exception {
    for (long id : ids) {
      ContainerInfo container = scmCm.getContainer(ContainerID.valueOf(id));
      // Recompute SCM health via the full RM handler chain in read-only mode
      // right before asserting, instead of relying on a previously cached
      // healthState value on ContainerInfo.
      cluster.getStorageContainerManager().getReplicationManager()
          .checkContainerStatus(container, new ReplicationManagerReport(MAX_RESULT));
      ContainerHealthState actual = container.getHealthState();
      assertEquals(expected, actual,
          String.format(
              "SCM must classify container %d as %s but got %s",
              id, label, actual));
    }
  }

  /**
   * Asserts that every container ID in {@code ids} is present in Recon's
   * UNHEALTHY_CONTAINERS records for the given health state label.
   */
  private void assertAllClassifiedByRecon(
      List<UnhealthyContainerRecord> records,
      List<Long> ids,
      String label) {
    for (long id : ids) {
      assertTrue(containsContainerId(records, id),
          String.format(
              "Recon UNHEALTHY_CONTAINERS must contain container %d in state %s",
              id, label));
    }
  }

  /**
   * Asserts that NONE of the container IDs in {@code ids} are present in the
   * given UNHEALTHY_CONTAINERS records list.
   *
   * <p>Used to verify that containers classified as {@code EMPTY} by SCM's RM
   * (e.g., CLOSED + 0 replicas + 0 keys) are NOT stored in Recon's
   * {@code UNHEALTHY_CONTAINERS} table under any health state.
   */
  private void assertNoneInRecon(
      List<UnhealthyContainerRecord> records,
      List<Long> ids,
      String message) {
    for (long id : ids) {
      assertFalse(containsContainerId(records, id),
          String.format("Container %d should not be in UNHEALTHY_CONTAINERS: %s",
              id, message));
    }
  }

  /**
   * Asserts that every container ID in {@code ids} has {@code numberOfKeys == 0}
   * in SCM's {@link ContainerInfo}, explicitly verifying the "EMPTY" content property.
   *
   * <p>Used alongside {@link #assertAllClassifiedBySCM} for EMPTY_MISSING containers
   * to confirm that both conditions for EMPTY_MISSING are present: the container is
   * MISSING (health=MISSING in SCM RM) AND EMPTY (numberOfKeys=0).
   */
  private void assertAllEmptyContent(
      ContainerManager scmCm,
      List<Long> ids) throws Exception {
    for (long id : ids) {
      long numKeys = scmCm.getContainer(ContainerID.valueOf(id)).getNumberOfKeys();
      assertEquals(0L, numKeys,
          String.format(
              "Container %d must have numberOfKeys=0 to qualify as EMPTY_MISSING "
                  + "(container is EMPTY in content and MISSING in replication)", id));
    }
  }

  // ===========================================================================
  // Validation and logging helpers
  // ===========================================================================

  /**
   * Validates that per lifecycle-state counts match between SCM and Recon,
   * logs the comparison, and returns a map of states where they differ.
   */
  private Map<HddsProtos.LifeCycleState, Integer> validateAndLogStateSummary(
      ContainerManager scmCm,
      ReconContainerManager reconCm) {
    return Arrays.stream(HddsProtos.LifeCycleState.values())
        .filter(state -> {
          int scmCount = scmCm.getContainers(state).size();
          int reconCount = reconCm.getContainers(state).size();
          LOG.info("{}: SCM={}, Recon={}",
              String.format("%-12s", state.name()), scmCount, reconCount);
          return scmCount != reconCount;
        })
        .collect(Collectors.toMap(
            state -> state,
            state -> scmCm.getContainers(state).size()
                - reconCm.getContainers(state).size()));
  }

  private void logStateSummaryHeader() {
    LOG.info("");
    LOG.info("Container State Summary (SCM vs Recon)");
    LOG.info("=======================================");
  }

  private void logHealthSummary(
      ReplicationManagerReport scmReport,
      List<UnhealthyContainerRecord> reconUnderRep,
      List<UnhealthyContainerRecord> reconOverRep,
      List<UnhealthyContainerRecord> reconMissing,
      List<UnhealthyContainerRecord> reconEmptyMissing,
      List<UnhealthyContainerRecord> reconMisRep) {
    LOG.info("");
    LOG.info("Container Health Summary (SCM RM Report vs Recon UNHEALTHY_CONTAINERS)");
    LOG.info("========================================================================");
    LOG.info("UNDER_REPLICATED: SCM={}, Recon={}",
        scmReport.getStat(ContainerHealthState.UNDER_REPLICATED),
        reconUnderRep.size());
    LOG.info("MIS_REPLICATED:   SCM={}, Recon={} [not induced]",
        scmReport.getStat(ContainerHealthState.MIS_REPLICATED),
        reconMisRep.size());
    LOG.info("OVER_REPLICATED:  SCM={}, Recon={}",
        scmReport.getStat(ContainerHealthState.OVER_REPLICATED),
        reconOverRep.size());
    LOG.info("MISSING:          SCM={}, Recon MISSING={} + EMPTY_MISSING={}",
        scmReport.getStat(ContainerHealthState.MISSING),
        reconMissing.size(), reconEmptyMissing.size());
  }

  private void logContainerSummaryReport(
      ContainerManager scmCm,
      ReconContainerManager reconCm,
      ReplicationManagerReport scmReport,
      ReconHealthRecords records) {
    LOG.info("");
    LOG.info("Container Summary Report");
    LOG.info("==========================================================");
    LOG.info("");
    LOG.info("Container State Summary (SCM vs Recon — counts must match)");
    LOG.info("=======================");
    for (HddsProtos.LifeCycleState state : HddsProtos.LifeCycleState.values()) {
      LOG.info("{}: SCM={}, Recon={}", String.format("%-12s", state.name()),
          scmCm.getContainers(state).size(), reconCm.getContainers(state).size());
    }

    LOG.info("");
    LOG.info("Container Health Summary (SCM RM Report vs Recon UNHEALTHY_CONTAINERS)");
    LOG.info("========================");
    LOG.info("HEALTHY:                             SCM={} (not stored in UNHEALTHY_CONTAINERS)",
        scmReport.getStat(ContainerHealthState.HEALTHY));
    LOG.info("UNDER_REPLICATED:                    SCM={}, Recon={}",
        scmReport.getStat(ContainerHealthState.UNDER_REPLICATED),
        records.underRep.size());
    LOG.info("MIS_REPLICATED:                      SCM={}, Recon={}"
            + " [not induced — rack-aware topology required]",
        scmReport.getStat(ContainerHealthState.MIS_REPLICATED),
        records.misRep.size());
    LOG.info("OVER_REPLICATED:                     SCM={}, Recon={}",
        scmReport.getStat(ContainerHealthState.OVER_REPLICATED),
        records.overRep.size());
    LOG.info("MISSING:                             SCM={}, Recon MISSING={},"
            + " Recon EMPTY_MISSING={} [SCM MISSING includes both MISSING + EMPTY_MISSING"
            + " containers; Recon differentiates via numberOfKeys]",
        scmReport.getStat(ContainerHealthState.MISSING),
        records.missing.size(), records.emptyMissing.size());
    LOG.info("UNHEALTHY:                           SCM={}",
        scmReport.getStat(ContainerHealthState.UNHEALTHY));
    LOG.info("EMPTY:                               SCM={}"
            + " [CLOSED+0-key+0-replica containers; EmptyContainerHandler fires first;"
            + " NOT stored in Recon UNHEALTHY_CONTAINERS — contrast to EMPTY_MISSING]",
        scmReport.getStat(ContainerHealthState.EMPTY));
    LOG.info("OPEN_UNHEALTHY:                      SCM={}",
        scmReport.getStat(ContainerHealthState.OPEN_UNHEALTHY));
    LOG.info("QUASI_CLOSED_STUCK:                  SCM={}",
        scmReport.getStat(ContainerHealthState.QUASI_CLOSED_STUCK));
    LOG.info("OPEN_WITHOUT_PIPELINE:               SCM={}",
        scmReport.getStat(ContainerHealthState.OPEN_WITHOUT_PIPELINE));
    LOG.info("UNHEALTHY_UNDER_REPLICATED:          SCM={}",
        scmReport.getStat(ContainerHealthState.UNHEALTHY_UNDER_REPLICATED));
    LOG.info("UNHEALTHY_OVER_REPLICATED:           SCM={}",
        scmReport.getStat(ContainerHealthState.UNHEALTHY_OVER_REPLICATED));
    LOG.info("MISSING_UNDER_REPLICATED:            SCM={}",
        scmReport.getStat(ContainerHealthState.MISSING_UNDER_REPLICATED));
    LOG.info("QUASI_CLOSED_STUCK_UNDER_REPLICATED: SCM={}",
        scmReport.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED));
    LOG.info("QUASI_CLOSED_STUCK_OVER_REPLICATED:  SCM={}",
        scmReport.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_OVER_REPLICATED));
    LOG.info("QUASI_CLOSED_STUCK_MISSING:          SCM={}",
        scmReport.getStat(ContainerHealthState.QUASI_CLOSED_STUCK_MISSING));
    LOG.info("NEGATIVE_SIZE:                       Recon={}"
            + " (Recon-only; no SCM RM equivalent)",
        records.negSize.size());
    LOG.info("REPLICA_MISMATCH:                    Recon={}"
            + " (Recon-only; no SCM RM equivalent)",
        records.replicaMismatch.size());
  }

  // ===========================================================================
  // Utility helpers
  // ===========================================================================

  private ReconHealthRecords loadReconHealthRecords(ReconContainerManager reconCm) {
    ContainerHealthSchemaManager healthMgr = reconCm.getContainerSchemaManager();
    ReconHealthRecords records = new ReconHealthRecords();
    records.underRep = queryUnhealthy(healthMgr,
        UnHealthyContainerStates.UNDER_REPLICATED);
    records.overRep = queryUnhealthy(healthMgr,
        UnHealthyContainerStates.OVER_REPLICATED);
    records.missing = queryUnhealthy(healthMgr,
        UnHealthyContainerStates.MISSING);
    records.emptyMissing = queryUnhealthy(healthMgr,
        UnHealthyContainerStates.EMPTY_MISSING);
    records.misRep = queryUnhealthy(healthMgr,
        UnHealthyContainerStates.MIS_REPLICATED);
    records.negSize = queryUnhealthy(healthMgr,
        UnHealthyContainerStates.NEGATIVE_SIZE);
    records.replicaMismatch = queryUnhealthy(healthMgr,
        UnHealthyContainerStates.REPLICA_MISMATCH);
    return records;
  }

  /**
   * Transitions a container to CLOSED state in both SCM and Recon by applying
   * FINALIZE (OPEN → CLOSING) then CLOSE (CLOSING → CLOSED) in both systems.
   * This is a metadata-only operation; no CLOSE command is dispatched to the
   * actual datanodes (those are dispatched by the ReplicationManager and
   * CloseContainerEventHandler, both idle during tests due to the 5m interval).
   */
  private void closeInBoth(ContainerManager scmCm, ReconContainerManager reconCm,
      ContainerID cid) throws Exception {
    scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
    scmCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.CLOSE);
    reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.FINALIZE);
    reconCm.updateContainerState(cid, HddsProtos.LifeCycleEvent.CLOSE);
  }

  private List<UnhealthyContainerRecord> queryUnhealthy(
      ContainerHealthSchemaManager healthMgr,
      UnHealthyContainerStates state) {
    return healthMgr.getUnhealthyContainers(state, 0L, 0L, MAX_RESULT);
  }

  private long countMatchingHealthState(
      ContainerManager scmCm,
      List<Long> ids,
      ContainerHealthState expected) throws Exception {
    long count = 0;
    for (long id : ids) {
      if (scmCm.getContainer(ContainerID.valueOf(id)).getHealthState() == expected) {
        count++;
      }
    }
    return count;
  }

  private long countMatchingReconRecords(
      List<UnhealthyContainerRecord> records,
      List<Long> ids) {
    return ids.stream()
        .filter(id -> containsContainerId(records, id))
        .count();
  }

  private boolean containsContainerId(
      List<UnhealthyContainerRecord> records, long containerId) {
    return records.stream().anyMatch(r -> r.getContainerId() == containerId);
  }

  private void syncAndWaitForReconContainers(
      ReconStorageContainerManagerFacade reconScm,
      ReconContainerManager reconCm,
      List<ContainerID> containerIDs) throws Exception {
    reconScm.syncWithSCMContainerInfo();
    drainScmAndReconEventQueues();
    backfillMissingContainersFromScm(reconCm, containerIDs);
    LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS,
        () -> containerIDs.stream().allMatch(reconCm::containerExist));
  }

  private void backfillMissingContainersFromScm(
      ReconContainerManager reconCm,
      List<ContainerID> containerIDs) throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManager scmCm = scm.getContainerManager();
    for (ContainerID containerID : containerIDs) {
      if (reconCm.containerExist(containerID)) {
        continue;
      }

      ContainerInfo scmInfo = scmCm.getContainer(containerID);
      ContainerInfo reconInfo = ContainerInfo.fromProtobuf(scmInfo.getProtobuf());
      Pipeline pipeline = null;
      if (scmInfo.getPipelineID() != null) {
        try {
          pipeline = scm.getPipelineManager().getPipeline(scmInfo.getPipelineID());
        } catch (PipelineNotFoundException ignored) {
          pipeline = null;
        }
      }
      reconCm.addNewContainer(new ContainerWithPipeline(reconInfo, pipeline));
    }
  }

  private void createContainerOnPipeline(ContainerInfo containerInfo)
      throws Exception {
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager()
        .getPipeline(containerInfo.getPipelineID());
    try (XceiverClientManager clientManager = new XceiverClientManager(conf)) {
      XceiverClientSpi client = clientManager.acquireClient(pipeline);
      try {
        ContainerProtocolCalls.createContainer(
            client, containerInfo.getContainerID(), null);
      } finally {
        clientManager.releaseClient(client, false);
      }
    }
  }

  private void deleteContainerReplica(
      MiniOzoneCluster ozoneCluster, DatanodeDetails dn, long containerId)
      throws Exception {
    OzoneContainer ozoneContainer =
        ozoneCluster.getHddsDatanode(dn).getDatanodeStateMachine().getContainer();
    Container<?> containerData =
        ozoneContainer.getContainerSet().getContainer(containerId);
    if (containerData != null) {
      ozoneContainer.getDispatcher().getHandler(KeyValueContainer)
          .deleteContainer(containerData, true);
    }
    ozoneCluster.getHddsDatanode(dn).getDatanodeStateMachine().triggerHeartbeat();
  }

  private void drainScmAndReconEventQueues() {
    ((EventQueue) cluster.getStorageContainerManager().getEventQueue())
        .processAll(5000L);
    getReconScm().getEventQueue().processAll(5000L);
  }

  @SafeVarargs
  private final List<ContainerID> combineContainerIds(List<ContainerID>... groups) {
    List<ContainerID> combined = new ArrayList<>();
    for (List<ContainerID> group : groups) {
      combined.addAll(group);
    }
    return combined;
  }

  private ReconStorageContainerManagerFacade getReconScm() {
    return (ReconStorageContainerManagerFacade)
        recon.getReconServer().getReconStorageContainerManager();
  }

  private static final class HealthSummarySetup {
    private List<Long> underReplicatedIds;
    private List<Long> overReplicatedIds;
    private List<Long> missingIds;
    private List<Long> emptyMissingIds;
    private List<Long> emptyOnlyIds;
  }

  private static final class ReconHealthRecords {
    private List<UnhealthyContainerRecord> underRep;
    private List<UnhealthyContainerRecord> overRep;
    private List<UnhealthyContainerRecord> missing;
    private List<UnhealthyContainerRecord> emptyMissing;
    private List<UnhealthyContainerRecord> misRep;
    private List<UnhealthyContainerRecord> negSize;
    private List<UnhealthyContainerRecord> replicaMismatch;
  }
}
