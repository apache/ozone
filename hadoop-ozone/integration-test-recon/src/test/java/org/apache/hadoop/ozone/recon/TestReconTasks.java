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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer.createContainerForTesting;
import static org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer.runTestOzoneContainerViaDataNode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2.UnhealthyContainerRecordV2;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Integration tests for Recon's ContainerHealthTaskV2.
 *
 * <p>Covered unhealthy states (all states tracked in the UNHEALTHY_CONTAINERS
 * table except the dead {@code ALL_REPLICAS_BAD} state):</p>
 * <ul>
 *   <li>{@code UNDER_REPLICATED}: RF3 CLOSED container loses one replica (node down)
 *       → {@link #testContainerHealthTaskV2DetectsUnderReplicatedAfterNodeFailure}</li>
 *   <li>{@code EMPTY_MISSING}: RF1 OPEN container's only replica lost (node down,
 *       container has no OM-tracked keys) →
 *       {@link #testContainerHealthTaskV2DetectsEmptyMissingWhenAllReplicasLost}</li>
 *   <li>{@code MISSING}: RF1 CLOSED container with OM-tracked keys loses its only
 *       replica (metadata manipulation) →
 *       {@link #testContainerHealthTaskV2DetectsMissingForContainerWithKeys}</li>
 *   <li>{@code OVER_REPLICATED}: RF1 CLOSED container gains a phantom extra replica
 *       (metadata injection) →
 *       {@link #testContainerHealthTaskV2DetectsOverReplicatedAndNegativeSize}</li>
 *   <li>{@code NEGATIVE_SIZE}: Co-detected alongside {@code OVER_REPLICATED} when
 *       the container's {@code usedBytes} is negative →
 *       {@link #testContainerHealthTaskV2DetectsOverReplicatedAndNegativeSize}</li>
 *   <li>{@code REPLICA_MISMATCH}: RF3 CLOSED container where one replica reports a
 *       different data checksum (metadata injection) →
 *       {@link #testContainerHealthTaskV2DetectsReplicaMismatch}</li>
 * </ul>
 *
 * <p>States NOT covered:</p>
 * <ul>
 *   <li>{@code MIS_REPLICATED}: Requires a rack-aware placement policy with a specific
 *       multi-rack topology — not practical to set up in a mini-cluster integration
 *       test.</li>
 *   <li>{@code ALL_REPLICAS_BAD}: Defined in the schema enum but currently not
 *       populated by {@code ReconReplicationManager} — this is a dead code path.</li>
 * </ul>
 */
public class TestReconTasks {
  private static final int PIPELINE_READY_TIMEOUT_MS = 30000;
  // Dead-node fires in stale(3s)+dead(4s)=7s; 20s is ample headroom.
  private static final int STATE_TRANSITION_TIMEOUT_MS = 20000;
  // Container reports every 1s; 10s is ample to see all replicas.
  private static final int REPLICA_SYNC_TIMEOUT_MS = 10000;
  private static final int POLL_INTERVAL_MS = 500;

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private ReconService recon;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    // 1s reports keep replica-sync waits short (down from the 2s default).
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "1s");
    conf.set(HDDS_PIPELINE_REPORT_INTERVAL, "1s");

    ReconTaskConfig taskConfig = conf.getObject(ReconTaskConfig.class);
    taskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));
    conf.setFromObject(taskConfig);

    conf.set("ozone.scm.stale.node.interval", "3s");
    conf.set("ozone.scm.dead.node.interval", "4s");
    // Keep SCM's remediation processors slow so Recon can deterministically
    // observe unhealthy states before SCM heals them.
    conf.set("hdds.scm.replication.under.replicated.interval", "1m");
    conf.set("hdds.scm.replication.over.replicated.interval", "2m");
    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder().build())
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ONE, 30000);
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE, 30000);

    // Wait for Recon's pipeline manager to be populated from SCM. This is
    // separate from the cluster-level wait above (which only checks SCM).
    // Doing it here once avoids a redundant 30s inner wait inside each RF3 test.
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    LambdaTestUtils.await(PIPELINE_READY_TIMEOUT_MS, POLL_INTERVAL_MS,
        () -> !reconScm.getPipelineManager().getPipelines().isEmpty());

    GenericTestUtils.setLogLevel(SCMDatanodeHeartbeatDispatcher.class,
        Level.DEBUG);
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Verifies that {@code syncWithSCMContainerInfo()} pulls CLOSED containers
   * from SCM into Recon when they are not yet known to Recon.
   */
  @Test
  public void testSyncSCMContainerInfo() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManager scmContainerManager = scm.getContainerManager();
    ContainerManager reconCm = reconScm.getContainerManager();

    final ContainerInfo container1 = scmContainerManager.allocateContainer(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE), "admin");
    final ContainerInfo container2 = scmContainerManager.allocateContainer(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE), "admin");
    scmContainerManager.updateContainerState(container1.containerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    scmContainerManager.updateContainerState(container2.containerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    scmContainerManager.updateContainerState(container1.containerID(),
        HddsProtos.LifeCycleEvent.CLOSE);
    scmContainerManager.updateContainerState(container2.containerID(),
        HddsProtos.LifeCycleEvent.CLOSE);

    int scmContainersCount = scmContainerManager.getContainers().size();
    int reconContainersCount = reconCm.getContainers().size();
    assertNotEquals(scmContainersCount, reconContainersCount);
    reconScm.syncWithSCMContainerInfo();
    reconContainersCount = reconCm.getContainers().size();
    assertEquals(scmContainersCount, reconContainersCount);
  }

  /**
   * Verifies that ContainerHealthTaskV2 correctly detects {@code UNDER_REPLICATED}
   * when a CLOSED RF3 container loses one replica due to a node failure, and that the
   * state clears after the node recovers.
   *
   * <p>Key design constraint: the container MUST be in CLOSED state (not OPEN or CLOSING)
   * before the health scan runs. The check chain's {@code OpenContainerHandler} and
   * {@code ClosingContainerHandler} both return early (stopping the chain) for OPEN/CLOSING
   * containers without recording UNDER_REPLICATED. Only {@code RatisReplicationCheckHandler}
   * — reached only for CLOSED/QUASI_CLOSED containers — records UNDER_REPLICATED.</p>
   *
   * <p>Note on key counts: {@code isEmptyMissing()} uses
   * {@link ContainerInfo#getNumberOfKeys()} (SCM-tracked, OM-maintained). This is
   * distinct from Recon's metadata key count store. Since this container is created
   * via XceiverClient (bypassing OM), its SCM key count remains 0. This is intentional
   * for this test as the container has 2 replicas and will be UNDER_REPLICATED, not
   * MISSING/EMPTY_MISSING, regardless of key count.</p>
   */
  @Test
  public void testContainerHealthTaskV2DetectsUnderReplicatedAfterNodeFailure()
      throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();

    ContainerManager scmContainerManager = scm.getContainerManager();
    ReconContainerManager reconCm = (ReconContainerManager) reconScm.getContainerManager();

    ContainerInfo containerInfo = scmContainerManager.allocateContainer(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        "test");
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline = scmPipelineManager.getPipeline(containerInfo.getPipelineID());

    // Use XceiverClientRatis (not XceiverClientGrpc) for RF3 Ratis pipelines.
    // XceiverClientGrpc bypasses Ratis and writes to a randomly-selected single
    // node; the container then only exists on that one node. XceiverClientRatis
    // propagates the CreateContainer command through Raft consensus, so all 3
    // datanodes end up with the container — which is required for Recon to
    // accumulate 3 replicas.
    //
    // We deliberately do NOT call runTestOzoneContainerViaDataNode here because
    // that helper tests UpdateContainer (a deprecated standalone-only operation)
    // which is not supported via Ratis and throws StateMachineException. We only
    // need the container to exist on all 3 nodes; data content is irrelevant for
    // the health-state detection under test.
    XceiverClientRatis client = XceiverClientRatis.newXceiverClientRatis(pipeline, conf);
    client.connect();
    createContainerForTesting(client, containerID);

    // Wait for Recon to receive container reports from all 3 datanodes before
    // proceeding. Container reports are asynchronous (sent every 2s), so we
    // must confirm Recon has registered all replicas before closing the container
    // or shutting down a node.
    LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      try {
        return reconCm.getContainerReplicas(
            ContainerID.valueOf(containerID)).size() == 3;
      } catch (Exception e) {
        return false;
      }
    });
    assertEquals(scmContainerManager.getContainers(), reconCm.getContainers());

    // Close the container to CLOSED state in both SCM and Recon BEFORE node shutdown.
    //
    // Rationale: the health-check handler chain is gated by container lifecycle state:
    //   OpenContainerHandler   → stops chain for OPEN containers (returns true)
    //   ClosingContainerHandler → stops chain for CLOSING containers in readOnly mode
    //   RatisReplicationCheckHandler → only reached for CLOSED/QUASI_CLOSED containers;
    //                                  this is the ONLY handler that records UNDER_REPLICATED
    //
    // syncWithSCMContainerInfo() only discovers *new* CLOSED containers, not state
    // changes to already-known ones, so we apply the transition to both managers directly.
    scmContainerManager.updateContainerState(containerInfo.containerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    scmContainerManager.updateContainerState(containerInfo.containerID(),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconCm.updateContainerState(containerInfo.containerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconCm.updateContainerState(containerInfo.containerID(),
        HddsProtos.LifeCycleEvent.CLOSE);

    // Shut down one datanode. DeadNodeHandler will remove its replica from
    // Recon's container manager after the dead-node timeout (~4s).
    cluster.shutdownHddsDatanode(pipeline.getFirstNode());
    forceContainerHealthScan(reconScm);

    // Wait until the container appears as UNDER_REPLICATED (2 of 3 replicas present)
    // and is NOT classified as MISSING or EMPTY_MISSING.
    LambdaTestUtils.await(STATE_TRANSITION_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      forceContainerHealthScan(reconScm);
      List<UnhealthyContainerRecordV2> underReplicated =
          reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
              ContainerSchemaDefinition.UnHealthyContainerStates.UNDER_REPLICATED,
              0L, 0L, 1000);
      List<UnhealthyContainerRecordV2> missing =
          reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
              ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
              0L, 0L, 1000);
      List<UnhealthyContainerRecordV2> emptyMissing =
          reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
              ContainerSchemaDefinition.UnHealthyContainerStates.EMPTY_MISSING,
              0L, 0L, 1000);
      return containsContainerId(underReplicated, containerID)
          && !containsContainerId(missing, containerID)
          && !containsContainerId(emptyMissing, containerID);
    });

    // Restart the dead datanode and wait for UNDER_REPLICATED to clear.
    cluster.restartHddsDatanode(pipeline.getFirstNode(), true);
    forceContainerHealthScan(reconScm);
    LambdaTestUtils.await(STATE_TRANSITION_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      forceContainerHealthScan(reconScm);
      List<UnhealthyContainerRecordV2> underReplicated =
          reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
              ContainerSchemaDefinition.UnHealthyContainerStates.UNDER_REPLICATED,
              0L, 0L, 1000);
      return !containsContainerId(underReplicated, containerID);
    });

    // After recovery: our container must not appear in any unhealthy state.
    List<UnhealthyContainerRecordV2> missingAfterRecovery =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
            0L, 0L, 1000);
    assertFalse(containsContainerId(missingAfterRecovery, containerID),
        "Container should not be MISSING after node recovery");

    List<UnhealthyContainerRecordV2> emptyMissingAfterRecovery =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.EMPTY_MISSING,
            0L, 0L, 1000);
    assertFalse(containsContainerId(emptyMissingAfterRecovery, containerID),
        "Container should not be EMPTY_MISSING after node recovery");

    IOUtils.closeQuietly(client);
  }

  /**
   * Verifies that ContainerHealthTaskV2 correctly detects {@code EMPTY_MISSING}
   * (not {@code MISSING} or {@code UNDER_REPLICATED}) when a CLOSING RF1 container
   * loses its only replica due to a node failure, and the container has no
   * OM-tracked keys (i.e., {@link ContainerInfo#getNumberOfKeys()} == 0).
   *
   * <p>Classification logic: When a CLOSING container has zero replicas,
   * {@code ClosingContainerHandler} samples it as {@code MISSING}. Then
   * {@code handleMissingContainer()} calls {@code isEmptyMissing()} which checks
   * {@link ContainerInfo#getNumberOfKeys()}. Since the container was created via
   * XceiverClient bypassing Ozone Manager, SCM's key count is 0, so the container
   * is classified as {@code EMPTY_MISSING} rather than {@code MISSING}.</p>
   *
   * <p>Note: this test relies on the CLOSING-state path (not the CLOSED-state path),
   * so no explicit container close is needed before node shutdown. The dead-node
   * handler fires CLOSE_CONTAINER for the OPEN container, transitioning it to
   * CLOSING; then removes the lone replica; leaving a CLOSING container with 0
   * replicas for the health scan to classify as EMPTY_MISSING.</p>
   */
  @Test
  public void testContainerHealthTaskV2DetectsEmptyMissingWhenAllReplicasLost()
      throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    ReconContainerManager reconCm = (ReconContainerManager) reconScm.getContainerManager();

    ContainerInfo containerInfo = scm.getContainerManager()
        .allocateContainer(RatisReplicationConfig.getInstance(ONE), "test");
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline = scmPipelineManager.getPipeline(containerInfo.getPipelineID());

    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);

    // Wait for Recon to receive the container report from the single datanode.
    // This ensures DeadNodeHandler can find and remove the replica when the node dies.
    LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      try {
        return reconCm.getContainerReplicas(
            ContainerID.valueOf(containerID)).size() == 1;
      } catch (Exception e) {
        return false;
      }
    });

    cluster.shutdownHddsDatanode(pipeline.getFirstNode());
    forceContainerHealthScan(reconScm);

    // Wait until the container appears as EMPTY_MISSING (not MISSING or UNDER_REPLICATED).
    // EMPTY_MISSING means: 0 replicas AND 0 OM-tracked keys (no data loss risk).
    LambdaTestUtils.await(STATE_TRANSITION_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      forceContainerHealthScan(reconScm);
      List<UnhealthyContainerRecordV2> emptyMissing =
          reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
              ContainerSchemaDefinition.UnHealthyContainerStates.EMPTY_MISSING,
              0L, 0L, 1000);
      List<UnhealthyContainerRecordV2> missing =
          reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
              ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
              0L, 0L, 1000);
      List<UnhealthyContainerRecordV2> underReplicated =
          reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
              ContainerSchemaDefinition.UnHealthyContainerStates.UNDER_REPLICATED,
              0L, 0L, 1000);
      // EMPTY_MISSING must be set; MISSING and UNDER_REPLICATED must NOT be set
      // for this specific container (0 replicas → missing, not under-replicated;
      // 0 keys → empty-missing, not regular missing).
      return containsContainerId(emptyMissing, containerID)
          && !containsContainerId(missing, containerID)
          && !containsContainerId(underReplicated, containerID);
    });

    // Restart the node and wait for EMPTY_MISSING to clear.
    cluster.restartHddsDatanode(pipeline.getFirstNode(), true);
    forceContainerHealthScan(reconScm);
    LambdaTestUtils.await(STATE_TRANSITION_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      forceContainerHealthScan(reconScm);
      List<UnhealthyContainerRecordV2> emptyMissing =
          reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
              ContainerSchemaDefinition.UnHealthyContainerStates.EMPTY_MISSING,
              0L, 0L, 1000);
      return !containsContainerId(emptyMissing, containerID);
    });

    // After recovery: our container must not appear in any unhealthy state.
    List<UnhealthyContainerRecordV2> missingAfterRecovery =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
            0L, 0L, 1000);
    assertFalse(containsContainerId(missingAfterRecovery, containerID),
        "Container should not be MISSING after node recovery");

    List<UnhealthyContainerRecordV2> underReplicatedAfterRecovery =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.UNDER_REPLICATED,
            0L, 0L, 1000);
    assertFalse(containsContainerId(underReplicatedAfterRecovery, containerID),
        "Container should not be UNDER_REPLICATED after node recovery");

    IOUtils.closeQuietly(client);
  }

  /**
   * Verifies that ContainerHealthTaskV2 correctly detects {@code MISSING}
   * (distinct from {@code EMPTY_MISSING}) when a CLOSED RF1 container that has
   * OM-tracked keys loses its only replica.
   *
   * <p>Classification logic: {@code MISSING} is chosen over {@code EMPTY_MISSING}
   * when {@link ContainerInfo#getNumberOfKeys()} &gt; 0. In this test we directly
   * set {@code numberOfKeys = 1} on Recon's in-memory {@link ContainerInfo}
   * (bypassing the OM write path for test efficiency). Because
   * {@link ContainerInfo} is stored by reference in the in-memory state map,
   * this mutation is reflected in all subsequent health-check reads.</p>
   *
   * <p>The replica is removed from Recon's container manager directly (no node
   * death required), making the test fast and deterministic. Recovery is verified
   * by re-adding the replica and running another health scan.</p>
   */
  @Test
  public void testContainerHealthTaskV2DetectsMissingForContainerWithKeys()
      throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    ReconContainerManager reconCm =
        (ReconContainerManager) reconScm.getContainerManager();

    ContainerInfo containerInfo = scm.getContainerManager()
        .allocateContainer(RatisReplicationConfig.getInstance(ONE), "test");
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline = scmPipelineManager.getPipeline(containerInfo.getPipelineID());

    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);

    // Wait for Recon to register the single replica from the datanode.
    LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      try {
        return reconCm.getContainerReplicas(
            ContainerID.valueOf(containerID)).size() == 1;
      } catch (Exception e) {
        return false;
      }
    });

    // Close the container in both SCM and Recon so the health-check chain
    // reaches RatisReplicationCheckHandler (which detects MISSING for CLOSED
    // containers with 0 replicas).
    scm.getContainerManager().updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.FINALIZE);
    scm.getContainerManager().updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.CLOSE);
    reconCm.updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.FINALIZE);
    reconCm.updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.CLOSE);

    // Simulate OM-tracked key count by setting numberOfKeys = 1 on Recon's
    // in-memory ContainerInfo. getContainer() returns a direct reference to
    // the object stored in the ContainerStateMap, so this mutation is visible
    // to all subsequent reads (including handleMissingContainer's isEmptyMissing
    // check) without needing a DB flush.
    ContainerID cid = ContainerID.valueOf(containerID);
    reconCm.getContainer(cid).setNumberOfKeys(1);

    // Capture the single known replica and remove it from Recon's manager to
    // simulate total replica loss (no node death required).
    Set<ContainerReplica> replicas = reconCm.getContainerReplicas(cid);
    assertEquals(1, replicas.size(), "Expected exactly 1 replica before removal");
    ContainerReplica theReplica = replicas.iterator().next();
    reconCm.removeContainerReplica(cid, theReplica);

    // Health scan: CLOSED container, 0 replicas, numberOfKeys=1 → MISSING
    // (not EMPTY_MISSING because numberOfKeys > 0).
    forceContainerHealthScan(reconScm);

    List<UnhealthyContainerRecordV2> missing =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
            0L, 0L, 1000);
    List<UnhealthyContainerRecordV2> emptyMissing =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.EMPTY_MISSING,
            0L, 0L, 1000);

    assertTrue(containsContainerId(missing, containerID),
        "Container with keys should be MISSING when all replicas are gone");
    assertFalse(containsContainerId(emptyMissing, containerID),
        "Container with keys must NOT be classified as EMPTY_MISSING");

    // Recovery: re-add the replica; the next health scan should clear MISSING.
    reconCm.updateContainerReplica(cid, theReplica);
    forceContainerHealthScan(reconScm);

    List<UnhealthyContainerRecordV2> missingAfterRecovery =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.MISSING,
            0L, 0L, 1000);
    assertFalse(containsContainerId(missingAfterRecovery, containerID),
        "Container should no longer be MISSING after replica is restored");

    IOUtils.closeQuietly(client);
  }

  /**
   * Verifies that ContainerHealthTaskV2 correctly detects {@code OVER_REPLICATED}
   * when a CLOSED RF1 container has more replicas in Recon than its replication
   * factor, and simultaneously detects {@code NEGATIVE_SIZE} when the same
   * container has a negative {@code usedBytes} value.
   *
   * <p>Strategy: inject a phantom {@link ContainerReplica} from a second alive
   * datanode directly into Recon's container manager (Recon trusts its own replica
   * set without re-validating against the DN). This makes Recon believe 2 replicas
   * exist for an RF1 container → OVER_REPLICATED. Setting {@code usedBytes = -1}
   * on the same container's in-memory {@link ContainerInfo} triggers the
   * {@code NEGATIVE_SIZE} co-detection path (which fires inside
   * {@code handleReplicaStateContainer}).</p>
   *
   * <p>Recovery is verified by removing the phantom replica and resetting
   * {@code usedBytes} to a non-negative value.</p>
   */
  @Test
  public void testContainerHealthTaskV2DetectsOverReplicatedAndNegativeSize()
      throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    ReconContainerManager reconCm =
        (ReconContainerManager) reconScm.getContainerManager();

    ContainerInfo containerInfo = scm.getContainerManager()
        .allocateContainer(RatisReplicationConfig.getInstance(ONE), "test");
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline = scmPipelineManager.getPipeline(containerInfo.getPipelineID());

    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf);
    runTestOzoneContainerViaDataNode(containerID, client);

    // Wait for Recon to register the single replica.
    LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      try {
        return reconCm.getContainerReplicas(
            ContainerID.valueOf(containerID)).size() == 1;
      } catch (Exception e) {
        return false;
      }
    });

    // Close in SCM and Recon so RatisReplicationCheckHandler processes this
    // container and can detect OVER_REPLICATED.
    scm.getContainerManager().updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.FINALIZE);
    scm.getContainerManager().updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.CLOSE);
    reconCm.updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.FINALIZE);
    reconCm.updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.CLOSE);

    ContainerID cid = ContainerID.valueOf(containerID);

    // Find a datanode that is alive in the cluster but does NOT have this
    // RF1 container (any DN other than the pipeline's primary node).
    DatanodeDetails primaryDn = pipeline.getFirstNode();
    DatanodeDetails secondDn = cluster.getHddsDatanodes().stream()
        .map(HddsDatanodeService::getDatanodeDetails)
        .filter(dd -> !dd.getUuid().equals(primaryDn.getUuid()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No second datanode available"));

    // Inject a phantom replica from the second DN so Recon sees 2 replicas
    // for an RF1 container → OVER_REPLICATED.
    ContainerReplica phantomReplica = ContainerReplica.newBuilder()
        .setContainerID(cid)
        .setContainerState(ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(secondDn)
        .setKeyCount(0)
        .setBytesUsed(0)
        .setSequenceId(0)
        .build();
    reconCm.updateContainerReplica(cid, phantomReplica);

    // Set usedBytes = -1 on Recon's in-memory ContainerInfo to trigger
    // NEGATIVE_SIZE detection. This fires inside handleReplicaStateContainer
    // alongside the OVER_REPLICATED record.
    reconCm.getContainer(cid).setUsedBytes(-1L);

    forceContainerHealthScan(reconScm);

    List<UnhealthyContainerRecordV2> overReplicated =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.OVER_REPLICATED,
            0L, 0L, 1000);
    List<UnhealthyContainerRecordV2> negativeSize =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.NEGATIVE_SIZE,
            0L, 0L, 1000);

    assertTrue(containsContainerId(overReplicated, containerID),
        "RF1 container with 2 replicas should be OVER_REPLICATED");
    assertTrue(containsContainerId(negativeSize, containerID),
        "Container with usedBytes=-1 should be NEGATIVE_SIZE");

    // Recovery: remove the phantom replica and restore usedBytes to a valid value.
    reconCm.removeContainerReplica(cid, phantomReplica);
    reconCm.getContainer(cid).setUsedBytes(0L);
    forceContainerHealthScan(reconScm);

    List<UnhealthyContainerRecordV2> overReplicatedAfterRecovery =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.OVER_REPLICATED,
            0L, 0L, 1000);
    List<UnhealthyContainerRecordV2> negativeSizeAfterRecovery =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.NEGATIVE_SIZE,
            0L, 0L, 1000);

    assertFalse(containsContainerId(overReplicatedAfterRecovery, containerID),
        "Container should no longer be OVER_REPLICATED after phantom replica removed");
    assertFalse(containsContainerId(negativeSizeAfterRecovery, containerID),
        "Container should no longer be NEGATIVE_SIZE after usedBytes restored");

    IOUtils.closeQuietly(client);
  }

  /**
   * Verifies that ContainerHealthTaskV2 correctly detects {@code REPLICA_MISMATCH}
   * when replicas of a CLOSED RF3 container report different data checksums, and
   * that the state clears once the checksums are made uniform again.
   *
   * <p>Strategy: after writing data and closing the RF3 container, one replica's
   * checksum is replaced with a non-zero value via {@link ContainerReplica#toBuilder()}.
   * Because {@link ContainerReplica} is immutable,
   * {@code reconCm.updateContainerReplica()} replaces the existing replica entry
   * (keyed by {@code containerID + datanodeDetails}) with the modified copy.
   * This makes the set have distinct checksums (e.g. {0, 0, 12345}), which triggers
   * {@code hasDataChecksumMismatch()}'s {@code distinctChecksums > 1} check.</p>
   *
   * <p>Note: {@code REPLICA_MISMATCH} records are now properly cleaned up by
   * {@code batchDeleteSCMStatesForContainers} on each scan cycle (previously they
   * were excluded and could linger indefinitely after a mismatch was resolved).</p>
   */
  @Test
  public void testContainerHealthTaskV2DetectsReplicaMismatch() throws Exception {
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    ReconContainerManager reconCm =
        (ReconContainerManager) reconScm.getContainerManager();

    ContainerInfo containerInfo = scm.getContainerManager().allocateContainer(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        "test");
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline = scmPipelineManager.getPipeline(containerInfo.getPipelineID());

    // Use XceiverClientRatis so CreateContainer is committed through Raft
    // consensus and all 3 nodes end up with the container. We skip
    // runTestOzoneContainerViaDataNode because UpdateContainer (tested inside
    // that helper) is a deprecated standalone-only operation that throws
    // StateMachineException when routed through Ratis.
    XceiverClientRatis client = XceiverClientRatis.newXceiverClientRatis(pipeline, conf);
    client.connect();
    createContainerForTesting(client, containerID);

    // Wait for Recon to register replicas from all 3 datanodes.
    ContainerID cid = ContainerID.valueOf(containerID);
    LambdaTestUtils.await(REPLICA_SYNC_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      try {
        return reconCm.getContainerReplicas(cid).size() == 3;
      } catch (Exception e) {
        return false;
      }
    });

    // Close in SCM and Recon so RatisReplicationCheckHandler is reached and
    // REPLICA_MISMATCH (detected in the additional Recon-specific pass in
    // ReconReplicationManager.processAll) can be evaluated.
    scm.getContainerManager().updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.FINALIZE);
    scm.getContainerManager().updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.CLOSE);
    reconCm.updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.FINALIZE);
    reconCm.updateContainerState(
        containerInfo.containerID(), HddsProtos.LifeCycleEvent.CLOSE);

    // Inject a checksum mismatch: replace one replica with an identical copy
    // that has a non-zero dataChecksum. The other two replicas have checksum=0
    // (ContainerChecksums.unknown()), giving distinct checksums {0, 12345} and
    // triggering distinctChecksums > 1.
    Set<ContainerReplica> currentReplicas = reconCm.getContainerReplicas(cid);
    ContainerReplica originalReplica = currentReplicas.iterator().next();
    ContainerReplica mismatchedReplica = originalReplica.toBuilder()
        .setChecksums(ContainerChecksums.of(12345L))
        .build();
    reconCm.updateContainerReplica(cid, mismatchedReplica);

    forceContainerHealthScan(reconScm);

    List<UnhealthyContainerRecordV2> replicaMismatch =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.REPLICA_MISMATCH,
            0L, 0L, 1000);
    assertTrue(containsContainerId(replicaMismatch, containerID),
        "Container with differing replica checksums should be REPLICA_MISMATCH");

    // Recovery: restore the original replica (uniform checksums → no mismatch).
    reconCm.updateContainerReplica(cid, originalReplica);
    forceContainerHealthScan(reconScm);

    List<UnhealthyContainerRecordV2> replicaMismatchAfterRecovery =
        reconCm.getContainerSchemaManagerV2().getUnhealthyContainers(
            ContainerSchemaDefinition.UnHealthyContainerStates.REPLICA_MISMATCH,
            0L, 0L, 1000);
    assertFalse(containsContainerId(replicaMismatchAfterRecovery, containerID),
        "Container should no longer be REPLICA_MISMATCH after checksums are uniform");

    IOUtils.closeQuietly(client);
  }

  private void forceContainerHealthScan(
      ReconStorageContainerManagerFacade reconScm) {
    reconScm.getReplicationManager().processAll();
  }

  private boolean containsContainerId(
      List<UnhealthyContainerRecordV2> records, long containerId) {
    return records.stream().anyMatch(r -> r.getContainerId() == containerId);
  }
}
