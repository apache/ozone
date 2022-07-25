/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.upgrade;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.upgrade.DefaultUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;

/**
 * Tests upgrade finalization failure scenarios and corner cases specific to SCM
 * HA.
 */
public class TestScmHAFinalization {
  private static final String CLIENT_ID = UUID.randomUUID().toString();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmHAFinalization.class);
  private static final String METHOD_SOURCE =
      "org.apache.hadoop.hdds.upgrade" +
          ".TestScmHAFinalization#injectionPointsToTest";

  private StorageContainerLocationProtocol scmClient;
  private MiniOzoneHAClusterImpl cluster;
  private static final int NUM_DATANODES = 3;
  private static final int NUM_SCMS = 3;
  private Future<?> finalizationFuture;

  public void init(OzoneConfiguration conf,
      UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor,
      int numInactiveSCMs) throws Exception {

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setUpgradeFinalizationExecutor(executor);

    MiniOzoneCluster.Builder clusterBuilder =
        new MiniOzoneHAClusterImpl.Builder(conf)
        .setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS - numInactiveSCMs)
        .setScmLayoutVersion(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion())
        .setSCMServiceId("scmservice")
        .setSCMConfigurator(configurator)
        .setNumOfOzoneManagers(1)
        .setNumDatanodes(NUM_DATANODES)
        .setDnLayoutVersion(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    this.cluster = (MiniOzoneHAClusterImpl) clusterBuilder.build();

    scmClient = cluster.getStorageContainerLocationClient();
    cluster.waitForClusterToBeReady();

    // Launch finalization from the client. In the current implementation,
    // this call will block until finalization completes. If the test
    // involves restarts or leader changes the client may be disconnected,
    // but finalization should still proceed.
    finalizationFuture = Executors.newSingleThreadExecutor().submit(
        () -> {
          try {
            scmClient.finalizeScmUpgrade(CLIENT_ID);
          } catch (IOException ex) {
            LOG.info("finalization client failed. This may be expected if the" +
                " test injected failures.", ex);
          }
        });
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Argument supplier for parameterized tests.
   */
  public static Stream<Arguments> injectionPointsToTest() {
    // Do not test from BEFORE_PRE_FINALIZE_UPGRADE injection point.
    // Finalization will not have started so there will be no persisted state
    // to resume from.
    return Stream.of(
        Arguments.of(UpgradeTestInjectionPoints.AFTER_PRE_FINALIZE_UPGRADE),
        Arguments.of(UpgradeTestInjectionPoints.AFTER_COMPLETE_FINALIZATION),
        Arguments.of(UpgradeTestInjectionPoints.AFTER_POST_FINALIZE_UPGRADE)
    );
  }

  @ParameterizedTest
  @MethodSource(METHOD_SOURCE)
  public void testFinalizationWithLeaderChange(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {

    CountDownLatch pauseLatch = new CountDownLatch(1);
    CountDownLatch unpauseLatch = new CountDownLatch(1);
    init(new OzoneConfiguration(),
        UpgradeTestUtils.newPausingFinalizationExecutor(haltingPoint,
            pauseLatch, unpauseLatch, LOG), 0);
    pauseLatch.await();

    // Stop the leader, forcing a leader change in the middle of finalization.
    // This will cause the initial client call for finalization
    // to be interrupted.
    StorageContainerManager oldLeaderScm = cluster.getActiveSCM();
    LOG.info("Stopping current SCM leader {} to initiate a leader change.",
        oldLeaderScm.getSCMNodeId());
    cluster.shutdownStorageContainerManager(oldLeaderScm);

    // While finalization is paused, check its state on the remaining SCMs.
    checkMidFinalizationConditions(haltingPoint,
        cluster.getStorageContainerManagersList());

    // Wait for the remaining two SCMs to elect a new leader.
    cluster.waitForClusterToBeReady();

    // Restart actually creates a new SCM.
    // Since this SCM will be a follower, the implementation of its upgrade
    // finalization executor does not matter for this test.
    cluster.restartStorageContainerManager(oldLeaderScm, true);

    // Make sure the original SCM leader is not the leader anymore.
    StorageContainerManager newLeaderScm  = cluster.getActiveSCM();
    Assertions.assertNotEquals(newLeaderScm.getSCMNodeId(),
        oldLeaderScm.getSCMNodeId());

    // Resume finalization from the new leader.
    unpauseLatch.countDown();

    // Client should complete exceptionally since the original SCM it
    // requested to was restarted.
    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    // Make sure old leader has caught up and all SCMs have finalized.
    waitForScmsToFinalize(cluster.getStorageContainerManagersList());

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);
  }

  @ParameterizedTest
  @MethodSource(METHOD_SOURCE)
  public void testFinalizationWithRestart(
      UpgradeTestInjectionPoints haltingPoint) throws Exception {
    CountDownLatch terminateLatch = new CountDownLatch(1);
    init(new OzoneConfiguration(),
        UpgradeTestUtils.newTerminatingFinalizationExecutor(haltingPoint,
            terminateLatch, LOG),
        0);
    terminateLatch.await();

    // Once upgrade finalization is stopped at the halting point, restart all
    // SCMs.
    LOG.info("Restarting all SCMs during upgrade finalization.");
    // Restarting an SCM from mini ozone actually replaces the SCM with a new
    // instance. We will use the normal upgrade finalization executor for
    // these new instances, since the last one aborted at the halting point.
    cluster.getSCMConfigurator()
        .setUpgradeFinalizationExecutor(
            new DefaultUpgradeFinalizationExecutor<>());
    List<StorageContainerManager> originalSCMs =
        cluster.getStorageContainerManagers();

    for (StorageContainerManager scm: originalSCMs) {
      cluster.restartStorageContainerManager(scm, false);
    }

    checkMidFinalizationConditions(haltingPoint,
        cluster.getStorageContainerManagersList());

    // After all SCMs were restarted, finalization should resume
    // automatically once a leader is elected.
    cluster.waitForClusterToBeReady();

    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    // Once the leader tells the client finalization is complete, wait for all
    // followers to catch up so we can check their state.
    waitForScmsToFinalize(cluster.getStorageContainerManagersList());

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);
  }

  @Test
  public void testSnapshotFinalization() throws Exception {
    int numInactiveSCMs = 1;
    // Require snapshot installation after only a few transactions.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_ENABLED, true);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_GAP, 5);
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
        5);

    init(conf, new DefaultUpgradeFinalizationExecutor<>(), numInactiveSCMs);

    GenericTestUtils.LogCapturer logCapture = GenericTestUtils.LogCapturer
        .captureLogs(FinalizationStateManagerImpl.LOG);

    StorageContainerManager inactiveScm = cluster.getInactiveSCM().next();
    LOG.info("Inactive SCM node ID: {}", inactiveScm.getSCMNodeId());

    List<StorageContainerManager> scms =
        cluster.getStorageContainerManagersList();
    List<StorageContainerManager> activeScms = new ArrayList<>();
    for (StorageContainerManager scm : scms) {
      if (!scm.getSCMNodeId().equals(inactiveScm.getSCMNodeId())) {
        activeScms.add(scm);
      }
    }

    // Wait for finalization from the client perspective.
    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    // Wait for two running SCMs to finish finalization.
    waitForScmsToFinalize(activeScms);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        activeScms, 0, NUM_DATANODES);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0, CLOSED);

    // Move SCM log index farther ahead to make sure a snapshot install
    // happens on the restarted SCM.
    for (int i = 0; i < 10; i++) {
      ContainerWithPipeline container =
          scmClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE, "owner");
      scmClient.closeContainer(
          container.getContainerInfo().getContainerID());
    }

    cluster.startInactiveSCM(inactiveScm.getSCMNodeId());
    waitForScmToFinalize(inactiveScm);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        inactiveScm, 0, NUM_DATANODES);

    // Use log to verify a snapshot was installed.
    Assertions.assertTrue(logCapture.getOutput().contains("New SCM snapshot " +
        "received with metadata layout version"));
  }

  private void waitForScmsToFinalize(Collection<StorageContainerManager> scms)
      throws Exception {
    for (StorageContainerManager scm: scms) {
      waitForScmToFinalize(scm);
    }
  }

  private void waitForScmToFinalize(StorageContainerManager scm)
      throws Exception {
    GenericTestUtils.waitFor(() -> !scm.isInSafeMode(), 500, 5000);
    GenericTestUtils.waitFor(() -> {
      FinalizationCheckpoint checkpoint =
          scm.getScmContext().getFinalizationCheckpoint();
      LOG.info("Waiting for SCM {} (leader? {}) to finalize. Current " +
          "finalization checkpoint is {}",
          scm.getSCMNodeId(), scm.checkLeader(), checkpoint);
      return checkpoint.hasCrossed(
          FinalizationCheckpoint.FINALIZATION_COMPLETE);
    }, 2_000, 60_000);
  }

  private void checkMidFinalizationConditions(
      UpgradeTestInjectionPoints haltingPoint,
      List<StorageContainerManager> scms) {
    for (StorageContainerManager scm: scms) {
      switch (haltingPoint) {
      case BEFORE_PRE_FINALIZE_UPGRADE:
        Assertions.assertFalse(
            scm.getPipelineManager().isPipelineCreationFrozen());
        Assertions.assertEquals(
            scm.getScmContext().getFinalizationCheckpoint(),
            FinalizationCheckpoint.FINALIZATION_REQUIRED);
        break;
      case AFTER_PRE_FINALIZE_UPGRADE:
        Assertions.assertTrue(
            scm.getPipelineManager().isPipelineCreationFrozen());
        Assertions.assertEquals(
            scm.getScmContext().getFinalizationCheckpoint(),
            FinalizationCheckpoint.FINALIZATION_STARTED);
        break;
      case AFTER_COMPLETE_FINALIZATION:
        Assertions.assertFalse(
            scm.getPipelineManager().isPipelineCreationFrozen());
        Assertions.assertEquals(
            scm.getScmContext().getFinalizationCheckpoint(),
            FinalizationCheckpoint.MLV_EQUALS_SLV);
        break;
      case AFTER_POST_FINALIZE_UPGRADE:
        Assertions.assertFalse(
            scm.getPipelineManager().isPipelineCreationFrozen());
        Assertions.assertEquals(
            scm.getScmContext().getFinalizationCheckpoint(),
            FinalizationCheckpoint.FINALIZATION_COMPLETE);
        break;
      default:
        Assertions.fail("Unknown halting point in test: " + haltingPoint);
      }
    }
  }
}
