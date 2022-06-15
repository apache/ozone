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
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;

/**
 * Tests upgrade finalization failure scenarios and corner cases specific to SCM
 * HA.
 */
public class TestScmHAFinalization {
  private static final String CLIENT_ID = UUID.randomUUID().toString();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmHAFinalization.class);
  
  private StorageContainerLocationProtocol scmClient;
  // TODO: Will be required for HDDS-6761 testing leader changes and restarts.
  // Notifies the test that the halting point has been reached and
  // finalization is paused.
  private CountDownLatch pauseSignal;
  // Used to notify the finalization executor to resume finalization from the
  // halting point once test conditions have been set up during finalization.
  private CountDownLatch unpauseSignal;
  private MiniOzoneHAClusterImpl cluster;
  private static final int NUM_DATANODES = 3;
  private static final int NUM_SCMS = 3;
  private Future<?> finalizationFuture;

  public void init(UpgradeTestInjectionPoints haltingPoint) throws Exception {
    // Make upgrade finalization halt at the specified halting point until
    // the countdown latch is decremented. This allows triggering conditions
    // on the SCM during finalization.
    unpauseSignal = new CountDownLatch(1);
    pauseSignal = new CountDownLatch(1);
    InjectedUpgradeFinalizationExecutor<SCMUpgradeFinalizationContext>
        executor = new InjectedUpgradeFinalizationExecutor<>();
    executor.configureTestInjectionFunction(haltingPoint, () -> {
      LOG.info("Halting upgrade finalization at point: {}", haltingPoint);
      try {
        pauseSignal.countDown();
        unpauseSignal.await();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IOException("SCM test finalization interrupted.", ex);
      }
      LOG.info("Upgrade finalization resumed from point: {}", haltingPoint);
      return false;
    });

    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setUpgradeFinalizationExecutor(executor);

    // For testing snapshot install.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_ENABLED, true);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_PURGE_GAP, 5);
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
        5);

    MiniOzoneCluster.Builder clusterBuilder =
        new MiniOzoneHAClusterImpl.Builder(conf)
        .setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS - 1)
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
          } catch (Exception ex) {
            LOG.info("finalization client failed.", ex);
          }
        });

    pauseSignal.await();
    // finalization executor has now paused at the halting point, waiting for
    // the tests to pick up finalization.
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSnapshotFinalization() throws Exception {
    // TODO: Since this test currently runs with one SCM down from the start,
    //  it does not need to test pausing at different points.
    //  HDDS-6761 will need this behavior, however.
    init(UpgradeTestInjectionPoints.BEFORE_PRE_FINALIZE_UPGRADE);

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

    // Finalize the two active SCMs.
    // TODO: unpausing is not required for this test but will be required for
    //  HDDS-6761 testing leader changes and restarts.
    unpauseSignal.countDown();
    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalization(scmClient, CLIENT_ID);

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
    GenericTestUtils.waitFor(() -> !inactiveScm.isInSafeMode(), 500, 5000);
    GenericTestUtils.waitFor(() ->
        inactiveScm.getScmContext().isFinalizationCheckpointCrossed(
                FinalizationCheckpoint.FINALIZATION_COMPLETE),
        500, 10000);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        inactiveScm, 0, NUM_DATANODES);

    // Use log to verify a snapshot was installed.
    Assertions.assertTrue(logCapture.getOutput().contains("New SCM snapshot " +
        "received with higher layout version"));
  }
}
