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

package org.apache.hadoop.hdds.upgrade;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.upgrade.DefaultUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests upgrade finalization failure scenarios and corner cases specific to SCM
 * HA.
 */
public class TestScmHAFinalization {
  private static final String CLIENT_ID = UUID.randomUUID().toString();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmHAFinalization.class);

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

    conf.setInt(SCMStorageConfig.TESTING_INIT_LAYOUT_VERSION_KEY, HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SERVER_RPC_FIRST_ELECTION_TIMEOUT, "5s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL_DEFAULT, "1s");

    MiniOzoneHAClusterImpl.Builder clusterBuilder = MiniOzoneCluster.newHABuilder(conf);
    clusterBuilder.setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS - numInactiveSCMs)
        .setSCMServiceId("scmservice")
        .setNumOfOzoneManagers(1)
        .setSCMConfigurator(configurator)
        .setNumDatanodes(NUM_DATANODES)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setLayoutVersion(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion())
            .build());
    this.cluster = clusterBuilder.build();

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

  @Test
  public void testFinalizationWithLeaderChange() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    init(conf, new DefaultUpgradeFinalizationExecutor<>(), 0);
    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    // Ensure all SCMs finalize, indicating the message has been propagated across them all
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

    LogCapturer logCapture = LogCapturer.captureLogs(FinalizationStateManagerImpl.class);

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
    assertThat(logCapture.getOutput()).contains("New SCM snapshot " +
        "received with metadata layout version");
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
      LOG.info("Waiting for SCM {} (leader? {}) to finalize.", scm.getSCMNodeId(), scm.checkLeader());
      return !scm.getLayoutVersionManager().needsFinalization();
    }, 2_000, 60_000);
  }
}
