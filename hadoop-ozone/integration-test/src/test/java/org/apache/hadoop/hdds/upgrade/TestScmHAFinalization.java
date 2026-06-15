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

import static org.apache.hadoop.hdds.upgrade.TestHddsUpgradeUtils.waitForScmToFinalize;
import static org.apache.hadoop.hdds.upgrade.TestHddsUpgradeUtils.waitForScmsToFinalize;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.container.common.states.endpoint.RegisterEndpointTask;
import org.apache.hadoop.ozone.upgrade.RatisBasedVersionManager;
import org.apache.hadoop.util.ExitUtil;
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
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmHAFinalization.class);

  private StorageContainerLocationProtocol scmClient;
  private MiniOzoneHAClusterImpl cluster;
  private static final int NUM_DATANODES = 3;
  private static final int NUM_SCMS = 3;

  public void init(OzoneConfiguration conf, int numInactiveSCMs) throws Exception {

    SCMConfigurator configurator = new SCMConfigurator();

    conf.setInt(SCMStorageConfig.TESTING_INIT_APPARENT_VERSION_KEY, HDDSLayoutFeature.INITIAL_VERSION.serialize());
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
            .setApparentVersion(HDDSLayoutFeature.INITIAL_VERSION.serialize())
            .build());
    this.cluster = clusterBuilder.build();

    scmClient = cluster.getStorageContainerLocationClient();
    cluster.waitForClusterToBeReady();
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFinalizedDatanodesShutDownWithPrefinalizedScm() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(SCMStorageConfig.TESTING_INIT_APPARENT_VERSION_KEY, HDDSLayoutFeature.INITIAL_VERSION.serialize());
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SERVER_RPC_FIRST_ELECTION_TIMEOUT, "5s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL_DEFAULT, "1s");

    MiniOzoneHAClusterImpl.Builder clusterBuilder = MiniOzoneCluster.newHABuilder(conf);
    clusterBuilder.setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS)
        .setSCMServiceId("scmservice")
        .setNumOfOzoneManagers(1)
        .setNumDatanodes(NUM_DATANODES)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setApparentVersion(HDDSVersion.SOFTWARE_VERSION.serialize())
            .build());

    // Prevent terminateDatanode() from calling System.exit(1) and killing the test JVM.
    ExitUtil.disableSystemExit();
    LogCapturer logCapture = LogCapturer.captureLogs(RegisterEndpointTask.class);
    // This starts the mini ozone cluster.
    cluster = clusterBuilder.build();

    // isStopped cannot be set to true unless a datanode was started first.
    // Each datanode should be rejected since its apparent version exceeds the pre-finalized SCM's.
    GenericTestUtils.waitFor(
        () -> cluster.getHddsDatanodes().stream().allMatch(HddsDatanodeService::isStopped),
        500, 30_000);

    assertThat(logCapture.getOutput()).contains("SCM rejected this datanode's registration");
    for (StorageContainerManager scm : cluster.getStorageContainerManagersList()) {
      assertThat(scm.getScmNodeManager().getAllNodes()).isEmpty();
    }
  }

  @Test
  public void testFinalization() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    init(conf, 0);
    scmClient.finalizeUpgrade();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient);
    // Ensure all SCMs finalize, indicating the message has been propagated across them all
    waitForScmsToFinalize(cluster.getStorageContainerManagersList());

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), 0);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0);
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

    init(conf, numInactiveSCMs);

    LogCapturer logCapture = LogCapturer.captureLogs(RatisBasedVersionManager.class);

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
    scmClient.finalizeUpgrade();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient);
    // Wait for two running SCMs to finish finalization.
    waitForScmsToFinalize(activeScms);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        activeScms, 0);
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), 0);

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
    LOG.info("Waiting for restarted SCM to finalize");
    // When the leader sends a snapshot to the follower, it should have flushed all entries to the DB, including the
    // apparent versin. This means the follower should see it in the DB it receives immediately to trigger finalization.
    waitForScmToFinalize(inactiveScm, true);

    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        inactiveScm, 0);

    // Use log to verify a snapshot was installed.
    assertThat(logCapture.getOutput()).contains("New snapshot received with higher apparent version " +
        HDDSVersion.SOFTWARE_VERSION + ". Attempting to finalize to that version.");
  }
}
