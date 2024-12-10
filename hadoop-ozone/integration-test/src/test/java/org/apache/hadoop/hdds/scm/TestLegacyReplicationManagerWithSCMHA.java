/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMClientConfig;
import org.apache.hadoop.hdds.scm.proxy.SCMContainerLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.event.Level;


import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test LegacyReplicationManager with SCM HA setup.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestLegacyReplicationManagerWithSCMHA {
  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String omServiceId;
  private String scmServiceId;
  private int numOfOMs = 1;
  private int numOfSCMs = 3;

  private static final long SNAPSHOT_THRESHOLD = 5;

  /**
   * Create a MiniOzoneCluster for testing.
   *
   * @throws IOException
   */
  @BeforeAll
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    omServiceId = "om-service-test1";
    scmServiceId = "scm-service-test1";
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
        SNAPSHOT_THRESHOLD);
    conf.setBoolean("hdds.scm.replication.enable.legacy", true);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId).setNumOfOzoneManagers(numOfOMs)
        .setNumOfStorageContainerManagers(numOfSCMs).setNumOfActiveSCMs(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @Test
  public void testLegacyReplicationManagerNotifyStatusChanged() throws Exception {
    SCMClientConfig scmClientConfig =
        conf.getObject(SCMClientConfig.class);
    scmClientConfig.setRetryCount(1);
    scmClientConfig.setRetryInterval(100);
    scmClientConfig.setMaxRetryTimeout(1500);
    assertEquals(15, scmClientConfig.getRetryCount());
    conf.setFromObject(scmClientConfig);
    StorageContainerManager leaderScm = getLeader(cluster);
    assertNotNull(leaderScm);

    // Setup SCM client
    SCMContainerLocationFailoverProxyProvider proxyProvider =
        new SCMContainerLocationFailoverProxyProvider(conf, null);
    GenericTestUtils.setLogLevel(SCMContainerLocationFailoverProxyProvider.LOG,
        Level.DEBUG);
    GenericTestUtils.LogCapturer logCapture = GenericTestUtils.LogCapturer
        .captureLogs(SCMContainerLocationFailoverProxyProvider.LOG);
    proxyProvider.changeCurrentProxy(leaderScm.getSCMNodeId());
    StorageContainerLocationProtocol scmContainerClient =
        TracingUtil.createProxy(
            new StorageContainerLocationProtocolClientSideTranslatorPB(
                proxyProvider), StorageContainerLocationProtocol.class, conf);

    // Steps to test the fix for HDDS-11750 in LegacyReplicationManager#notifyStatusChanged
    // 1. Create a container with Replication.ONE having one replica in one DN that will be the source replica
    // 2. Pick another datanode as the target replica and use LegacyReplicationManager.MoveScheduler#startMove
    //    between the source and target replica
    // 3. Do a transfer leadership to another SCM to trigger SCMStateMachine#notifyLeaderReady,
    //    SCMServiceManager#notifyStatusChanged, and LegacyReplicationManager#notifyStatusChanged
    // 4. Check the RM log capturer that there is no TimeoutException due to the deadlock
    // 5. Create multiple containers and ensure that they are not blocked (no deadlock)
    //    - each create container must be finished faster than ozone.scm.ha.ratis.request.timeout
    //    - also keep checking whether there are TimeoutException exception

    // Used to capture the TimeoutException in ReplicationManager (Step 4)
    GenericTestUtils.LogCapturer rmLogCapture = GenericTestUtils.LogCapturer
        .captureLogs(LegacyReplicationManager.LOG);

    // Step 1
    ContainerWithPipeline container = scmContainerClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, "ozone");
    assertThat(logCapture.getOutput())
        .contains("Performing failover to suggested leader");
    leaderScm = getLeader(cluster);
    assertNotNull(leaderScm);
    assertThat(container.getPipeline().getNodes()).hasSize(1);

    // Step 2
    final ContainerID id = container.getContainerInfo().containerID();
    final DatanodeDetails sourceDN = container.getPipeline().getFirstNode();
    DatanodeDetails targetDN = null;
    for (HddsDatanodeService datanode: cluster.getHddsDatanodes()) {
      if (!datanode.getDatanodeDetails().equals(sourceDN)) {
        targetDN = datanode.getDatanodeDetails();
      }
    }
    assertNotNull(targetDN);

    leaderScm.getReplicationManager().getMoveScheduler().startMove(id.getProtobuf(),
        (new MoveDataNodePair(sourceDN, targetDN))
            .getProtobufMessage(ClientVersion.CURRENT_VERSION));

    // Step 3
    StorageContainerManager newLeaderScm = null;
    for (StorageContainerManager s : cluster.getStorageContainerManagers()) {
      if (s != leaderScm) {
        newLeaderScm = s;
        break;
      }
    }
    assertNotNull(newLeaderScm);

    // This should trigger the LegacyReplicationManager#notifyStatusChanged
    scmContainerClient.transferLeadership(newLeaderScm.getScmId());
    assertEquals(newLeaderScm, getLeader(cluster));

    // Step 4
    cluster.waitForClusterToBeReady();
    assertThat(rmLogCapture.getOutput()).doesNotContain("TimeoutException");
    assertThat(rmLogCapture.getOutput()).doesNotContain("Exception while moving container");

    // Step 5
    for (int i = 0; i < 5; i++) {
      scmContainerClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE, "ozone");
      assertThat(rmLogCapture.getOutput()).doesNotContain("TimeoutException");
      assertThat(rmLogCapture.getOutput()).doesNotContain("Exception while moving container");
    }
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static StorageContainerManager getLeader(MiniOzoneHAClusterImpl impl) {
    for (StorageContainerManager scm : impl.getStorageContainerManagers()) {
      if (scm.checkLeader()) {
        return scm;
      }
    }
    return null;
  }
}
