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

package org.apache.hadoop.hdds.scm.safemode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.BackgroundPipelineScrubber;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.MockClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests SCM Safe mode with pipeline rules.
 */

public class TestSCMSafeModeWithPipelineRules {

  private MiniOzoneCluster cluster;
  private PipelineManager pipelineManager;

  public void setup(int numDatanodes) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    configureTestCluster(conf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    pipelineManager = scm.getPipelineManager();
  }

  private static void configureTestCluster(OzoneConfiguration conf) {
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.set(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "10s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_DATANODE_DISALLOW_SAME_PEERS, true);
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
  }

  @Test
  void testScmSafeMode() throws Exception {
    int datanodeCount = 6;
    setup(datanodeCount);
    waitForRatis3NodePipelines(datanodeCount / 3);
    waitForRatis1NodePipelines(datanodeCount);

    int totalPipelineCount = datanodeCount + (datanodeCount / 3);

    //Cluster is started successfully
    cluster.stop();

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(false);

    pipelineManager = cluster.getStorageContainerManager().getPipelineManager();
    List<Pipeline> pipelineList =
        pipelineManager.getPipelines(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE));


    pipelineList.get(0).getNodes().forEach(datanodeDetails -> {
      try {
        cluster.restartHddsDatanode(datanodeDetails, false);
      } catch (Exception ex) {
        fail("Datanode restart failed");
      }
    });


    SCMSafeModeManager scmSafeModeManager =
        cluster.getStorageContainerManager().getScmSafeModeManager();

    // Ceil(0.1 * 2) is 1, as one pipeline is healthy the healthy pipeline
    // threshold is satisfied. The rule still does not pass while open
    // pipelines have datanodes which are not registered.
    final HealthyPipelineSafeModeRule healthyPipelineRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(HealthyPipelineSafeModeRule.class);
    GenericTestUtils.waitFor(() -> healthyPipelineRule.getCurrentHealthyPipelineCount()
        >= healthyPipelineRule.getHealthyPipelineThresholdCount(), 1000, 60000);
    assertFalse(healthyPipelineRule.validate());

    // As Ceil(0.9 * 2) is 2, and from second pipeline no datanodes's are
    // reported this rule is not met yet.
    final OneReplicaPipelineSafeModeRule oneReplicaPipelineRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(OneReplicaPipelineSafeModeRule.class);
    GenericTestUtils.waitFor(() -> !oneReplicaPipelineRule.validate(), 1000, 60000);

    assertTrue(cluster.getStorageContainerManager().isInSafeMode());

    DatanodeDetails restartedDatanode = pipelineList.get(1).getFirstNode();
    // Now restart one datanode from the 2nd pipeline
    cluster.restartHddsDatanode(restartedDatanode, false);

    GenericTestUtils.waitFor(oneReplicaPipelineRule::validate, 1000, 60000);

    // Two datanodes of the 2nd pipeline are still unregistered. Once the
    // container rules pass and the stale node interval elapses, the scrubber
    // closes the open pipelines containing them while SCM is in safe mode,
    // and then SCM exits safe mode. Skip ahead past the stale node interval
    // instead of waiting for it.
    BackgroundPipelineScrubber scrubber = ((PipelineManagerImpl) pipelineManager).getBackgroundPipelineScrubber();
    MockClock clock = new MockClock(Instant.now(), ZoneOffset.UTC);
    clock.fastForward(HddsServerUtil.getStaleNodeInterval(cluster.getConf()));
    scrubber.setClock(clock);
    GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(), 1000, 60000);

    // No open pipeline has unregistered datanodes at safe mode exit.
    NodeManager nodeManager = cluster.getStorageContainerManager().getScmNodeManager();
    for (Pipeline pipeline : pipelineManager.getPipelines()) {
      assertFalse(PipelineManagerImpl.isOpenWithUnregisteredNodes(pipeline, nodeManager),
          "Open pipeline " + pipeline.getId() + " has unregistered datanodes");
    }
    // All pipelines are still known to SCM. The scrubbed ones are closed.
    assertThat(pipelineManager.getPipelines().size()).isGreaterThanOrEqualTo(totalPipelineCount);

    // The 1 node pipelines of the two unregistered datanodes and the 2nd
    // 3 node pipeline are closed. The 4 registered datanodes cannot form
    // another 3 node pipeline with a pipeline limit of 1.
    waitForRatis1NodePipelines(datanodeCount - 2);
    waitForRatis3NodePipelines(1);

    ReplicationManager replicationManager =
        cluster.getStorageContainerManager().getReplicationManager();

    GenericTestUtils.waitFor(replicationManager::isRunning, 1000, 60000);
  }

  @Test
  void testSafeModeExitAfterScmRestartWithMixedEcAndRatisThreeKeys()
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    configureTestCluster(conf);
    conf.set(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
        HddsProtos.ReplicationType.EC.name());
    conf.set(OzoneConfigKeys.OZONE_REPLICATION, "rs-3-2-1024k");
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATE_RATIS_THREE,
        true);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    pipelineManager = cluster.getStorageContainerManager().getPipelineManager();
    waitForRatis3NodePipelines(1);

    try (OzoneClient client = cluster.newClient()) {
      OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client);
      DataTestUtil.createKey(bucket, "ec-key",
          new ECReplicationConfig("rs-3-2-1024k"),
          "ec-data".getBytes(UTF_8));
      DataTestUtil.createKey(bucket, "ratis3-key",
          RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
          "ratis-data".getBytes(UTF_8));
    }

    cluster.restartStorageContainerManager(false);
    SCMSafeModeManager scmSafeModeManager =
        cluster.getStorageContainerManager().getScmSafeModeManager();
    final ECMinDataNodeSafeModeRule ecRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(ECMinDataNodeSafeModeRule.class);

    assertTrue(ecRule.isEnabled());
    ecRule.setValidateBasedOnReportProcessing(false);
    GenericTestUtils.waitFor(ecRule::validate, 1000, 60000);
    GenericTestUtils.waitFor(() -> {
      scmSafeModeManager.refreshAndValidate();
      return !scmSafeModeManager.getInSafeMode();
    }, 1000, 60000);
  }

  @AfterEach
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void waitForRatis3NodePipelines(int numPipelines)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pipelineManager
        .getPipelines(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN)
        .size() == numPipelines, 100, 60000);
  }

  private void waitForRatis1NodePipelines(int numPipelines)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pipelineManager
        .getPipelines(RatisReplicationConfig.getInstance(ReplicationFactor.ONE),
            Pipeline.PipelineState.OPEN)
        .size() == numPipelines, 100, 60000);
  }
}
