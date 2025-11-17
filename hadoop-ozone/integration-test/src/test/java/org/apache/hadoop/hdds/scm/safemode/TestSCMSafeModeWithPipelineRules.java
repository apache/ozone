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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.GenericTestUtils;
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

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    pipelineManager = scm.getPipelineManager();
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

    // Ceil(0.1 * 2) is 1, as one pipeline is healthy pipeline rule is
    // satisfied

    final HealthyPipelineSafeModeRule healthyPipelineRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(HealthyPipelineSafeModeRule.class);
    GenericTestUtils.waitFor(healthyPipelineRule::validate, 1000, 60000);

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

    // All safeMode preChecks are now satisfied, SCM should be out of safe mode.

    GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(), 1000,
        60000);

    // As after safeMode wait time is not completed, we should have total
    // pipeline's as original count 6(1 node pipelines) + 2 (3 node pipeline)
    assertEquals(totalPipelineCount, pipelineManager.getPipelines().size());

    // The below check calls pipelineManager.getPipelines()
    // which is a call to the SCM to get the list of pipeline infos.
    // This is independent of DN reports or whether any number of DataNodes are
    // alive as the pipeline info is persisted to SCM upon creation and loaded
    // back upon restart.

    waitForRatis1NodePipelines(datanodeCount);
    waitForRatis3NodePipelines(datanodeCount / 3);

    ReplicationManager replicationManager =
        cluster.getStorageContainerManager().getReplicationManager();

    GenericTestUtils.waitFor(replicationManager::isRunning, 1000, 60000);
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
