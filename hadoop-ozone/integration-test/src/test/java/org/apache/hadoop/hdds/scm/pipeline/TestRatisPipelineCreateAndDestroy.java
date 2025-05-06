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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMService.Event;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for RatisPipelineUtils.
 */
public class TestRatisPipelineCreateAndDestroy {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private PipelineManager pipelineManager;

  public void init(int numDatanodes) throws Exception {
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 2);
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, numDatanodes + numDatanodes / 3);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 2000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, 500,
        TimeUnit.MILLISECONDS);
    cluster = MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(numDatanodes)
            .build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    pipelineManager = scm.getPipelineManager();
  }

  @AfterEach
  public void cleanup() {
    cluster.shutdown();
  }

  @Test
  public void testAutomaticPipelineCreationOnPipelineDestroy()
      throws Exception {
    int numOfDatanodes = 6;
    init(numOfDatanodes);
    // make sure two pipelines are created
    waitForPipelines(2);
    assertEquals(numOfDatanodes, pipelineManager.getPipelines(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.ONE)).size());

    List<Pipeline> pipelines = pipelineManager
        .getPipelines(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), Pipeline.PipelineState.OPEN);
    for (Pipeline pipeline : pipelines) {
      pipelineManager.closePipeline(pipeline.getId());
    }
    // make sure two pipelines are created
    waitForPipelines(2);
  }

  @Test
  public void testAutomaticPipelineCreationDisablingFactorONE()
      throws Exception {
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    init(6);
    // make sure two pipelines are created
    waitForPipelines(2);
    // No Factor ONE pipeline is auto created.
    assertEquals(0, pipelineManager.getPipelines(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.ONE)).size());

    List<Pipeline> pipelines = pipelineManager
        .getPipelines(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), Pipeline.PipelineState.OPEN);
    for (Pipeline pipeline : pipelines) {
      pipelineManager.closePipeline(pipeline.getId());
    }

    // make sure two pipelines are created
    waitForPipelines(2);
  }

  @Test
  public void testPipelineCreationOnNodeRestart() throws Exception {
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        5, TimeUnit.SECONDS);
    init(3);
    // make sure a pipelines is created
    waitForPipelines(1);
    List<HddsDatanodeService> dns = new ArrayList<>(cluster.getHddsDatanodes());

    List<Pipeline> pipelines =
        pipelineManager.getPipelines(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE));
    for (HddsDatanodeService dn : dns) {
      cluster.shutdownHddsDatanode(dn.getDatanodeDetails());
    }

    GenericTestUtils.waitFor(() ->
                    cluster.getStorageContainerManager().getScmNodeManager()
                            .getNodeCount(NodeStatus.inServiceHealthy()) == 0,
                    100, 10 * 1000);

    // try creating another pipeline now
    SCMException ioe = assertThrows(SCMException.class, () ->
        pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE)),
        "pipeline creation should fail after shutting down pipeline");
    assertEquals(SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE, ioe.getResult());

    // make sure pipelines is destroyed
    waitForPipelines(0);
    for (HddsDatanodeService dn : dns) {
      cluster.restartHddsDatanode(dn.getDatanodeDetails(), false);
    }

    // destroy the existing pipelines
    for (Pipeline pipeline : pipelines) {
      pipelineManager.closePipeline(pipeline.getId());
    }

    if (cluster.getStorageContainerManager()
        .getScmNodeManager().getNodeCount(NodeStatus.inServiceHealthy()) >=
        HddsProtos.ReplicationFactor.THREE.getNumber()) {
      // make sure pipelines is created after node start
      cluster.getStorageContainerManager()
          .getSCMServiceManager()
          .notifyEventTriggered(Event.PRE_CHECK_COMPLETED);
      waitForPipelines(1);
    }
  }

  private void waitForPipelines(int numPipelines)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pipelineManager
        .getPipelines(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), Pipeline.PipelineState.OPEN)
        .size() >= numPipelines, 100, 60000);
  }
}
