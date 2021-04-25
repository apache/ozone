/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMService.Event;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests for RatisPipelineUtils.
 */
@Ignore
public class TestRatisPipelineCreateAndDestroy {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private PipelineManager pipelineManager;

  public void init(int numDatanodes) throws Exception {
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        GenericTestUtils.getRandomizedTempPath());
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 2);

    cluster = MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(numDatanodes)
            .setTotalPipelineNumLimit(numDatanodes + numDatanodes/3)
            .setHbInterval(2000)
            .setHbProcessorInterval(1000)
            .build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    pipelineManager = scm.getPipelineManager();
  }

  @After
  public void cleanup() {
    cluster.shutdown();
  }

  @Test(timeout = 180000)
  public void testAutomaticPipelineCreationOnPipelineDestroy()
      throws Exception {
    int numOfDatanodes = 6;
    init(numOfDatanodes);
    // make sure two pipelines are created
    waitForPipelines(2);
    Assert.assertEquals(numOfDatanodes, pipelineManager.getPipelines(
        new RatisReplicationConfig(
            ReplicationFactor.ONE)).size());

    List<Pipeline> pipelines = pipelineManager
        .getPipelines(new RatisReplicationConfig(
            ReplicationFactor.THREE), Pipeline.PipelineState.OPEN);
    for (Pipeline pipeline : pipelines) {
      pipelineManager.closePipeline(pipeline, false);
    }
    // make sure two pipelines are created
    waitForPipelines(2);
  }

  @Test(timeout = 180000)
  public void testAutomaticPipelineCreationDisablingFactorONE()
      throws Exception {
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    init(6);
    // make sure two pipelines are created
    waitForPipelines(2);
    // No Factor ONE pipeline is auto created.
    Assert.assertEquals(0, pipelineManager.getPipelines(
        new RatisReplicationConfig(
            ReplicationFactor.ONE)).size());

    List<Pipeline> pipelines = pipelineManager
        .getPipelines(new RatisReplicationConfig(
            ReplicationFactor.THREE), Pipeline.PipelineState.OPEN);
    for (Pipeline pipeline : pipelines) {
      pipelineManager.closePipeline(pipeline, false);
    }

    // make sure two pipelines are created
    waitForPipelines(2);
  }

  @Test(timeout = 180000)
  public void testPipelineCreationOnNodeRestart() throws Exception {
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        5, TimeUnit.SECONDS);
    init(3);
    // make sure a pipelines is created
    waitForPipelines(1);
    List<HddsDatanodeService> dns = new ArrayList<>(cluster.getHddsDatanodes());

    List<Pipeline> pipelines =
        pipelineManager.getPipelines(new RatisReplicationConfig(
            ReplicationFactor.THREE));
    for (HddsDatanodeService dn : dns) {
      cluster.shutdownHddsDatanode(dn.getDatanodeDetails());
    }

    // try creating another pipeline now
    try {
      pipelineManager.createPipeline(new RatisReplicationConfig(
          ReplicationFactor.THREE));
      Assert.fail("pipeline creation should fail after shutting down pipeline");
    } catch (IOException ioe) {
      // As now all datanodes are shutdown, they move to stale state, there
      // will be no sufficient datanodes to create the pipeline.
      Assert.assertTrue(ioe instanceof SCMException);
      Assert.assertEquals(SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE,
          ((SCMException) ioe).getResult());
    }

    // make sure pipelines is destroyed
    waitForPipelines(0);
    for (HddsDatanodeService dn : dns) {
      cluster.restartHddsDatanode(dn.getDatanodeDetails(), false);
    }

    // destroy the existing pipelines
    for (Pipeline pipeline : pipelines) {
      pipelineManager.closePipeline(pipeline, false);
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
        .getPipelines(new RatisReplicationConfig(
            ReplicationFactor.THREE), Pipeline.PipelineState.OPEN)
        .size() >= numPipelines, 100, 60000);
  }
}
