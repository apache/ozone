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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests for RatisPipelineUtils.
 */
@Ignore
public class TestRatisPipelineCreateAndDestroy {

  private static MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private static PipelineManager pipelineManager;

  public void init(int numDatanodes, int datanodePipelineLimit)
      throws Exception {
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        GenericTestUtils.getRandomizedTempPath());
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, datanodePipelineLimit);

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
    init(numOfDatanodes, 2);
    // make sure two pipelines are created
    waitForPipelines(2);
    Assert.assertEquals(numOfDatanodes, pipelineManager.getPipelines(
        HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE).size());

    List<Pipeline> pipelines = pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN);
    for (Pipeline pipeline : pipelines) {
      pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    }
    // make sure two pipelines are created
    waitForPipelines(2);
  }

  private void checkLeaderBalance(int dnNum, int leaderNumOfEachDn)
      throws Exception {
    List<Pipeline> pipelines = pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN);

    for (Pipeline pipeline : pipelines) {
      LambdaTestUtils.await(30000, 500, () ->
          pipeline.getLeaderId().equals(pipeline.getSuggestedLeaderId()));
    }

    Map<UUID, Integer> leaderCount = new HashMap<>();
    for (Pipeline pipeline : pipelines) {
      UUID leader = pipeline.getLeaderId();
      if (!leaderCount.containsKey(leader)) {
        leaderCount.put(leader, 0);
      }

      leaderCount.put(leader, leaderCount.get(leader) + 1);
    }

    Assert.assertTrue(leaderCount.size() == dnNum);
    for (UUID key : leaderCount.keySet()) {
      Assert.assertTrue(leaderCount.get(key) == leaderNumOfEachDn);
    }
  }

  @Test(timeout = 360000)
  public void testRestoreSuggestedLeader() throws Exception {
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    int dnNum = 3;
    int dnPipelineLimit = 3;
    int leaderNumOfEachDn = dnPipelineLimit / dnNum;
    int pipelineNum = 3;

    init(dnNum, dnPipelineLimit);
    // make sure two pipelines are created
    waitForPipelines(pipelineNum);
    // No Factor ONE pipeline is auto created.
    Assert.assertEquals(0, pipelineManager.getPipelines(
        HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE).size());

    // pipelineNum pipelines in 3 datanodes,
    // each datanode has leaderNumOfEachDn leaders after balance
    checkLeaderBalance(dnNum, leaderNumOfEachDn);
    List<Pipeline> pipelinesBeforeRestart =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipelines();

    cluster.restartStorageContainerManager(true);

    checkLeaderBalance(dnNum, leaderNumOfEachDn);
    List<Pipeline> pipelinesAfterRestart =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipelines();

    Assert.assertEquals(
        pipelinesBeforeRestart.size(), pipelinesAfterRestart.size());

    for (Pipeline p : pipelinesBeforeRestart) {
      boolean equal = false;
      for (Pipeline q : pipelinesAfterRestart) {
        if (p.getId().equals(q.getId())
            && p.getSuggestedLeaderId().equals(q.getSuggestedLeaderId())) {
          equal = true;
        }
      }

      Assert.assertTrue(equal);
    }
  }

  @Test(timeout = 360000)
  public void testPipelineLeaderBalance() throws Exception {
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    int dnNum = 3;
    int dnPipelineLimit = 3;
    int leaderNumOfEachDn = dnPipelineLimit / dnNum;
    int pipelineNum = 3;

    init(dnNum, dnPipelineLimit);
    // make sure two pipelines are created
    waitForPipelines(pipelineNum);
    // No Factor ONE pipeline is auto created.
    Assert.assertEquals(0, pipelineManager.getPipelines(
        HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE).size());

    // pipelineNum pipelines in 3 datanodes,
    // each datanode has leaderNumOfEachDn leaders after balance
    checkLeaderBalance(dnNum, leaderNumOfEachDn);

    //cluster.restartStorageContainerManager(true);

    //checkLeaderBalance();

    Random r = new Random(0);
    for (int i = 0; i < 10; i++) {
      // destroy some pipelines, wait new pipelines created,
      // then check leader balance

      List<Pipeline> pipelines = pipelineManager
          .getPipelines(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN);

      int destroyNum = r.nextInt(pipelines.size());
      for (int k = 0; k <= destroyNum; k++) {
        pipelineManager.finalizeAndDestroyPipeline(pipelines.get(k), false);
      }

      waitForPipelines(pipelineNum);

      checkLeaderBalance(dnNum, leaderNumOfEachDn);
    }
  }

  @Test(timeout = 180000)
  public void testAutomaticPipelineCreationDisablingFactorONE()
      throws Exception {
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    init(6, 2);
    // make sure two pipelines are created
    waitForPipelines(2);
    // No Factor ONE pipeline is auto created.
    Assert.assertEquals(0, pipelineManager.getPipelines(
        HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE).size());

    List<Pipeline> pipelines = pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN);
    for (Pipeline pipeline : pipelines) {
      pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    }

    // make sure two pipelines are created
    waitForPipelines(2);
  }

  @Test(timeout = 180000)
  public void testPipelineCreationOnNodeRestart() throws Exception {
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        5, TimeUnit.SECONDS);
    init(3, 2);
    // make sure a pipelines is created
    waitForPipelines(1);
    List<HddsDatanodeService> dns = new ArrayList<>(cluster.getHddsDatanodes());

    List<Pipeline> pipelines =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    for (HddsDatanodeService dn : dns) {
      cluster.shutdownHddsDatanode(dn.getDatanodeDetails());
    }

    // try creating another pipeline now
    try {
      pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
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
      pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    }

    if (cluster.getStorageContainerManager()
        .getScmNodeManager().getNodeCount(HddsProtos.NodeState.HEALTHY) >=
        HddsProtos.ReplicationFactor.THREE.getNumber()) {
      // make sure pipelines is created after node start
      pipelineManager.triggerPipelineCreation();
      waitForPipelines(1);
    }
  }

  private void waitForPipelines(int numPipelines)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN)
        .size() >= numPipelines, 100, 60000);
  }
}
