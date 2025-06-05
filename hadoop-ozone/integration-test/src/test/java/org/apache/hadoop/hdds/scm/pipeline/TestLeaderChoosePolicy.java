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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for LeaderChoosePolicy.
 */
@Unhealthy("This test was never enabled")
public class TestLeaderChoosePolicy {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private PipelineManager pipelineManager;

  public void init(int numDatanodes, int datanodePipelineLimit)
      throws Exception {
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, datanodePipelineLimit);
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, numDatanodes + numDatanodes / 3);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 2000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000, TimeUnit.MILLISECONDS);

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

  private void checkLeaderBalance(int dnNum, int leaderNumOfEachDn)
      throws Exception {
    List<Pipeline> pipelines = pipelineManager
        .getPipelines(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), Pipeline.PipelineState.OPEN);

    for (Pipeline pipeline : pipelines) {
      LambdaTestUtils.await(30000, 500, () ->
          pipeline.getLeaderId().equals(pipeline.getSuggestedLeaderId()));
    }

    Map<DatanodeID, Integer> leaderCount = new HashMap<>();
    for (Pipeline pipeline : pipelines) {
      DatanodeID leader = pipeline.getLeaderId();
      if (!leaderCount.containsKey(leader)) {
        leaderCount.put(leader, 0);
      }

      leaderCount.put(leader, leaderCount.get(leader) + 1);
    }

    assertEquals(dnNum, leaderCount.size());
    for (Map.Entry<DatanodeID, Integer> entry: leaderCount.entrySet()) {
      assertEquals(leaderNumOfEachDn, leaderCount.get(entry.getKey()));
    }
  }

  @Test
  public void testRestoreSuggestedLeader() throws Exception {
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    conf.set(OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY,
        "org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms" +
            ".MinLeaderCountChoosePolicy");
    int dnNum = 3;
    int dnPipelineLimit = 3;
    int leaderNumOfEachDn = dnPipelineLimit / dnNum;
    int pipelineNum = 3;

    init(dnNum, dnPipelineLimit);
    // make sure two pipelines are created
    waitForPipelines(pipelineNum);
    // No Factor ONE pipeline is auto created.
    assertEquals(0,
        pipelineManager.getPipelines(RatisReplicationConfig.getInstance(
            ReplicationFactor.ONE)).size());

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

    assertEquals(
        pipelinesBeforeRestart.size(), pipelinesAfterRestart.size());

    for (Pipeline p : pipelinesBeforeRestart) {
      boolean equal = false;
      for (Pipeline q : pipelinesAfterRestart) {
        if (p.getId().equals(q.getId())
            && p.getSuggestedLeaderId().equals(q.getSuggestedLeaderId())) {
          equal = true;
        }
      }

      assertTrue(equal);
    }
  }

  @Test
  public void testMinLeaderCountChoosePolicy() throws Exception {
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    conf.set(OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY,
        "org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms" +
            ".MinLeaderCountChoosePolicy");
    int dnNum = 3;
    int dnPipelineLimit = 3;
    int leaderNumOfEachDn = dnPipelineLimit / dnNum;
    int pipelineNum = 3;

    init(dnNum, dnPipelineLimit);
    // make sure pipelines are created
    waitForPipelines(pipelineNum);
    // No Factor ONE pipeline is auto created.
    assertEquals(0, pipelineManager.getPipelines(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.ONE)).size());

    // pipelineNum pipelines in 3 datanodes,
    // each datanode has leaderNumOfEachDn leaders after balance
    checkLeaderBalance(dnNum, leaderNumOfEachDn);

    for (int i = 0; i < 10; i++) {
      // destroy some pipelines, wait new pipelines created,
      // then check leader balance

      List<Pipeline> pipelines = pipelineManager
          .getPipelines(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE), Pipeline.PipelineState.OPEN);

      int destroyNum = RandomUtils.secure().randomInt(0, pipelines.size());
      for (int k = 0; k <= destroyNum; k++) {
        pipelineManager.closePipeline(pipelines.get(k).getId());
      }

      waitForPipelines(pipelineNum);

      checkLeaderBalance(dnNum, leaderNumOfEachDn);
    }
  }

  @Test
  public void testDefaultLeaderChoosePolicy() throws Exception {
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    conf.set(OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY,
        "org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms" +
            ".DefaultLeaderChoosePolicy");
    int dnNum = 3;
    int dnPipelineLimit = 3;
    int pipelineNum = 3;

    init(dnNum, dnPipelineLimit);
    // make sure pipelines are created
    waitForPipelines(pipelineNum);
  }

  private void waitForPipelines(int numPipelines)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pipelineManager
        .getPipelines(RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), Pipeline.PipelineState.OPEN)
        .size() >= numPipelines, 100, 60000);
  }
}
