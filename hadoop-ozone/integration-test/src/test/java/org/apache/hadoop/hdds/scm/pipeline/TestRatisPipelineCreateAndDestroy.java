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
import org.apache.hadoop.ozone.container.common.transport.server
    .XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server
    .ratis.XceiverServerRatis;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests for RatisPipelineUtils.
 */
public class TestRatisPipelineCreateAndDestroy {

  private static MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private static PipelineManager pipelineManager;
  private static int MAX_PIPELINE_PER_NODE = 4;

  public void init(int numDatanodes) throws Exception {
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        GenericTestUtils.getRandomizedTempPath());
    conf.setInt(OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT, MAX_PIPELINE_PER_NODE);

    cluster = MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(numDatanodes)
            .setTotalPipelineNumLimit(numDatanodes + numDatanodes)
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
  public void testAutomaticPipelineCreationDisablingFactorONE()
      throws Exception {
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    init(6);
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
    init(3);
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

  @Test(timeout = 300000)
  public void testMultiRaftStorageDir() throws Exception {
    final String suffix = "-testMultiRaftStorageDir-";
    Map<String, AtomicInteger> directories = new ConcurrentHashMap<>();
    int maxPipelinePerNode = MAX_PIPELINE_PER_NODE;
    int index = 0;
    while(maxPipelinePerNode > 1) {
      directories.put("ratis" + suffix + (index++),  new AtomicInteger(0));
      maxPipelinePerNode--;
    }

    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        5, TimeUnit.SECONDS);
    conf.set("dfs.container.ratis.datanode.storage.dir.suffix", suffix);

    // Create 3 RATIS THREE pipeline
    init(3);
    // make sure a pipelines is created
    waitForPipelines(3);
    List<Pipeline> pipelines =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    List<RaftGroupId> raftGroupIds = new ArrayList<>();
    pipelines.stream().forEach(pipeline ->
        raftGroupIds.add(RaftGroupId.valueOf(pipeline.getId().getId())));

    List<HddsDatanodeService> dns = new ArrayList<>(cluster.getHddsDatanodes());
    dns.stream().forEach(dn -> {
      XceiverServerSpi writeChannel =
          dn.getDatanodeStateMachine().getContainer().getWriteChannel();
      RaftServerProxy server =
          (RaftServerProxy)((XceiverServerRatis)writeChannel).getServer();
      raftGroupIds.stream().forEach(group -> {
        try {
          RaftServerImpl raft = server.getImpl(group);
          String raftDir =
              raft.getState().getStorage().getStorageDir().getRoot().toString();
          directories.keySet().stream().forEach(path -> {
            if (raftDir.contains(path)) {
              directories.get(path).incrementAndGet();
            }
          });
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    });

    directories.values().stream().forEach(
        count -> Assert.assertEquals(MAX_PIPELINE_PER_NODE - 1, count.get()));
  }

  private void waitForPipelines(int numPipelines)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, Pipeline.PipelineState.OPEN)
        .size() >= numPipelines, 100, 40000);
  }
}
