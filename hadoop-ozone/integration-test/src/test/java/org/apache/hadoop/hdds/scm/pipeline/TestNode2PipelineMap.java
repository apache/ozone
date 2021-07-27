/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * Test for the Node2Pipeline map.
 */
public class TestNode2PipelineMap {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private StorageContainerManager scm;
  private ContainerWithPipeline ratisContainer;
  private ContainerManagerV2 containerManager;
  private PipelineManager pipelineManager;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    pipelineManager = scm.getPipelineManager();
    ContainerInfo containerInfo = containerManager.allocateContainer(
        new RatisReplicationConfig(
            ReplicationFactor.THREE), "testOwner");
    ratisContainer = new ContainerWithPipeline(containerInfo,
        pipelineManager.getPipeline(containerInfo.getPipelineID()));
    pipelineManager = scm.getPipelineManager();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPipelineMap() throws IOException,
      InvalidStateTransitionException {

    Set<ContainerID> set = pipelineManager
        .getContainersInPipeline(ratisContainer.getPipeline().getId());

    ContainerID cId = ratisContainer.getContainerInfo().containerID();
    Assert.assertEquals(1, set.size());
    set.forEach(containerID ->
        Assert.assertEquals(containerID, cId));

    List<DatanodeDetails> dns = ratisContainer.getPipeline().getNodes();
    Assert.assertEquals(3, dns.size());

    // get pipeline details by dnid
    Set<PipelineID> pipelines = scm.getScmNodeManager()
        .getPipelines(dns.get(0));
    Assert.assertTrue(pipelines.contains(ratisContainer.getPipeline().getId()));

    // Now close the container and it should not show up while fetching
    // containers by pipeline
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.CLOSE);
    Set<ContainerID> set2 = pipelineManager.getContainersInPipeline(
        ratisContainer.getPipeline().getId());
    Assert.assertEquals(0, set2.size());

    pipelineManager
        .closePipeline(ratisContainer.getPipeline(), false);
    pipelines = scm.getScmNodeManager()
        .getPipelines(dns.get(0));
    Assert
        .assertFalse(pipelines.contains(ratisContainer.getPipeline().getId()));
  }
}
