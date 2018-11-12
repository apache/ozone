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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .ReplicationType.RATIS;

public class TestPipelineClose {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static ContainerWithPipeline ratisContainer1;
  private static ContainerWithPipeline ratisContainer2;
  private static ContainerManager containerManager;
  private static PipelineManager pipelineManager;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(6).build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    pipelineManager = scm.getPipelineManager();
    ContainerInfo containerInfo1 = containerManager
        .allocateContainer(RATIS, THREE, "testOwner");
    ratisContainer1 = new ContainerWithPipeline(containerInfo1,
        pipelineManager.getPipeline(containerInfo1.getPipelineID()));
    ContainerInfo containerInfo2 = containerManager
        .allocateContainer(RATIS, THREE, "testOwner");
    ratisContainer2 = new ContainerWithPipeline(containerInfo2,
        pipelineManager.getPipeline(containerInfo2.getPipelineID()));
    pipelineManager = scm.getPipelineManager();
    // At this stage, there should be 2 pipeline one with 1 open container each.
    // Try closing the both the pipelines, one with a closed container and
    // the other with an open container.
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  @Test
  public void testPipelineCloseWithClosedContainer() throws IOException {
    Set<ContainerID> set = pipelineManager
        .getContainersInPipeline(ratisContainer1.getPipeline().getId());

    ContainerID cId = ratisContainer1.getContainerInfo().containerID();
    Assert.assertEquals(1, set.size());
    set.forEach(containerID -> Assert.assertEquals(containerID, cId));

    // Now close the container and it should not show up while fetching
    // containers by pipeline
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.CLOSE);

    Set<ContainerID> setClosed = pipelineManager
        .getContainersInPipeline(ratisContainer1.getPipeline().getId());
    Assert.assertEquals(0, setClosed.size());

    pipelineManager.finalizePipeline(ratisContainer1.getPipeline().getId());
    Pipeline pipeline1 = pipelineManager
        .getPipeline(ratisContainer1.getPipeline().getId());
    Assert.assertEquals(pipeline1.getPipelineState(),
        Pipeline.PipelineState.CLOSED);
    pipelineManager.removePipeline(pipeline1.getId());
    for (DatanodeDetails dn : ratisContainer1.getPipeline().getNodes()) {
      // Assert that the pipeline has been removed from Node2PipelineMap as well
      Assert.assertEquals(scm.getScmNodeManager().getPipelines(
          dn).size(), 0);
    }
  }

  @Test
  public void testPipelineCloseWithOpenContainer() throws IOException,
      TimeoutException, InterruptedException {
    Set<ContainerID> setOpen = pipelineManager.getContainersInPipeline(
        ratisContainer2.getPipeline().getId());
    Assert.assertEquals(1, setOpen.size());

    ContainerID cId2 = ratisContainer2.getContainerInfo().containerID();
    pipelineManager.finalizePipeline(ratisContainer2.getPipeline().getId());
    Assert.assertEquals(
        pipelineManager.getPipeline(ratisContainer2.getPipeline().getId())
            .getPipelineState(), Pipeline.PipelineState.CLOSED);
    Pipeline pipeline2 = pipelineManager
        .getPipeline(ratisContainer2.getPipeline().getId());
    Assert.assertEquals(pipeline2.getPipelineState(),
        Pipeline.PipelineState.CLOSED);
  }
}