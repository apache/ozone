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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for the Node2Pipeline map.
 */
public abstract class TestNode2PipelineMap implements NonHATests.TestCase {

  private StorageContainerManager scm;
  private ContainerWithPipeline ratisContainer;
  private ContainerManager containerManager;
  private PipelineManager pipelineManager;

  @BeforeEach
  public void init() throws Exception {
    scm = cluster().getStorageContainerManager();
    containerManager = scm.getContainerManager();
    pipelineManager = scm.getPipelineManager();
    ContainerInfo containerInfo = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE), "testOwner");
    ratisContainer = new ContainerWithPipeline(containerInfo,
        pipelineManager.getPipeline(containerInfo.getPipelineID()));
    pipelineManager = scm.getPipelineManager();
  }

  @Test
  public void testPipelineMap() throws IOException,
      InvalidStateTransitionException, TimeoutException {

    Set<ContainerID> set = pipelineManager
        .getContainersInPipeline(ratisContainer.getPipeline().getId());

    ContainerID cId = ratisContainer.getContainerInfo().containerID();
    assertThat(set).contains(cId);
    final int size = set.size();

    List<DatanodeDetails> dns = ratisContainer.getPipeline().getNodes();
    assertEquals(3, dns.size());

    // get pipeline details by dnid
    Set<PipelineID> pipelines = scm.getScmNodeManager()
        .getPipelines(dns.get(0));
    assertThat(pipelines).contains(ratisContainer.getPipeline().getId());

    // Now close the container and it should not show up while fetching
    // containers by pipeline
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager
        .updateContainerState(cId, HddsProtos.LifeCycleEvent.CLOSE);
    Set<ContainerID> set2 = pipelineManager.getContainersInPipeline(
        ratisContainer.getPipeline().getId());
    assertEquals(size - 1, set2.size());

    pipelineManager.closePipeline(ratisContainer.getPipeline().getId());
    pipelineManager.deletePipeline(ratisContainer.getPipeline().getId());
    pipelines = scm.getScmNodeManager()
        .getPipelines(dns.get(0));
    assertThat(pipelines).doesNotContain(ratisContainer.getPipeline().getId());
  }
}
