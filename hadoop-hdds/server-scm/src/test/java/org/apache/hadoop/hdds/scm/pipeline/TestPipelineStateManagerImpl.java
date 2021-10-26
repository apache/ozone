/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.TestContainerManagerImpl;
import org.apache.hadoop.hdds.scm.ha.MockSCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersions;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Test for PipelineStateManagerImpl.
 */
public class TestPipelineStateManagerImpl {

  private PipelineStateManager stateManager;
  private File testDir;
  private DBStore dbStore;

  @Before
  public void init() throws Exception {
    final OzoneConfiguration conf = SCMTestUtils.getConf();
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());

    SCMHAManager scmhaManager = MockSCMHAManager.getInstance(true);
    NodeManager nodeManager = new MockNodeManager(true, 10);

    stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
  }

  @After
  public void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  private Pipeline createDummyPipeline(int numNodes) {
    return createDummyPipeline(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, numNodes);
  }

  private Pipeline createDummyPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, int numNodes) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    return Pipeline.newBuilder()
        .setReplicationConfig(ReplicationConfig.fromTypeAndFactor(type, factor))
        .setNodes(nodes)
        .setState(Pipeline.PipelineState.ALLOCATED)
        .setId(PipelineID.randomId())
        .build();
  }

  @Test
  public void testAddAndGetPipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(0);
    HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
        ClientVersions.CURRENT_VERSION);
    try {
      stateManager.addPipeline(pipelineProto);
      Assert.fail("Pipeline should not have been added");
    } catch (StateMachineException e) {
      // replication factor and number of nodes in the pipeline do not match
      Assert.assertTrue(e.getMessage().contains("do not match"));
    }

    // add a pipeline
    pipeline = createDummyPipeline(1);
    pipelineProto = pipeline.getProtobufMessage(
        ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);

    try {
      stateManager.addPipeline(pipelineProto);
      Assert.fail("Pipeline should not have been added");
    } catch (IOException e) {
      // Can not add a pipeline twice
      Assert.assertTrue(e.getMessage().contains("Duplicate pipeline ID"));
    }

    // verify pipeline returned is same
    Pipeline pipeline1 = stateManager.getPipeline(pipeline.getId());
    Assert.assertTrue(pipeline.getId().equals(pipeline1.getId()));

    // clean up
    finalizePipeline(pipelineProto);
    removePipeline(pipelineProto);
  }

  @Test
  public void testGetPipelines() throws IOException {
    // In start there should be no pipelines
    Assert.assertTrue(stateManager.getPipelines().isEmpty());

    Set<HddsProtos.Pipeline> pipelines = new HashSet<>();
    HddsProtos.Pipeline pipeline = createDummyPipeline(1).getProtobufMessage(
        ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipeline);
    pipelines.add(pipeline);
    pipeline = createDummyPipeline(1).getProtobufMessage(
        ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipeline);
    pipelines.add(pipeline);

    Set<Pipeline> pipelines1 = new HashSet<>(stateManager
        .getPipelines(new RatisReplicationConfig(ReplicationFactor.ONE)));
    Assert.assertEquals(pipelines1.size(), pipelines.size());

    pipelines1 = new HashSet<>(stateManager.getPipelines());
    Assert.assertEquals(pipelines1.size(), pipelines.size());

    // clean up
    for (HddsProtos.Pipeline pipeline1 : pipelines) {
      finalizePipeline(pipeline1);
      removePipeline(pipeline1);
    }
  }

  @Test
  public void testGetPipelinesByTypeAndFactor() throws IOException {
    Set<HddsProtos.Pipeline> pipelines = new HashSet<>();
    for (HddsProtos.ReplicationType type : new ReplicationType[] {
        ReplicationType.RATIS, ReplicationType.STAND_ALONE}) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (int i = 0; i < 5; i++) {
          // 5 pipelines in allocated state for each type and factor
          HddsProtos.Pipeline pipeline =
              createDummyPipeline(type, factor, factor.getNumber())
                  .getProtobufMessage(ClientVersions.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in open state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber())
              .getProtobufMessage(ClientVersions.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in closed state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber())
              .getProtobufMessage(ClientVersions.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          pipelines.add(pipeline);
        }
      }
    }

    for (HddsProtos.ReplicationType type : new ReplicationType[] {
        ReplicationType.RATIS, ReplicationType.STAND_ALONE}) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        // verify pipelines received
        List<Pipeline> pipelines1 =
            stateManager.getPipelines(
                ReplicationConfig.fromTypeAndFactor(type, factor));
        Assert.assertEquals(15, pipelines1.size());
        pipelines1.stream().forEach(p -> {
          Assert.assertEquals(type, p.getType());
        });
      }
    }

    //clean up
    for (HddsProtos.Pipeline pipeline : pipelines) {
      finalizePipeline(pipeline);
      removePipeline(pipeline);
    }
  }

  @Test
  public void testGetPipelinesByTypeFactorAndState() throws IOException {
    Set<HddsProtos.Pipeline> pipelines = new HashSet<>();
    for (HddsProtos.ReplicationType type : new ReplicationType[] {
        ReplicationType.RATIS, ReplicationType.STAND_ALONE}) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (int i = 0; i < 5; i++) {
          // 5 pipelines in allocated state for each type and factor
          HddsProtos.Pipeline pipeline =
              createDummyPipeline(type, factor, factor.getNumber())
                  .getProtobufMessage(ClientVersions.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in open state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber())
              .getProtobufMessage(ClientVersions.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          openPipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in dormant state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber())
              .getProtobufMessage(ClientVersions.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          openPipeline(pipeline);
          deactivatePipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in closed state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber())
              .getProtobufMessage(ClientVersions.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          finalizePipeline(pipeline);
          pipelines.add(pipeline);
        }
      }
    }

    for (HddsProtos.ReplicationType type : new HddsProtos.ReplicationType[] {
        ReplicationType.RATIS, ReplicationType.STAND_ALONE}) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (Pipeline.PipelineState state : Pipeline.PipelineState.values()) {
          // verify pipelines received
          List<Pipeline> pipelines1 =
              stateManager.getPipelines(
                  ReplicationConfig.fromTypeAndFactor(type, factor), state);
          Assert.assertEquals(5, pipelines1.size());
          pipelines1.forEach(p -> {
            Assert.assertEquals(type, p.getType());
            Assert.assertEquals(state, p.getPipelineState());
          });
        }
      }
    }

    //clean up
    for (HddsProtos.Pipeline pipeline : pipelines) {
      finalizePipeline(pipeline);
      removePipeline(pipeline);
    }
  }

  @Test
  public void testAddAndGetContainer() throws IOException {
    long containerID = 0;
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    pipeline = stateManager.getPipeline(pipeline.getId());
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));

    // move pipeline to open state
    openPipeline(pipelineProto);
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));

    //verify the number of containers returned
    Set<ContainerID> containerIDs =
        stateManager.getContainers(pipeline.getId());
    Assert.assertEquals(containerIDs.size(), containerID);

    finalizePipeline(pipelineProto);
    removePipeline(pipelineProto);
    try {
      stateManager.addContainerToPipeline(pipeline.getId(),
          ContainerID.valueOf(++containerID));
      Assert.fail("Container should not have been added");
    } catch (IOException e) {
      // Can not add a container to removed pipeline
      Assert.assertTrue(e.getMessage().contains("not found"));
    }
  }

  @Test
  public void testRemovePipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    // close the pipeline
    openPipeline(pipelineProto);
    stateManager
        .addContainerToPipeline(pipeline.getId(), ContainerID.valueOf(1));

    try {
      removePipeline(pipelineProto);
      Assert.fail("Pipeline should not have been removed");
    } catch (IOException e) {
      // can not remove a pipeline which already has containers
      Assert.assertTrue(e.getMessage().contains("not yet closed"));
    }

    // close the pipeline
    finalizePipeline(pipelineProto);
    // remove containers and then remove the pipeline
    removePipeline(pipelineProto);
  }

  @Test
  public void testRemoveContainer() throws IOException {
    long containerID = 1;
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersions.CURRENT_VERSION);
    // create an open pipeline in stateMap
    stateManager.addPipeline(pipelineProto);
    openPipeline(pipelineProto);

    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(containerID));
    Assert.assertEquals(1, stateManager.getContainers(pipeline.getId()).size());
    stateManager.removeContainerFromPipeline(pipeline.getId(),
        ContainerID.valueOf(containerID));
    Assert.assertEquals(0, stateManager.getContainers(pipeline.getId()).size());

    // add two containers in the pipeline
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));
    Assert.assertEquals(2, stateManager.getContainers(pipeline.getId()).size());

    // move pipeline to closing state
    finalizePipeline(pipelineProto);

    stateManager.removeContainerFromPipeline(pipeline.getId(),
        ContainerID.valueOf(containerID));
    stateManager.removeContainerFromPipeline(pipeline.getId(),
        ContainerID.valueOf(--containerID));
    Assert.assertEquals(0, stateManager.getContainers(pipeline.getId()).size());

    // clean up
    removePipeline(pipelineProto);
  }

  @Test
  public void testFinalizePipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    // finalize on ALLOCATED pipeline
    finalizePipeline(pipelineProto);
    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipelineProto);

    pipeline = createDummyPipeline(1);
    pipelineProto = pipeline
        .getProtobufMessage(ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    openPipeline(pipelineProto);
    // finalize on OPEN pipeline
    finalizePipeline(pipelineProto);
    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipelineProto);

    pipeline = createDummyPipeline(1);
    pipelineProto = pipeline
        .getProtobufMessage(ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    openPipeline(pipelineProto);
    finalizePipeline(pipelineProto);
    // finalize should work on already closed pipeline
    finalizePipeline(pipelineProto);
    Assert.assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipelineProto);
  }

  @Test
  public void testOpenPipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    // open on ALLOCATED pipeline
    openPipeline(pipelineProto);
    Assert.assertEquals(Pipeline.PipelineState.OPEN,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());

    openPipeline(pipelineProto);
    // open should work on already open pipeline
    Assert.assertEquals(Pipeline.PipelineState.OPEN,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    finalizePipeline(pipelineProto);
    removePipeline(pipelineProto);
  }

  @Test
  public void testQueryPipeline() throws IOException {
    Pipeline pipeline = createDummyPipeline(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.THREE, 3);
    // pipeline in allocated state should not be reported
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersions.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    Assert.assertEquals(0, stateManager
        .getPipelines(new RatisReplicationConfig(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN)
        .size());

    // pipeline in open state should be reported
    openPipeline(pipelineProto);
    Assert.assertEquals(1, stateManager
        .getPipelines(new RatisReplicationConfig(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN)
        .size());

    Pipeline pipeline2 = createDummyPipeline(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.THREE, 3);
    pipeline2 = Pipeline.newBuilder(pipeline2)
        .setState(Pipeline.PipelineState.OPEN)
        .build();
    HddsProtos.Pipeline pipelineProto2 = pipeline2
        .getProtobufMessage(ClientVersions.CURRENT_VERSION);
    // pipeline in open state should be reported
    stateManager.addPipeline(pipelineProto2);
    Assert.assertEquals(2, stateManager
        .getPipelines(new RatisReplicationConfig(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN)
        .size());

    // pipeline in closed state should not be reported
    finalizePipeline(pipelineProto2);
    Assert.assertEquals(1, stateManager
        .getPipelines(new RatisReplicationConfig(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN)
        .size());

    // clean up
    finalizePipeline(pipelineProto);
    removePipeline(pipelineProto);
    finalizePipeline(pipelineProto2);
    removePipeline(pipelineProto2);
  }

  private void removePipeline(HddsProtos.Pipeline pipeline) throws IOException {
    stateManager.removePipeline(pipeline.getId());
  }

  private void openPipeline(HddsProtos.Pipeline pipeline) throws IOException {
    stateManager.updatePipelineState(pipeline.getId(),
        HddsProtos.PipelineState.PIPELINE_OPEN);
  }

  private void finalizePipeline(HddsProtos.Pipeline pipeline)
      throws IOException {
    stateManager.updatePipelineState(pipeline.getId(),
        HddsProtos.PipelineState.PIPELINE_CLOSED);
  }

  private void deactivatePipeline(HddsProtos.Pipeline pipeline)
      throws IOException {
    stateManager.updatePipelineState(pipeline.getId(),
        HddsProtos.PipelineState.PIPELINE_DORMANT);
  }
}