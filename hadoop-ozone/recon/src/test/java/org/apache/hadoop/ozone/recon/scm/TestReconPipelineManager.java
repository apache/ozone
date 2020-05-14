/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.scm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineFactory;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineFactory.ReconPipelineProvider;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.mockito.Mockito.mock;

/**
 * Class to test Recon Pipeline Manager.
 */
public class TestReconPipelineManager {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private SCMStorageConfig scmStorageConfig;
  private DBStore store;

  @Before
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().getAbsolutePath());
    conf.set(OZONE_SCM_NAMES, "localhost");
    scmStorageConfig = new ReconStorageConfig(conf);
    store = DBStoreBuilder.createDBStore(conf, new ReconDBDefinition());
  }

  @After
  public void tearDown() throws Exception {
    store.close();
  }

  @Test
  public void testInitialize() throws IOException {

    // Get 3 OPEN pipelines from SCM.
    List<Pipeline> pipelinesFromScm = getPipelines(3);

    // Recon has 2 pipelines in ALLOCATED state. (1 is valid and 1 is obsolete)

    // Valid pipeline in Allocated state.
    Pipeline validPipeline = Pipeline.newBuilder()
        .setReplication(1)
        .setId(pipelinesFromScm.get(0).getId())
        .setNodes(pipelinesFromScm.get(0).getNodes())
        .setState(Pipeline.PipelineState.ALLOCATED)
        .setType(ReplicationType.STAND_ALONE)
        .build();

    // Invalid pipeline.
    Pipeline invalidPipeline = Pipeline.newBuilder()
        .setReplication(1)
        .setId(PipelineID.randomId())
        .setNodes(Collections.singletonList(randomDatanodeDetails()))
        .setState(Pipeline.PipelineState.CLOSED)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .build();

    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    NodeManager nodeManager =
        new SCMNodeManager(conf, scmStorageConfig, eventQueue, clusterMap);

    try (ReconPipelineManager reconPipelineManager =
        new ReconPipelineManager(conf, nodeManager,
            ReconDBDefinition.PIPELINES.getTable(store), eventQueue)) {
      reconPipelineManager.addPipeline(validPipeline);
      reconPipelineManager.addPipeline(invalidPipeline);

      reconPipelineManager.initializePipelines(pipelinesFromScm);
      List<Pipeline> newReconPipelines = reconPipelineManager.getPipelines();

      // Test if the number of pipelines in SCM is as expected.
      assertEquals(3, newReconPipelines.size());

      // Test if new pipelines from SCM are picked up.
      for (Pipeline pipeline : pipelinesFromScm) {
        assertTrue(reconPipelineManager.containsPipeline(pipeline.getId()));
      }

      // Test if existing pipeline state is updated.
      assertEquals(Pipeline.PipelineState.OPEN, reconPipelineManager
          .getPipeline(validPipeline.getId()).getPipelineState());

      // Test if obsolete pipelines in Recon are removed.
      assertFalse(reconPipelineManager.containsPipeline(
          invalidPipeline.getId()));
    }
  }

  @Test
  public void testAddPipeline() throws IOException {

    Pipeline pipeline = getRandomPipeline();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    NodeManager nodeManager =
        new SCMNodeManager(conf, scmStorageConfig, eventQueue, clusterMap);

    ReconPipelineManager reconPipelineManager =
        new ReconPipelineManager(conf, nodeManager,
            ReconDBDefinition.PIPELINES.getTable(store), eventQueue);
    assertFalse(reconPipelineManager.containsPipeline(pipeline.getId()));
    reconPipelineManager.addPipeline(pipeline);
    assertTrue(reconPipelineManager.containsPipeline(pipeline.getId()));
  }

  @Test
  public void testStubbedReconPipelineFactory() throws IOException {

    NodeManager nodeManagerMock = mock(NodeManager.class);

    ReconPipelineManager reconPipelineManager = new ReconPipelineManager(
        conf, nodeManagerMock, ReconDBDefinition.PIPELINES.getTable(store),
        new EventQueue());
    PipelineFactory pipelineFactory = reconPipelineManager.getPipelineFactory();
    assertTrue(pipelineFactory instanceof ReconPipelineFactory);
    ReconPipelineFactory reconPipelineFactory =
        (ReconPipelineFactory) pipelineFactory;
    assertTrue(reconPipelineFactory.getProviders().isEmpty());
    for (ReplicationType type  : reconPipelineFactory.getProviders().keySet()) {
      PipelineProvider pipelineProvider =
          reconPipelineFactory.getProviders().get(type);
      assertTrue(pipelineProvider instanceof ReconPipelineProvider);
    }
  }

  private List<Pipeline> getPipelines(int size) {
    List<Pipeline> pipelines = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      pipelines.add(getRandomPipeline());
    }
    return pipelines;
  }
}
