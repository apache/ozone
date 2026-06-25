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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBufferStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineFactory;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineFactory.ReconPipelineProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Class to test Recon Pipeline Manager.
 */
public class TestReconPipelineManager {

  @TempDir
  private Path temporaryFolder;

  private OzoneConfiguration conf;
  private SCMStorageConfig scmStorageConfig;
  private DBStore store;
  private SCMHAManager scmhaManager;
  private SCMContext scmContext;

  @BeforeEach
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,
        temporaryFolder.toAbsolutePath().toString());
    conf.set(OZONE_SCM_NAMES, "localhost");
    scmStorageConfig = new ReconStorageConfig(conf, new ReconUtils());
    store = DBStoreBuilder.createDBStore(conf, ReconSCMDBDefinition.get());
    scmhaManager = SCMHAManagerStub.getInstance(
        true, new SCMHADBTransactionBufferStub(store));
    scmContext = SCMContext.emptyContext();
  }

  @AfterEach
  public void tearDown() throws Exception {
    store.close();
  }

  @Test
  public void testInitialize() throws IOException, TimeoutException {

    // Get 3 OPEN pipelines from SCM.
    List<Pipeline> pipelinesFromScm = getPipelines(3);

    // Recon has 2 pipelines in ALLOCATED state. (1 is valid and 1 is obsolete)

    // Valid pipeline in Allocated state.
    Pipeline validPipeline = Pipeline.newBuilder()
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setId(pipelinesFromScm.get(0).getId())
        .setNodes(pipelinesFromScm.get(0).getNodes())
        .setState(Pipeline.PipelineState.ALLOCATED)

        .build();

    // Invalid pipeline.
    Pipeline invalidPipeline = Pipeline.newBuilder()
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setId(PipelineID.randomId())
        .setNodes(Collections.singletonList(randomDatanodeDetails()))
        .setState(Pipeline.PipelineState.CLOSED)
        .build();

    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    HDDSLayoutVersionManager versionManager = mock(HDDSLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(versionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    NodeManager nodeManager = new SCMNodeManager(conf, scmStorageConfig,
        eventQueue, clusterMap, SCMContext.emptyContext(), versionManager);

    try (ReconPipelineManager reconPipelineManager =
             ReconPipelineManager.newReconPipelineManager(
                 conf,
                 nodeManager,
                 ReconSCMDBDefinition.PIPELINES.getTable(store),
                 eventQueue,
                 scmhaManager,
                 scmContext)) {
      StorageContainerManager mock = mock(StorageContainerManager.class);
      when(mock.getScmNodeDetails())
          .thenReturn(mock(SCMNodeDetails.class));
      scmContext = new SCMContext.Builder()
              .setSafeModeStatus(SCMSafeModeManager.SafeModeStatus.PRE_CHECKS_PASSED)
              .setLeader(true)
              .setSCM(mock).build();
      reconPipelineManager.setScmContext(scmContext);
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
  public void testAddPipeline() throws IOException, TimeoutException {

    Pipeline pipeline = getRandomPipeline();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    HDDSLayoutVersionManager versionManager = mock(HDDSLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(versionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    NodeManager nodeManager = new SCMNodeManager(conf, scmStorageConfig,
        eventQueue, clusterMap, SCMContext.emptyContext(), versionManager);

    ReconPipelineManager reconPipelineManager =
        ReconPipelineManager.newReconPipelineManager(
            conf,
            nodeManager,
            ReconSCMDBDefinition.PIPELINES.getTable(store),
            eventQueue,
            scmhaManager,
            scmContext);

    assertFalse(reconPipelineManager.containsPipeline(pipeline.getId()));
    reconPipelineManager.addPipeline(pipeline);
    assertTrue(reconPipelineManager.containsPipeline(pipeline.getId()));
  }

  @Test
  public void testDuplicatePipelineHandling() throws IOException {
    Pipeline pipeline = getRandomPipeline();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    HDDSLayoutVersionManager versionManager = mock(HDDSLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion()).thenReturn(maxLayoutVersion());
    when(versionManager.getSoftwareLayoutVersion()).thenReturn(maxLayoutVersion());
    NodeManager nodeManager =
        new SCMNodeManager(conf, scmStorageConfig, eventQueue, clusterMap,
            SCMContext.emptyContext(), versionManager);

    ReconPipelineManager reconPipelineManager =
        ReconPipelineManager.newReconPipelineManager(conf, nodeManager,
            ReconSCMDBDefinition.PIPELINES.getTable(store), eventQueue,
            scmhaManager, scmContext);

    // Add the pipeline for the first time
    reconPipelineManager.addPipeline(pipeline);

    // Attempt to add the same pipeline again and ensure no exception is thrown
    assertDoesNotThrow(() -> {
      reconPipelineManager.addPipeline(pipeline);
    }, "Exception was thrown when adding a duplicate pipeline.");
  }

  @Test
  public void testStubbedReconPipelineFactory() throws IOException {

    NodeManager nodeManagerMock = mock(NodeManager.class);

    ReconPipelineManager reconPipelineManager =
        ReconPipelineManager.newReconPipelineManager(
            conf,
            nodeManagerMock,
            ReconSCMDBDefinition.PIPELINES.getTable(store),
            new EventQueue(),
            scmhaManager,
            scmContext);

    PipelineFactory pipelineFactory = reconPipelineManager.getPipelineFactory();
    ReconPipelineFactory reconPipelineFactory =
        assertInstanceOf(ReconPipelineFactory.class, pipelineFactory);
    assertThat(reconPipelineFactory.getProviders()).isEmpty();
    for (ReplicationType type  : reconPipelineFactory.getProviders().keySet()) {
      PipelineProvider pipelineProvider =
          reconPipelineFactory.getProviders().get(type);
      assertInstanceOf(ReconPipelineProvider.class, pipelineProvider);
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
