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

package org.apache.hadoop.hdds.scm.safemode;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * This class tests OneReplicaPipelineSafeModeRule.
 */
public class TestOneReplicaPipelineSafeModeRule {

  @TempDir
  private Path tempDir;
  private OneReplicaPipelineSafeModeRule rule;
  private PipelineManagerImpl pipelineManager;
  private EventQueue eventQueue;
  private MockNodeManager mockNodeManager;

  private void setup(int nodes, int pipelineFactorThreeCount,
      int pipelineFactorOneCount) throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.toString());
    ozoneConfiguration.setBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);

    List<ContainerInfo> containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(1));
    mockNodeManager = new MockNodeManager(true, nodes);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers()).thenReturn(containers);
    eventQueue = new EventQueue();
    SCMServiceManager serviceManager = new SCMServiceManager();
    SCMContext scmContext = SCMContext.emptyContext();

    SCMMetadataStore scmMetadataStore =
            new SCMMetadataStoreImpl(ozoneConfiguration);

    pipelineManager = PipelineManagerImpl.newPipelineManager(
        ozoneConfiguration,
        SCMHAManagerStub.getInstance(true),
        mockNodeManager,
        scmMetadataStore.getPipelineTable(),
        eventQueue,
        scmContext,
        serviceManager,
        new TestClock(Instant.now(), ZoneOffset.UTC));

    PipelineProvider<RatisReplicationConfig> mockRatisProvider =
        new MockRatisPipelineProvider(mockNodeManager,
            pipelineManager.getStateManager(), ozoneConfiguration);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    createPipelines(pipelineFactorThreeCount,
        HddsProtos.ReplicationFactor.THREE);
    createPipelines(pipelineFactorOneCount,
        HddsProtos.ReplicationFactor.ONE);

    SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(ozoneConfiguration,
        mockNodeManager, pipelineManager, containerManager, serviceManager, eventQueue, scmContext);
    scmSafeModeManager.start();

    rule = SafeModeRuleFactory.getInstance().getSafeModeRule(OneReplicaPipelineSafeModeRule.class);
  }

  @Test
  public void testOneReplicaPipelineRule() throws Exception {

    // As with 30 nodes, We can create 7 pipelines with replication factor 3.
    // (This is because in node manager for every 10 nodes, 7 nodes are
    // healthy, 2 are stale one is dead.)
    int nodes = 30;
    int pipelineFactorThreeCount = 7;
    int pipelineCountOne = 0;
    setup(nodes, pipelineFactorThreeCount, pipelineCountOne);

    LogCapturer logCapturer = LogCapturer.captureLogs(SCMSafeModeManager.class);

    List<Pipeline> pipelines = pipelineManager.getPipelines();
    firePipelineEvent(pipelines.subList(0, pipelineFactorThreeCount - 1));

    // As 90% of 7 with ceil is 7, if we send 6 pipeline reports, rule
    // validate should be still false.

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "reported count is 6"), 1000, 5000);

    assertFalse(rule.validate());

    //Fire last pipeline event from datanode.
    firePipelineEvent(pipelines.subList(pipelineFactorThreeCount - 1,
            pipelineFactorThreeCount));

    GenericTestUtils.waitFor(() -> rule.validate(), 1000, 5000);
  }

  @Test
  public void testOneReplicaPipelineRuleMixedPipelines() throws Exception {

    // As with 30 nodes, We can create 7 pipelines with replication factor 3.
    // (This is because in node manager for every 10 nodes, 7 nodes are
    // healthy, 2 are stale one is dead.)
    int nodes = 30;
    int pipelineCountThree = 7;
    int pipelineCountOne = 21;

    setup(nodes, pipelineCountThree, pipelineCountOne);

    LogCapturer logCapturer = LogCapturer.captureLogs(SCMSafeModeManager.class);

    List<Pipeline> pipelines =
        pipelineManager.getPipelines(RatisReplicationConfig.getInstance(
            ReplicationFactor.ONE));
    firePipelineEvent(pipelines);
    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "reported count is 0"), 1000, 5000);

    // fired events for one node ratis pipeline, so we will be still false.
    assertFalse(rule.validate());

    pipelines =
        pipelineManager.getPipelines(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE));

    firePipelineEvent(pipelines.subList(0, pipelineCountThree - 1));

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "reported count is 6"), 1000, 5000);

    //Fire last pipeline event from datanode.
    firePipelineEvent(pipelines.subList(pipelineCountThree - 1,
            pipelineCountThree));

    GenericTestUtils.waitFor(() -> rule.validate(), 1000, 5000);
  }

  @Test
  public void testOneReplicaPipelineRuleWithReportProcessingFalse() {
    EventQueue localEventQueue = new EventQueue();
    PipelineManager mockedPipelineManager = mock(PipelineManager.class);
    SCMSafeModeManager mockedSafeModeManager = mock(SCMSafeModeManager.class);
    SafeModeMetrics mockedMetrics = mock(SafeModeMetrics.class);
    when(mockedSafeModeManager.getSafeModeMetrics()).thenReturn(mockedMetrics);

    OzoneConfiguration conf = new OzoneConfiguration();

    PipelineID pipelineID = PipelineID.randomId();
    Pipeline mockedPipeline = mock(Pipeline.class);
    when(mockedPipeline.getId()).thenReturn(pipelineID);

    // First validate(): pipeline has no nodes -> not counted as reported.
    // Second validate(): pipeline has at least one node -> counted as reported.
    when(mockedPipeline.getNodeSet())
        .thenReturn(java.util.Collections.emptySet(),
            new java.util.HashSet<>(
                java.util.Collections.singletonList(mock(DatanodeDetails.class))));

    when(mockedPipelineManager.getPipelines(
        Mockito.any(ReplicationConfig.class),
        Mockito.eq(Pipeline.PipelineState.OPEN)))
        .thenReturn(java.util.Collections.singletonList(mockedPipeline));

    OneReplicaPipelineSafeModeRule localRule =
        new OneReplicaPipelineSafeModeRule(localEventQueue, mockedPipelineManager,
            mockedSafeModeManager, conf);

    localRule.setValidateBasedOnReportProcessing(false);

    // With no nodes in the pipeline, the rule should not be satisfied.
    assertFalse(localRule.validate());

    // After at least one node is present in the pipeline, the rule should pass.
    assertTrue(localRule.validate());
    assertTrue(localRule.getReportedPipelineIDSet().contains(pipelineID));
  }

  private void createPipelines(int count,
      HddsProtos.ReplicationFactor factor) throws Exception {
    for (int i = 0; i < count; i++) {
      Pipeline pipeline = pipelineManager.createPipeline(
              RatisReplicationConfig.getInstance(factor));
      pipelineManager.openPipeline(pipeline.getId());

    }
  }

  private void firePipelineEvent(List<Pipeline> pipelines) {
    Map<DatanodeDetails, PipelineReportsProto.Builder>
            reportMap = new HashMap<>();
    for (Pipeline pipeline : pipelines) {
      for (DatanodeDetails dn : pipeline.getNodes()) {
        reportMap.putIfAbsent(dn, PipelineReportsProto.newBuilder());
      }
    }
    for (DatanodeDetails dn : reportMap.keySet()) {
      List<PipelineReport> reports = new ArrayList<>();
      for (PipelineID pipeline : mockNodeManager.
              getNode2PipelineMap().getPipelines(dn.getID())) {
        try {
          if (!pipelines.contains(pipelineManager.getPipeline(pipeline))) {
            continue;
          }
        } catch (PipelineNotFoundException pnfe) {
          continue;
        }
        HddsProtos.PipelineID pipelineID = pipeline.getProtobuf();
        reports.add(PipelineReport.newBuilder()
                .setPipelineID(pipelineID)
                .setIsLeader(true)
                .build());
      }
      PipelineReportsProto.Builder pipelineReportsProto =
              PipelineReportsProto.newBuilder();
      pipelineReportsProto.addAllPipelineReport(reports);
      eventQueue.fireEvent(SCMEvents.PIPELINE_REPORT, new
              SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode(dn,
              pipelineReportsProto.build()));
    }
  }
}
