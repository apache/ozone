/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.safemode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.MockSCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerV2Impl;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.ozone.test.GenericTestUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

/**
 * This class tests OneReplicaPipelineSafeModeRule.
 */
public class TestOneReplicaPipelineSafeModeRule {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private OneReplicaPipelineSafeModeRule rule;
  private PipelineManagerV2Impl pipelineManager;
  private EventQueue eventQueue;
  private SCMServiceManager serviceManager;
  private SCMContext scmContext;
  private MockNodeManager mockNodeManager;

  private void setup(int nodes, int pipelineFactorThreeCount,
      int pipelineFactorOneCount) throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK, true);
    ozoneConfiguration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        folder.newFolder().toString());
    ozoneConfiguration.setBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);

    List<ContainerInfo> containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(1));
    mockNodeManager = new MockNodeManager(true, nodes);

    eventQueue = new EventQueue();
    serviceManager = new SCMServiceManager();
    scmContext = SCMContext.emptyContext();

    SCMMetadataStore scmMetadataStore =
            new SCMMetadataStoreImpl(ozoneConfiguration);

    pipelineManager = PipelineManagerV2Impl.newPipelineManager(
        ozoneConfiguration,
        MockSCMHAManager.getInstance(true),
        mockNodeManager,
        scmMetadataStore.getPipelineTable(),
        eventQueue,
        scmContext,
        serviceManager);

    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(mockNodeManager,
            pipelineManager.getStateManager(), ozoneConfiguration);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    createPipelines(pipelineFactorThreeCount,
        HddsProtos.ReplicationFactor.THREE);
    createPipelines(pipelineFactorOneCount,
        HddsProtos.ReplicationFactor.ONE);

    SCMSafeModeManager scmSafeModeManager =
        new SCMSafeModeManager(ozoneConfiguration, containers, null,
            pipelineManager, eventQueue, serviceManager, scmContext);

    rule = scmSafeModeManager.getOneReplicaPipelineSafeModeRule();
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

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(
            LoggerFactory.getLogger(SCMSafeModeManager.class));

    List<Pipeline> pipelines = pipelineManager.getPipelines();
    firePipelineEvent(pipelines.subList(0, pipelineFactorThreeCount -1));

    // As 90% of 7 with ceil is 7, if we send 6 pipeline reports, rule
    // validate should be still false.

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "reported count is 6"), 1000, 5000);

    Assert.assertFalse(rule.validate());

    //Fire last pipeline event from datanode.
    firePipelineEvent(pipelines.subList(pipelineFactorThreeCount -1,
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

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(
            LoggerFactory.getLogger(SCMSafeModeManager.class));

    List<Pipeline> pipelines =
        pipelineManager.getPipelines(new RatisReplicationConfig(
            ReplicationFactor.ONE));
    firePipelineEvent(pipelines);
    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "reported count is 0"), 1000, 5000);

    // fired events for one node ratis pipeline, so we will be still false.
    Assert.assertFalse(rule.validate());

    pipelines =
        pipelineManager.getPipelines(
            new RatisReplicationConfig(ReplicationFactor.THREE));

    firePipelineEvent(pipelines.subList(0, pipelineCountThree -1));

    GenericTestUtils.waitFor(() -> logCapturer.getOutput().contains(
        "reported count is 6"), 1000, 5000);

    //Fire last pipeline event from datanode.
    firePipelineEvent(pipelines.subList(pipelineCountThree -1,
            pipelineCountThree));

    GenericTestUtils.waitFor(() -> rule.validate(), 1000, 5000);
  }

  private void createPipelines(int count,
      HddsProtos.ReplicationFactor factor) throws Exception {
    for (int i = 0; i < count; i++) {
      Pipeline pipeline = pipelineManager.createPipeline(
              new RatisReplicationConfig(factor));
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
              getNode2PipelineMap().getPipelines(dn.getUuid())) {
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
                .setBytesWritten(0)
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
