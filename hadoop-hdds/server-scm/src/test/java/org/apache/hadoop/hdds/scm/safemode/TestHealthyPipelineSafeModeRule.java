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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
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
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests HealthyPipelineSafeMode rule.
 */
public class TestHealthyPipelineSafeModeRule {
  @TempDir
  private File tempFile;

  @Test
  public void testHealthyPipelineSafeModeRuleWithNoPipelines()
      throws Exception {
    EventQueue eventQueue = new EventQueue();
    SCMServiceManager serviceManager = new SCMServiceManager();
    SCMContext scmContext = SCMContext.emptyContext();
    List<ContainerInfo> containers =
            new ArrayList<>(HddsTestUtils.getContainerInfo(1));

    OzoneConfiguration config = new OzoneConfiguration();
    MockNodeManager nodeManager = new MockNodeManager(true, 0);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers()).thenReturn(containers);
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFile.getPath());
    config.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, 0);
    // enable pipeline check
    config.setBoolean(
            HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(config);

    try {
      PipelineManagerImpl pipelineManager =
          PipelineManagerImpl.newPipelineManager(
              config,
              SCMHAManagerStub.getInstance(true),
              nodeManager,
              scmMetadataStore.getPipelineTable(),
              eventQueue,
              scmContext,
              serviceManager,
              Clock.system(ZoneOffset.UTC));
      PipelineProvider mockRatisProvider =
          new MockRatisPipelineProvider(nodeManager,
              pipelineManager.getStateManager(), config);
      pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
          mockRatisProvider);
      SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(config,
          nodeManager, pipelineManager, containerManager, serviceManager, eventQueue, scmContext);
      scmSafeModeManager.start();

      HealthyPipelineSafeModeRule healthyPipelineSafeModeRule = SafeModeRuleFactory.getInstance()
          .getSafeModeRule(HealthyPipelineSafeModeRule.class);

      // This should be immediately satisfied, as no pipelines are there yet.
      assertTrue(healthyPipelineSafeModeRule.validate());
    } finally {
      scmMetadataStore.getStore().close();
    }
  }

  @Test
  public void testHealthyPipelineSafeModeRuleWithPipelines() throws Exception {

    EventQueue eventQueue = new EventQueue();
    SCMServiceManager serviceManager = new SCMServiceManager();
    SCMContext scmContext = SCMContext.emptyContext();
    List<ContainerInfo> containers =
            new ArrayList<>(HddsTestUtils.getContainerInfo(1));

    OzoneConfiguration config = new OzoneConfiguration();
    // In Mock Node Manager, first 8 nodes are healthy, next 2 nodes are
    // stale and last one is dead, and this repeats. So for a 12 node, 9
    // healthy, 2 stale and one dead.
    MockNodeManager nodeManager = new MockNodeManager(true, 12);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers()).thenReturn(containers);
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFile.getPath());
    // enable pipeline check
    config.setBoolean(
            HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);

    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(config);
    try {
      PipelineManagerImpl pipelineManager =
          PipelineManagerImpl.newPipelineManager(
              config,
              SCMHAManagerStub.getInstance(true),
              nodeManager,
              scmMetadataStore.getPipelineTable(),
              eventQueue,
              scmContext,
              serviceManager,
              Clock.system(ZoneOffset.UTC));

      PipelineProvider mockRatisProvider =
          new MockRatisPipelineProvider(nodeManager,
              pipelineManager.getStateManager(), config);
      pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
          mockRatisProvider);

      // Create 3 pipelines
      Pipeline pipeline1 =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));
      pipelineManager.openPipeline(pipeline1.getId());
      Pipeline pipeline2 =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));
      pipelineManager.openPipeline(pipeline2.getId());
      Pipeline pipeline3 =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));
      pipelineManager.openPipeline(pipeline3.getId());

      // Mark pipeline healthy
      pipeline1 = pipelineManager.getPipeline(pipeline1.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline1);

      pipeline2 = pipelineManager.getPipeline(pipeline2.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline2);

      pipeline3 = pipelineManager.getPipeline(pipeline3.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline3);

      SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(config,
          nodeManager, pipelineManager, containerManager, serviceManager, eventQueue, scmContext);
      scmSafeModeManager.start();

      HealthyPipelineSafeModeRule healthyPipelineSafeModeRule = SafeModeRuleFactory.getInstance()
          .getSafeModeRule(HealthyPipelineSafeModeRule.class);

      // Fire pipeline report from all datanodes in first pipeline, as here we
      // have 3 pipelines, 10% is 0.3, when doing ceil it is 1. So, we should
      // validate should return true after fire pipeline event


      //Here testing with out pipelinereport handler, so not moving created
      // pipelines to allocated state, as pipelines changing to healthy is
      // handled by pipeline report handler. So, leaving pipeline's in pipeline
      // manager in open state for test case simplicity.

      firePipelineEvent(pipeline1, eventQueue);
      GenericTestUtils.waitFor(() -> healthyPipelineSafeModeRule.validate(),
          1000, 5000);
    } finally {
      scmMetadataStore.getStore().close();
    }
  }

  @Test
  public void testHealthyPipelineSafeModeRuleWithMixedPipelines()
      throws Exception {
    EventQueue eventQueue = new EventQueue();
    SCMServiceManager serviceManager = new SCMServiceManager();
    SCMContext scmContext = SCMContext.emptyContext();
    List<ContainerInfo> containers =
            new ArrayList<>(HddsTestUtils.getContainerInfo(1));

    OzoneConfiguration config = new OzoneConfiguration();

    // In Mock Node Manager, first 8 nodes are healthy, next 2 nodes are
    // stale and last one is dead, and this repeats. So for a 12 node, 9
    // healthy, 2 stale and one dead.
    MockNodeManager nodeManager = new MockNodeManager(true, 12);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers()).thenReturn(containers);
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFile.getPath());
    // enable pipeline check
    config.setBoolean(
            HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);

    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(config);
    try {
      PipelineManagerImpl pipelineManager =
          PipelineManagerImpl.newPipelineManager(
              config,
              SCMHAManagerStub.getInstance(true),
              nodeManager,
              scmMetadataStore.getPipelineTable(),
              eventQueue,
              scmContext,
              serviceManager,
              Clock.system(ZoneOffset.UTC));

      PipelineProvider mockRatisProvider =
          new MockRatisPipelineProvider(nodeManager,
              pipelineManager.getStateManager(), config);
      pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
          mockRatisProvider);

      // Create 3 pipelines
      Pipeline pipeline1 =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.ONE));
      pipelineManager.openPipeline(pipeline1.getId());
      Pipeline pipeline2 =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));
      pipelineManager.openPipeline(pipeline2.getId());
      Pipeline pipeline3 =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));
      pipelineManager.openPipeline(pipeline3.getId());

      // Mark pipelines healthy
      pipeline1 = pipelineManager.getPipeline(pipeline1.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline1);

      pipeline2 = pipelineManager.getPipeline(pipeline2.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline2);

      pipeline3 = pipelineManager.getPipeline(pipeline3.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline3);

      SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(config,
          nodeManager, pipelineManager, containerManager, serviceManager, eventQueue, scmContext);
      scmSafeModeManager.start();

      HealthyPipelineSafeModeRule healthyPipelineSafeModeRule = SafeModeRuleFactory.getInstance()
          .getSafeModeRule(HealthyPipelineSafeModeRule.class);

      //No need of pipeline events.
      GenericTestUtils.waitFor(() -> healthyPipelineSafeModeRule.validate(),
          1000, 5000);

    } finally {
      scmMetadataStore.getStore().close();
    }

  }

  @Test
  public void testHealthyPipelineThresholdIncreasesWithMorePipelinesAndReports()
      throws Exception {
    EventQueue eventQueue = new EventQueue();
    SCMServiceManager serviceManager = new SCMServiceManager();
    SCMContext scmContext = SCMContext.emptyContext();
    List<ContainerInfo> containers =
        new ArrayList<>(HddsTestUtils.getContainerInfo(1));

    OzoneConfiguration config = new OzoneConfiguration();
    MockNodeManager nodeManager = new MockNodeManager(true, 12);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers()).thenReturn(containers);
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFile.getPath());
    config.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    config.setDouble(HddsConfigKeys.HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT,
        0.5);
    config.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, 0);

    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(config);
    try {
      PipelineManagerImpl pipelineManager =
          PipelineManagerImpl.newPipelineManager(
              config,
              SCMHAManagerStub.getInstance(true),
              nodeManager,
              scmMetadataStore.getPipelineTable(),
              eventQueue,
              scmContext,
              serviceManager,
              Clock.system(ZoneOffset.UTC));

      PipelineProvider mockRatisProvider =
          new MockRatisPipelineProvider(nodeManager,
              pipelineManager.getStateManager(), config);
      pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
          mockRatisProvider);

      // Create all pipelines before SCM enters safe mode. Pipeline creation is
      // blocked once safe mode prechecks have not passed.
      Pipeline pipeline1 =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));
      Pipeline pipeline2 =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));
      Pipeline pipeline3 =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));

      // Start with one healthy open pipeline. Threshold is small at this point.
      pipelineManager.openPipeline(pipeline1.getId());
      pipeline1 = pipelineManager.getPipeline(pipeline1.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline1);

      SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(config,
          nodeManager, pipelineManager, containerManager, serviceManager,
          eventQueue, scmContext);
      scmSafeModeManager.start();

      HealthyPipelineSafeModeRule healthyPipelineSafeModeRule =
          SafeModeRuleFactory.getInstance()
              .getSafeModeRule(HealthyPipelineSafeModeRule.class);

      firePipelineEvent(pipeline1, eventQueue);
      assertTrue(healthyPipelineSafeModeRule.validate());
      assertEquals(1, healthyPipelineSafeModeRule.getHealthyPipelineThresholdCount());

      // Open more pipelines so threshold increases.
      pipelineManager.openPipeline(pipeline2.getId());
      pipeline2 = pipelineManager.getPipeline(pipeline2.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline2);

      pipelineManager.openPipeline(pipeline3.getId());
      pipeline3 = pipelineManager.getPipeline(pipeline3.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline3);

      // Simulate DN reports causing pipelines to be considered unhealthy.
      for (DatanodeDetails dn : pipeline2.getNodes()) {
        nodeManager.setNodeState(dn, HddsProtos.NodeState.DEAD);
      }
      for (DatanodeDetails dn : pipeline3.getNodes()) {
        nodeManager.setNodeState(dn, HddsProtos.NodeState.DEAD);
      }
      firePipelineEvent(pipeline2, eventQueue);
      firePipelineEvent(pipeline3, eventQueue);

      assertFalse(healthyPipelineSafeModeRule.validate());
      assertEquals(2, healthyPipelineSafeModeRule.getHealthyPipelineThresholdCount());

      // Simulate more DN reports and recovery to healthy state, then exit rule.
      for (DatanodeDetails dn : pipeline1.getNodes()) {
        nodeManager.setNodeState(dn, HddsProtos.NodeState.HEALTHY);
      }
      for (DatanodeDetails dn : pipeline2.getNodes()) {
        nodeManager.setNodeState(dn, HddsProtos.NodeState.HEALTHY);
      }
      for (DatanodeDetails dn : pipeline3.getNodes()) {
        nodeManager.setNodeState(dn, HddsProtos.NodeState.HEALTHY);
      }
      firePipelineEvent(pipeline1, eventQueue);
      firePipelineEvent(pipeline2, eventQueue);
      firePipelineEvent(pipeline3, eventQueue);

      assertTrue(healthyPipelineSafeModeRule.validate());
      assertEquals(2, healthyPipelineSafeModeRule.getHealthyPipelineThresholdCount());
    } finally {
      scmMetadataStore.getStore().close();
    }
  }

  @Test
  public void testPipelineIgnoredWhenDnIsUnhealthy() throws Exception {
    EventQueue eventQueue = new EventQueue();
    SCMServiceManager serviceManager = new SCMServiceManager();
    SCMContext scmContext = SCMContext.emptyContext();
    List<ContainerInfo> containers =
        new ArrayList<>(HddsTestUtils.getContainerInfo(1));

    OzoneConfiguration config = new OzoneConfiguration();
    MockNodeManager nodeManager = new MockNodeManager(true, 12);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers()).thenReturn(containers);
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFile.getPath());
    config.setBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);

    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(config);
    try {
      PipelineManagerImpl pipelineManager =
          PipelineManagerImpl.newPipelineManager(
              config,
              SCMHAManagerStub.getInstance(true),
              nodeManager,
              scmMetadataStore.getPipelineTable(),
              eventQueue,
              scmContext,
              serviceManager,
              Clock.system(ZoneOffset.UTC));

      PipelineProvider mockRatisProvider =
          new MockRatisPipelineProvider(nodeManager,
              pipelineManager.getStateManager(), config);
      pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
          mockRatisProvider);

      // Create a Ratis pipeline with 3 replicas
      Pipeline pipeline =
          pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));
      pipelineManager.openPipeline(pipeline.getId());
      pipeline = pipelineManager.getPipeline(pipeline.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline);

      // Mark one DN as DEAD
      DatanodeDetails dnDead = pipeline.getNodes().get(0);
      nodeManager.setNodeState(dnDead, HddsProtos.NodeState.DEAD);

      SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(config,
          nodeManager, pipelineManager, containerManager, serviceManager, eventQueue, scmContext);
      scmSafeModeManager.start();

      HealthyPipelineSafeModeRule healthyPipelineSafeModeRule = SafeModeRuleFactory.getInstance()
          .getSafeModeRule(HealthyPipelineSafeModeRule.class);

      // Fire the pipeline report
      firePipelineEvent(pipeline, eventQueue);
      assertFalse(healthyPipelineSafeModeRule.isPipelineHealthy(pipeline));
      // Ensure the rule is NOT satisfied due to unhealthy DN
      assertFalse(healthyPipelineSafeModeRule.validate());
    } finally {
      scmMetadataStore.getStore().close();
    }
  }

  private void firePipelineEvent(Pipeline pipeline, EventQueue eventQueue) {
    eventQueue.fireEvent(SCMEvents.OPEN_PIPELINE, pipeline);
  }
}
