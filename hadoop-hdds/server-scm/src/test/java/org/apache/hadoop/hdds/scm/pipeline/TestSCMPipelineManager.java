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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases to verify PipelineManager.
 */
public class TestSCMPipelineManager {
  private static MockNodeManager nodeManager;
  private static File testDir;
  private static Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(OZONE_DATANODE_MAX_PIPELINE_ENGAGEMENT, 1);
    testDir = GenericTestUtils
        .getTestDir(TestSCMPipelineManager.class.getSimpleName());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    boolean folderExisted = testDir.exists() || testDir.mkdirs();
    if (!folderExisted) {
      throw new IOException("Unable to create test directory path");
    }
    nodeManager = new MockNodeManager(true, 20);
  }

  @After
  public void cleanup() {
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testPipelineReload() throws IOException {
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager, new EventQueue());
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    Set<Pipeline> pipelines = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      Pipeline pipeline = pipelineManager
          .createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      pipelines.add(pipeline);
    }
    pipelineManager.close();

    // new pipeline manager should be able to load the pipelines from the db
    pipelineManager =
        new SCMPipelineManager(conf, nodeManager, new EventQueue());
    mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    for (Pipeline p : pipelines) {
      pipelineManager.openPipeline(p.getId());
    }
    List<Pipeline> pipelineList =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipelines, new HashSet<>(pipelineList));

    // clean up
    for (Pipeline pipeline : pipelines) {
      pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    }
    pipelineManager.close();
  }

  @Test
  public void testRemovePipeline() throws IOException {
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager, new EventQueue());
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    Pipeline pipeline = pipelineManager
        .createPipeline(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    pipelineManager.openPipeline(pipeline.getId());
    pipelineManager
        .addContainerToPipeline(pipeline.getId(), ContainerID.valueof(1));
    pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    pipelineManager.close();

    // new pipeline manager should not be able to load removed pipelines
    pipelineManager =
        new SCMPipelineManager(conf, nodeManager, new EventQueue());
    try {
      pipelineManager.getPipeline(pipeline.getId());
      Assert.fail("Pipeline should not have been retrieved");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("not found"));
    }

    // clean up
    pipelineManager.close();
  }

  @Test
  public void testPipelineReport() throws IOException {
    EventQueue eventQueue = new EventQueue();
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager, eventQueue);
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    SCMSafeModeManager scmSafeModeManager =
        new SCMSafeModeManager(conf, new ArrayList<>(), pipelineManager,
            eventQueue);

    // create a pipeline in allocated state with no dns yet reported
    Pipeline pipeline = pipelineManager
        .createPipeline(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);

    Assert
        .assertFalse(pipelineManager.getPipeline(pipeline.getId()).isHealthy());
    Assert
        .assertFalse(pipelineManager.getPipeline(pipeline.getId()).isOpen());

    // pipeline is not healthy until all dns report
    List<DatanodeDetails> nodes = pipeline.getNodes();
    Assert.assertFalse(
        pipelineManager.getPipeline(pipeline.getId()).isHealthy());
    // get pipeline report from each dn in the pipeline
    PipelineReportHandler pipelineReportHandler =
        new PipelineReportHandler(scmSafeModeManager, pipelineManager, conf);
    nodes.subList(0, 2).forEach(dn -> sendPipelineReport(dn, pipeline,
        pipelineReportHandler, false, eventQueue));
    sendPipelineReport(nodes.get(nodes.size() - 1), pipeline,
        pipelineReportHandler, true, eventQueue);

    // pipeline is healthy when all dns report
    Assert
        .assertTrue(pipelineManager.getPipeline(pipeline.getId()).isHealthy());
    // pipeline should now move to open state
    Assert
        .assertTrue(pipelineManager.getPipeline(pipeline.getId()).isOpen());

    // close the pipeline
    pipelineManager.finalizeAndDestroyPipeline(pipeline, false);

    // pipeline report for destroyed pipeline should be ignored
    nodes.subList(0, 2).forEach(dn -> sendPipelineReport(dn, pipeline,
        pipelineReportHandler, false, eventQueue));
    sendPipelineReport(nodes.get(nodes.size() - 1), pipeline,
        pipelineReportHandler, true, eventQueue);

    try {
      pipelineManager.getPipeline(pipeline.getId());
      Assert.fail("Pipeline should not have been retrieved");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("not found"));
    }

    // clean up
    pipelineManager.close();
  }

  @Test
  public void testPipelineCreationFailedMetric() throws Exception {
    MockNodeManager nodeManagerMock = new MockNodeManager(true,
        20);
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManagerMock, new EventQueue());
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManagerMock,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    MetricsRecordBuilder metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    long numPipelineAllocated = getLongCounter("NumPipelineAllocated",
        metrics);
    Assert.assertTrue(numPipelineAllocated == 0);

    // 3 DNs are unhealthy.
    // Create 5 pipelines (Use up 15 Datanodes)
    for (int i = 0; i < 5; i++) {
      Pipeline pipeline = pipelineManager
          .createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      Assert.assertNotNull(pipeline);
    }

    metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    numPipelineAllocated = getLongCounter("NumPipelineAllocated", metrics);
    Assert.assertTrue(numPipelineAllocated == 5);

    long numPipelineCreateFailed = getLongCounter(
        "NumPipelineCreationFailed", metrics);
    Assert.assertTrue(numPipelineCreateFailed == 0);

    //This should fail...
    try {
      pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
      Assert.fail();
    } catch (SCMException ioe) {
      // pipeline creation failed this time.
      Assert.assertEquals(SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE,
          ioe.getResult());
    }

    metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    numPipelineAllocated = getLongCounter("NumPipelineAllocated", metrics);
    Assert.assertTrue(numPipelineAllocated == 5);

    numPipelineCreateFailed = getLongCounter(
        "NumPipelineCreationFailed", metrics);
    Assert.assertTrue(numPipelineCreateFailed == 1);

    // clean up
    pipelineManager.close();
  }

  @Test
  public void testActivateDeactivatePipeline() throws IOException {
    final SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager, new EventQueue());
    final PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);

    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    final Pipeline pipeline = pipelineManager
        .createPipeline(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    final PipelineID pid = pipeline.getId();

    pipelineManager.openPipeline(pid);
    pipelineManager.addContainerToPipeline(pid, ContainerID.valueof(1));

    Assert.assertTrue(pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            Pipeline.PipelineState.OPEN).contains(pipeline));

    Assert.assertEquals(Pipeline.PipelineState.OPEN,
        pipelineManager.getPipeline(pid).getPipelineState());

    pipelineManager.deactivatePipeline(pid);
    Assert.assertEquals(Pipeline.PipelineState.DORMANT,
        pipelineManager.getPipeline(pid).getPipelineState());

    Assert.assertFalse(pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            Pipeline.PipelineState.OPEN).contains(pipeline));

    pipelineManager.activatePipeline(pid);

    Assert.assertTrue(pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            Pipeline.PipelineState.OPEN).contains(pipeline));

    pipelineManager.close();
  }

  @Test
  public void testPipelineOpenOnlyWhenLeaderReported() throws Exception {
    EventQueue eventQueue = new EventQueue();
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager, eventQueue);
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    Pipeline pipeline = pipelineManager
        .createPipeline(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    // close manager
    pipelineManager.close();
    // new pipeline manager loads the pipelines from the db in ALLOCATED state
    pipelineManager =
        new SCMPipelineManager(conf, nodeManager, eventQueue);
    mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    Assert.assertEquals(Pipeline.PipelineState.ALLOCATED,
        pipelineManager.getPipeline(pipeline.getId()).getPipelineState());

    SCMSafeModeManager scmSafeModeManager =
        new SCMSafeModeManager(new OzoneConfiguration(),
            new ArrayList<>(), pipelineManager, eventQueue);
    PipelineReportHandler pipelineReportHandler =
        new PipelineReportHandler(scmSafeModeManager, pipelineManager, conf);

    // Report pipelines with leaders
    List<DatanodeDetails> nodes = pipeline.getNodes();
    Assert.assertEquals(3, nodes.size());
    // Send report for all but no leader
    nodes.forEach(dn -> sendPipelineReport(dn, pipeline, pipelineReportHandler,
        false, eventQueue));

    Assert.assertEquals(Pipeline.PipelineState.ALLOCATED,
        pipelineManager.getPipeline(pipeline.getId()).getPipelineState());

    nodes.subList(0, 2).forEach(dn -> sendPipelineReport(dn, pipeline,
        pipelineReportHandler, false, eventQueue));
    sendPipelineReport(nodes.get(nodes.size() - 1), pipeline,
        pipelineReportHandler, true, eventQueue);

    Assert.assertEquals(Pipeline.PipelineState.OPEN,
        pipelineManager.getPipeline(pipeline.getId()).getPipelineState());

    pipelineManager.close();
  }

  @Test
  public void testScrubPipeline() throws IOException {
    // No timeout for pipeline scrubber.
    conf.setTimeDuration(
        OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT, -1,
        TimeUnit.MILLISECONDS);

    final SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager, new EventQueue(), null);
    final PipelineProvider ratisProvider = new MockRatisPipelineProvider(
        nodeManager, pipelineManager.getStateManager(), conf, false);

    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        ratisProvider);

    Pipeline pipeline = pipelineManager
        .createPipeline(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    // At this point, pipeline is not at OPEN stage.
    Assert.assertEquals(pipeline.getPipelineState(),
        Pipeline.PipelineState.ALLOCATED);

    // pipeline should be seen in pipelineManager as ALLOCATED.
    Assert.assertTrue(pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            Pipeline.PipelineState.ALLOCATED).contains(pipeline));
    pipelineManager.scrubPipeline(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.THREE);

    // pipeline should be scrubbed.
    Assert.assertFalse(pipelineManager
        .getPipelines(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE,
            Pipeline.PipelineState.ALLOCATED).contains(pipeline));

    pipelineManager.close();
  }

  private void sendPipelineReport(DatanodeDetails dn,
      Pipeline pipeline, PipelineReportHandler pipelineReportHandler,
      boolean isLeader, EventQueue eventQueue) {
    PipelineReportFromDatanode report =
        TestUtils.getPipelineReportFromDatanode(dn, pipeline.getId(), isLeader);
    pipelineReportHandler.onMessage(report, eventQueue);
  }
}
