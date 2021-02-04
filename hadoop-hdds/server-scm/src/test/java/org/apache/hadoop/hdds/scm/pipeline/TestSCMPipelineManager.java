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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.metadata.PipelineIDCodec;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.common.base.Supplier;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;


import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.INFO;

/**
 * Test cases to verify PipelineManager.
 */
public class TestSCMPipelineManager {
  private static MockNodeManager nodeManager;
  private static File testDir;
  private static OzoneConfiguration conf;
  private static SCMMetadataStore scmMetadataStore;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 1);
    testDir = GenericTestUtils
        .getTestDir(TestSCMPipelineManager.class.getSimpleName());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    boolean folderExisted = testDir.exists() || testDir.mkdirs();
    if (!folderExisted) {
      throw new IOException("Unable to create test directory path");
    }
    nodeManager = new MockNodeManager(true, 20);

    scmMetadataStore = new SCMMetadataStoreImpl(conf);
  }

  @After
  public void cleanup() throws Exception {
    scmMetadataStore.getStore().close();
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testPipelineReload() throws IOException {
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf,
            nodeManager,
            scmMetadataStore.getPipelineTable(),
            new EventQueue());
    pipelineManager.allowPipelineCreation();
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    int pipelineNum = 5;

    Set<Pipeline> pipelines = new HashSet<>();
    for (int i = 0; i < pipelineNum; i++) {
      Pipeline pipeline = pipelineManager
          .createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      pipelineManager.openPipeline(pipeline.getId());
      pipelines.add(pipeline);
    }
    pipelineManager.close();

    // new pipeline manager should be able to load the pipelines from the db
    pipelineManager =
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), new EventQueue());
    pipelineManager.allowPipelineCreation();
    mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    for (Pipeline p : pipelines) {
      // After reload, pipelines should be in open state
      Assert.assertTrue(pipelineManager.getPipeline(p.getId()).isOpen());
    }
    List<Pipeline> pipelineList =
        pipelineManager.getPipelines(HddsProtos.ReplicationType.RATIS);
    Assert.assertEquals(pipelines, new HashSet<>(pipelineList));

    Set<Set<DatanodeDetails>> originalPipelines = pipelineList.stream()
        .map(Pipeline::getNodeSet).collect(Collectors.toSet());
    Set<Set<DatanodeDetails>> reloadedPipelineHash = pipelines.stream()
        .map(Pipeline::getNodeSet).collect(Collectors.toSet());
    Assert.assertEquals(reloadedPipelineHash, originalPipelines);
    Assert.assertEquals(pipelineNum, originalPipelines.size());

    // clean up
    for (Pipeline pipeline : pipelines) {
      pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    }
    pipelineManager.close();
  }

  @Test
  public void testRemovePipeline() throws IOException {
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), new EventQueue());
    pipelineManager.allowPipelineCreation();
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
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), new EventQueue());
    try {
      pipelineManager.getPipeline(pipeline.getId());
      fail("Pipeline should not have been retrieved");
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
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), eventQueue);
    pipelineManager.allowPipelineCreation();
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
      fail("Pipeline should not have been retrieved");
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
        new SCMPipelineManager(conf, nodeManagerMock,
            scmMetadataStore.getPipelineTable(), new EventQueue());
    pipelineManager.allowPipelineCreation();
    nodeManagerMock.setNumPipelinePerDatanode(1);
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManagerMock,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    MetricsRecordBuilder metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    long numPipelineAllocated = getLongCounter("NumPipelineAllocated",
        metrics);
    Assert.assertEquals(0, numPipelineAllocated);

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
    Assert.assertEquals(5, numPipelineAllocated);

    long numPipelineCreateFailed = getLongCounter(
        "NumPipelineCreationFailed", metrics);
    Assert.assertEquals(0, numPipelineCreateFailed);

    LogCapturer logs = LogCapturer.captureLogs(SCMPipelineManager.getLog());
    GenericTestUtils.setLogLevel(SCMPipelineManager.getLog(), INFO);
    //This should fail...
    try {
      pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
      fail();
    } catch (SCMException ioe) {
      // pipeline creation failed this time.
      Assert.assertEquals(SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE,
          ioe.getResult());
      Assert.assertFalse(logs.getOutput().contains(
          "Failed to create pipeline of type"));
    } finally {
      logs.stopCapturing();
    }

    metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    numPipelineAllocated = getLongCounter("NumPipelineAllocated", metrics);
    Assert.assertEquals(5, numPipelineAllocated);

    numPipelineCreateFailed = getLongCounter(
        "NumPipelineCreationFailed", metrics);
    Assert.assertEquals(1, numPipelineCreateFailed);

    // clean up
    pipelineManager.close();
  }

  @Test
  public void testPipelineLimit() throws Exception {
    int numMetaDataVolumes = 2;
    final OzoneConfiguration config = new OzoneConfiguration();
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    config.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION,
        false);
    // turning off this config will ensure, pipeline creation is determined by
    // metadata volume count.
    config.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 0);
    MockNodeManager nodeManagerMock = new MockNodeManager(true,
        3);
    nodeManagerMock.setNumMetaDataVolumes(numMetaDataVolumes);
    int pipelinePerDn = numMetaDataVolumes *
        MockNodeManager.NUM_PIPELINE_PER_METADATA_DISK;
    nodeManagerMock.setNumPipelinePerDatanode(pipelinePerDn);
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(config, nodeManagerMock,
            scmMetadataStore.getPipelineTable(), new EventQueue());
    pipelineManager.allowPipelineCreation();
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManagerMock,
            pipelineManager.getStateManager(), config);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    MetricsRecordBuilder metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    long numPipelineAllocated = getLongCounter("NumPipelineAllocated",
        metrics);
    Assert.assertEquals(0, numPipelineAllocated);

    // one node pipeline creation will not be accounted for
    // pipeline limit determination
    pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE);
    // max limit on no of pipelines is 4
    for (int i = 0; i < pipelinePerDn; i++) {
      Pipeline pipeline = pipelineManager
          .createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      Assert.assertNotNull(pipeline);
    }

    metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    numPipelineAllocated = getLongCounter("NumPipelineAllocated", metrics);
    Assert.assertEquals(5, numPipelineAllocated);

    long numPipelineCreateFailed = getLongCounter(
        "NumPipelineCreationFailed", metrics);
    Assert.assertEquals(0, numPipelineCreateFailed);
    //This should fail...
    try {
      pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
      fail();
    } catch (SCMException ioe) {
      // pipeline creation failed this time.
      Assert.assertEquals(SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE,
          ioe.getResult());
    }

    metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    numPipelineAllocated = getLongCounter("NumPipelineAllocated", metrics);
    Assert.assertEquals(5, numPipelineAllocated);

    numPipelineCreateFailed = getLongCounter(
        "NumPipelineCreationFailed", metrics);
    Assert.assertEquals(1, numPipelineCreateFailed);

    // clean up
    pipelineManager.close();
  }

  @Test
  public void testActivateDeactivatePipeline() throws IOException {
    final SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), new EventQueue());
    pipelineManager.allowPipelineCreation();
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
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), eventQueue);
    pipelineManager.allowPipelineCreation();
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    pipelineManager.onMessage(
        new SCMSafeModeManager.SafeModeStatus(true, true), null);
    Pipeline pipeline = pipelineManager
        .createPipeline(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    // close manager
    pipelineManager.close();
    // new pipeline manager loads the pipelines from the db in ALLOCATED state
    pipelineManager =
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), eventQueue);
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

    EventQueue eventQueue = new EventQueue();
    final SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), eventQueue);
    pipelineManager.allowPipelineCreation();
    final PipelineProvider ratisProvider = new MockRatisPipelineProvider(
        nodeManager, pipelineManager.getStateManager(), conf, eventQueue,
        false);

    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        ratisProvider);

    Pipeline pipeline = pipelineManager
        .createPipeline(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    // At this point, pipeline is not at OPEN stage.
    Assert.assertEquals(Pipeline.PipelineState.ALLOCATED,
        pipeline.getPipelineState());

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

  @Test
  public void testPipelineNotCreatedUntilSafeModePrecheck()
      throws IOException, TimeoutException, InterruptedException {
    // No timeout for pipeline scrubber.
    conf.setTimeDuration(
        OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT, -1,
        TimeUnit.MILLISECONDS);

    EventQueue eventQueue = new EventQueue();
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), eventQueue);
    final PipelineProvider ratisProvider = new MockRatisPipelineProvider(
        nodeManager, pipelineManager.getStateManager(), conf, eventQueue,
        false);

    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        ratisProvider);

    try {
      Pipeline pipeline = pipelineManager
          .createPipeline(HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.THREE);
      fail("Pipelines should not have been created");
    } catch (IOException e) {
      // expected
    }

    // Ensure a pipeline of factor ONE can be created - no exceptions should be
    // raised.
    Pipeline pipeline = pipelineManager
        .createPipeline(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE);

    // Simulate safemode check exiting.
    pipelineManager.onMessage(
        new SCMSafeModeManager.SafeModeStatus(true, true), null);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return pipelineManager.getPipelines().size() != 0;
      }
    }, 100, 10000);
    pipelineManager.close();
  }

  @Test
  public void testSafeModeUpdatedOnSafemodeExit()
      throws IOException, TimeoutException, InterruptedException {
    // No timeout for pipeline scrubber.
    conf.setTimeDuration(
        OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT, -1,
        TimeUnit.MILLISECONDS);

    EventQueue eventQueue = new EventQueue();
    SCMPipelineManager pipelineManager =
        new SCMPipelineManager(conf, nodeManager,
            scmMetadataStore.getPipelineTable(), eventQueue);
    final PipelineProvider ratisProvider = new MockRatisPipelineProvider(
        nodeManager, pipelineManager.getStateManager(), conf, eventQueue,
        false);

    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        ratisProvider);

    assertEquals(true, pipelineManager.getSafeModeStatus());
    assertEquals(false, pipelineManager.isPipelineCreationAllowed());
    // First pass pre-check as true, but safemode still on
    pipelineManager.onMessage(
        new SCMSafeModeManager.SafeModeStatus(true, true), null);
    assertEquals(true, pipelineManager.getSafeModeStatus());
    assertEquals(true, pipelineManager.isPipelineCreationAllowed());

    // Then also turn safemode off
    pipelineManager.onMessage(
        new SCMSafeModeManager.SafeModeStatus(false, true), null);
    assertEquals(false, pipelineManager.getSafeModeStatus());
    assertEquals(true, pipelineManager.isPipelineCreationAllowed());
    pipelineManager.close();
  }

  /**
   * This test was created for HDDS-3925 to check whether the db handling is
   * proper at the SCMPipelineManager level. We should remove this test
   * when we remove the key swap from the SCMPipelineManager code.
   *
   * The test emulates internally the values that the iterator will provide
   * back to the check-fix code path. The iterator internally deserialize the
   * key stored in RocksDB using the PipelineIDCodec. The older version of the
   * codec serialized the PipelineIDs by taking the byte[] representation of
   * the protobuf representation of the PipelineID, and deserialization was not
   * implemented.
   *
   * In order to be able to check and fix the change, the deserialization was
   * introduced, and deserialisation of the old protobuf byte representation
   * with the new deserialization logic of the keys are
   * checked against the PipelineID serialized in the value as well via
   * protobuf.
   * The DB is storing the keys now based on a byte[] serialized from the UUID
   * inside the PipelineID.
   * For this we emulate the getKey of the KeyValue returned by the
   * iterator to return a PipelineID that is deserialized from the byte[]
   * representation of the protobuf representation of the PipelineID in the
   * test, as that would be the value we get from the iterator when iterating
   * through a table with the old key format.
   *
   * @throws Exception when something goes wrong
   */
  @Test
  public void testPipelineDBKeyFormatChange() throws Exception {
    Pipeline p1 = pipelineStub();
    Pipeline p2 = pipelineStub();
    Pipeline p3 = pipelineStub();

    TableIterator<PipelineID, KeyValue<PipelineID, Pipeline>> iteratorMock =
        mock(TableIterator.class);

    KeyValue<PipelineID, Pipeline> kv1 =
        mockKeyValueToProvideOldKeyFormat(p1);
    KeyValue<PipelineID, Pipeline> kv2 =
        mockKeyValueToProvideNormalFormat(p2);
    KeyValue<PipelineID, Pipeline> kv3 =
        mockKeyValueToProvideOldKeyFormat(p3);

    when(iteratorMock.next())
        .thenReturn(kv1, kv2, kv3)
        .thenThrow(new NoSuchElementException());
    when(iteratorMock.hasNext())
        .thenReturn(true, true, true, false);

    Table<PipelineID, Pipeline> pipelineStore = mock(Table.class);
    doReturn(iteratorMock).when(pipelineStore).iterator();
    when(pipelineStore.isEmpty()).thenReturn(false);

    InOrder inorderVerifier = inOrder(pipelineStore, iteratorMock);

    new SCMPipelineManager(conf, nodeManager, pipelineStore, new EventQueue());

    inorderVerifier.verify(iteratorMock).removeFromDB();
    inorderVerifier.verify(pipelineStore).put(p1.getId(), p1);
    inorderVerifier.verify(iteratorMock).removeFromDB();
    inorderVerifier.verify(pipelineStore).put(p3.getId(), p3);

    verify(pipelineStore, never()).put(p2.getId(), p2);
  }

  @Test
  public void testScmWithPipelineDBKeyFormatChange() throws Exception {
    TemporaryFolder tempDir = new TemporaryFolder();
    tempDir.create();
    File dir = tempDir.newFolder();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.getAbsolutePath());

    SCMMetadataStore scmDbWithOldKeyFormat = null;
    Map<UUID, Pipeline> oldPipelines = new HashMap<>();
    try {
      scmDbWithOldKeyFormat =
          new TestSCMStoreImplWithOldPipelineIDKeyFormat(conf);
      // Create 3 pipelines.
      for (int i = 0; i < 3; i++) {
        Pipeline pipeline = pipelineStub();
        scmDbWithOldKeyFormat.getPipelineTable()
            .put(pipeline.getId(), pipeline);
        oldPipelines.put(pipeline.getId().getId(), pipeline);
      }
    } finally {
      if (scmDbWithOldKeyFormat != null) {
        scmDbWithOldKeyFormat.stop();
      }
    }

    LogCapturer logCapturer =
        LogCapturer.captureLogs(SCMPipelineManager.getLog());

    // Create SCMPipelineManager with new DBDefinition.
    SCMMetadataStore newScmMetadataStore = null;
    try {
      newScmMetadataStore = new SCMMetadataStoreImpl(conf);
      SCMPipelineManager pipelineManager = new SCMPipelineManager(conf,
          nodeManager,
          newScmMetadataStore.getPipelineTable(),
          new EventQueue());

      waitForLog(logCapturer);
      assertEquals(3, pipelineManager.getPipelines().size());
      oldPipelines.values().forEach(p ->
          pipelineManager.containsPipeline(p.getId()));
    } finally {
      newScmMetadataStore.stop();
    }

    // Mimicking another restart.
    try {
      logCapturer.clearOutput();
      newScmMetadataStore = new SCMMetadataStoreImpl(conf);
      SCMPipelineManager pipelineManager = new SCMPipelineManager(conf,
          nodeManager,
          newScmMetadataStore.getPipelineTable(),
          new EventQueue());
      try {
        waitForLog(logCapturer);
        Assert.fail("Unexpected log: " + logCapturer.getOutput());
      } catch (TimeoutException ex) {
        Assert.assertTrue(ex.getMessage().contains("Timed out"));
      }
      assertEquals(3, pipelineManager.getPipelines().size());
      oldPipelines.values().forEach(p ->
          pipelineManager.containsPipeline(p.getId()));
    } finally {
      newScmMetadataStore.stop();
    }
  }

  private static void waitForLog(LogCapturer logCapturer)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
            .contains("Found pipeline in old format key"),
        1000, 5000);
  }

  private Pipeline pipelineStub() {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(
            Arrays.asList(
                nodeManager.getNodes(NodeStatus.inServiceHealthy()).get(0)
            )
        )
        .setNodesInOrder(Arrays.asList(0))
        .build();
  }

  private KeyValue<PipelineID, Pipeline>
      mockKeyValueToProvideOldKeyFormat(Pipeline pipeline)
      throws IOException {
    KeyValue<PipelineID, Pipeline> kv = mock(KeyValue.class);
    when(kv.getValue()).thenReturn(pipeline);
    when(kv.getKey())
        .thenReturn(
            new PipelineIDCodec().fromPersistedFormat(
                pipeline.getId().getProtobuf().toByteArray()
            ));
    return kv;
  }

  private KeyValue<PipelineID, Pipeline>
      mockKeyValueToProvideNormalFormat(Pipeline pipeline)
      throws IOException {
    KeyValue<PipelineID, Pipeline> kv = mock(KeyValue.class);
    when(kv.getValue()).thenReturn(pipeline);
    when(kv.getKey()).thenReturn(pipeline.getId());
    return kv;
  }

  private void sendPipelineReport(DatanodeDetails dn,
      Pipeline pipeline, PipelineReportHandler pipelineReportHandler,
      boolean isLeader, EventQueue eventQueue) {
    PipelineReportFromDatanode report =
        TestUtils.getPipelineReportFromDatanode(dn, pipeline.getId(), isLeader);
    pipelineReportHandler.onMessage(report, eventQueue);
  }
}
