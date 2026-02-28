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

import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.ALLOCATED;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;
import static org.apache.ozone.test.MetricsAsserts.getLongCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.apache.ratis.util.Preconditions.assertInstanceOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBuffer;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBufferStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.HealthyPipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.TestClock;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.util.function.CheckedRunnable;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests for PipelineManagerImpl.
 */
public class TestPipelineManagerImpl {
  private OzoneConfiguration conf;
  private DBStore dbStore;
  private MockNodeManager nodeManager;
  private int maxPipelineCount;
  private SCMContext scmContext;
  private SCMServiceManager serviceManager;
  private StorageContainerManager scm;
  private TestClock testClock;

  @BeforeEach
  void init(@TempDir File testDir, @TempDir File dbDir) throws Exception {
    testClock = new TestClock(Instant.now(), ZoneOffset.UTC);
    conf = SCMTestUtils.getConf(dbDir);
    scm = HddsTestUtils.getScm(SCMTestUtils.getConf(testDir));

    // Mock Node Manager is not able to correctly set up things for the EC
    // placement policy (Rack Scatter), so just use the random one.
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_EC_IMPL_KEY,
        SCMContainerPlacementRandom.class.getName());
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    nodeManager = new MockNodeManager(true, 20);
    maxPipelineCount = nodeManager.getNodeCount(
        HddsProtos.NodeOperationalState.IN_SERVICE,
        HddsProtos.NodeState.HEALTHY) *
        conf.getInt(OZONE_DATANODE_PIPELINE_LIMIT,
            OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT) /
        HddsProtos.ReplicationFactor.THREE.getNumber();
    scmContext = new SCMContext.Builder()
        .setSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED)
        .setLeader(true)
        .setSCM(scm).build();
    serviceManager = new SCMServiceManager();
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  private PipelineManagerImpl createPipelineManager(boolean isLeader)
      throws IOException {
    return PipelineManagerImpl.newPipelineManager(conf,
        SCMHAManagerStub.getInstance(isLeader),
        nodeManager,
        SCMDBDefinition.PIPELINES.getTable(dbStore),
        new EventQueue(),
        scmContext,
        serviceManager,
        testClock);
  }

  private PipelineManagerImpl createPipelineManager(
      boolean isLeader, SCMHADBTransactionBuffer buffer) throws IOException {
    return PipelineManagerImpl.newPipelineManager(conf,
        SCMHAManagerStub.getInstance(isLeader, buffer),
        nodeManager,
        SCMDBDefinition.PIPELINES.getTable(dbStore),
        new EventQueue(),
        SCMContext.emptyContext(),
        serviceManager,
        new TestClock(Instant.now(), ZoneOffset.UTC));
  }

  @Test
  public void testCreatePipeline() throws Exception {
    SCMHADBTransactionBuffer buffer1 =
        new SCMHADBTransactionBufferStub(dbStore);
    PipelineManagerImpl pipelineManager =
        createPipelineManager(true, buffer1);
    assertTrue(pipelineManager.getPipelines().isEmpty());
    Pipeline pipeline1 = pipelineManager.createPipeline(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE));
    assertEquals(1, pipelineManager.getPipelines().size());
    assertTrue(pipelineManager.containsPipeline(pipeline1.getId()));

    Pipeline pipeline2 = pipelineManager.createPipeline(
        RatisReplicationConfig.getInstance(ReplicationFactor.ONE));
    assertEquals(2, pipelineManager.getPipelines().size());
    assertTrue(pipelineManager.containsPipeline(pipeline2.getId()));

    Pipeline builtPipeline = pipelineManager.buildECPipeline(
        new ECReplicationConfig(3, 2),
        Collections.emptyList(), Collections.emptyList());
    pipelineManager.addEcPipeline(builtPipeline);

    assertEquals(3, pipelineManager.getPipelines().size());
    assertTrue(pipelineManager.containsPipeline(
        builtPipeline.getId()));

    buffer1.close();
    pipelineManager.close();

    SCMHADBTransactionBuffer buffer2 =
        new SCMHADBTransactionBufferStub(dbStore);
    PipelineManagerImpl pipelineManager2 =
        createPipelineManager(true, buffer2);
    // Should be able to load previous pipelines.
    assertThat(pipelineManager2.getPipelines()).isNotEmpty();
    assertEquals(3, pipelineManager.getPipelines().size());
    Pipeline pipeline3 = pipelineManager2.createPipeline(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE));
    buffer2.close();
    assertEquals(4, pipelineManager2.getPipelines().size());
    assertTrue(pipelineManager2.containsPipeline(pipeline3.getId()));

    pipelineManager2.close();
  }

  @Test
  public void testCreatePipelineShouldFailOnFollower() throws Exception {
    try (PipelineManager pipelineManager = createPipelineManager(false)) {
      assertTrue(pipelineManager.getPipelines().isEmpty());
      assertFailsNotLeader(() -> pipelineManager.createPipeline(
              RatisReplicationConfig.getInstance(ReplicationFactor.THREE)));
    }
  }

  @Test
  public void testUpdatePipelineStates() throws Exception {
    SCMHADBTransactionBuffer buffer = new SCMHADBTransactionBufferStub(dbStore);
    PipelineManagerImpl pipelineManager =
        createPipelineManager(true, buffer);
    Table<PipelineID, Pipeline> pipelineStore =
        SCMDBDefinition.PIPELINES.getTable(dbStore);
    Pipeline pipeline = assertAllocate(pipelineManager);
    buffer.flush();
    assertEquals(ALLOCATED, pipelineStore.get(pipeline.getId()).getPipelineState());
    PipelineID pipelineID = pipeline.getId();

    pipelineManager.openPipeline(pipelineID);
    pipelineManager.addContainerToPipeline(pipelineID, ContainerID.valueOf(1));
    assertTrue(pipelineManager
        .getPipelines(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN).contains(pipeline));
    buffer.flush();
    assertTrue(pipelineStore.get(pipeline.getId()).isOpen());

    pipelineManager.deactivatePipeline(pipeline.getId());
    assertEquals(Pipeline.PipelineState.DORMANT, pipelineManager.getPipeline(pipelineID).getPipelineState());
    buffer.flush();
    assertEquals(Pipeline.PipelineState.DORMANT, pipelineStore.get(pipeline.getId()).getPipelineState());
    assertThat(pipelineManager
        .getPipelines(RatisReplicationConfig
            .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN)).doesNotContain(pipeline);
    assertEquals(1, pipelineManager.getPipelineCount(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.DORMANT));

    pipelineManager.activatePipeline(pipeline.getId());
    assertTrue(pipelineManager
        .getPipelines(RatisReplicationConfig
            .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN).contains(pipeline));
    assertEquals(1, pipelineManager.getPipelineCount(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.OPEN));
    buffer.flush();
    assertTrue(pipelineStore.get(pipeline.getId()).isOpen());
    pipelineManager.close();
  }

  @Test
  public void testOpenPipelineShouldFailOnFollower() throws Exception {
    try (PipelineManagerImpl pipelineManager = createPipelineManager(true)) {
      Pipeline pipeline = assertAllocate(pipelineManager);
      changeToFollower(pipelineManager);
      assertFailsNotLeader(
          () -> pipelineManager.openPipeline(pipeline.getId()));
    }
  }

  @Test
  public void testActivatePipelineShouldFailOnFollower() throws Exception {
    try (PipelineManagerImpl pipelineManager = createPipelineManager(true)) {
      Pipeline pipeline = assertAllocate(pipelineManager);
      changeToFollower(pipelineManager);
      assertFailsNotLeader(
          () -> pipelineManager.activatePipeline(pipeline.getId()));
    }
  }

  @Test
  public void testDeactivatePipelineShouldFailOnFollower() throws Exception {
    try (PipelineManagerImpl pipelineManager = createPipelineManager(true)) {
      Pipeline pipeline = assertAllocate(pipelineManager);
      changeToFollower(pipelineManager);
      assertThrows(SCMException.class,
          () -> pipelineManager.deactivatePipeline(pipeline.getId()));
    }
  }

  @Test
  public void testRemovePipeline() throws Exception {
    try (PipelineManagerImpl pipelineManager = createPipelineManager(true)) {
      Pipeline pipeline = assertAllocate(pipelineManager);

      // Open the pipeline
      pipelineManager.openPipeline(pipeline.getId());
      ContainerManager containerManager = scm.getContainerManager();
      ContainerInfo containerInfo = HddsTestUtils.
          getContainer(HddsProtos.LifeCycleState.CLOSED, pipeline.getId());
      ContainerID containerID = containerInfo.containerID();
      //Add Container to ContainerMap
      containerManager.getContainerStateManager().
          addContainer(containerInfo.getProtobuf());
      //Add Container to PipelineStateMap
      pipelineManager.addContainerToPipeline(pipeline.getId(), containerID);
      assertTrue(pipelineManager
          .getPipelines(RatisReplicationConfig
                  .getInstance(ReplicationFactor.THREE),
              Pipeline.PipelineState.OPEN).contains(pipeline));
      assertThrows(IOException.class, () -> pipelineManager.removePipeline(pipeline));
      // Should not be able to remove the OPEN pipeline.
      assertEquals(1, pipelineManager.getPipelines().size());

      // Destroy pipeline
      pipelineManager.closePipeline(pipeline.getId());
      pipelineManager.deletePipeline(pipeline.getId());

      assertThrows(PipelineNotFoundException.class, () -> pipelineManager.getPipeline(pipeline.getId()),
          "Pipeline should not have been retrieved");
    }
  }

  @Test
  public void testClosePipelineShouldFailOnFollower() throws Exception {
    try (PipelineManagerImpl pipelineManager = createPipelineManager(true)) {
      Pipeline pipeline = assertAllocate(pipelineManager);
      changeToFollower(pipelineManager);
      assertFailsNotLeader(
          () -> pipelineManager.closePipeline(pipeline.getId()));
    }
  }

  @Test
  public void testPipelineReport() throws Exception {
    try (PipelineManagerImpl pipelineManager = createPipelineManager(true)) {
      SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(conf,
          mock(NodeManager.class), pipelineManager, mock(ContainerManager.class),
          serviceManager, new EventQueue(), scmContext);
      Pipeline pipeline = pipelineManager
          .createPipeline(RatisReplicationConfig
              .getInstance(ReplicationFactor.THREE));

      // pipeline is not healthy until all dns report
      List<DatanodeDetails> nodes = pipeline.getNodes();
      assertFalse(
          pipelineManager.getPipeline(pipeline.getId()).isHealthy());
      // get pipeline report from each dn in the pipeline
      PipelineReportHandler pipelineReportHandler =
          new PipelineReportHandler(scmSafeModeManager, pipelineManager,
              SCMContext.emptyContext(), conf);
      nodes.subList(0, 2).forEach(dn -> sendPipelineReport(dn, pipeline,
          pipelineReportHandler, false));
      sendPipelineReport(nodes.get(nodes.size() - 1), pipeline,
          pipelineReportHandler, true);

      // pipeline is healthy when all dns report
      assertTrue(
          pipelineManager.getPipeline(pipeline.getId()).isHealthy());
      // pipeline should now move to open state
      assertTrue(
          pipelineManager.getPipeline(pipeline.getId()).isOpen());

      // close the pipeline
      pipelineManager.closePipeline(pipeline.getId());
      pipelineManager.deletePipeline(pipeline.getId());

      // pipeline report for destroyed pipeline should be ignored
      nodes.subList(0, 2).forEach(dn -> sendPipelineReport(dn, pipeline,
          pipelineReportHandler, false));
      sendPipelineReport(nodes.get(nodes.size() - 1), pipeline,
          pipelineReportHandler, true);

      assertThrows(PipelineNotFoundException.class,
          () -> pipelineManager.getPipeline(pipeline.getId()));
    }
  }

  @Test
  public void testPipelineCreationFailedMetric() throws Exception {
    PipelineManagerImpl pipelineManager = createPipelineManager(true);

    // No pipeline at start
    MetricsRecordBuilder metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    long numPipelineAllocated = getLongCounter("NumPipelineAllocated",
        metrics);
    assertEquals(0, numPipelineAllocated);

    // 3 DNs are unhealthy.
    // Create 5 pipelines (Use up 15 Datanodes)

    for (int i = 0; i < maxPipelineCount; i++) {
      Pipeline pipeline = pipelineManager
          .createPipeline(RatisReplicationConfig
              .getInstance(ReplicationFactor.THREE));
      assertNotNull(pipeline);
    }

    metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    numPipelineAllocated = getLongCounter("NumPipelineAllocated", metrics);
    assertEquals(maxPipelineCount, numPipelineAllocated);

    long numPipelineCreateFailed = getLongCounter(
        "NumPipelineCreationFailed", metrics);
    assertEquals(0, numPipelineCreateFailed);

    //This should fail...
    SCMException e =
        assertThrows(SCMException.class,
            () -> pipelineManager.createPipeline(RatisReplicationConfig.getInstance(ReplicationFactor.THREE)));
    // pipeline creation failed this time.
    assertEquals(ResultCodes.FAILED_TO_FIND_SUITABLE_NODE, e.getResult());

    metrics = getMetrics(
        SCMPipelineMetrics.class.getSimpleName());
    numPipelineAllocated = getLongCounter("NumPipelineAllocated", metrics);
    assertEquals(maxPipelineCount, numPipelineAllocated);

    numPipelineCreateFailed = getLongCounter(
        "NumPipelineCreationFailed", metrics);
    assertEquals(1, numPipelineCreateFailed);

    // clean up
    pipelineManager.close();
  }

  @Test
  public void testPipelineOpenOnlyWhenLeaderReported() throws Exception {
    SCMHADBTransactionBuffer buffer1 =
        new SCMHADBTransactionBufferStub(dbStore);
    PipelineManagerImpl pipelineManager =
        createPipelineManager(true, buffer1);

    Pipeline pipeline = pipelineManager
        .createPipeline(RatisReplicationConfig
            .getInstance(ReplicationFactor.THREE));
    // close manager
    buffer1.close();
    pipelineManager.close();
    // new pipeline manager loads the pipelines from the db in ALLOCATED state
    pipelineManager = createPipelineManager(true);
    assertEquals(Pipeline.PipelineState.ALLOCATED,
        pipelineManager.getPipeline(pipeline.getId()).getPipelineState());

    SCMSafeModeManager scmSafeModeManager = new SCMSafeModeManager(new OzoneConfiguration(),
        mock(NodeManager.class), pipelineManager, mock(ContainerManager.class),
        serviceManager, new EventQueue(), scmContext);
    PipelineReportHandler pipelineReportHandler =
        new PipelineReportHandler(scmSafeModeManager, pipelineManager,
            SCMContext.emptyContext(), conf);

    // Report pipelines with leaders
    List<DatanodeDetails> nodes = pipeline.getNodes();
    assertEquals(3, nodes.size());
    // Send report for all but no leader
    nodes.forEach(dn -> sendPipelineReport(dn, pipeline, pipelineReportHandler,
        false));

    assertEquals(Pipeline.PipelineState.ALLOCATED,
        pipelineManager.getPipeline(pipeline.getId()).getPipelineState());

    nodes.subList(0, 2).forEach(dn -> sendPipelineReport(dn, pipeline,
        pipelineReportHandler, false));
    sendPipelineReport(nodes.get(nodes.size() - 1), pipeline,
        pipelineReportHandler, true);

    assertEquals(Pipeline.PipelineState.OPEN,
        pipelineManager.getPipeline(pipeline.getId()).getPipelineState());

    pipelineManager.close();
  }

  @Test
  public void testScrubPipelines() throws Exception {
    // Allocated pipelines should not be scrubbed for 50 seconds.
    conf.setTimeDuration(
        OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT, 50, TimeUnit.SECONDS);
    conf.setTimeDuration(
        OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, 50, TimeUnit.SECONDS);

    PipelineManagerImpl pipelineManager = createPipelineManager(true);
    Pipeline allocatedPipeline = pipelineManager
        .createPipeline(RatisReplicationConfig
            .getInstance(ReplicationFactor.THREE));
    // At this point, pipeline is not at OPEN stage.
    assertEquals(Pipeline.PipelineState.ALLOCATED,
        allocatedPipeline.getPipelineState());

    // pipeline should be seen in pipelineManager as ALLOCATED.
    assertTrue(pipelineManager
        .getPipelines(RatisReplicationConfig
            .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.ALLOCATED).contains(allocatedPipeline));

    Pipeline closedPipeline = pipelineManager
        .createPipeline(RatisReplicationConfig
            .getInstance(ReplicationFactor.THREE));
    pipelineManager.openPipeline(closedPipeline.getId());
    pipelineManager.closePipeline(closedPipeline.getId());

    // pipeline should be seen in pipelineManager as CLOSED.
    assertTrue(pipelineManager
        .getPipelines(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.CLOSED).contains(closedPipeline));

    // Set the clock to "now". All pipelines were created before this.
    testClock.set(Instant.now());

    pipelineManager.scrubPipelines();

    // The allocatedPipeline should not be scrubbed as the interval has not
    // yet passed.
    assertTrue(pipelineManager
        .getPipelines(RatisReplicationConfig
            .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.ALLOCATED).contains(allocatedPipeline));

    // The closedPipeline should not be scrubbed as the interval has not
    // yet passed.
    assertTrue(pipelineManager
        .getPipelines(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.CLOSED).contains(closedPipeline));

    testClock.fastForward((60000));

    pipelineManager.scrubPipelines();

    // The allocatedPipeline should now be scrubbed as the interval has passed
    assertThat(pipelineManager
        .getPipelines(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.ALLOCATED)).doesNotContain(allocatedPipeline);

    // The closedPipeline should now be scrubbed as the interval has passed
    assertThat(pipelineManager
        .getPipelines(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE),
            Pipeline.PipelineState.CLOSED)).doesNotContain(closedPipeline);

    pipelineManager.close();
  }

  @Test
  public void testScrubOpenWithUnregisteredNodes() throws Exception {
    PipelineManagerImpl pipelineManager = createPipelineManager(true);
    Pipeline pipeline = pipelineManager
        .createPipeline(new ECReplicationConfig(3, 2));
    pipelineManager.openPipeline(pipeline.getId());

    // Scrubbing the pipelines should not affect this pipeline
    pipelineManager.scrubPipelines();
    pipeline = pipelineManager.getPipeline(pipeline.getId());
    assertEquals(Pipeline.PipelineState.OPEN,
        pipeline.getPipelineState());

    // Now, "unregister" one of the nodes in the pipeline
    DatanodeDetails firstDN = nodeManager.getNode(pipeline.getNodes().get(0).getID());
    nodeManager.getClusterNetworkTopologyMap().remove(firstDN);

    pipelineManager.scrubPipelines();
    pipeline = pipelineManager.getPipeline(pipeline.getId());
    assertEquals(Pipeline.PipelineState.CLOSED,
        pipeline.getPipelineState());
  }

  @Test
  public void testScrubPipelinesShouldFailOnFollower() throws Exception {
    conf.setTimeDuration(
        OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT, 10, TimeUnit.SECONDS);

    try (PipelineManagerImpl pipelineManager = createPipelineManager(true)) {
      assertAllocate(pipelineManager);
      changeToFollower(pipelineManager);
      testClock.fastForward(20000);
      assertThrows(SCMException.class,
          pipelineManager::scrubPipelines);
    }
  }

  @Test
  public void testPipelineNotCreatedUntilSafeModePrecheck() throws Exception {
    // No timeout for pipeline scrubber.
    conf.setTimeDuration(
        OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT, -1,
        TimeUnit.MILLISECONDS);

    scmContext.updateSafeModeStatus(SafeModeStatus.INITIAL);

    PipelineManagerImpl pipelineManager = createPipelineManager(true);
    assertThrows(IOException.class,
        () -> pipelineManager.createPipeline(RatisReplicationConfig.getInstance(ReplicationFactor.THREE)),
        "Pipelines should not have been created");
    // No pipeline is created.
    assertTrue(pipelineManager.getPipelines().isEmpty());

    // Ensure a pipeline of factor ONE can be created - no exceptions should be
    // raised.
    Pipeline pipeline = pipelineManager
        .createPipeline(RatisReplicationConfig
            .getInstance(ReplicationFactor.ONE));
    assertTrue(pipelineManager
        .getPipelines(RatisReplicationConfig
            .getInstance(ReplicationFactor.ONE))
        .contains(pipeline));

    // Simulate safemode check exiting.
    scmContext.updateSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED);
    GenericTestUtils.waitFor(() -> !pipelineManager.getPipelines().isEmpty(),
        100, 10000);
    pipelineManager.close();
  }

  @Test
  public void testSafeModeUpdatedOnSafemodeExit() throws Exception {
    // No timeout for pipeline scrubber.
    conf.setTimeDuration(
        OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT, -1,
        TimeUnit.MILLISECONDS);

    PipelineManagerImpl pipelineManager = createPipelineManager(true);

    scmContext.updateSafeModeStatus(SafeModeStatus.INITIAL);
    assertTrue(pipelineManager.getSafeModeStatus());
    assertFalse(pipelineManager.isPipelineCreationAllowed());

    // First pass pre-check as true, but safemode still on
    // Simulate safemode check exiting.
    scmContext.updateSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED);
    assertTrue(pipelineManager.getSafeModeStatus());
    assertTrue(pipelineManager.isPipelineCreationAllowed());

    // Then also turn safemode off
    scmContext.updateSafeModeStatus(SafeModeStatus.OUT_OF_SAFE_MODE);
    assertFalse(pipelineManager.getSafeModeStatus());
    assertTrue(pipelineManager.isPipelineCreationAllowed());
    pipelineManager.close();
  }

  @Test
  public void testAddContainerWithClosedPipelineScmStart() throws Exception {
    LogCapturer logCapturer = LogCapturer.captureLogs(PipelineStateMap.class);
    SCMHADBTransactionBuffer buffer = new SCMHADBTransactionBufferStub(dbStore);
    PipelineManagerImpl pipelineManager =
            createPipelineManager(true, buffer);
    Table<PipelineID, Pipeline> pipelineStore =
            SCMDBDefinition.PIPELINES.getTable(dbStore);
    Pipeline pipeline = pipelineManager.createPipeline(
            RatisReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.THREE));
    PipelineID pipelineID = pipeline.getId();
    pipelineManager.addContainerToPipeline(pipelineID, ContainerID.valueOf(1));
    pipelineManager.getStateManager().updatePipelineState(
            pipelineID.getProtobuf(), HddsProtos.PipelineState.PIPELINE_CLOSED);
    buffer.flush();
    assertTrue(pipelineStore.get(pipelineID).isClosed());
    pipelineManager.addContainerToPipelineSCMStart(pipelineID,
            ContainerID.valueOf(2));
    assertThat(logCapturer.getOutput()).contains("Container " +
            ContainerID.valueOf(2) + " in open state for pipeline=" +
            pipelineID + " in closed state");
  }

  @Test
  public void testAddContainerWithClosedPipeline() throws Exception {
    SCMHADBTransactionBuffer buffer = new SCMHADBTransactionBufferStub(dbStore);
    PipelineManagerImpl pipelineManager =
        createPipelineManager(true, buffer);
    Table<PipelineID, Pipeline> pipelineStore =
        SCMDBDefinition.PIPELINES.getTable(dbStore);
    Pipeline pipeline = pipelineManager.createPipeline(
        RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE));
    PipelineID pipelineID = pipeline.getId();
    pipelineManager.addContainerToPipeline(pipelineID, ContainerID.valueOf(1));
    pipelineManager.getStateManager().updatePipelineState(
        pipelineID.getProtobuf(), HddsProtos.PipelineState.PIPELINE_CLOSED);
    buffer.flush();
    assertTrue(pipelineStore.get(pipelineID).isClosed());
    assertThrows(InvalidPipelineStateException.class,
        () -> pipelineManager.addContainerToPipeline(pipelineID,
        ContainerID.valueOf(2)));
  }

  @Test
  public void testPipelineCloseFlow() throws IOException {
    LogCapturer logCapturer = LogCapturer.captureLogs(PipelineManagerImpl.class);
    PipelineManagerImpl pipelineManager = createPipelineManager(true);
    Pipeline pipeline = pipelineManager.createPipeline(
            RatisReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.THREE));
    PipelineID pipelineID = pipeline.getId();
    ContainerManager containerManager = scm.getContainerManager();
    ContainerInfo containerInfo = HddsTestUtils.
        getContainer(HddsProtos.LifeCycleState.CLOSED, pipelineID);
    ContainerID containerID = containerInfo.containerID();
    //Add Container to ContainerMap
    containerManager.getContainerStateManager().
            addContainer(containerInfo.getProtobuf());
    //Add Container to PipelineStateMap
    pipelineManager.addContainerToPipeline(pipelineID, containerID);
    pipelineManager.closePipeline(pipelineID);
    String containerExpectedOutput = "Container " + containerID +
            " closed for pipeline=" + pipelineID;
    String pipelineExpectedOutput =
        "Pipeline " + pipelineID + " moved to CLOSED state";
    String logOutput = logCapturer.getOutput();
    assertThat(logOutput).contains(containerExpectedOutput);
    assertThat(logOutput).contains(pipelineExpectedOutput);

    int containerLogIdx = logOutput.indexOf(containerExpectedOutput);
    int pipelineLogIdx = logOutput.indexOf(pipelineExpectedOutput);
    assertThat(containerLogIdx).isLessThan(pipelineLogIdx);
  }

  @Test
  public void testGetStalePipelines() throws IOException {
    SCMHADBTransactionBuffer buffer =
            new SCMHADBTransactionBufferStub(dbStore);
    PipelineManagerImpl pipelineManager =
            spy(createPipelineManager(true, buffer));

    // For existing pipelines
    List<Pipeline> pipelines = new ArrayList<>();
    final DatanodeID[] ids = new DatanodeID[3];
    String[] ipAddresses = new String[3];
    String[] hostNames = new String[3];
    for (int i = 0; i < 3; i++) {
      ids[i] = DatanodeID.randomID();
      ipAddresses[i] = "1.2.3." + (i + 1);
      hostNames[i] = "host" + i;

      Pipeline pipeline = mock(Pipeline.class);
      DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);
      when(datanodeDetails.getID()).thenReturn(ids[i]);
      when(datanodeDetails.getIpAddress()).thenReturn(ipAddresses[i]);
      when(datanodeDetails.getHostName()).thenReturn(hostNames[i]);
      List<DatanodeDetails> nodes = new ArrayList<>();
      nodes.add(datanodeDetails);
      when(pipeline.getNodes()).thenReturn(nodes);
      pipelines.add(pipeline);
    }

    List<DatanodeDetails> nodes = new ArrayList<>();
    nodes.add(pipelines.get(0).getNodes().get(0));
    nodes.add(pipelines.get(1).getNodes().get(0));
    nodes.add(pipelines.get(2).getNodes().get(0));
    Pipeline pipeline = mock(Pipeline.class);
    when(pipeline.getNodes()).thenReturn(nodes);
    pipelines.add(pipeline);

    doReturn(pipelines).when(pipelineManager).getPipelines();

    // node with changed uuid
    DatanodeDetails node0 = mock(DatanodeDetails.class);
    DatanodeID changedUUID = DatanodeID.randomID();
    when(node0.getID()).thenReturn(changedUUID);
    when(node0.getIpAddress()).thenReturn(ipAddresses[0]);
    when(node0.getHostName()).thenReturn(hostNames[0]);

    // test uuid change
    assertTrue(pipelineManager.getStalePipelines(node0).isEmpty());

    // node with changed IP
    DatanodeDetails node1 = mock(DatanodeDetails.class);
    when(node1.getID()).thenReturn(ids[0]);
    when(node1.getIpAddress()).thenReturn("1.2.3.100");
    when(node1.getHostName()).thenReturn(hostNames[0]);

    // test IP change
    List<Pipeline> pipelineList1 = pipelineManager.getStalePipelines(node1);
    assertEquals(2, pipelineList1.size());
    assertEquals(pipelines.get(0), pipelineList1.get(0));
    assertEquals(pipelines.get(3), pipelineList1.get(1));

    // node with changed host name
    DatanodeDetails node2 = mock(DatanodeDetails.class);
    when(node2.getID()).thenReturn(ids[0]);
    when(node2.getIpAddress()).thenReturn(ipAddresses[0]);
    when(node2.getHostName()).thenReturn("host100");

    // test IP change
    List<Pipeline> pipelineList2 = pipelineManager.getStalePipelines(node2);
    assertEquals(2, pipelineList2.size());
    assertEquals(pipelines.get(0), pipelineList2.get(0));
    assertEquals(pipelines.get(3), pipelineList2.get(1));
  }

  @Test
  public void testCloseStalePipelines() throws IOException {
    SCMHADBTransactionBuffer buffer =
            new SCMHADBTransactionBufferStub(dbStore);
    PipelineManagerImpl pipelineManager =
            spy(createPipelineManager(true, buffer));

    Pipeline pipeline0 = mock(Pipeline.class);
    Pipeline pipeline1 = mock(Pipeline.class);
    when(pipeline0.getId()).thenReturn(mock(PipelineID.class));
    when(pipeline1.getId()).thenReturn(mock(PipelineID.class));
    DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);
    List<Pipeline> stalePipelines = Lists.newArrayList(pipeline0, pipeline1);
    doReturn(stalePipelines).when(pipelineManager)
            .getStalePipelines(datanodeDetails);

    pipelineManager.closeStalePipelines(datanodeDetails);
    verify(pipelineManager, times(1))
            .closePipeline(stalePipelines.get(0).getId());
    verify(pipelineManager, times(1))
            .closePipeline(stalePipelines.get(1).getId());
  }

  @Test
  public void testWaitForAllocatedPipeline() throws IOException {
    SCMHADBTransactionBuffer buffer =
            new SCMHADBTransactionBufferStub(dbStore);
    PipelineManagerImpl pipelineManager =
        createPipelineManager(true, buffer);

    PipelineManagerImpl pipelineManagerSpy = spy(pipelineManager);
    ReplicationConfig repConfig =
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
    PipelineChoosePolicy pipelineChoosingPolicy
        = new HealthyPipelineChoosePolicy();
    ContainerManager containerManager
        = mock(ContainerManager.class);
    
    WritableContainerProvider<ReplicationConfig> provider;
    String owner = "TEST";
    Pipeline allocatedPipeline;

    // Throw on pipeline creates, so no new pipelines can be created
    doThrow(SCMException.class).when(pipelineManagerSpy)
        .createPipeline(any(), any(), anyList());
    provider = new WritableRatisContainerProvider(
        pipelineManagerSpy, containerManager, pipelineChoosingPolicy);

    // Add a single pipeline to manager, (in the allocated state)
    allocatedPipeline = pipelineManager.createPipeline(repConfig);
    pipelineManager.getStateManager()
        .updatePipelineState(allocatedPipeline.getId()
            .getProtobuf(), HddsProtos.PipelineState.PIPELINE_ALLOCATED);

    // Assign a container to that pipeline
    ContainerInfo container = HddsTestUtils.
            getContainer(HddsProtos.LifeCycleState.OPEN,
                allocatedPipeline.getId());
    
    pipelineManager.addContainerToPipeline(
        allocatedPipeline.getId(), container.containerID());
    doReturn(container).when(containerManager).getMatchingContainer(anyLong(),
        anyString(), eq(allocatedPipeline), any());


    assertTrue(pipelineManager.getPipelines(repConfig,  OPEN)
        .isEmpty(), "No open pipelines exist");
    assertTrue(pipelineManager.getPipelines(repConfig,  ALLOCATED)
        .contains(allocatedPipeline), "An allocated pipeline exists");

    // Instrument waitOnePipelineReady to open pipeline a bit after it is called
    Runnable r = () -> {
      try {
        Thread.sleep(100);
        pipelineManager.openPipeline(allocatedPipeline.getId());
      } catch (Exception e) {
        fail("exception on opening pipeline", e);
      }
    };
    doAnswer(call -> {
      new Thread(r).start();
      return call.callRealMethod();
    }).when(pipelineManagerSpy).waitOnePipelineReady(any(), anyLong());

    
    ContainerInfo c = provider.getContainer(1, repConfig,
        owner, new ExcludeList());
    assertEquals(c, container, "Expected container was returned");

    // Confirm that waitOnePipelineReady was called on allocated pipelines
    ArgumentCaptor<Collection<PipelineID>> captor =
        ArgumentCaptor.forClass(Collection.class);
    verify(pipelineManagerSpy, times(1))
        .waitOnePipelineReady(captor.capture(), anyLong());
    Collection<PipelineID> coll = captor.getValue();
    assertThat(coll)
        .withFailMessage("waitOnePipelineReady() was called on allocated pipeline")
        .contains(allocatedPipeline.getId());
    pipelineManager.close();
  }

  @Test
  public void testCreatePipelineWithStorageType() throws Exception {
    PipelineManagerImpl pipelineManager = createPipelineManager(true);

    // MockNodeManager creates storage reports with DISK type by default.
    // DISK-typed pipeline should succeed.
    Pipeline diskPipeline = pipelineManager.createPipeline(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        StorageType.DISK);
    assertNotNull(diskPipeline);
    assertEquals(3, diskPipeline.getNodes().size());

    // SSD-typed pipeline should fail since no nodes have SSD storage.
    assertThrows(IOException.class,
        () -> pipelineManager.createPipeline(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
            StorageType.SSD));

    // null StorageType should fall through to untyped creation.
    Pipeline untypedPipeline = pipelineManager.createPipeline(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        (StorageType) null);
    assertNotNull(untypedPipeline);
    assertEquals(3, untypedPipeline.getNodes().size());

    pipelineManager.close();
  }

  public void testCreatePipelineForRead() throws IOException {
    PipelineManager pipelineManager = createPipelineManager(true);
    List<DatanodeDetails> dns = nodeManager
        .getNodes(NodeStatus.inServiceHealthy())
        .stream()
        .limit(3)
        .collect(Collectors.toList());
    Set<ContainerReplica> replicas = createContainerReplicasList(dns);
    Pipeline pipeline = pipelineManager.createPipelineForRead(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE), replicas);
    assertEquals(3, pipeline.getNodes().size());
    for (DatanodeDetails dn : pipeline.getNodes())  {
      assertThat(dns).contains(dn);
    }
  }

  /**
   * {@link PipelineManager#hasEnoughSpace(Pipeline, long)} should return false if all the
   * volumes on any Datanode in the pipeline have less than equal to the space required for creating a new container.
   */
  @Test
  public void testHasEnoughSpace() throws IOException {
    // create a Mock NodeManager, the MockNodeManager class doesn't work for this test
    NodeManager mockedNodeManager = Mockito.mock(NodeManager.class);
    PipelineManagerImpl pipelineManager = PipelineManagerImpl.newPipelineManager(conf,
        SCMHAManagerStub.getInstance(true),
        mockedNodeManager,
        SCMDBDefinition.PIPELINES.getTable(dbStore),
        new EventQueue(),
        scmContext,
        serviceManager,
        testClock);

    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setNodes(ImmutableList.of(MockDatanodeDetails.randomDatanodeDetails(),
            MockDatanodeDetails.randomDatanodeDetails(),
            MockDatanodeDetails.randomDatanodeDetails()))
        .setState(OPEN)
        .setReplicationConfig(ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, THREE))
        .build();
    List<DatanodeDetails> nodes = pipeline.getNodes();
    assertEquals(3, nodes.size());

    long containerSize = 100L;

    // Case 1: All nodes have enough space.
    List<DatanodeInfo> datanodeInfoList = new ArrayList<>();
    for (DatanodeDetails dn : nodes) {
      // the method being tested needs NodeManager to return DatanodeInfo because DatanodeInfo has storage
      // information (it extends DatanodeDetails)
      DatanodeInfo info = new DatanodeInfo(dn, null, null);
      info.updateStorageReports(HddsTestUtils.createStorageReports(dn.getID(), 200L, 200L, 10L));
      doReturn(info).when(mockedNodeManager).getDatanodeInfo(dn);
      datanodeInfoList.add(info);
    }
    assertTrue(pipelineManager.hasEnoughSpace(pipeline, containerSize));

    // Case 2: One node does not have enough space.
    /*
     Interestingly, SCMCommonPlacementPolicy#hasEnoughSpace returns false if exactly the required amount of space
      is available. Which means it won't allow creating a pipeline on a node if all volumes have exactly 5 GB
      available. We follow the same behavior here in the case of a new replica.

      So here, remaining - committed == containerSize, and hasEnoughSpace returns false.
      TODO should this return true instead?
     */
    DatanodeInfo datanodeInfo = datanodeInfoList.get(0);
    datanodeInfo.updateStorageReports(HddsTestUtils.createStorageReports(datanodeInfo.getID(), 200L, 120L,
        20L));
    assertFalse(pipelineManager.hasEnoughSpace(pipeline, containerSize));

    // Case 3: All nodes do not have enough space.
    for (DatanodeInfo info : datanodeInfoList) {
      info.updateStorageReports(HddsTestUtils.createStorageReports(info.getID(), 200L, 100L, 20L));
    }
    assertFalse(pipelineManager.hasEnoughSpace(pipeline, containerSize));
  }

  private Set<ContainerReplica> createContainerReplicasList(
      List <DatanodeDetails> dns) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (DatanodeDetails dn : dns) {
      ContainerReplica r = ContainerReplica.newBuilder()
          .setBytesUsed(1)
          .setContainerID(ContainerID.valueOf(1))
          .setContainerState(StorageContainerDatanodeProtocolProtos
              .ContainerReplicaProto.State.CLOSED)
          .setKeyCount(1)
          .setOriginNodeId(DatanodeID.randomID())
          .setSequenceId(1)
          .setReplicaIndex(0)
          .setDatanodeDetails(dn)
          .build();
      replicas.add(r);
    }
    return replicas;
  }

  private void sendPipelineReport(
      DatanodeDetails dn, Pipeline pipeline,
      PipelineReportHandler pipelineReportHandler,
      boolean isLeader) {
    SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode report =
        HddsTestUtils.getPipelineReportFromDatanode(dn, pipeline.getId(),
            isLeader);
    pipelineReportHandler.onMessage(report, new EventQueue());
  }

  private static Pipeline assertAllocate(PipelineManagerImpl pipelineManager) {
    Pipeline pipeline = assertDoesNotThrow(
        () -> pipelineManager.createPipeline(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE)));
    assertEquals(1, pipelineManager.getPipelines().size());
    assertTrue(pipelineManager.containsPipeline(pipeline.getId()));
    assertEquals(ALLOCATED, pipeline.getPipelineState());
    return pipeline;
  }

  private static void changeToFollower(PipelineManagerImpl pipelineManager) {
    assertInstanceOf(pipelineManager.getScmhaManager(), SCMHAManagerStub.class)
        .setIsLeader(false);
  }

  private static void assertFailsNotLeader(CheckedRunnable<?> block) {
    SCMException e = assertThrows(SCMException.class, block::run);
    assertEquals(ResultCodes.SCM_NOT_LEADER, e.getResult());
    assertInstanceOf(NotLeaderException.class, e.getCause());
  }
}
