/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.chillmode;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.*;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/** Test class for SCMChillModeManager.
 */
public class TestSCMChillModeManager {

  private static EventQueue queue;
  private SCMChillModeManager scmChillModeManager;
  private static Configuration config;
  private List<ContainerInfo> containers;

  @Rule
  public Timeout timeout = new Timeout(1000 * 300);

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @BeforeClass
  public static void setUp() {
    queue = new EventQueue();
    config = new OzoneConfiguration();
  }

  @Test
  public void testChillModeState() throws Exception {
    // Test 1: test for 0 containers
    testChillMode(0);

    // Test 2: test for 20 containers
    testChillMode(20);
  }

  @Test
  public void testChillModeStateWithNullContainers() {
    new SCMChillModeManager(config, null, null, queue);
  }

  private void testChillMode(int numContainers) throws Exception {
    containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(numContainers));
    // Assign open state to containers to be included in the chill mode
    // container list
    for (ContainerInfo container : containers) {
      container.setState(HddsProtos.LifeCycleState.OPEN);
    }
    scmChillModeManager = new SCMChillModeManager(
        config, containers, null, queue);

    assertTrue(scmChillModeManager.getInChillMode());
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        HddsTestUtils.createNodeRegistrationContainerReport(containers));
    GenericTestUtils.waitFor(() -> {
      return !scmChillModeManager.getInChillMode();
    }, 100, 1000 * 5);
  }

  @Test
  public void testChillModeExitRule() throws Exception {
    containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(25 * 4));
    // Assign open state to containers to be included in the chill mode
    // container list
    for (ContainerInfo container : containers) {
      container.setState(HddsProtos.LifeCycleState.CLOSED);
    }
    scmChillModeManager = new SCMChillModeManager(
        config, containers, null, queue);

    assertTrue(scmChillModeManager.getInChillMode());

    testContainerThreshold(containers.subList(0, 25), 0.25);
    assertTrue(scmChillModeManager.getInChillMode());
    testContainerThreshold(containers.subList(25, 50), 0.50);
    assertTrue(scmChillModeManager.getInChillMode());
    testContainerThreshold(containers.subList(50, 75), 0.75);
    assertTrue(scmChillModeManager.getInChillMode());
    testContainerThreshold(containers.subList(75, 100), 1.0);

    GenericTestUtils.waitFor(() -> {
      return !scmChillModeManager.getInChillMode();
    }, 100, 1000 * 5);
  }


  private OzoneConfiguration createConf(double healthyPercent,
      double oneReplicaPercent) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.newFolder().toString());
    conf.setBoolean(
        HddsConfigKeys.HDDS_SCM_CHILLMODE_PIPELINE_AVAILABILITY_CHECK,
        true);
    conf.setDouble(HddsConfigKeys.
        HDDS_SCM_CHILLMODE_HEALTHY_PIPELINE_THRESHOLD_PCT, healthyPercent);
    conf.setDouble(HddsConfigKeys.
        HDDS_SCM_CHILLMODE_ONE_NODE_REPORTED_PIPELINE_PCT, oneReplicaPercent);

    return conf;
  }

  @Test
  public void testChillModeExitRuleWithPipelineAvailabilityCheck()
      throws Exception{
    testChillModeExitRuleWithPipelineAvailabilityCheck(100, 30, 8, 0.90, 1);
    testChillModeExitRuleWithPipelineAvailabilityCheck(100, 90, 22, 0.10, 0.9);
    testChillModeExitRuleWithPipelineAvailabilityCheck(100, 30, 8, 0, 0.9);
    testChillModeExitRuleWithPipelineAvailabilityCheck(100, 90, 22, 0, 0);
    testChillModeExitRuleWithPipelineAvailabilityCheck(100, 90, 22, 0, 0.5);
  }

  @Test
  public void testFailWithIncorrectValueForHealthyPipelinePercent()
      throws Exception {
    try {
      OzoneConfiguration conf = createConf(100,
          0.9);
      MockNodeManager mockNodeManager = new MockNodeManager(true, 10);
      PipelineManager pipelineManager = new SCMPipelineManager(conf,
          mockNodeManager, queue);
      scmChillModeManager = new SCMChillModeManager(
          conf, containers, pipelineManager, queue);
      fail("testFailWithIncorrectValueForHealthyPipelinePercent");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("value should be >= 0.0 and <=" +
          " 1.0", ex);
    }
  }

  @Test
  public void testFailWithIncorrectValueForOneReplicaPipelinePercent()
      throws Exception {
    try {
      OzoneConfiguration conf = createConf(0.9,
          200);
      MockNodeManager mockNodeManager = new MockNodeManager(true, 10);
      PipelineManager pipelineManager = new SCMPipelineManager(conf,
          mockNodeManager, queue);
      scmChillModeManager = new SCMChillModeManager(
          conf, containers, pipelineManager, queue);
      fail("testFailWithIncorrectValueForOneReplicaPipelinePercent");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("value should be >= 0.0 and <=" +
          " 1.0", ex);
    }
  }

  @Test
  public void testFailWithIncorrectValueForChillModePercent() throws Exception {
    try {
      OzoneConfiguration conf = createConf(0.9, 0.1);
      conf.setDouble(HddsConfigKeys.HDDS_SCM_CHILLMODE_THRESHOLD_PCT, -1.0);
      MockNodeManager mockNodeManager = new MockNodeManager(true, 10);
      PipelineManager pipelineManager = new SCMPipelineManager(conf,
          mockNodeManager, queue);
      scmChillModeManager = new SCMChillModeManager(
          conf, containers, pipelineManager, queue);
      fail("testFailWithIncorrectValueForChillModePercent");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("value should be >= 0.0 and <=" +
          " 1.0", ex);
    }
  }


  public void testChillModeExitRuleWithPipelineAvailabilityCheck(
      int containerCount, int nodeCount, int pipelineCount,
      double healthyPipelinePercent, double oneReplicaPercent)
      throws Exception {

    OzoneConfiguration conf = createConf(healthyPipelinePercent,
        oneReplicaPercent);

    containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(containerCount));

    MockNodeManager mockNodeManager = new MockNodeManager(true, nodeCount);
    SCMPipelineManager pipelineManager = new SCMPipelineManager(conf,
        mockNodeManager, queue);
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(mockNodeManager,
            pipelineManager.getStateManager(), config);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);


    for (int i=0; i < pipelineCount; i++) {
      pipelineManager.createPipeline(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
    }

    for (ContainerInfo container : containers) {
      container.setState(HddsProtos.LifeCycleState.CLOSED);
    }

    scmChillModeManager = new SCMChillModeManager(conf, containers,
        pipelineManager, queue);

    assertTrue(scmChillModeManager.getInChillMode());

    testContainerThreshold(containers, 1.0);

    List<Pipeline> pipelines = pipelineManager.getPipelines();

    int healthyPipelineThresholdCount =
        scmChillModeManager.getHealthyPipelineChillModeRule()
            .getHealthyPipelineThresholdCount();
    int oneReplicaThresholdCount =
        scmChillModeManager.getOneReplicaPipelineChillModeRule()
            .getThresholdCount();

    // Because even if no pipelines are there, and threshold we set to zero,
    // we shall a get an event when datanode is registered. In that case,
    // validate will return true, and add this to validatedRules.
    if (Math.max(healthyPipelinePercent, oneReplicaThresholdCount) == 0) {
      firePipelineEvent(pipelines.get(0));
    }

    for (int i = 0; i < Math.max(healthyPipelineThresholdCount,
        oneReplicaThresholdCount); i++) {
      firePipelineEvent(pipelines.get(i));

      if (i < healthyPipelineThresholdCount) {
        checkHealthy(i + 1);
      }

      if (i < oneReplicaThresholdCount) {
        checkOpen(i + 1);
      }
    }


    GenericTestUtils.waitFor(() -> {
      return !scmChillModeManager.getInChillMode();
    }, 100, 1000 * 5);
  }

  private void checkHealthy(int expectedCount) throws Exception{
    GenericTestUtils.waitFor(() -> scmChillModeManager
            .getHealthyPipelineChillModeRule()
            .getCurrentHealthyPipelineCount() == expectedCount,
        100,  5000);
  }

  private void checkOpen(int expectedCount) throws Exception {
    GenericTestUtils.waitFor(() -> scmChillModeManager
            .getOneReplicaPipelineChillModeRule()
            .getCurrentReportedPipelineCount() == expectedCount,
        1000,  5000);
  }

  private void firePipelineEvent(Pipeline pipeline) throws Exception {
    PipelineReportsProto.Builder reportBuilder =
        PipelineReportsProto.newBuilder();

    reportBuilder.addPipelineReport(PipelineReport.newBuilder()
        .setPipelineID(pipeline.getId().getProtobuf()));
    queue.fireEvent(SCMEvents.PROCESSED_PIPELINE_REPORT,
        new PipelineReportFromDatanode(pipeline.getNodes().get(0),
            reportBuilder.build()));
  }


  @Test
  public void testDisableChillMode() {
    OzoneConfiguration conf = new OzoneConfiguration(config);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED, false);
    PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    Mockito.doNothing().when(pipelineManager).startPipelineCreator();
    scmChillModeManager =
        new SCMChillModeManager(conf, containers, pipelineManager, queue);
    assertFalse(scmChillModeManager.getInChillMode());
  }

  @Test
  public void testChillModeDataNodeExitRule() throws Exception {
    containers = new ArrayList<>();
    testChillModeDataNodes(0);
    testChillModeDataNodes(3);
    testChillModeDataNodes(5);
  }

  /**
   * Check that containers in Allocated state are not considered while
   * computing percentage of containers with at least 1 reported replica in
   * chill mode exit rule.
   */
  @Test
  public void testContainerChillModeRule() throws Exception {
    containers = new ArrayList<>();
    // Add 100 containers to the list of containers in SCM
    containers.addAll(HddsTestUtils.getContainerInfo(25 * 4));
    // Assign CLOSED state to first 25 containers and OPEM state to rest
    // of the containers
    for (ContainerInfo container : containers.subList(0, 25)) {
      container.setState(HddsProtos.LifeCycleState.CLOSED);
    }
    for (ContainerInfo container : containers.subList(25, 100)) {
      container.setState(HddsProtos.LifeCycleState.OPEN);
    }

    scmChillModeManager = new SCMChillModeManager(
        config, containers, null, queue);

    assertTrue(scmChillModeManager.getInChillMode());

    // When 10 CLOSED containers are reported by DNs, the computed container
    // threshold should be 10/25 as there are only 25 CLOSED containers.
    // Containers in OPEN state should not contribute towards list of
    // containers while calculating container threshold in SCMChillNodeManager
    testContainerThreshold(containers.subList(0, 10), 0.4);
    assertTrue(scmChillModeManager.getInChillMode());

    // When remaining 15 OPEN containers are reported by DNs, the container
    // threshold should be (10+15)/25.
    testContainerThreshold(containers.subList(10, 25), 1.0);

    GenericTestUtils.waitFor(() -> {
      return !scmChillModeManager.getInChillMode();
    }, 100, 1000 * 5);
  }

  private void testChillModeDataNodes(int numOfDns) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration(config);
    conf.setInt(HddsConfigKeys.HDDS_SCM_CHILLMODE_MIN_DATANODE, numOfDns);
    scmChillModeManager = new SCMChillModeManager(
        conf, containers, null, queue);

    // Assert SCM is in Chill mode.
    assertTrue(scmChillModeManager.getInChillMode());

    // Register all DataNodes except last one and assert SCM is in chill mode.
    for (int i = 0; i < numOfDns-1; i++) {
      queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
          HddsTestUtils.createNodeRegistrationContainerReport(containers));
      assertTrue(scmChillModeManager.getInChillMode());
      assertTrue(scmChillModeManager.getCurrentContainerThreshold() == 1);
    }

    if(numOfDns == 0){
      GenericTestUtils.waitFor(() -> {
        return scmChillModeManager.getInChillMode();
      }, 10, 1000 * 10);
      return;
    }
    // Register last DataNode and check that SCM is out of Chill mode.
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        HddsTestUtils.createNodeRegistrationContainerReport(containers));
    GenericTestUtils.waitFor(() -> {
      return !scmChillModeManager.getInChillMode();
    }, 10, 1000 * 10);
  }

  private void testContainerThreshold(List<ContainerInfo> dnContainers,
      double expectedThreshold)
      throws Exception {
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        HddsTestUtils.createNodeRegistrationContainerReport(dnContainers));
    GenericTestUtils.waitFor(() -> {
      double threshold = scmChillModeManager.getCurrentContainerThreshold();
      return threshold == expectedThreshold;
    }, 100, 2000 * 9);
  }

  @Test
  public void testChillModePipelineExitRule() throws Exception {
    containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(25 * 4));
    String storageDir = GenericTestUtils.getTempPath(
        TestSCMChillModeManager.class.getName() + UUID.randomUUID());
    try{
      MockNodeManager nodeManager = new MockNodeManager(true, 3);
      config.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);
      // enable pipeline check
      config.setBoolean(
          HddsConfigKeys.HDDS_SCM_CHILLMODE_PIPELINE_AVAILABILITY_CHECK, true);

      SCMPipelineManager pipelineManager = new SCMPipelineManager(config,
          nodeManager, queue);

      PipelineProvider mockRatisProvider =
          new MockRatisPipelineProvider(nodeManager,
              pipelineManager.getStateManager(), config);
      pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
          mockRatisProvider);

      Pipeline pipeline = pipelineManager.createPipeline(
          HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
      PipelineReportsProto.Builder reportBuilder = PipelineReportsProto
          .newBuilder();
      reportBuilder.addPipelineReport(PipelineReport.newBuilder()
          .setPipelineID(pipeline.getId().getProtobuf()));

      scmChillModeManager = new SCMChillModeManager(
          config, containers, pipelineManager, queue);

      queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
          HddsTestUtils.createNodeRegistrationContainerReport(containers));
      assertTrue(scmChillModeManager.getInChillMode());

      // Trigger the processed pipeline report event
      queue.fireEvent(SCMEvents.PROCESSED_PIPELINE_REPORT,
          new PipelineReportFromDatanode(pipeline.getNodes().get(0),
              reportBuilder.build()));

      GenericTestUtils.waitFor(() -> {
        return !scmChillModeManager.getInChillMode();
      }, 100, 1000 * 10);
      pipelineManager.close();
    } finally {
      config.setBoolean(
          HddsConfigKeys.HDDS_SCM_CHILLMODE_PIPELINE_AVAILABILITY_CHECK,
          false);
      FileUtil.fullyDelete(new File(storageDir));
    }
  }
}