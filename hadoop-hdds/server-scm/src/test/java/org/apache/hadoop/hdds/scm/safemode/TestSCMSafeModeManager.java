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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Test class for SCMSafeModeManager.
 */
public class TestSCMSafeModeManager {

  private EventQueue queue;
  private SCMContext scmContext;
  private SCMServiceManager serviceManager;
  private SCMSafeModeManager scmSafeModeManager;
  private OzoneConfiguration config;
  private List<ContainerInfo> containers = Collections.emptyList();

  private SCMMetadataStore scmMetadataStore;
  @TempDir
  private File tempDir;

  @BeforeEach
  public void setUp() throws IOException {
    queue = new EventQueue();
    scmContext = SCMContext.emptyContext();
    serviceManager = new SCMServiceManager();
    config = new OzoneConfiguration();
    config.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION,
        false);
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.getAbsolutePath());
    config.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, 1);
    scmMetadataStore = new SCMMetadataStoreImpl(config);
  }

  @AfterEach
  public void destroyDbStore() throws Exception {
    if (scmSafeModeManager != null) {
      scmSafeModeManager.getSafeModeMetrics().unRegister();
    }
    if (scmMetadataStore.getStore() != null) {
      scmMetadataStore.getStore().close();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 20})
  public void testSafeModeState(int numContainers) throws Exception {
    testSafeMode(numContainers);
  }

  private void testSafeMode(int numContainers) throws Exception {
    containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(numContainers));

    // Currently, only considered containers which are not in open state.
    for (ContainerInfo container : containers) {
      container.setState(HddsProtos.LifeCycleState.CLOSED);
      container.setNumberOfKeys(10);
    }
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);
    scmSafeModeManager = new SCMSafeModeManager(config, null, null, containerManager,
        serviceManager, queue, scmContext);
    scmSafeModeManager.start();

    assertTrue(scmSafeModeManager.getInSafeMode());
    assertEquals(1, scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value());
    validateRuleStatus("DatanodeSafeModeRule", "registered datanodes 0");
    SCMDatanodeProtocolServer.NodeRegistrationContainerReport nodeRegistrationContainerReport =
        HddsTestUtils.createNodeRegistrationContainerReport(containers);
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT, nodeRegistrationContainerReport);
    queue.fireEvent(SCMEvents.CONTAINER_REGISTRATION_REPORT, nodeRegistrationContainerReport);

    long cutOff = (long) Math.ceil(numContainers * config.getDouble(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT));

    assertEquals(cutOff, scmSafeModeManager.getSafeModeMetrics()
        .getNumContainerWithOneReplicaReportedThreshold().value());

    GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(),
        100, 1000 * 5);
    GenericTestUtils.waitFor(() ->
            scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value() == 0,
        100, 1000 * 5);

    assertEquals(cutOff, scmSafeModeManager.getSafeModeMetrics()
        .getCurrentContainersWithOneReplicaReportedCount().value());

  }

  @Test
  public void testSafeModeExitRule() throws Exception {
    containers = new ArrayList<>();
    int numContainers = 100;
    containers.addAll(HddsTestUtils.getContainerInfo(numContainers));
    // Assign open state to containers to be included in the safe mode
    // container list
    for (ContainerInfo container : containers) {
      container.setState(HddsProtos.LifeCycleState.CLOSED);
      container.setNumberOfKeys(10);
    }
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);
    scmSafeModeManager = new SCMSafeModeManager(config, null, null, containerManager,
        serviceManager, queue, scmContext);
    scmSafeModeManager.start();

    long cutOff = (long) Math.ceil(numContainers * config.getDouble(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT));

    assertEquals(cutOff, scmSafeModeManager.getSafeModeMetrics()
        .getNumContainerWithOneReplicaReportedThreshold().value());

    assertTrue(scmSafeModeManager.getInSafeMode());
    assertEquals(1, scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value());
    validateRuleStatus("ContainerSafeModeRule",
        "0.00% of [Ratis] Containers(0 / 100) with at least one reported");
    testContainerThreshold(containers.subList(0, 25), 0.25);
    assertEquals(25, scmSafeModeManager.getSafeModeMetrics()
        .getCurrentContainersWithOneReplicaReportedCount().value());
    assertTrue(scmSafeModeManager.getInSafeMode());
    testContainerThreshold(containers.subList(25, 50), 0.50);
    assertEquals(50, scmSafeModeManager.getSafeModeMetrics()
        .getCurrentContainersWithOneReplicaReportedCount().value());
    assertTrue(scmSafeModeManager.getInSafeMode());
    testContainerThreshold(containers.subList(50, 75), 0.75);
    assertEquals(75, scmSafeModeManager.getSafeModeMetrics()
        .getCurrentContainersWithOneReplicaReportedCount().value());
    assertTrue(scmSafeModeManager.getInSafeMode());
    testContainerThreshold(containers.subList(75, 100), 1.0);
    assertEquals(100, scmSafeModeManager.getSafeModeMetrics()
        .getCurrentContainersWithOneReplicaReportedCount().value());

    GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(),
        100, 1000 * 5);
    GenericTestUtils.waitFor(() ->
            scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value() == 0,
        100, 1000 * 5);
  }

  private OzoneConfiguration createConf(double healthyPercent,
      double oneReplicaPercent) {
    OzoneConfiguration conf = new OzoneConfiguration(config);
    conf.setDouble(HddsConfigKeys.
        HDDS_SCM_SAFEMODE_HEALTHY_PIPELINE_THRESHOLD_PCT, healthyPercent);
    conf.setDouble(HddsConfigKeys.
        HDDS_SCM_SAFEMODE_ONE_NODE_REPORTED_PIPELINE_PCT, oneReplicaPercent);
    return conf;
  }

  @ParameterizedTest
  @CsvSource(value = {"100,0.9,false", "0.9,200,false", "0.9,0.1,true"})
  public void testHealthyPipelinePercentWithIncorrectValue(double healthyPercent,
                                                           double oneReplicaPercent,
                                                           boolean overrideScmSafeModeThresholdPct) throws Exception {
    OzoneConfiguration conf = createConf(healthyPercent, oneReplicaPercent);
    if (overrideScmSafeModeThresholdPct) {
      conf.setDouble(HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT, -1.0);
    }
    MockNodeManager mockNodeManager = new MockNodeManager(true, 10);
    PipelineManager pipelineManager = PipelineManagerImpl.newPipelineManager(
        conf,
        SCMHAManagerStub.getInstance(true),
        mockNodeManager,
        scmMetadataStore.getPipelineTable(),
        queue,
        scmContext,
        serviceManager,
        Clock.system(ZoneOffset.UTC));
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> new SCMSafeModeManager(conf, mockNodeManager, pipelineManager, containerManager,
            serviceManager, queue, scmContext));
    assertThat(exception).hasMessageEndingWith("value should be >= 0.0 and <= 1.0");
  }

  private static Stream<Arguments> testCaseForSafeModeExitRuleWithPipelineAvailabilityCheck() {
    return Stream.of(
        Arguments.of(100, 30, 8, 0.90, 1),
        Arguments.of(100, 90, 22, 0.10, 0.9),
        Arguments.of(100, 30, 8, 0, 0.9),
        Arguments.of(100, 90, 22, 0, 0.5)
    );
  }

  @ParameterizedTest
  @MethodSource("testCaseForSafeModeExitRuleWithPipelineAvailabilityCheck")
  public void testSafeModeExitRuleWithPipelineAvailabilityCheck(
      int containerCount, int nodeCount, int pipelineCount,
      double healthyPipelinePercent, double oneReplicaPercent)
      throws Exception {

    OzoneConfiguration conf = createConf(healthyPipelinePercent,
        oneReplicaPercent);

    containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(containerCount));

    MockNodeManager mockNodeManager = new MockNodeManager(true, nodeCount);
    PipelineManagerImpl pipelineManager =
        PipelineManagerImpl.newPipelineManager(
            conf,
            SCMHAManagerStub.getInstance(true),
            mockNodeManager,
            scmMetadataStore.getPipelineTable(),
            queue,
            scmContext,
            serviceManager,
            Clock.system(ZoneOffset.UTC));
    PipelineProvider<RatisReplicationConfig> mockRatisProvider =
        new MockRatisPipelineProvider(mockNodeManager,
            pipelineManager.getStateManager(), config);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    pipelineManager.getBackgroundPipelineCreator().stop();
    pipelineManager.getBackgroundPipelineScrubber().stop();

    for (int i = 0; i < pipelineCount; i++) {
      // Create pipeline
      Pipeline pipeline = pipelineManager.createPipeline(
          RatisReplicationConfig.getInstance(
              ReplicationFactor.THREE));

      pipelineManager.openPipeline(pipeline.getId());
      // Mark pipeline healthy
      pipeline = pipelineManager.getPipeline(pipeline.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline);
    }

    for (ContainerInfo container : containers) {
      container.setState(HddsProtos.LifeCycleState.CLOSED);
    }

    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);

    scmSafeModeManager =  new SCMSafeModeManager(conf, mockNodeManager, pipelineManager,
        containerManager, serviceManager, queue, scmContext);
    scmSafeModeManager.start();

    assertTrue(scmSafeModeManager.getInSafeMode());
    assertEquals(1, scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value());
    if (healthyPipelinePercent > 0) {
      validateRuleStatus("HealthyPipelineSafeModeRule",
          "healthy Ratis/THREE pipelines");
    }
    validateRuleStatus("OneReplicaPipelineSafeModeRule",
        "reported Ratis/THREE pipelines with at least one datanode");

    testContainerThreshold(containers, 1.0);

    List<Pipeline> pipelines = pipelineManager.getPipelines();

    int healthyPipelineThresholdCount = SafeModeRuleFactory.getInstance().
        getSafeModeRule(HealthyPipelineSafeModeRule.class).getHealthyPipelineThresholdCount();
    int oneReplicaThresholdCount = SafeModeRuleFactory.getInstance().
        getSafeModeRule(OneReplicaPipelineSafeModeRule.class).getThresholdCount();

    assertEquals(healthyPipelineThresholdCount,
        scmSafeModeManager.getSafeModeMetrics()
            .getNumHealthyPipelinesThreshold().value());

    assertEquals(oneReplicaThresholdCount,
        scmSafeModeManager.getSafeModeMetrics()
            .getNumPipelinesWithAtleastOneReplicaReportedThreshold().value());

    // Because even if no pipelines are there, and threshold we set to zero,
    // we shall a get an event when datanode is registered. In that case,
    // validate will return true, and add this to validatedRules.
    if (Math.max(healthyPipelinePercent, oneReplicaThresholdCount) == 0) {
      firePipelineEvent(pipelineManager, pipelines.get(0));
    }

    for (int i = 0; i < Math.max(healthyPipelineThresholdCount,
        Math.min(oneReplicaThresholdCount, pipelines.size())); i++) {
      firePipelineEvent(pipelineManager, pipelines.get(i));

      if (i < healthyPipelineThresholdCount) {
        checkHealthy(i + 1);
        assertEquals(i + 1,
            scmSafeModeManager.getSafeModeMetrics()
                .getCurrentHealthyPipelinesCount().value());
      }

      if (i < oneReplicaThresholdCount) {
        checkOpen(i + 1);
        assertEquals(i + 1,
            scmSafeModeManager.getSafeModeMetrics()
                .getCurrentPipelinesWithAtleastOneReplicaCount().value());
      }
    }

    assertEquals(healthyPipelineThresholdCount,
        scmSafeModeManager.getSafeModeMetrics()
            .getCurrentHealthyPipelinesCount().value());

    assertEquals(oneReplicaThresholdCount,
        scmSafeModeManager.getSafeModeMetrics()
            .getCurrentPipelinesWithAtleastOneReplicaCount().value());


    GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(),
        100, 1000 * 5);
    GenericTestUtils.waitFor(() ->
            scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value() == 0,
        100, 1000 * 5);
  }

  /**
   * @param safeModeRule verify that this rule is not satisfied
   * @param stringToMatch string to match in the rule status.
   */
  private void validateRuleStatus(String safeModeRule, String stringToMatch) {
    Set<Map.Entry<String, Pair<Boolean, String>>> ruleStatuses =
        scmSafeModeManager.getRuleStatus().entrySet();
    for (Map.Entry<String, Pair<Boolean, String>> entry : ruleStatuses) {
      if (entry.getKey().equals(safeModeRule)) {
        Pair<Boolean, String> value = entry.getValue();
        assertEquals(false, value.getLeft());
        assertThat(value.getRight()).contains(stringToMatch);
      }
    }
  }

  private void checkHealthy(int expectedCount) throws Exception {
    final HealthyPipelineSafeModeRule pipelineRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(HealthyPipelineSafeModeRule.class);
    GenericTestUtils.waitFor(() -> pipelineRule.getCurrentHealthyPipelineCount() == expectedCount,
        100,  5000);
  }

  private void checkOpen(int expectedCount) throws Exception {
    final OneReplicaPipelineSafeModeRule pipelineRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(OneReplicaPipelineSafeModeRule.class);
    GenericTestUtils.waitFor(() -> pipelineRule.getCurrentReportedPipelineCount() == expectedCount,
        1000,  5000);
  }

  private void firePipelineEvent(PipelineManager pipelineManager,
      Pipeline pipeline) throws Exception {
    pipelineManager.openPipeline(pipeline.getId());
    queue.fireEvent(SCMEvents.OPEN_PIPELINE,
        pipelineManager.getPipeline(pipeline.getId()));

    for (DatanodeDetails dn : pipeline.getNodes()) {
      List<StorageContainerDatanodeProtocolProtos.
              PipelineReport> reports = new ArrayList<>();
      HddsProtos.PipelineID pipelineID = pipeline.getId().getProtobuf();
      reports.add(StorageContainerDatanodeProtocolProtos
              .PipelineReport.newBuilder()
              .setPipelineID(pipelineID)
              .setIsLeader(true)
              .build());
      StorageContainerDatanodeProtocolProtos
              .PipelineReportsProto.Builder pipelineReportsProto =
              StorageContainerDatanodeProtocolProtos
                      .PipelineReportsProto.newBuilder();
      pipelineReportsProto.addAllPipelineReport(reports);
      queue.fireEvent(SCMEvents.PIPELINE_REPORT, new
              SCMDatanodeHeartbeatDispatcher
                      .PipelineReportFromDatanode(dn,
              pipelineReportsProto.build()));
    }
  }

  @Test
  public void testDisableSafeMode() {
    OzoneConfiguration conf = new OzoneConfiguration(config);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED, false);
    PipelineManager pipelineManager = mock(PipelineManager.class);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);
    NodeManager nodeManager = mock(SCMNodeManager.class);
    scmSafeModeManager = new SCMSafeModeManager(conf, nodeManager, pipelineManager,
        containerManager, serviceManager, queue, scmContext);
    assertFalse(scmSafeModeManager.getInSafeMode());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 5})
  public void testSafeModeDataNodeExitRule(int numberOfDns) throws Exception {
    containers = new ArrayList<>();
    testSafeModeDataNodes(numberOfDns);
  }

  /**
   * Check that containers in Allocated state are not considered while
   * computing percentage of containers with at least 1 reported replica in
   * safe mode exit rule.
   */
  @Test
  public void testContainerSafeModeRule() throws Exception {
    containers = new ArrayList<>();
    // Add 100 containers to the list of containers in SCM
    containers.addAll(HddsTestUtils.getContainerInfo(25 * 4));
    // Assign CLOSED state to first 25 containers and OPEN state to rest
    // of the containers. Set container key count = 10 in each container.
    for (ContainerInfo container : containers.subList(0, 25)) {
      container.setState(HddsProtos.LifeCycleState.CLOSED);
      container.setNumberOfKeys(10);
    }
    for (ContainerInfo container : containers.subList(25, 100)) {
      container.setState(HddsProtos.LifeCycleState.OPEN);
      container.setNumberOfKeys(10);
    }

    // Set the last 5 closed containers to be empty
    for (ContainerInfo container : containers.subList(20, 25)) {
      container.setNumberOfKeys(0);
    }

    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);

    scmSafeModeManager = new SCMSafeModeManager(config, null, null,
        containerManager, serviceManager, queue, scmContext);
    scmSafeModeManager.start();

    assertTrue(scmSafeModeManager.getInSafeMode());
    assertEquals(1, scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value());

    // When 10 CLOSED containers are reported by DNs, the computed container
    // threshold should be 10/20 as there are only 20 CLOSED NON-EMPTY
    // containers.
    // Containers in OPEN state should not contribute towards list of
    // containers while calculating container threshold in SCMSafeNodeManager
    testContainerThreshold(containers.subList(0, 10), 0.5);
    assertTrue(scmSafeModeManager.getInSafeMode());

    // When remaining 10 CLOSED NON-EMPTY containers are reported by DNs,
    // the container threshold should be (10+10)/20.
    testContainerThreshold(containers.subList(10, 25), 1.0);

    GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(),
        100, 1000 * 5);
    GenericTestUtils.waitFor(() ->
            scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value() == 0,
        100, 1000 * 5);
  }

  // We simulate common EC types: EC-2-2-1024K, EC-3-2-1024K, EC-6-3-1024K.
  static Stream<Arguments> processECDataParityCombination() {
    Stream<Arguments> args = Stream.of(arguments(2, 2),
        arguments(3, 2), arguments(6, 3));
    return args;
  }

  @ParameterizedTest
  @MethodSource("processECDataParityCombination")
  public void testContainerSafeModeRuleEC(int data, int parity) throws Exception {
    containers = new ArrayList<>();

    // We generate 100 EC Containers.
    containers.addAll(HddsTestUtils.getECContainerInfo(25 * 4, data, parity));

    // Prepare the data for the container.
    // We have prepared 25 containers in the CLOSED state and 75 containers in the OPEN state.
    // Out of the 25 containers, only 20 containers have a NumberOfKeys greater than 0.
    for (ContainerInfo container : containers.subList(0, 25)) {
      container.setState(HddsProtos.LifeCycleState.CLOSED);
      container.setNumberOfKeys(10);
    }

    for (ContainerInfo container : containers.subList(25, 100)) {
      container.setState(HddsProtos.LifeCycleState.OPEN);
      container.setNumberOfKeys(10);
    }

    // Set the last 5 closed containers to be empty
    for (ContainerInfo container : containers.subList(20, 25)) {
      container.setNumberOfKeys(0);
    }

    for (ContainerInfo container : containers) {
      scmMetadataStore.getContainerTable().put(container.containerID(), container);
    }

    // Declare SCMSafeModeManager and confirm entry into Safe Mode.
    EventQueue eventQueue = new EventQueue();
    MockNodeManager nodeManager = new MockNodeManager(true, 0);
    PipelineManager pipelineManager = PipelineManagerImpl.newPipelineManager(
        config,
        SCMHAManagerStub.getInstance(true),
        nodeManager,
        scmMetadataStore.getPipelineTable(),
        eventQueue,
        scmContext,
        serviceManager,
        Clock.system(ZoneOffset.UTC));

    ContainerManager containerManager = new ContainerManagerImpl(config,
        SCMHAManagerStub.getInstance(true), null, pipelineManager,
        scmMetadataStore.getContainerTable(),
        new ContainerReplicaPendingOps(Clock.system(ZoneId.systemDefault()), null));

    scmSafeModeManager = new SCMSafeModeManager(config, nodeManager, pipelineManager,
        containerManager, serviceManager, queue, scmContext);
    assertTrue(scmSafeModeManager.getInSafeMode());

    // Only 20 containers are involved in the calculation,
    // so when 10 containers complete registration, our threshold is 50%.
    testECContainerThreshold(containers.subList(0, 10), 0.5, data);
    assertTrue(scmSafeModeManager.getInSafeMode());

    // When the registration of the remaining containers is completed,
    // the threshold will reach 100%.
    testECContainerThreshold(containers.subList(10, 20), 1.0, data);

    ECContainerSafeModeRule ecContainerSafeModeRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(ECContainerSafeModeRule.class);
    assertTrue(ecContainerSafeModeRule.validate());

    RatisContainerSafeModeRule ratisContainerSafeModeRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(RatisContainerSafeModeRule.class);
    assertTrue(ratisContainerSafeModeRule.validate());
  }

  private void testSafeModeDataNodes(int numOfDns) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration(config);
    conf.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, numOfDns);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);
    scmSafeModeManager = new SCMSafeModeManager(conf, null, null,
        containerManager, serviceManager, queue, scmContext);
    scmSafeModeManager.start();

    // Assert SCM is in Safe mode.
    assertTrue(scmSafeModeManager.getInSafeMode());
    assertEquals(1, scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value());

    // Register all DataNodes except last one and assert SCM is in safe mode.
    for (int i = 0; i < numOfDns - 1; i++) {
      SCMDatanodeProtocolServer.NodeRegistrationContainerReport nodeRegistrationContainerReport =
          HddsTestUtils.createNodeRegistrationContainerReport(containers);
      queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT, nodeRegistrationContainerReport);
      queue.fireEvent(SCMEvents.CONTAINER_REGISTRATION_REPORT, nodeRegistrationContainerReport);
      assertTrue(scmSafeModeManager.getInSafeMode());
      assertEquals(1, SafeModeRuleFactory.getInstance()
          .getSafeModeRule(RatisContainerSafeModeRule.class).getCurrentContainerThreshold());
    }

    if (numOfDns == 0) {
      GenericTestUtils.waitFor(() -> scmSafeModeManager.getInSafeMode(),
          10, 1000 * 10);
      return;
    }
    // Register last DataNode and check that SCM is out of Safe mode.
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        HddsTestUtils.createNodeRegistrationContainerReport(containers));
    GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(),
        10, 1000 * 10);
    GenericTestUtils.waitFor(() ->
            scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value() == 0,
        100, 1000 * 5);
  }

  private void testContainerThreshold(List<ContainerInfo> dnContainers,
      double expectedThreshold)
      throws Exception {
    SCMDatanodeProtocolServer.NodeRegistrationContainerReport nodeRegistrationContainerReport =
        HddsTestUtils.createNodeRegistrationContainerReport(dnContainers);
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        nodeRegistrationContainerReport);
    queue.fireEvent(SCMEvents.CONTAINER_REGISTRATION_REPORT,
        nodeRegistrationContainerReport);
    final RatisContainerSafeModeRule containerRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(RatisContainerSafeModeRule.class);
    GenericTestUtils.waitFor(() ->
        containerRule.getCurrentContainerThreshold() == expectedThreshold,
        100, 2000 * 9);
  }

  /**
   * Test ECContainer reaching SafeMode threshold.
   *
   * @param dnContainers
   * The list of containers that need to reach the threshold.
   * @param expectedThreshold
   * The expected threshold.
   * @param dataBlockNum
   * The number of data blocks. For EC-3-2-1024K,
   * we need 3 registration requests to ensure the EC Container is confirmed.
   * For EC-6-3-1024K, we need 6 registration requests to ensure the EC Container is confirmed.
   * @throws Exception The thrown exception message.
   */
  private void testECContainerThreshold(List<ContainerInfo> dnContainers,
      double expectedThreshold, int dataBlockNum) throws Exception {

    // Step1. We need to ensure the number of confirmed EC data blocks
    // based on the quantity of dataBlockNum.
    for (int i = 0; i < dataBlockNum; i++) {
      SCMDatanodeProtocolServer.NodeRegistrationContainerReport nodeRegistrationContainerReport =
          HddsTestUtils.createNodeRegistrationContainerReport(dnContainers);
      queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
          nodeRegistrationContainerReport);
      queue.fireEvent(SCMEvents.CONTAINER_REGISTRATION_REPORT,
          nodeRegistrationContainerReport);
    }

    // Step2. Wait for the threshold to be reached.
    ECContainerSafeModeRule containerRule = SafeModeRuleFactory.getInstance()
        .getSafeModeRule(ECContainerSafeModeRule.class);
    GenericTestUtils.waitFor(() -> containerRule.getCurrentContainerThreshold() == expectedThreshold,
        100, 2000 * 9);
  }

  @Test
  public void testSafeModePipelineExitRule() throws Exception {
    containers = new ArrayList<>();
    containers.addAll(HddsTestUtils.getContainerInfo(25 * 4));
    MockNodeManager nodeManager = new MockNodeManager(true, 3);
    PipelineManagerImpl pipelineManager =
        PipelineManagerImpl.newPipelineManager(
            config,
            SCMHAManagerStub.getInstance(true),
            nodeManager,
            scmMetadataStore.getPipelineTable(),
            queue,
            scmContext,
            serviceManager,
            Clock.system(ZoneOffset.UTC));

    PipelineProvider<RatisReplicationConfig> mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), config);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    Pipeline pipeline = pipelineManager.createPipeline(
        RatisReplicationConfig.getInstance(
            ReplicationFactor.THREE));

    pipeline = pipelineManager.getPipeline(pipeline.getId());
    MockRatisPipelineProvider.markPipelineHealthy(pipeline);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);

    scmSafeModeManager = new SCMSafeModeManager(config, nodeManager, pipelineManager,
        containerManager, serviceManager, queue, scmContext);
    scmSafeModeManager.start();

    SCMDatanodeProtocolServer.NodeRegistrationContainerReport nodeRegistrationContainerReport =
        HddsTestUtils.createNodeRegistrationContainerReport(containers);
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT, nodeRegistrationContainerReport);
    queue.fireEvent(SCMEvents.CONTAINER_REGISTRATION_REPORT, nodeRegistrationContainerReport);
      

    assertTrue(scmSafeModeManager.getInSafeMode());
    assertEquals(1, scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value());

    firePipelineEvent(pipelineManager, pipeline);

    GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(),
        100, 1000 * 10);
    GenericTestUtils.waitFor(() ->
            scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value() == 0,
        100, 1000 * 5);
    pipelineManager.close();
  }

  @Test
  public void testPipelinesNotCreatedUntilPreCheckPasses() throws Exception {
    int numOfDns = 5;
    // enable pipeline check
    config.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, numOfDns);
    config.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION,
        true);

    MockNodeManager nodeManager = new MockNodeManager(true, numOfDns);

    PipelineManagerImpl pipelineManager =
        PipelineManagerImpl.newPipelineManager(
            config,
            SCMHAManagerStub.getInstance(true),
            nodeManager,
            scmMetadataStore.getPipelineTable(),
            queue,
            scmContext,
            serviceManager,
            Clock.system(ZoneOffset.UTC));

    PipelineProvider<RatisReplicationConfig> mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), config);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);

    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);

    scmSafeModeManager = new SCMSafeModeManager(config, nodeManager, pipelineManager,
        containerManager, serviceManager, queue, scmContext);
    scmSafeModeManager.start();

    // Assert SCM is in Safe mode.
    assertTrue(scmSafeModeManager.getInSafeMode());
    assertEquals(1, scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value());

    // stop background pipeline creator as we manually create
    // pipeline below
    pipelineManager.getBackgroundPipelineCreator().stop();

    // Register all DataNodes except last one and assert SCM is in safe mode.
    for (int i = 0; i < numOfDns - 1; i++) {
      SCMDatanodeProtocolServer.NodeRegistrationContainerReport nodeRegistrationContainerReport =
          HddsTestUtils.createNodeRegistrationContainerReport(containers);
      queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT, nodeRegistrationContainerReport);
      queue.fireEvent(SCMEvents.CONTAINER_REGISTRATION_REPORT, nodeRegistrationContainerReport);
      assertTrue(scmSafeModeManager.getInSafeMode());
      assertFalse(scmSafeModeManager.getPreCheckComplete());
    }
    queue.processAll(5000);

    // Register last DataNode and check that the SafeModeEvent gets fired, but
    // SafeMode is still enabled with preCheck completed.
    queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        HddsTestUtils.createNodeRegistrationContainerReport(containers));
    queue.processAll(5000);

    assertTrue(scmSafeModeManager.getPreCheckComplete());
    assertTrue(scmSafeModeManager.getInSafeMode());

    Pipeline pipeline = pipelineManager.createPipeline(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE));

    // Mark pipeline healthy
    pipeline = pipelineManager.getPipeline(pipeline.getId());
    MockRatisPipelineProvider.markPipelineHealthy(pipeline);

    firePipelineEvent(pipelineManager, pipeline);

    queue.processAll(5000);
    assertTrue(scmSafeModeManager.getPreCheckComplete());
    assertFalse(scmSafeModeManager.getInSafeMode());
    GenericTestUtils.waitFor(() ->
            scmSafeModeManager.getSafeModeMetrics().getScmInSafeMode().value() == 0,
        100, 1000 * 5);
  }

  /**
   * Test that each safemode rule's getStatusText is being logged periodically
   * while SCM is in safe mode, and that the logger stops with a final summary
   * when safe mode is force-exited.
   */
  @Test
  public void testSafeModePeriodicLoggingStopsOnForceExit() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration(config);
    conf.set(HddsConfigKeys.HDDS_SCM_SAFEMODE_LOG_INTERVAL, "500ms");
    conf.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, 3);

    containers = new ArrayList<>(HddsTestUtils.getContainerInfo(50));
    for (ContainerInfo container : containers) {
      container.setState(HddsProtos.LifeCycleState.CLOSED);
      container.setNumberOfKeys(10);
    }

    MockNodeManager mockNodeManager = new MockNodeManager(true, 5);
    PipelineManager pipelineManager = mock(PipelineManager.class);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(SCMSafeModeManager.getLogger());

    scmSafeModeManager = new SCMSafeModeManager(conf, mockNodeManager, pipelineManager,
        containerManager, serviceManager, queue, scmContext);
    scmSafeModeManager.start();
    assertTrue(scmSafeModeManager.getInSafeMode());

    try {
      // Verify periodic logging is active while in safe mode
      verifyPeriodicLoggingActive(logCapturer);

      logCapturer.clearOutput();
      scmSafeModeManager.forceExitSafeMode();
      assertFalse(scmSafeModeManager.getInSafeMode());

      // Verify final summary was logged with OUT_OF_SAFE_MODE state
      String exitLog = logCapturer.getOutput();
      assertThat(exitLog).contains("SCM SafeMode Status | state=OUT_OF_SAFE_MODE");
      assertThat(exitLog).contains("Stopped periodic Safe Mode logging");
      assertThat(exitLog).contains("SCM force-exiting safe mode");

    } finally {
      logCapturer.stopCapturing();
    }
  }

  /**
   * Test that each safemode rule's getStatusText is being logged periodically
   * while SCM is in safe mode, and that the logger stops with a final summary
   * when safe mode is exited normally.
   */
  @Test
  public void testSafeModePeriodicLoggingStopsOnNormalExit() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration(config);
    conf.set(HddsConfigKeys.HDDS_SCM_SAFEMODE_LOG_INTERVAL, "500ms");
    conf.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, 1);

    MockNodeManager mockNodeManager = new MockNodeManager(true, 3);
    ContainerManager containerManager = mock(ContainerManager.class);
    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(new ArrayList<>());
    
    PipelineManagerImpl pipelineManager = PipelineManagerImpl.newPipelineManager(
        conf,
        SCMHAManagerStub.getInstance(true),
        mockNodeManager,
        scmMetadataStore.getPipelineTable(),
        queue,
        scmContext,
        serviceManager,
        Clock.system(ZoneOffset.UTC));

    PipelineProvider<RatisReplicationConfig> mockRatisProvider =
        new MockRatisPipelineProvider(mockNodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    pipelineManager.getBackgroundPipelineCreator().stop();

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(SCMSafeModeManager.getLogger());

    scmSafeModeManager = new SCMSafeModeManager(conf, mockNodeManager, pipelineManager,
        containerManager, serviceManager, queue, scmContext);
    scmSafeModeManager.start();
    assertTrue(scmSafeModeManager.getInSafeMode());

    try {
      // Verify periodic logging is active while in safe mode
      verifyPeriodicLoggingActive(logCapturer);
      logCapturer.clearOutput();
      
      SCMDatanodeProtocolServer.NodeRegistrationContainerReport nodeReport =
          HddsTestUtils.createNodeRegistrationContainerReport(new ArrayList<>());
      queue.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT, nodeReport);
      queue.fireEvent(SCMEvents.CONTAINER_REGISTRATION_REPORT, nodeReport);
      queue.processAll(5000);
      GenericTestUtils.waitFor(() -> scmSafeModeManager.getPreCheckComplete(), 100, 5000);
      assertThat(logCapturer.getOutput()).contains("SCM SafeMode Status | state=PRE_CHECKS_PASSED");
      
      Pipeline pipeline = pipelineManager.createPipeline(
          RatisReplicationConfig.getInstance(ReplicationFactor.THREE));
      pipeline = pipelineManager.getPipeline(pipeline.getId());
      MockRatisPipelineProvider.markPipelineHealthy(pipeline);
      firePipelineEvent(pipelineManager, pipeline);
      queue.processAll(5000);
      
      GenericTestUtils.waitFor(() -> !scmSafeModeManager.getInSafeMode(), 100, 5000);

      // Verify final summary was logged with OUT_OF_SAFE_MODE state
      String exitLog = logCapturer.getOutput();
      assertThat(exitLog).contains("SCM SafeMode Status | state=OUT_OF_SAFE_MODE");
      assertThat(exitLog).contains("Stopped periodic Safe Mode logging");
      assertThat(exitLog).contains("SCM exiting safe mode");
      assertFalse(scmSafeModeManager.getInSafeMode());
      
    } finally {
      logCapturer.stopCapturing();
    }
  }
  
  /**
   * Verifies that periodic safe mode logging is working.
   * Checks that status messages are being logged at the configured interval.
   */
  private void verifyPeriodicLoggingActive(GenericTestUtils.LogCapturer logCapturer)
      throws InterruptedException {
    Map<String, Pair<Boolean, String>> ruleStatuses = scmSafeModeManager.getRuleStatus();
    for (int i = 0; i < 2; i++) {
      logCapturer.clearOutput();
      // Wait for configured interval (500ms + small buffer) for next log message
      Thread.sleep(600);
      String logOutput = logCapturer.getOutput();

      assertThat(logOutput).contains("SCM SafeMode Status | state=");
      for (String ruleName : ruleStatuses.keySet()) {
        assertThat(logOutput).contains("SCM SafeMode Status | " + ruleName);
      }
    }
  }
}
