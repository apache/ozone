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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * This class tests RatisContainerSafeModeRule.
 */
public class TestRatisContainerSafeModeRule {

  private ContainerManager containerManager;

  private RatisContainerSafeModeRule rule;

  @BeforeEach
  public void setup() {
    containerManager = mock(ContainerManager.class);
    ConfigurationSource conf = mock(ConfigurationSource.class);
    EventQueue eventQueue = mock(EventQueue.class);
    SCMSafeModeManager safeModeManager = mock(SCMSafeModeManager.class);
    SafeModeMetrics metrics = mock(SafeModeMetrics.class);

    when(safeModeManager.getSafeModeMetrics()).thenReturn(metrics);

    rule = new RatisContainerSafeModeRule(eventQueue, conf, containerManager, safeModeManager);
    rule.setValidateBasedOnReportProcessing(false);
  }

  @Test
  public void testRefreshInitializeRatisContainers() {
    List<ContainerInfo> containers = Arrays.asList(
        mockRatisContainer(LifeCycleState.CLOSED, 1L),
        mockRatisContainer(LifeCycleState.OPEN, 2L)
    );

    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(containers);

    rule.refresh(false);

    assertEquals(0.0, rule.getCurrentContainerThreshold());
  }

  @ParameterizedTest
  @EnumSource(value = LifeCycleState.class,
      names = {"OPEN", "CLOSING", "QUASI_CLOSED", "CLOSED", "DELETING", "DELETED", "RECOVERING"})
  public void testValidateReturnsTrueAndFalse(LifeCycleState state) {
    ContainerInfo container = mockRatisContainer(state, 1L);

    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(Collections.singletonList(container));

    boolean expected = state != LifeCycleState.QUASI_CLOSED && state != LifeCycleState.CLOSED;
    assertEquals(expected, rule.validate());
  }

  @Test
  public void testProcessRatisContainer() {
    long containerId = 123L;
    ContainerInfo container = mockRatisContainer(LifeCycleState.CLOSED, containerId);

    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(Collections.singletonList(container));
    rule.refresh(true);

    assertEquals(0.0, rule.getCurrentContainerThreshold());

    ContainerReplicaProto replica = mock(ContainerReplicaProto.class);
    List<ContainerReplicaProto> replicas = new ArrayList<>();
    replicas.add(replica);
    ContainerReportsProto containerReport = mock(ContainerReportsProto.class);
    NodeRegistrationContainerReport report = mock(NodeRegistrationContainerReport.class);

    when(replica.getContainerID()).thenReturn(containerId);
    when(containerReport.getReportsList()).thenReturn(replicas);
    when(report.getReport()).thenReturn(containerReport);

    rule.process(report);

    assertEquals(1.0, rule.getCurrentContainerThreshold());
  }

  @Test
  public void testAllContainersClosed() {
    List<ContainerInfo> closedContainers = Arrays.asList(
        mockRatisContainer(LifeCycleState.CLOSED, 11L),
        mockRatisContainer(LifeCycleState.CLOSED, 32L)
    );

    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(closedContainers);

    rule.refresh(false);

    assertEquals(0.0, rule.getCurrentContainerThreshold(), "Threshold should be 0.0 when all containers are closed");
    assertFalse(rule.validate(), "Validate should return false when all containers are closed");
  }

  @Test
  public void testAllContainersOpen() {
    List<ContainerInfo> openContainers = Arrays.asList(
        mockRatisContainer(LifeCycleState.OPEN, 11L),
        mockRatisContainer(LifeCycleState.OPEN, 32L)
    );

    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(openContainers);

    rule.refresh(false);

    assertEquals(1.0, rule.getCurrentContainerThreshold(), "Threshold should be 1.0 when all containers are open");
    assertTrue(rule.validate(), "Validate should return true when all containers are open");
  }

  @Test
  public void testDuplicateContainerIdsInReports() {
    long containerId = 42L;
    ContainerInfo container = mockRatisContainer(LifeCycleState.OPEN, containerId);

    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(Collections.singletonList(container));

    rule.refresh(false);

    ContainerReplicaProto replica = mock(ContainerReplicaProto.class);
    ContainerReportsProto containerReport = mock(ContainerReportsProto.class);
    NodeRegistrationContainerReport report = mock(NodeRegistrationContainerReport.class);

    when(replica.getContainerID()).thenReturn(containerId);
    when(containerReport.getReportsList()).thenReturn(Collections.singletonList(replica));
    when(report.getReport()).thenReturn(containerReport);

    rule.process(report);
    rule.process(report);

    assertEquals(1.0, rule.getCurrentContainerThreshold(), "Duplicated containers should be counted only once");
  }

  @Test
  public void testValidateBasedOnReportProcessingTrue() throws Exception {
    rule.setValidateBasedOnReportProcessing(true);
    long containerId = 1L;
    ContainerInfo container = mockRatisContainer(LifeCycleState.OPEN, containerId);

    when(containerManager.getContainers(ReplicationType.RATIS)).thenReturn(Collections.singletonList(container));

    rule.refresh(false);

    ContainerReplicaProto replica = mock(ContainerReplicaProto.class);
    ContainerReportsProto reportsProto = mock(ContainerReportsProto.class);
    NodeRegistrationContainerReport report = mock(NodeRegistrationContainerReport.class);

    when(replica.getContainerID()).thenReturn(containerId);
    when(reportsProto.getReportsList()).thenReturn(Collections.singletonList(replica));
    when(report.getReport()).thenReturn(reportsProto);

    rule.process(report);

    assertTrue(rule.validate(), "Should validate based on reported containers");
  }

  private static ContainerInfo mockRatisContainer(LifeCycleState state, long containerID) {
    ContainerInfo container = mock(ContainerInfo.class);
    when(container.getReplicationType()).thenReturn(ReplicationType.RATIS);
    when(container.getState()).thenReturn(state);
    when(container.getContainerID()).thenReturn(containerID);
    when(container.getNumberOfKeys()).thenReturn(1L);
    return container;
  }

}
