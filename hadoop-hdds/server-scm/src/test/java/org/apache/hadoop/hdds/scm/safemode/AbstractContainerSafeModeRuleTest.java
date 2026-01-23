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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Abstract base class for container safe mode rule tests.
 */
public abstract class AbstractContainerSafeModeRuleTest {
  private List<ContainerInfo> containers;
  private AbstractContainerSafeModeRule rule;

  @BeforeEach
  public void setup() throws ContainerNotFoundException {
    final ContainerManager containerManager = mock(ContainerManager.class);
    final ConfigurationSource conf = mock(ConfigurationSource.class);
    final EventQueue eventQueue = mock(EventQueue.class);
    final SCMSafeModeManager safeModeManager = mock(SCMSafeModeManager.class);
    final SafeModeMetrics metrics = mock(SafeModeMetrics.class);

    when(safeModeManager.getSafeModeMetrics()).thenReturn(metrics);
    containers = new ArrayList<>();
    when(containerManager.getContainers(getReplicationType())).thenReturn(containers);
    when(containerManager.getContainer(any(ContainerID.class))).thenAnswer(invocation -> {
      ContainerID id = invocation.getArgument(0);
      return containers.stream()
          .filter(c -> c.containerID().equals(id))
          .findFirst()
          .orElseThrow(ContainerNotFoundException::new);
    });

    rule = createRule(eventQueue, conf, containerManager, safeModeManager);
    rule.setValidateBasedOnReportProcessing(false);
  }

  @Test
  public void testRefreshInitializeContainers() {
    containers.add(mockContainer(LifeCycleState.OPEN, 1L));
    containers.add(mockContainer(LifeCycleState.CLOSED, 2L));
    rule.refresh(true);

    assertEquals(0.0, rule.getCurrentContainerThreshold());
  }

  @ParameterizedTest
  @EnumSource(value = LifeCycleState.class,
      names = {"OPEN", "CLOSING", "QUASI_CLOSED", "CLOSED", "DELETING", "DELETED", "RECOVERING"})
  public void testValidateReturnsTrueAndFalse(LifeCycleState state) {
    containers.add(mockContainer(state, 1L));
    rule.refresh(true);

    boolean expected = state != LifeCycleState.QUASI_CLOSED && state != LifeCycleState.CLOSED;
    assertEquals(expected, rule.validate());
  }

  @Test
  public void testProcessContainer() {
    long containerId = 123L;
    containers.add(mockContainer(LifeCycleState.CLOSED, containerId));
    rule.refresh(true);

    assertEquals(0.0, rule.getCurrentContainerThreshold());

    // Send as many distinct reports as the container's minReplica requires
    int minReplica = rule.getMinReplica(ContainerID.valueOf(containerId));
    for (int i = 0; i < minReplica; i++) {
      rule.process(getNewContainerReport(containerId));
    }

    assertEquals(1.0, rule.getCurrentContainerThreshold());
  }

  private NodeRegistrationContainerReport getNewContainerReport(long containerID) {
    ContainerReplicaProto replica = mock(ContainerReplicaProto.class);
    ContainerReportsProto containerReport = mock(ContainerReportsProto.class);
    NodeRegistrationContainerReport report = mock(NodeRegistrationContainerReport.class);
    DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);

    when(replica.getContainerID()).thenReturn(containerID);
    when(containerReport.getReportsList()).thenReturn(Collections.singletonList(replica));
    when(report.getReport()).thenReturn(containerReport);
    when(report.getDatanodeDetails()).thenReturn(datanodeDetails);
    when(datanodeDetails.getID()).thenReturn(DatanodeID.randomID());

    return report;
  }

  @Test
  public void testAllContainersClosed() {
    containers.add(mockContainer(LifeCycleState.CLOSED, 11L));
    containers.add(mockContainer(LifeCycleState.CLOSED, 32L));
    rule.refresh(true);

    assertEquals(0.0, rule.getCurrentContainerThreshold(), "Threshold should be 0.0 when all containers are closed");
    assertFalse(rule.validate(), "Validate should return false when all containers are closed");
  }

  @Test
  public void testAllContainersOpen() {
    containers.add(mockContainer(LifeCycleState.OPEN, 11L));
    containers.add(mockContainer(LifeCycleState.OPEN, 32L));
    rule.refresh(true);

    assertEquals(1.0, rule.getCurrentContainerThreshold(), "Threshold should be 1.0 when all containers are open");
    assertTrue(rule.validate(), "Validate should return true when all containers are open");
  }

  @Test
  public void testDuplicateContainerIdsInReports() {
    long containerId = 42L;
    containers.add(mockContainer(LifeCycleState.OPEN, containerId));
    rule.refresh(true);

    ContainerReplicaProto replica = mock(ContainerReplicaProto.class);
    ContainerReportsProto containerReport = mock(ContainerReportsProto.class);
    NodeRegistrationContainerReport report = mock(NodeRegistrationContainerReport.class);
    DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);

    when(replica.getContainerID()).thenReturn(containerId);
    when(containerReport.getReportsList()).thenReturn(Collections.singletonList(replica));
    when(report.getReport()).thenReturn(containerReport);
    when(report.getDatanodeDetails()).thenReturn(datanodeDetails);
    when(datanodeDetails.getID()).thenReturn(DatanodeID.randomID());

    rule.process(report);
    rule.process(report);

    assertEquals(1.0, rule.getCurrentContainerThreshold(), "Duplicated containers should be counted only once");
  }

  @Test
  public void testValidateBasedOnReportProcessingTrue() {
    rule.setValidateBasedOnReportProcessing(true);
    long containerId = 1L;
    containers.add(mockContainer(LifeCycleState.OPEN, containerId));
    rule.refresh(true);

    ContainerReplicaProto replica = mock(ContainerReplicaProto.class);
    ContainerReportsProto reportsProto = mock(ContainerReportsProto.class);
    NodeRegistrationContainerReport report = mock(NodeRegistrationContainerReport.class);
    DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);

    when(replica.getContainerID()).thenReturn(containerId);
    when(reportsProto.getReportsList()).thenReturn(Collections.singletonList(replica));
    when(report.getReport()).thenReturn(reportsProto);
    when(report.getDatanodeDetails()).thenReturn(datanodeDetails);
    when(datanodeDetails.getID()).thenReturn(DatanodeID.randomID());

    rule.process(report);

    assertTrue(rule.validate(), "Should validate based on reported containers");
  }

  protected abstract ReplicationType getReplicationType();

  protected abstract AbstractContainerSafeModeRule createRule(
      EventQueue eventQueue,
      ConfigurationSource conf,
      ContainerManager containerManager,
      SCMSafeModeManager safeModeManager
  );

  protected abstract ContainerInfo mockContainer(LifeCycleState state, long containerID);
}
