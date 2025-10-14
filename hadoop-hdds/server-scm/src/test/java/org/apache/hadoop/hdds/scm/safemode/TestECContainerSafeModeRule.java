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
import org.apache.hadoop.hdds.client.ECReplicationConfig;
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
 * This class tests ECContainerSafeModeRule.
 */
public class TestECContainerSafeModeRule {
  private List<ContainerInfo> containers;
  private ECContainerSafeModeRule rule;

  @BeforeEach
  public void setup() throws ContainerNotFoundException {
    final ContainerManager containerManager = mock(ContainerManager.class);
    final ConfigurationSource conf = mock(ConfigurationSource.class);
    final EventQueue eventQueue = mock(EventQueue.class);
    final SCMSafeModeManager safeModeManager = mock(SCMSafeModeManager.class);
    final SafeModeMetrics metrics = mock(SafeModeMetrics.class);
    containers = new ArrayList<>();
    when(containerManager.getContainers(ReplicationType.EC)).thenReturn(containers);
    when(containerManager.getContainer(any(ContainerID.class))).thenAnswer(invocation -> {
      ContainerID id = invocation.getArgument(0);
      return containers.stream()
          .filter(c -> c.containerID().equals(id))
          .findFirst()
          .orElseThrow(ContainerNotFoundException::new);
    });

    when(safeModeManager.getSafeModeMetrics()).thenReturn(metrics);

    rule = new ECContainerSafeModeRule(eventQueue, conf, containerManager, safeModeManager);
    rule.setValidateBasedOnReportProcessing(false);
  }

  @Test
  public void testRefreshInitializeECContainers() {
    containers.add(mockECContainer(LifeCycleState.CLOSED, 1L));
    containers.add(mockECContainer(LifeCycleState.OPEN, 2L));

    rule.refresh(true);

    assertEquals(0.0, rule.getCurrentContainerThreshold());
  }

  @ParameterizedTest
  @EnumSource(value = LifeCycleState.class,
      names = {"OPEN", "CLOSING", "QUASI_CLOSED", "CLOSED", "DELETING", "DELETED", "RECOVERING"})
  public void testValidateReturnsTrueAndFalse(LifeCycleState state) {
    containers.add(mockECContainer(state, 1L));
    rule.refresh(true);
    boolean expected = state != LifeCycleState.QUASI_CLOSED && state != LifeCycleState.CLOSED;
    assertEquals(expected, rule.validate());
  }

  @Test
  public void testProcessECContainer() {
    long containerId = 123L;
    containers.add(mockECContainer(LifeCycleState.CLOSED, containerId));
    rule.refresh(true);

    assertEquals(0.0, rule.getCurrentContainerThreshold());

    // We need at least 3 replicas to be reported to validate the rule
    rule.process(getNewContainerReport(containerId));
    rule.process(getNewContainerReport(containerId));
    rule.process(getNewContainerReport(containerId));

    assertEquals(1.0, rule.getCurrentContainerThreshold());
  }

  private NodeRegistrationContainerReport getNewContainerReport(long containerID) {
    DatanodeDetails datanode = mock(DatanodeDetails.class);
    ContainerReportsProto containerReport = mock(ContainerReportsProto.class);
    NodeRegistrationContainerReport report = mock(NodeRegistrationContainerReport.class);
    ContainerReplicaProto replica = mock(ContainerReplicaProto.class);

    when(report.getDatanodeDetails()).thenReturn(datanode);
    when(datanode.getID()).thenReturn(DatanodeID.randomID());
    when(replica.getContainerID()).thenReturn(containerID);
    when(containerReport.getReportsList()).thenReturn(Collections.singletonList(replica));
    when(report.getReport()).thenReturn(containerReport);
    return report;
  }

  @Test
  public void testAllContainersClosed() {
    containers.add(mockECContainer(LifeCycleState.CLOSED, 11L));
    containers.add(mockECContainer(LifeCycleState.CLOSED, 32L));

    rule.refresh(true);

    assertEquals(0.0, rule.getCurrentContainerThreshold(), "Threshold should be 0.0 when all containers are closed");
    assertFalse(rule.validate(), "Validate should return false when all containers are closed");
  }

  @Test
  public void testAllContainersOpen() {
    containers.add(mockECContainer(LifeCycleState.OPEN, 11L));
    containers.add(mockECContainer(LifeCycleState.OPEN, 32L));

    rule.refresh(true);

    assertEquals(1.0, rule.getCurrentContainerThreshold(), "Threshold should be 1.0 when all containers are open");
    assertTrue(rule.validate(), "Validate should return true when all containers are open");
  }

  @Test
  public void testDuplicateContainerIdsInReports() {
    long containerId = 42L;
    containers.add(mockECContainer(LifeCycleState.OPEN, containerId));

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
  public void testValidateBasedOnReportProcessingTrue() throws Exception {
    rule.setValidateBasedOnReportProcessing(true);
    long containerId = 1L;
    containers.add(mockECContainer(LifeCycleState.OPEN, containerId));

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

  private static ContainerInfo mockECContainer(LifeCycleState state, long containerID) {
    ContainerInfo container = mock(ContainerInfo.class);
    when(container.getReplicationType()).thenReturn(ReplicationType.EC);
    when(container.getState()).thenReturn(state);
    when(container.getContainerID()).thenReturn(containerID);
    when(container.containerID()).thenReturn(ContainerID.valueOf(containerID));
    when(container.getNumberOfKeys()).thenReturn(1L);
    when(container.getReplicationConfig()).thenReturn(new ECReplicationConfig(3, 2));
    return container;
  }
}
