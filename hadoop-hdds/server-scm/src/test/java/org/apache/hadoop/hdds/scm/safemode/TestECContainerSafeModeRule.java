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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
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
  private ContainerManager containerManager;
  private ConfigurationSource conf;
  private EventQueue eventQueue;
  private SCMSafeModeManager safeModeManager;
  private SafeModeMetrics metrics;

  private ECContainerSafeModeRule rule;

  @BeforeEach
  public void setup() {
    containerManager = mock(ContainerManager.class);
    conf = mock(ConfigurationSource.class);
    eventQueue = mock(EventQueue.class);
    safeModeManager = mock(SCMSafeModeManager.class);
    metrics = mock(SafeModeMetrics.class);

    when(safeModeManager.getSafeModeMetrics()).thenReturn(metrics);

    rule = new ECContainerSafeModeRule(eventQueue, conf, containerManager, safeModeManager);
    rule.setValidateBasedOnReportProcessing(false);
  }

  @Test
  public void testRefreshInitializeECContainers() {
    ContainerInfo container1 = mockECContainer(LifeCycleState.CLOSED, 1L);
    ContainerInfo container2 = mockECContainer(LifeCycleState.OPEN, 2L);

    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(container1);
    containers.add(container2);

    when(containerManager.getContainers(ReplicationType.EC)).thenReturn(containers);

    rule.refresh(false);

    assertEquals(0.0, rule.getCurrentContainerThreshold());
  }

  @ParameterizedTest
  @EnumSource(value = LifeCycleState.class, names = {"OPEN", "CLOSED"})
  public void testValidateReturnsTrueAndFalse(LifeCycleState state)  {
    ContainerInfo container = mockECContainer(state, 1L);
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(container);

    when(containerManager.getContainers(ReplicationType.EC)).thenReturn(containers);

    boolean expected = state != LifeCycleState.CLOSED;
    assertEquals(expected, rule.validate());
  }

  @Test
  public void testProcessECContainer() {
    long containerId = 123L;
    ContainerInfo container = mockECContainer(LifeCycleState.CLOSED, containerId);
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(container);

    when(containerManager.getContainers(ReplicationType.EC)).thenReturn(containers);
    rule.refresh(true);

    assertEquals(0.0, rule.getCurrentContainerThreshold());

    ContainerReplicaProto replica = mock(ContainerReplicaProto.class);
    List<ContainerReplicaProto> replicas = new ArrayList<>();
    replicas.add(replica);
    ContainerReportsProto containerReport = mock(ContainerReportsProto.class);
    NodeRegistrationContainerReport report = mock(NodeRegistrationContainerReport.class);
    DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);

    when(report.getDatanodeDetails()).thenReturn(datanodeDetails);
    when(datanodeDetails.getUuid()).thenReturn(UUID.randomUUID());
    when(replica.getContainerID()).thenReturn(containerId);
    when(containerReport.getReportsList()).thenReturn(replicas);
    when(report.getReport()).thenReturn(containerReport);

    rule.process(report);

    assertEquals(1.0, rule.getCurrentContainerThreshold());
  }

  private static ContainerInfo mockECContainer(LifeCycleState cycleState, long containerID) {
    ContainerInfo container = mock(ContainerInfo.class);
    when(container.getReplicationType()).thenReturn(ReplicationType.EC);
    when(container.getState()).thenReturn(cycleState);
    when(container.getContainerID()).thenReturn(containerID);
    when(container.containerID()).thenReturn(ContainerID.valueOf(containerID));
    when(container.getNumberOfKeys()).thenReturn(1L);
    return container;
  }
}
