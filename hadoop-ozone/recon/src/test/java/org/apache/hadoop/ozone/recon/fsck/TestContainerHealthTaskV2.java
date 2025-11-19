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

package org.apache.hadoop.ozone.recon.fsck;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.recon.schema.ContainerSchemaDefinitionV2;
import org.apache.ozone.recon.schema.generated.tables.daos.UnhealthyContainersV2Dao;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainersV2;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ContainerHealthTaskV2 that uses SCM as single source of truth.
 */
public class TestContainerHealthTaskV2 extends AbstractReconSqlDBTest {

  public TestContainerHealthTaskV2() {
    super();
  }

  @Test
  public void testSCMReportsUnhealthyContainers() throws Exception {
    UnhealthyContainersV2Dao unHealthyContainersV2TableHandle =
        getDao(UnhealthyContainersV2Dao.class);

    ContainerHealthSchemaManagerV2 schemaManagerV2 =
        new ContainerHealthSchemaManagerV2(
            getSchemaDefinition(ContainerSchemaDefinitionV2.class),
            unHealthyContainersV2TableHandle);

    ContainerManager containerManagerMock = mock(ContainerManager.class);
    StorageContainerServiceProvider scmClientMock =
        mock(StorageContainerServiceProvider.class);
    PlacementPolicy placementPolicyMock = mock(PlacementPolicy.class);
    ReconContainerMetadataManager reconContainerMetadataManager =
        mock(ReconContainerMetadataManager.class);

    // Create 5 containers in Recon
    List<ContainerInfo> mockContainers = getMockContainers(5);
    when(containerManagerMock.getContainers(any(ContainerID.class), anyInt()))
        .thenReturn(mockContainers);

    for (ContainerInfo c : mockContainers) {
      when(containerManagerMock.getContainer(c.containerID())).thenReturn(c);
    }

    // Container 1: SCM reports UNDER_REPLICATED
    ContainerInfo container1 = mockContainers.get(0);
    ReplicationManagerReport report1 = createMockReport(0, 1, 0, 0);
    when(scmClientMock.checkContainerStatus(container1)).thenReturn(report1);
    when(containerManagerMock.getContainerReplicas(container1.containerID()))
        .thenReturn(getMockReplicas(1L, State.CLOSED, State.CLOSED));

    // Container 2: SCM reports OVER_REPLICATED
    ContainerInfo container2 = mockContainers.get(1);
    ReplicationManagerReport report2 = createMockReport(0, 0, 1, 0);
    when(scmClientMock.checkContainerStatus(container2)).thenReturn(report2);
    when(containerManagerMock.getContainerReplicas(container2.containerID()))
        .thenReturn(getMockReplicas(2L, State.CLOSED, State.CLOSED,
            State.CLOSED, State.CLOSED));

    // Container 3: SCM reports MIS_REPLICATED
    ContainerInfo container3 = mockContainers.get(2);
    ReplicationManagerReport report3 = createMockReport(0, 0, 0, 1);
    when(scmClientMock.checkContainerStatus(container3)).thenReturn(report3);
    when(containerManagerMock.getContainerReplicas(container3.containerID()))
        .thenReturn(getMockReplicas(3L, State.CLOSED, State.CLOSED, State.CLOSED));

    // Container 4: SCM reports MISSING
    ContainerInfo container4 = mockContainers.get(3);
    ReplicationManagerReport report4 = createMockReport(1, 0, 0, 0);
    when(scmClientMock.checkContainerStatus(container4)).thenReturn(report4);
    when(containerManagerMock.getContainerReplicas(container4.containerID()))
        .thenReturn(Collections.emptySet());

    // Container 5: SCM reports HEALTHY
    ContainerInfo container5 = mockContainers.get(4);
    ReplicationManagerReport report5 = createMockReport(0, 0, 0, 0);
    when(scmClientMock.checkContainerStatus(container5)).thenReturn(report5);
    when(containerManagerMock.getContainerReplicas(container5.containerID()))
        .thenReturn(getMockReplicas(5L, State.CLOSED, State.CLOSED, State.CLOSED));

    ReconTaskConfig reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));

    ContainerHealthTaskV2 taskV2 = new ContainerHealthTaskV2(
        containerManagerMock,
        scmClientMock,
        schemaManagerV2,
        placementPolicyMock,
        reconContainerMetadataManager,
        new OzoneConfiguration(),
        reconTaskConfig,
        getMockTaskStatusUpdaterManager());

    taskV2.start();

    // Wait for task to process all containers
    LambdaTestUtils.await(60000, 1000, () ->
        (unHealthyContainersV2TableHandle.count() == 4));

    // Verify UNDER_REPLICATED
    List<UnhealthyContainersV2> records =
        unHealthyContainersV2TableHandle.fetchByContainerId(1L);
    assertEquals(1, records.size());
    assertEquals("UNDER_REPLICATED", records.get(0).getContainerState());
    assertEquals(1, records.get(0).getReplicaDelta().intValue());

    // Verify OVER_REPLICATED
    records = unHealthyContainersV2TableHandle.fetchByContainerId(2L);
    assertEquals(1, records.size());
    assertEquals("OVER_REPLICATED", records.get(0).getContainerState());
    assertEquals(-1, records.get(0).getReplicaDelta().intValue());

    // Verify MIS_REPLICATED
    records = unHealthyContainersV2TableHandle.fetchByContainerId(3L);
    assertEquals(1, records.size());
    assertEquals("MIS_REPLICATED", records.get(0).getContainerState());

    // Verify MISSING
    records = unHealthyContainersV2TableHandle.fetchByContainerId(4L);
    assertEquals(1, records.size());
    assertEquals("MISSING", records.get(0).getContainerState());

    // Verify container 5 is NOT in the table (healthy)
    records = unHealthyContainersV2TableHandle.fetchByContainerId(5L);
    assertEquals(0, records.size());

    taskV2.stop();
    // Give time for the task thread to fully stop before test cleanup
    Thread.sleep(1000);
  }

  @Test
  public void testReplicaMismatchDetection() throws Exception {
    UnhealthyContainersV2Dao unHealthyContainersV2TableHandle =
        getDao(UnhealthyContainersV2Dao.class);

    ContainerHealthSchemaManagerV2 schemaManagerV2 =
        new ContainerHealthSchemaManagerV2(
            getSchemaDefinition(ContainerSchemaDefinitionV2.class),
            unHealthyContainersV2TableHandle);

    ContainerManager containerManagerMock = mock(ContainerManager.class);
    StorageContainerServiceProvider scmClientMock =
        mock(StorageContainerServiceProvider.class);

    // Create container with checksum mismatch
    List<ContainerInfo> mockContainers = getMockContainers(1);
    when(containerManagerMock.getContainers(any(ContainerID.class), anyInt()))
        .thenReturn(mockContainers);

    ContainerInfo container1 = mockContainers.get(0);
    when(containerManagerMock.getContainer(container1.containerID())).thenReturn(container1);

    // SCM reports healthy, but replicas have checksum mismatch
    ReplicationManagerReport report1 = createMockReport(0, 0, 0, 0);
    when(scmClientMock.checkContainerStatus(container1)).thenReturn(report1);
    when(containerManagerMock.getContainerReplicas(container1.containerID()))
        .thenReturn(getMockReplicasChecksumMismatch(1L, State.CLOSED,
            State.CLOSED, State.CLOSED));

    ReconTaskConfig reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));

    ContainerHealthTaskV2 taskV2 = new ContainerHealthTaskV2(
        containerManagerMock,
        scmClientMock,
        schemaManagerV2,
        mock(PlacementPolicy.class),
        mock(ReconContainerMetadataManager.class),
        new OzoneConfiguration(),
        reconTaskConfig,
        getMockTaskStatusUpdaterManager());

    taskV2.start();

    // Wait for task to detect REPLICA_MISMATCH
    LambdaTestUtils.await(10000, 500, () ->
        (unHealthyContainersV2TableHandle.count() == 1));

    List<UnhealthyContainersV2> records =
        unHealthyContainersV2TableHandle.fetchByContainerId(1L);
    assertEquals(1, records.size());
    assertEquals("REPLICA_MISMATCH", records.get(0).getContainerState());
    assertThat(records.get(0).getReason()).contains("Checksum mismatch");

    taskV2.stop();
  }

  @Test
  public void testContainerTransitionsFromUnhealthyToHealthy() throws Exception {
    UnhealthyContainersV2Dao unHealthyContainersV2TableHandle =
        getDao(UnhealthyContainersV2Dao.class);

    ContainerHealthSchemaManagerV2 schemaManagerV2 =
        new ContainerHealthSchemaManagerV2(
            getSchemaDefinition(ContainerSchemaDefinitionV2.class),
            unHealthyContainersV2TableHandle);

    ContainerManager containerManagerMock = mock(ContainerManager.class);
    StorageContainerServiceProvider scmClientMock =
        mock(StorageContainerServiceProvider.class);

    List<ContainerInfo> mockContainers = getMockContainers(1);
    when(containerManagerMock.getContainers(any(ContainerID.class), anyInt()))
        .thenReturn(mockContainers);

    ContainerInfo container1 = mockContainers.get(0);
    when(containerManagerMock.getContainer(container1.containerID())).thenReturn(container1);

    // Initially SCM reports UNDER_REPLICATED
    ReplicationManagerReport underRepReport = createMockReport(0, 1, 0, 0);
    when(scmClientMock.checkContainerStatus(container1)).thenReturn(underRepReport);
    when(containerManagerMock.getContainerReplicas(container1.containerID()))
        .thenReturn(getMockReplicas(1L, State.CLOSED, State.CLOSED));

    ReconTaskConfig reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));

    ContainerHealthTaskV2 taskV2 = new ContainerHealthTaskV2(
        containerManagerMock,
        scmClientMock,
        schemaManagerV2,
        mock(PlacementPolicy.class),
        mock(ReconContainerMetadataManager.class),
        new OzoneConfiguration(),
        reconTaskConfig,
        getMockTaskStatusUpdaterManager());

    taskV2.start();

    // Wait for container to be marked unhealthy
    LambdaTestUtils.await(10000, 500, () ->
        (unHealthyContainersV2TableHandle.count() == 1));

    // Now SCM reports healthy
    ReplicationManagerReport healthyReport = createMockReport(0, 0, 0, 0);
    when(scmClientMock.checkContainerStatus(container1)).thenReturn(healthyReport);
    when(containerManagerMock.getContainerReplicas(container1.containerID()))
        .thenReturn(getMockReplicas(1L, State.CLOSED, State.CLOSED, State.CLOSED));

    // Wait for container to be removed from unhealthy table
    LambdaTestUtils.await(10000, 500, () ->
        (unHealthyContainersV2TableHandle.count() == 0));

    taskV2.stop();
  }

  @Test
  public void testContainerInSCMButNotInRecon() throws Exception {
    UnhealthyContainersV2Dao unHealthyContainersV2TableHandle =
        getDao(UnhealthyContainersV2Dao.class);

    ContainerHealthSchemaManagerV2 schemaManagerV2 =
        new ContainerHealthSchemaManagerV2(
            getSchemaDefinition(ContainerSchemaDefinitionV2.class),
            unHealthyContainersV2TableHandle);

    ContainerManager containerManagerMock = mock(ContainerManager.class);
    StorageContainerServiceProvider scmClientMock =
        mock(StorageContainerServiceProvider.class);

    // Recon has no containers
    when(containerManagerMock.getContainers(any(ContainerID.class), anyInt()))
        .thenReturn(Collections.emptyList());

    // SCM has 1 UNDER_REPLICATED container
    List<ContainerInfo> scmContainers = getMockContainers(1);
    ContainerInfo scmContainer = scmContainers.get(0);

    // Mock getListOfContainers to handle pagination correctly
    // Return container for CLOSED state with startId=0, empty otherwise
    when(scmClientMock.getListOfContainers(anyLong(), anyInt(),
        any(HddsProtos.LifeCycleState.class))).thenAnswer(invocation -> {
          long startId = invocation.getArgument(0);
          HddsProtos.LifeCycleState state = invocation.getArgument(2);
          // Only return container for CLOSED state and startId=0
          if (state == HddsProtos.LifeCycleState.CLOSED && startId == 0) {
            return scmContainers;
          }
          return Collections.emptyList();
        });
    when(containerManagerMock.getContainer(scmContainer.containerID()))
        .thenThrow(new ContainerNotFoundException("Container not found in Recon"));

    ReplicationManagerReport report = createMockReport(0, 1, 0, 0);
    // Use any() matcher since getListOfContainers returns a list with the same container
    when(scmClientMock.checkContainerStatus(any(ContainerInfo.class))).thenReturn(report);

    ReconTaskConfig reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));

    ContainerHealthTaskV2 taskV2 = new ContainerHealthTaskV2(
        containerManagerMock,
        scmClientMock,
        schemaManagerV2,
        mock(PlacementPolicy.class),
        mock(ReconContainerMetadataManager.class),
        new OzoneConfiguration(),
        reconTaskConfig,
        getMockTaskStatusUpdaterManager());

    taskV2.start();

    // V2 table should have the unhealthy container from SCM
    LambdaTestUtils.await(10000, 500, () ->
        (unHealthyContainersV2TableHandle.count() == 1));

    List<UnhealthyContainersV2> records =
        unHealthyContainersV2TableHandle.fetchByContainerId(1L);
    assertEquals(1, records.size());
    assertEquals("UNDER_REPLICATED", records.get(0).getContainerState());
    assertThat(records.get(0).getReason()).contains("not in Recon");

    taskV2.stop();
  }

  @Test
  public void testContainerInReconButNotInSCM() throws Exception {
    UnhealthyContainersV2Dao unHealthyContainersV2TableHandle =
        getDao(UnhealthyContainersV2Dao.class);

    ContainerHealthSchemaManagerV2 schemaManagerV2 =
        new ContainerHealthSchemaManagerV2(
            getSchemaDefinition(ContainerSchemaDefinitionV2.class),
            unHealthyContainersV2TableHandle);

    ContainerManager containerManagerMock = mock(ContainerManager.class);
    StorageContainerServiceProvider scmClientMock =
        mock(StorageContainerServiceProvider.class);

    // Recon has 1 container
    List<ContainerInfo> reconContainers = getMockContainers(1);
    when(containerManagerMock.getContainers(any(ContainerID.class), anyInt()))
        .thenReturn(reconContainers);

    ContainerInfo reconContainer = reconContainers.get(0);
    when(containerManagerMock.getContainer(reconContainer.containerID()))
        .thenReturn(reconContainer);

    // SCM doesn't have this container
    when(scmClientMock.checkContainerStatus(reconContainer))
        .thenThrow(new ContainerNotFoundException("Container not found in SCM"));
    when(scmClientMock.getListOfContainers(anyLong(), anyInt(),
        any(HddsProtos.LifeCycleState.class))).thenReturn(Collections.emptyList());

    // Insert a record for this container first
    UnhealthyContainersV2 record = new UnhealthyContainersV2();
    record.setContainerId(1L);
    record.setContainerState("UNDER_REPLICATED");
    record.setExpectedReplicaCount(3);
    record.setActualReplicaCount(2);
    record.setReplicaDelta(1);
    record.setInStateSince(System.currentTimeMillis());
    record.setReason("Test");
    unHealthyContainersV2TableHandle.insert(record);

    ReconTaskConfig reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));

    ContainerHealthTaskV2 taskV2 = new ContainerHealthTaskV2(
        containerManagerMock,
        scmClientMock,
        schemaManagerV2,
        mock(PlacementPolicy.class),
        mock(ReconContainerMetadataManager.class),
        new OzoneConfiguration(),
        reconTaskConfig,
        getMockTaskStatusUpdaterManager());

    taskV2.start();

    // Container should be removed from V2 table since it doesn't exist in SCM
    LambdaTestUtils.await(10000, 500, () ->
        (unHealthyContainersV2TableHandle.count() == 0));

    taskV2.stop();
  }

  private ReconTaskStatusUpdaterManager getMockTaskStatusUpdaterManager() {
    ReconTaskStatusUpdaterManager reconTaskStatusUpdaterManager =
        mock(ReconTaskStatusUpdaterManager.class);
    ReconTaskStatusUpdater mockUpdater = mock(ReconTaskStatusUpdater.class);
    when(reconTaskStatusUpdaterManager.getTaskStatusUpdater(any(String.class)))
        .thenReturn(mockUpdater);
    return reconTaskStatusUpdaterManager;
  }

  private ReplicationManagerReport createMockReport(
      long missing, long underRep, long overRep, long misRep) {
    ReplicationManagerReport report = mock(ReplicationManagerReport.class);
    when(report.getStat(ReplicationManagerReport.HealthState.MISSING)).thenReturn(missing);
    when(report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED)).thenReturn(underRep);
    when(report.getStat(ReplicationManagerReport.HealthState.OVER_REPLICATED)).thenReturn(overRep);
    when(report.getStat(ReplicationManagerReport.HealthState.MIS_REPLICATED)).thenReturn(misRep);
    return report;
  }

  private Set<ContainerReplica> getMockReplicas(long containerId, State...states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (State s : states) {
      replicas.add(ContainerReplica.newBuilder()
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
          .setContainerState(s)
          .setContainerID(ContainerID.valueOf(containerId))
          .setSequenceId(1)
          .setDataChecksum(1234L)
          .build());
    }
    return replicas;
  }

  private Set<ContainerReplica> getMockReplicasChecksumMismatch(
      long containerId, State...states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    long checksum = 1234L;
    for (State s : states) {
      replicas.add(ContainerReplica.newBuilder()
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
          .setContainerState(s)
          .setContainerID(ContainerID.valueOf(containerId))
          .setSequenceId(1)
          .setDataChecksum(checksum)
          .build());
      checksum++;
    }
    return replicas;
  }

  private List<ContainerInfo> getMockContainers(int num) {
    List<ContainerInfo> containers = new ArrayList<>();
    for (int i = 1; i <= num; i++) {
      ContainerInfo c = mock(ContainerInfo.class);
      when(c.getContainerID()).thenReturn((long)i);
      when(c.getReplicationConfig())
          .thenReturn(RatisReplicationConfig.getInstance(THREE));
      when(c.getReplicationFactor()).thenReturn(THREE);
      when(c.getState()).thenReturn(HddsProtos.LifeCycleState.CLOSED);
      when(c.containerID()).thenReturn(ContainerID.valueOf(i));
      containers.add(c);
    }
    return containers;
  }
}
