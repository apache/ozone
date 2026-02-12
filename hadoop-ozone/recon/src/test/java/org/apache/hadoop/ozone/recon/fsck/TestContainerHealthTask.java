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
import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates.ALL_REPLICAS_BAD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicatedReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.TestContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.ozone.recon.metrics.ContainerHealthMetrics;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.daos.UnhealthyContainersDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Class to test a single run of the Container Health Task.
 */
public class TestContainerHealthTask extends AbstractReconSqlDBTest {

  public TestContainerHealthTask() {
    super();
  }

  @SuppressWarnings("checkstyle:methodlength")
  @Test
  public void testRun() throws Exception {
    UnhealthyContainersDao unHealthyContainersTableHandle =
        getDao(UnhealthyContainersDao.class);

    ContainerHealthSchemaManager containerHealthSchemaManager =
        new ContainerHealthSchemaManager(
            getSchemaDefinition(ContainerSchemaDefinition.class),
            unHealthyContainersTableHandle);
    ReconStorageContainerManagerFacade scmMock =
        mock(ReconStorageContainerManagerFacade.class);
    ReconContainerMetadataManager reconContainerMetadataManager =
        mock(ReconContainerMetadataManager.class);
    MockPlacementPolicy placementMock = new MockPlacementPolicy();
    ContainerManager containerManagerMock = mock(ContainerManager.class);
    StorageContainerServiceProvider scmClientMock =
        mock(StorageContainerServiceProvider.class);
    ContainerReplica unhealthyReplicaMock = mock(ContainerReplica.class);
    when(unhealthyReplicaMock.getState()).thenReturn(State.UNHEALTHY);
    ContainerReplica healthyReplicaMock = mock(ContainerReplica.class);
    when(healthyReplicaMock.getState()).thenReturn(State.CLOSED);

    // Create 7 containers. The first 5 will have various unhealthy states
    // defined below. The container with ID=6 will be healthy and
    // container with ID=7 will be EMPTY_MISSING (but not inserted into DB)
    List<ContainerInfo> mockContainers = getMockContainers(8);
    when(scmMock.getScmServiceProvider()).thenReturn(scmClientMock);
    when(scmMock.getContainerManager()).thenReturn(containerManagerMock);
    when(containerManagerMock.getContainers(any(ContainerID.class),
        anyInt())).thenReturn(mockContainers);
    for (ContainerInfo c : mockContainers) {
      when(containerManagerMock.getContainer(c.containerID())).thenReturn(c);
      when(scmClientMock.getContainerWithPipeline(c.getContainerID()))
          .thenReturn(new ContainerWithPipeline(c, null));
    }

    ReplicatedReplicationConfig replicationConfig = RatisReplicationConfig.getInstance(THREE);
    // Under replicated
    ContainerInfo containerInfo1 =
        TestContainerInfo.newBuilderForTest().setContainerID(1).setReplicationConfig(replicationConfig).build();
    when(containerManagerMock.getContainer(ContainerID.valueOf(1L))).thenReturn(containerInfo1);
    when(containerManagerMock.getContainerReplicas(containerInfo1.containerID()))
        .thenReturn(getMockReplicas(1L, State.CLOSED, State.UNHEALTHY));

    // return all UNHEALTHY replicas for container ID 2 -> UNDER_REPLICATED
    ContainerInfo containerInfo2 =
        TestContainerInfo.newBuilderForTest().setContainerID(2).setReplicationConfig(replicationConfig).build();
    when(containerManagerMock.getContainer(ContainerID.valueOf(2L))).thenReturn(containerInfo2);
    when(containerManagerMock.getContainerReplicas(containerInfo2.containerID()))
        .thenReturn(getMockReplicas(2L, State.UNHEALTHY));

    // return 0 replicas for container ID 3 -> EMPTY_MISSING (will not be inserted into DB)
    ContainerInfo containerInfo3 =
        TestContainerInfo.newBuilderForTest().setContainerID(3).setReplicationConfig(replicationConfig).build();
    when(containerManagerMock.getContainer(ContainerID.valueOf(3L))).thenReturn(containerInfo3);
    when(containerManagerMock.getContainerReplicas(containerInfo3.containerID()))
        .thenReturn(Collections.emptySet());

    // Return 5 Healthy Replicas -> Over-replicated
    ContainerInfo containerInfo4 =
        TestContainerInfo.newBuilderForTest().setContainerID(4).setReplicationConfig(replicationConfig).build();
    when(containerManagerMock.getContainer(ContainerID.valueOf(4L))).thenReturn(containerInfo4);
    when(containerManagerMock.getContainerReplicas(containerInfo4.containerID()))
        .thenReturn(getMockReplicas(4L, State.CLOSED, State.CLOSED,
            State.CLOSED, State.CLOSED, State.CLOSED));

    // Mis-replicated
    ContainerInfo containerInfo5 =
        TestContainerInfo.newBuilderForTest().setContainerID(5).setReplicationConfig(replicationConfig).build();
    when(containerManagerMock.getContainer(ContainerID.valueOf(5L))).thenReturn(containerInfo5);
    Set<ContainerReplica> misReplicas = getMockReplicas(5L,
        State.CLOSED, State.CLOSED, State.CLOSED);
    placementMock.setMisRepWhenDnPresent(
        misReplicas.iterator().next().getDatanodeDetails().getUuid());
    when(containerManagerMock.getContainerReplicas(containerInfo5.containerID()))
        .thenReturn(misReplicas);

    // Return 3 Healthy Replicas -> Healthy container
    ContainerInfo containerInfo6 =
        TestContainerInfo.newBuilderForTest().setContainerID(6).setReplicationConfig(replicationConfig).build();
    when(containerManagerMock.getContainer(ContainerID.valueOf(6L))).thenReturn(containerInfo6);
    when(containerManagerMock.getContainerReplicas(containerInfo6.containerID()))
        .thenReturn(getMockReplicas(6L,
            State.CLOSED, State.CLOSED, State.CLOSED));

    // return 0 replicas for container ID 7 -> MISSING (will later transition to EMPTY_MISSING but not inserted into DB)
    ContainerInfo containerInfo7 =
        TestContainerInfo.newBuilderForTest().setContainerID(7).setReplicationConfig(replicationConfig).build();
    when(containerManagerMock.getContainer(ContainerID.valueOf(7L))).thenReturn(containerInfo7);
    when(containerManagerMock.getContainerReplicas(containerInfo7.containerID()))
        .thenReturn(Collections.emptySet());
    when(reconContainerMetadataManager.getKeyCountForContainer(
        7L)).thenReturn(5L);  // Indicates non-empty container 7 for now

    // container ID 8 - REPLICA_MISMATCH
    ContainerInfo containerInfo8 =
        TestContainerInfo.newBuilderForTest().setContainerID(8).setReplicationConfig(replicationConfig).build();
    when(containerManagerMock.getContainer(ContainerID.valueOf(8L))).thenReturn(containerInfo8);
    Set<ContainerReplica> mismatchReplicas = getMockReplicasChecksumMismatch(8L,
        State.CLOSED, State.CLOSED, State.CLOSED);
    when(containerManagerMock.getContainerReplicas(containerInfo8.containerID()))
        .thenReturn(mismatchReplicas);

    List<UnhealthyContainers> all = unHealthyContainersTableHandle.findAll();
    assertThat(all).isEmpty();

    long currentTime = System.currentTimeMillis();
    ReconTaskStatusDao reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    ReconTaskConfig reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(10));

    // Start container health task
    ContainerHealthTask containerHealthTask =
        new ContainerHealthTask(scmMock.getContainerManager(),
            scmMock.getScmServiceProvider(), containerHealthSchemaManager,
            placementMock, reconTaskConfig, reconContainerMetadataManager,
            new OzoneConfiguration(), getMockTaskStatusUpdaterManager());
    containerHealthTask.start();

    // Ensure unhealthy container count in DB matches expected
    LambdaTestUtils.await(60000, 1000, () ->
        (unHealthyContainersTableHandle.count() == 6));

    // Check for UNDER_REPLICATED container states
    UnhealthyContainers rec =
        unHealthyContainersTableHandle.fetchByContainerId(1L).get(0);
    assertEquals("UNDER_REPLICATED", rec.getContainerState());
    assertEquals(2, rec.getReplicaDelta().intValue());

    rec = unHealthyContainersTableHandle.fetchByContainerId(2L).get(0);
    assertEquals("UNDER_REPLICATED", rec.getContainerState());
    assertEquals(3, rec.getReplicaDelta().intValue());

    // Assert that EMPTY_MISSING state containers were never added to DB.
    assertEquals(0,
        unHealthyContainersTableHandle.fetchByContainerId(3L).size());

    List<UnhealthyContainers> unhealthyContainers =
        containerHealthSchemaManager.getUnhealthyContainers(
            ALL_REPLICAS_BAD, 0L, Optional.empty(), Integer.MAX_VALUE);
    assertEquals(1, unhealthyContainers.size());
    assertEquals(2L,
        unhealthyContainers.get(0).getContainerId().longValue());
    assertEquals(0,
        unhealthyContainers.get(0).getActualReplicaCount().intValue());

    // Check for MISSING state in container ID 7
    rec = unHealthyContainersTableHandle.fetchByContainerId(7L).get(0);
    assertEquals("MISSING", rec.getContainerState());
    assertEquals(3, rec.getReplicaDelta().intValue());

    Field field = ContainerHealthTask.class.getDeclaredField("containerHealthMetrics");
    field.setAccessible(true);

    // Read private field value
    ContainerHealthMetrics containerHealthMetrics = (ContainerHealthMetrics) field.get(containerHealthTask);

    // Only Container ID: 7 is MISSING, so count of missing container count metrics should be equal to 1
    assertEquals(1, containerHealthMetrics.getMissingContainerCount());
    // Container ID: 1 and Container ID: 2, both are UNDER_REPLICATED, so UNDER_REPLICATED
    // container count metric should be 2
    assertEquals(2, containerHealthMetrics.getUnderReplicatedContainerCount());

    rec = unHealthyContainersTableHandle.fetchByContainerId(4L).get(0);
    assertEquals("OVER_REPLICATED", rec.getContainerState());
    assertEquals(-2, rec.getReplicaDelta().intValue());

    rec = unHealthyContainersTableHandle.fetchByContainerId(5L).get(0);
    assertEquals("MIS_REPLICATED", rec.getContainerState());
    assertEquals(1, rec.getReplicaDelta().intValue());
    assertEquals(2, rec.getExpectedReplicaCount().intValue());
    assertEquals(1, rec.getActualReplicaCount().intValue());
    assertNotNull(rec.getReason());

    rec = unHealthyContainersTableHandle.fetchByContainerId(8L).get(0);
    assertEquals("REPLICA_MISMATCH", rec.getContainerState());
    assertEquals(0, rec.getReplicaDelta().intValue());
    assertEquals(3, rec.getExpectedReplicaCount().intValue());
    assertEquals(3, rec.getActualReplicaCount().intValue());

    ReconTaskStatus taskStatus =
        reconTaskStatusDao.findById(containerHealthTask.getTaskName());
    assertThat(taskStatus.getLastUpdatedTimestamp())
        .isGreaterThan(currentTime);

    // Adjust the mock results and rerun to check for updates or removal of records
    when(containerManagerMock.getContainerReplicas(ContainerID.valueOf(1L)))
        .thenReturn(getMockReplicas(1L, State.CLOSED, State.CLOSED));

    // ID 2 was UNDER_REPLICATED - make it healthy now and after this step, UNDER_REPLICATED
    // container count metric will be 1.
    when(containerManagerMock.getContainerReplicas(ContainerID.valueOf(2L)))
        .thenReturn(getMockReplicas(2L,
            State.CLOSED, State.CLOSED, State.CLOSED));

    // Container 3 remains EMPTY_MISSING, but no DB insertion
    when(containerManagerMock.getContainerReplicas(ContainerID.valueOf(3L)))
        .thenReturn(Collections.emptySet());

    // Return 4 Healthy -> Delta changes from -2 to -1
    when(containerManagerMock.getContainerReplicas(ContainerID.valueOf(4L)))
        .thenReturn(getMockReplicas(4L, State.CLOSED, State.CLOSED,
            State.CLOSED, State.CLOSED));

    // Convert container 7 which was MISSING to EMPTY_MISSING (not inserted into DB)
    when(reconContainerMetadataManager.getKeyCountForContainer(
        7L)).thenReturn(0L);

    placementMock.setMisRepWhenDnPresent(null);

    // Ensure count is reduced after EMPTY_MISSING containers are not inserted
    LambdaTestUtils.await(60000, 1000, () ->
        (unHealthyContainersTableHandle.count() == 3));

    rec = unHealthyContainersTableHandle.fetchByContainerId(1L).get(0);
    assertEquals("UNDER_REPLICATED", rec.getContainerState());
    assertEquals(1, rec.getReplicaDelta().intValue());

    // This container is now healthy, it should not be in the table any more
    assertEquals(0,
        unHealthyContainersTableHandle.fetchByContainerId(2L).size());

    // Now since container ID: 2 is gone back to HEALTHY state in above step, so UNDER-REPLICATED
    // container count should be just 1 (denoting only for container ID : 1)
    assertEquals(1, containerHealthMetrics.getUnderReplicatedContainerCount());

    // Assert that for container 7 no records exist in DB because it's now EMPTY_MISSING
    assertEquals(0,
        unHealthyContainersTableHandle.fetchByContainerId(7L).size());

    // Since Container ID: 7 is now EMPTY_MISSING, so MISSING container count metric
    // will now be 0 as there is no missing container now.
    assertEquals(0, containerHealthMetrics.getMissingContainerCount());

    rec = unHealthyContainersTableHandle.fetchByContainerId(4L).get(0);
    assertEquals("OVER_REPLICATED", rec.getContainerState());
    assertEquals(-1, rec.getReplicaDelta().intValue());

    // Ensure container 5 is now healthy and not in the table
    assertEquals(0,
        unHealthyContainersTableHandle.fetchByContainerId(5L).size());

    // Just check once again that count remains consistent
    LambdaTestUtils.await(60000, 1000, () ->
        (unHealthyContainersTableHandle.count() == 3));

    // Since other container states have been changing, but no change in UNDER_REPLICATED
    // container count, UNDER_REPLICATED count metric should not be affected from previous
    // assertion count.
    assertEquals(1, containerHealthMetrics.getUnderReplicatedContainerCount());
    assertEquals(0, containerHealthMetrics.getMissingContainerCount());

    containerHealthTask.stop();
  }

  @Test
  public void testDeletedContainer() throws Exception {
    UnhealthyContainersDao unHealthyContainersTableHandle =
        getDao(UnhealthyContainersDao.class);

    ContainerHealthSchemaManager containerHealthSchemaManager =
        new ContainerHealthSchemaManager(
            getSchemaDefinition(ContainerSchemaDefinition.class),
            unHealthyContainersTableHandle);
    ReconStorageContainerManagerFacade scmMock =
        mock(ReconStorageContainerManagerFacade.class);
    MockPlacementPolicy placementMock = new MockPlacementPolicy();
    ContainerManager containerManagerMock = mock(ContainerManager.class);
    StorageContainerServiceProvider scmClientMock =
        mock(StorageContainerServiceProvider.class);
    ReconContainerMetadataManager reconContainerMetadataManager =
        mock(ReconContainerMetadataManager.class);

    // Create 2 containers. The first is OPEN will no replicas, the second is
    // CLOSED with no replicas.
    List<ContainerInfo> mockContainers = getMockContainers(3);
    when(scmMock.getScmServiceProvider()).thenReturn(scmClientMock);
    when(scmMock.getContainerManager()).thenReturn(containerManagerMock);
    when(containerManagerMock.getContainers(any(ContainerID.class),
        anyInt())).thenReturn(mockContainers);
    for (ContainerInfo c : mockContainers) {
      when(containerManagerMock.getContainer(c.containerID())).thenReturn(c);
      when(scmClientMock.getContainerWithPipeline(c.getContainerID()))
          .thenReturn(new ContainerWithPipeline(c, null));
    }
    // Empty Container with OPEN State and no replicas
    when(containerManagerMock.getContainer(ContainerID.valueOf(1L)).getState())
        .thenReturn(HddsProtos.LifeCycleState.OPEN);
    when(containerManagerMock.getContainerReplicas(ContainerID.valueOf(1L)))
        .thenReturn(Collections.emptySet());
    when(scmClientMock.getContainerWithPipeline(1))
        .thenReturn(new ContainerWithPipeline(mockContainers.get(0), null));

    // Container State CLOSED with no replicas
    when(containerManagerMock.getContainer(ContainerID.valueOf(2L)).getState())
        .thenReturn(HddsProtos.LifeCycleState.CLOSED);
    when(containerManagerMock.getContainerReplicas(ContainerID.valueOf(2L)))
        .thenReturn(Collections.emptySet());
    ContainerInfo mockDeletedContainer = getMockDeletedContainer(2);
    when(scmClientMock.getContainerWithPipeline(2))
        .thenReturn(new ContainerWithPipeline(mockDeletedContainer, null));

    // Container with OPEN State and no replicas
    when(containerManagerMock.getContainer(ContainerID.valueOf(3L)).getState())
        .thenReturn(HddsProtos.LifeCycleState.OPEN);
    when(containerManagerMock.getContainerReplicas(ContainerID.valueOf(3L)))
        .thenReturn(Collections.emptySet());
    when(scmClientMock.getContainerWithPipeline(3))
        .thenReturn(new ContainerWithPipeline(mockContainers.get(0), null));

    List<UnhealthyContainers> all = unHealthyContainersTableHandle.findAll();
    assertThat(all).isEmpty();

    long currentTime = System.currentTimeMillis();
    ReconTaskStatusDao reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    ReconTaskConfig reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));
    when(reconContainerMetadataManager.getKeyCountForContainer(
        1L)).thenReturn(5L);
    ContainerHealthTask containerHealthTask =
        new ContainerHealthTask(scmMock.getContainerManager(),
            scmMock.getScmServiceProvider(), containerHealthSchemaManager,
            placementMock, reconTaskConfig, reconContainerMetadataManager,
            new OzoneConfiguration(), getMockTaskStatusUpdaterManager());
    containerHealthTask.start();
    LambdaTestUtils.await(6000, 1000, () ->
        (unHealthyContainersTableHandle.count() == 1));
    UnhealthyContainers rec =
        unHealthyContainersTableHandle.fetchByContainerId(1L).get(0);
    assertEquals("MISSING", rec.getContainerState());
    assertEquals(3, rec.getReplicaDelta().intValue());

    ReconTaskStatus taskStatus =
        reconTaskStatusDao.findById(containerHealthTask.getTaskName());
    assertThat(taskStatus.getLastUpdatedTimestamp())
        .isGreaterThan(currentTime);
  }

  @Test
  public void testAllContainerStateInsertions() {
    UnhealthyContainersDao unHealthyContainersTableHandle =
        getDao(UnhealthyContainersDao.class);

    ContainerHealthSchemaManager containerHealthSchemaManager =
        new ContainerHealthSchemaManager(
            getSchemaDefinition(ContainerSchemaDefinition.class),
            unHealthyContainersTableHandle);

    // Iterate through each state in the UnHealthyContainerStates enum
    for (ContainerSchemaDefinition.UnHealthyContainerStates state :
        ContainerSchemaDefinition.UnHealthyContainerStates.values()) {

      // Create a dummy UnhealthyContainer record with the current state
      UnhealthyContainers unhealthyContainer = new UnhealthyContainers();
      unhealthyContainer.setContainerId(state.ordinal() + 1L);

      // Set replica counts based on the state
      switch (state) {
      case MISSING:
      case EMPTY_MISSING:
        unhealthyContainer.setExpectedReplicaCount(3);
        unhealthyContainer.setActualReplicaCount(0);
        unhealthyContainer.setReplicaDelta(3);
        break;

      case UNDER_REPLICATED:
        unhealthyContainer.setExpectedReplicaCount(3);
        unhealthyContainer.setActualReplicaCount(1);
        unhealthyContainer.setReplicaDelta(2);
        break;

      case OVER_REPLICATED:
        unhealthyContainer.setExpectedReplicaCount(3);
        unhealthyContainer.setActualReplicaCount(4);
        unhealthyContainer.setReplicaDelta(-1);
        break;

      case MIS_REPLICATED:
      case NEGATIVE_SIZE:
      case REPLICA_MISMATCH:
        unhealthyContainer.setExpectedReplicaCount(3);
        unhealthyContainer.setActualReplicaCount(3);
        unhealthyContainer.setReplicaDelta(0);
        break;

      case ALL_REPLICAS_BAD:
        unhealthyContainer.setExpectedReplicaCount(3);
        unhealthyContainer.setActualReplicaCount(0);
        unhealthyContainer.setReplicaDelta(3);
        break;

      default:
        fail("Unhandled state: " + state.name() + ". Please add this state to the switch case.");
      }

      unhealthyContainer.setContainerState(state.name());
      unhealthyContainer.setInStateSince(System.currentTimeMillis());

      // Try inserting the record and catch any exception that occurs
      Exception exception = null;
      try {
        containerHealthSchemaManager.insertUnhealthyContainerRecords(
            Collections.singletonList(unhealthyContainer));
      } catch (Exception e) {
        exception = e;
      }

      // Assert no exception should be thrown for each state
      assertNull(exception,
          "Exception was thrown during insertion for state " + state.name() +
              ": " + exception);

      // Optionally, verify the record was inserted correctly
      List<UnhealthyContainers> insertedRecords =
          unHealthyContainersTableHandle.fetchByContainerId(
              state.ordinal() + 1L);
      assertFalse(insertedRecords.isEmpty(),
          "Record was not inserted for state " + state.name() + ".");
      assertEquals(insertedRecords.get(0).getContainerState(), state.name(),
          "The inserted container state does not match for state " +
              state.name() + ".");
    }
  }

  @Test
  public void testInsertFailureAndUpdateBehavior() {
    UnhealthyContainersDao unHealthyContainersTableHandle =
        getDao(UnhealthyContainersDao.class);

    ContainerHealthSchemaManager containerHealthSchemaManager =
        new ContainerHealthSchemaManager(
            getSchemaDefinition(ContainerSchemaDefinition.class),
            unHealthyContainersTableHandle);

    ContainerSchemaDefinition.UnHealthyContainerStates state =
        ContainerSchemaDefinition.UnHealthyContainerStates.MISSING;

    long insertedTime = System.currentTimeMillis();
    // Create a dummy UnhealthyContainer record with the current state
    UnhealthyContainers unhealthyContainer = new UnhealthyContainers();
    unhealthyContainer.setContainerId(state.ordinal() + 1L);
    unhealthyContainer.setExpectedReplicaCount(3);
    unhealthyContainer.setActualReplicaCount(0);
    unhealthyContainer.setReplicaDelta(3);
    unhealthyContainer.setContainerState(state.name());
    unhealthyContainer.setInStateSince(insertedTime);

    // Try inserting the record and catch any exception that occurs
    Exception exception = null;
    try {
      containerHealthSchemaManager.insertUnhealthyContainerRecords(
          Collections.singletonList(unhealthyContainer));
    } catch (Exception e) {
      exception = e;
    }

    // Assert no exception should be thrown for each state
    assertNull(exception,
        "Exception was thrown during insertion for state " + state.name() +
            ": " + exception);

    long updatedTime = System.currentTimeMillis();
    unhealthyContainer.setExpectedReplicaCount(3);
    unhealthyContainer.setActualReplicaCount(0);
    unhealthyContainer.setReplicaDelta(3);
    unhealthyContainer.setContainerState(state.name());
    unhealthyContainer.setInStateSince(updatedTime);

    try {
      containerHealthSchemaManager.insertUnhealthyContainerRecords(
          Collections.singletonList(unhealthyContainer));
    } catch (Exception e) {
      exception = e;
    }

    // Optionally, verify the record was updated correctly
    List<UnhealthyContainers> updatedRecords =
        unHealthyContainersTableHandle.fetchByContainerId(
            state.ordinal() + 1L);
    assertFalse(updatedRecords.isEmpty(),
        "Record was not updated for state " + state.name() + ".");
    assertEquals(updatedRecords.get(0).getContainerState(), state.name(),
        "The inserted container state does not match for state " +
            state.name() + ".");
    assertEquals(updatedRecords.get(0).getInStateSince(), updatedTime);
  }

  @Test
  public void testMissingAndEmptyMissingContainerDeletion() throws Exception {
    // Setup mock DAOs and managers
    UnhealthyContainersDao unHealthyContainersTableHandle =
        getDao(UnhealthyContainersDao.class);
    ContainerHealthSchemaManager containerHealthSchemaManager =
        new ContainerHealthSchemaManager(
            getSchemaDefinition(ContainerSchemaDefinition.class),
            unHealthyContainersTableHandle);
    ReconStorageContainerManagerFacade scmMock =
        mock(ReconStorageContainerManagerFacade.class);
    MockPlacementPolicy placementMock = new MockPlacementPolicy();
    ContainerManager containerManagerMock = mock(ContainerManager.class);
    StorageContainerServiceProvider scmClientMock =
        mock(StorageContainerServiceProvider.class);
    ReconContainerMetadataManager reconContainerMetadataManager =
        mock(ReconContainerMetadataManager.class);
    mock(ReconContainerMetadataManager.class);

    // Create 2 containers. They start in CLOSED state in Recon.
    List<ContainerInfo> mockContainers = getMockContainers(2);
    when(scmMock.getScmServiceProvider()).thenReturn(scmClientMock);
    when(scmMock.getContainerManager()).thenReturn(containerManagerMock);
    when(containerManagerMock.getContainers(any(ContainerID.class),
        anyInt())).thenReturn(mockContainers);

    // Mark both containers as initially CLOSED in Recon
    for (ContainerInfo c : mockContainers) {
      when(containerManagerMock.getContainer(c.containerID())).thenReturn(c);
    }

    // Simulate SCM reporting the containers as DELETED
    ContainerInfo deletedContainer1 = getMockDeletedContainer(1);
    ContainerInfo deletedContainer2 = getMockDeletedContainer(2);

    when(scmClientMock.getContainerWithPipeline(1))
        .thenReturn(new ContainerWithPipeline(deletedContainer1, null));
    when(scmClientMock.getContainerWithPipeline(2))
        .thenReturn(new ContainerWithPipeline(deletedContainer2, null));

    // Both containers start as CLOSED in Recon (MISSING or EMPTY_MISSING)
    when(containerManagerMock.getContainer(ContainerID.valueOf(1L)).getState())
        .thenReturn(HddsProtos.LifeCycleState.CLOSED);
    when(containerManagerMock.getContainer(ContainerID.valueOf(2L)).getState())
        .thenReturn(HddsProtos.LifeCycleState.CLOSED);

    // Replicas are empty, so both containers should be considered for deletion
    when(containerManagerMock.getContainerReplicas(ContainerID.valueOf(1L)))
        .thenReturn(Collections.emptySet());
    when(containerManagerMock.getContainerReplicas(ContainerID.valueOf(2L)))
        .thenReturn(Collections.emptySet());

    // Initialize UnhealthyContainers in DB (MISSING and EMPTY_MISSING)
    // Create and set up the first UnhealthyContainer for a MISSING container
    UnhealthyContainers container1 = new UnhealthyContainers();
    container1.setContainerId(1L);
    container1.setContainerState("MISSING");
    container1.setExpectedReplicaCount(3);
    container1.setActualReplicaCount(0);
    container1.setReplicaDelta(3);
    container1.setInStateSince(System.currentTimeMillis());

    // Create and set up the second UnhealthyContainer for an EMPTY_MISSING container
    UnhealthyContainers container2 = new UnhealthyContainers();
    container2.setContainerId(2L);
    container2.setContainerState("MISSING");
    container2.setExpectedReplicaCount(3);
    container2.setActualReplicaCount(0);
    container2.setReplicaDelta(3);
    container2.setInStateSince(System.currentTimeMillis());

    unHealthyContainersTableHandle.insert(container1);
    unHealthyContainersTableHandle.insert(container2);

    when(reconContainerMetadataManager.getKeyCountForContainer(1L)).thenReturn(5L);
    when(reconContainerMetadataManager.getKeyCountForContainer(2L)).thenReturn(0L);

    // Start the container health task
    ReconTaskConfig reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));
    ContainerHealthTask containerHealthTask =
        new ContainerHealthTask(scmMock.getContainerManager(),
            scmMock.getScmServiceProvider(), containerHealthSchemaManager,
            placementMock, reconTaskConfig, reconContainerMetadataManager,
            new OzoneConfiguration(), getMockTaskStatusUpdaterManager());

    containerHealthTask.start();

    // Wait for the task to complete and ensure that updateContainerState is invoked for
    // container IDs 1 and 2 to mark the containers as DELETED, since they are DELETED in SCM.
    LambdaTestUtils.await(60000, 1000, () -> {
      verify(containerManagerMock, times(1))
          .updateContainerState(ContainerID.valueOf(1L), HddsProtos.LifeCycleEvent.DELETE);
      verify(containerManagerMock, times(1))
          .updateContainerState(ContainerID.valueOf(2L), HddsProtos.LifeCycleEvent.DELETE);
      return true;
    });
  }

  private ReconTaskStatusUpdaterManager getMockTaskStatusUpdaterManager() {
    ReconTaskStatusUpdaterManager reconTaskStatusUpdaterManager = mock(ReconTaskStatusUpdaterManager.class);
    when(reconTaskStatusUpdaterManager.getTaskStatusUpdater(anyString())).thenAnswer(inv -> {
      String taskName = inv.getArgument(0);
      return new ReconTaskStatusUpdater(getDao(ReconTaskStatusDao.class), taskName);
    });
    return reconTaskStatusUpdaterManager;
  }

  private Set<ContainerReplica> getMockReplicas(
      long containerId, State...states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (State s : states) {
      replicas.add(ContainerReplica.newBuilder()
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
          .setContainerState(s)
          .setContainerID(ContainerID.valueOf(containerId))
          .setSequenceId(1)
          .setChecksums(ContainerChecksums.of(1234L, 0L))
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
          .setChecksums(ContainerChecksums.of(checksum, 0L))
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
          .thenReturn(RatisReplicationConfig.getInstance(
              THREE));
      when(c.getReplicationFactor())
          .thenReturn(THREE);
      when(c.getState()).thenReturn(HddsProtos.LifeCycleState.CLOSED);
      when(c.containerID()).thenReturn(ContainerID.valueOf(i));
      containers.add(c);
    }
    return containers;
  }

  private ContainerInfo getMockDeletedContainer(int containerID) {
    ContainerInfo c = mock(ContainerInfo.class);
    when(c.getContainerID()).thenReturn((long)containerID);
    when(c.getReplicationConfig())
        .thenReturn(RatisReplicationConfig
            .getInstance(THREE));
    when(c.containerID()).thenReturn(ContainerID.valueOf(containerID));
    when(c.getState()).thenReturn(HddsProtos.LifeCycleState.DELETED);
    return c;
  }

  /**
   * This is a simple implementation of PlacementPolicy, so that when
   * validateContainerPlacement() is called, by default it will return a value
   * placement object. To get an invalid placement object, simply pass a UUID
   * of a datanode via setMisRepWhenDnPresent. If a DN with that UUID is passed
   * to validateContainerPlacement, then it will return an invalid placement.
   */
  private static class MockPlacementPolicy implements
          PlacementPolicy {

    private UUID misRepWhenDnPresent = null;

    public void setMisRepWhenDnPresent(UUID dn) {
      misRepWhenDnPresent = dn;
    }

    @Override
    public List<DatanodeDetails> chooseDatanodes(
        List<DatanodeDetails> usedNodes, List<DatanodeDetails> excludedNodes,
        List<DatanodeDetails> favoredNodes,
        int nodesRequired, long metadataSizeRequired, long dataSizeRequired)
        throws IOException {
      return null;
    }

    @Override
    public ContainerPlacementStatus validateContainerPlacement(
        List<DatanodeDetails> dns, int replicas) {
      if (misRepWhenDnPresent != null && isDnPresent(dns)) {
        return new ContainerPlacementStatusDefault(1, 2, 3);
      } else {
        return new ContainerPlacementStatusDefault(1, 1, 1);
      }
    }

    @Override
    public Set<ContainerReplica> replicasToCopyToFixMisreplication(
            Map<ContainerReplica, Boolean> replicas) {
      return Collections.emptySet();
    }

    @Override
    public Set<ContainerReplica> replicasToRemoveToFixOverreplication(
            Set<ContainerReplica> replicas, int expectedCountPerUniqueReplica) {
      return null;
    }

    private boolean isDnPresent(List<DatanodeDetails> dns) {
      for (DatanodeDetails dn : dns) {
        if (misRepWhenDnPresent != null
            && dn.getUuid().equals(misRepWhenDnPresent)) {
          return true;
        }
      }
      return false;
    }
  }

}
