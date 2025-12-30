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

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.ozone.recon.ReconConstants.CONTAINER_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.pojos.UnhealthyContainers;
import org.apache.ozone.recon.schema.generated.tables.records.UnhealthyContainersRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to validate the ContainerHealthTask Record Generator creates the correct
 * records to store in the database.
 */
public class TestContainerHealthTaskRecordGenerator {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerHealthTaskRecordGenerator.class);
  private PlacementPolicy placementPolicy;
  private ContainerInfo container;
  private ContainerInfo emptyContainer;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  @BeforeEach
  public void setup() throws IOException {
    placementPolicy = mock(PlacementPolicy.class);
    container = mock(ContainerInfo.class);
    emptyContainer = mock(ContainerInfo.class);
    reconContainerMetadataManager = mock(ReconContainerMetadataManager.class);
    when(container.getReplicationFactor())
        .thenReturn(HddsProtos.ReplicationFactor.THREE);
    when(container.getReplicationConfig())
        .thenReturn(
            RatisReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.THREE));
    when(container.getState()).thenReturn(HddsProtos.LifeCycleState.CLOSED);
    when(container.containerID()).thenReturn(ContainerID.valueOf(123456));
    when(container.getContainerID()).thenReturn((long)123456);
    when(reconContainerMetadataManager.getKeyCountForContainer(
        (long) 123456)).thenReturn(5L);
    when(emptyContainer.getReplicationFactor())
        .thenReturn(HddsProtos.ReplicationFactor.THREE);
    when(emptyContainer.getReplicationConfig())
        .thenReturn(
            RatisReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.THREE));
    when(emptyContainer.containerID()).thenReturn(ContainerID.valueOf(345678));
    when(emptyContainer.getContainerID()).thenReturn((long) 345678);
    when(placementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 1, 1));
  }

  @Test
  public void testMissingRecordRetained() {
    Set<ContainerReplica> replicas = new HashSet<>();
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    // Missing record should be retained
    assertTrue(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()
        ));
    // Under / Over / Mis replicated should not be retained as if a container is
    // missing then it is not in any other category.
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, underReplicatedRecord()
        ));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, overReplicatedRecord()
        ));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, misReplicatedRecord()
        ));

    replicas = generateReplicas(container, CLOSED, CLOSED, CLOSED);
    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()
        ));
  }

  @Test
  public void testEmptyMissingRecordNotInsertedButLogged() {
    // Create a container that is in EMPTY_MISSING state
    Set<ContainerReplica> replicas = new HashSet<>();
    ContainerHealthStatus status = new ContainerHealthStatus(emptyContainer, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);

    // Initialize stats map
    Map<UnHealthyContainerStates, Map<String, Long>> unhealthyContainerStateStatsMap = new HashMap<>();
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    // Generate records for EMPTY_MISSING container
    List<UnhealthyContainers> records = ContainerHealthTask.ContainerHealthRecords.generateUnhealthyRecords(
            status, (long) 345678, unhealthyContainerStateStatsMap);

    // Assert that no records are created for EMPTY_MISSING state
    assertEquals(0, records.size());

    // Assert that the EMPTY_MISSING state is logged
    assertEquals(1, unhealthyContainerStateStatsMap.get(UnHealthyContainerStates.EMPTY_MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
  }

  @Test
  public void testNegativeSizeRecordNotInsertedButLogged() {
    // Simulate a container with NEGATIVE_SIZE state
    when(container.getUsedBytes()).thenReturn(-10L); // Negative size
    Set<ContainerReplica> replicas = generateReplicas(container, CLOSED, CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy, reconContainerMetadataManager, CONF);

    // Initialize stats map
    Map<UnHealthyContainerStates, Map<String, Long>>
        unhealthyContainerStateStatsMap = new HashMap<>();
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    // Generate records for NEGATIVE_SIZE container
    List<UnhealthyContainers> records =
        ContainerHealthTask.ContainerHealthRecords.generateUnhealthyRecords(
            status, (long) 123456, unhealthyContainerStateStatsMap);

    // Assert that none of the records are for negative.
    records.forEach(record -> assertNotEquals(
        UnHealthyContainerStates.NEGATIVE_SIZE.toString(), record.getContainerState()));


    // Assert that the NEGATIVE_SIZE state is logged
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.NEGATIVE_SIZE).getOrDefault(CONTAINER_COUNT, 0L));
  }

  @Test
  public void testUnderReplicatedRecordRetainedAndUpdated() {
    // under replicated container
    Set<ContainerReplica> replicas =
        generateReplicas(container, CLOSED, CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);

    UnhealthyContainersRecord rec = underReplicatedRecord();
    assertTrue(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
    // The record actual count should be updated from 1 -> 2
    assertEquals(2, rec.getActualReplicaCount().intValue());
    assertEquals(1, rec.getReplicaDelta().intValue());

    // Missing / Over / Mis replicated should not be retained
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()
        ));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, overReplicatedRecord()
        ));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, misReplicatedRecord()
        ));

    // Container is now replicated OK - should be removed.
    replicas = generateReplicas(container, CLOSED, CLOSED, CLOSED);
    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
  }

  @Test
  public void testOverReplicatedRecordRetainedAndUpdated() {
    // under replicated container
    Set<ContainerReplica> replicas =
        generateReplicas(container, CLOSED, CLOSED, CLOSED, CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);

    UnhealthyContainersRecord rec = overReplicatedRecord();
    assertTrue(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
    // The record actual count should be updated from 5 -> 4
    assertEquals(4, rec.getActualReplicaCount().intValue());
    assertEquals(-1, rec.getReplicaDelta().intValue());

    // Missing / Over / Mis replicated should not be retained
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()
        ));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, underReplicatedRecord()
        ));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, misReplicatedRecord()
        ));

    // Container is now replicated OK - should be removed.
    replicas = generateReplicas(container, CLOSED, CLOSED, CLOSED);
    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
  }

  @Test
  public void testMisReplicatedRecordRetainedAndUpdated() {
    // under replicated container
    Set<ContainerReplica> replicas =
        generateReplicas(container, CLOSED, CLOSED, CLOSED);
    when(placementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 3, 5));
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);

    UnhealthyContainersRecord rec = misReplicatedRecord();
    assertTrue(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
    // The record actual count should be updated from 1 -> 2
    assertEquals(2, rec.getActualReplicaCount().intValue());
    assertEquals(1, rec.getReplicaDelta().intValue());
    assertNotNull(rec.getReason());

    // Missing / Over / Mis replicated should not be retained
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()
        ));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, underReplicatedRecord()
        ));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, overReplicatedRecord()
        ));

    // Container is now placed OK - should be removed.
    when(placementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(3, 3, 5));
    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
  }

  @Test
  @SuppressWarnings("checkstyle:methodlength")
  public void testCorrectRecordsGenerated() {
    Set<ContainerReplica> replicas =
        generateReplicas(container, CLOSED, CLOSED, CLOSED);
    Map<UnHealthyContainerStates, Map<String, Long>>
        unhealthyContainerStateStatsMap =
        new HashMap<>();
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);
    // HEALTHY container - no records generated.
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    List<UnhealthyContainers> records =
        ContainerHealthTask.ContainerHealthRecords
            .generateUnhealthyRecords(status, (long) 1234567,
                unhealthyContainerStateStatsMap);
    assertEquals(0, records.size());
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.EMPTY_MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.OVER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.UNDER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MIS_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));

    logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    // Over-replicated - expect 1 over replicated record
    replicas =
        generateReplicas(container, CLOSED, CLOSED, CLOSED, CLOSED, CLOSED);
    status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, (long) 1234567,
            unhealthyContainerStateStatsMap);
    assertEquals(1, records.size());
    UnhealthyContainers rec = records.get(0);
    assertEquals(UnHealthyContainerStates.OVER_REPLICATED.toString(),
        rec.getContainerState());
    assertEquals(3, rec.getExpectedReplicaCount().intValue());
    assertEquals(5, rec.getActualReplicaCount().intValue());
    assertEquals(-2, rec.getReplicaDelta().intValue());
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.EMPTY_MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.OVER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.UNDER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MIS_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));

    logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    // Replica mismatch
    replicas = generateMismatchedReplicas(container, CLOSED, CLOSED, CLOSED);
    status =
            new ContainerHealthStatus(container, replicas, placementPolicy,
                    reconContainerMetadataManager, CONF);
    records = ContainerHealthTask.ContainerHealthRecords
            .generateUnhealthyRecords(status, (long) 1234567,
                    unhealthyContainerStateStatsMap);
    assertEquals(1, records.size());
    assertEquals(1, unhealthyContainerStateStatsMap.get(
                    UnHealthyContainerStates.REPLICA_MISMATCH)
            .getOrDefault(CONTAINER_COUNT, 0L));

    logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    // Same data checksum replicas
    replicas = generateReplicas(container, CLOSED, CLOSED, CLOSED);
    status =
            new ContainerHealthStatus(container, replicas, placementPolicy,
                    reconContainerMetadataManager, CONF);
    records = ContainerHealthTask.ContainerHealthRecords
            .generateUnhealthyRecords(status, (long) 1234567,
                    unhealthyContainerStateStatsMap);
    assertEquals(0, records.size());
    assertEquals(0, unhealthyContainerStateStatsMap.get(
                    UnHealthyContainerStates.REPLICA_MISMATCH)
            .getOrDefault(CONTAINER_COUNT, 0L));

    logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    // Under and Mis Replicated - expect 2 records - mis and under replicated
    replicas =
        generateReplicas(container, CLOSED, CLOSED);
    when(placementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 5));
    status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, (long) 1234567,
            unhealthyContainerStateStatsMap);
    assertEquals(2, records.size());
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.EMPTY_MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.OVER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.UNDER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MIS_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));

    logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    rec = findRecordForState(records, UnHealthyContainerStates.MIS_REPLICATED);
    assertEquals(UnHealthyContainerStates.MIS_REPLICATED.toString(),
        rec.getContainerState());
    assertEquals(2, rec.getExpectedReplicaCount().intValue());
    assertEquals(1, rec.getActualReplicaCount().intValue());
    assertEquals(1, rec.getReplicaDelta().intValue());
    assertNotNull(rec.getReason());

    rec = findRecordForState(records,
        UnHealthyContainerStates.UNDER_REPLICATED);
    assertEquals(UnHealthyContainerStates.UNDER_REPLICATED.toString(),
        rec.getContainerState());
    assertEquals(3, rec.getExpectedReplicaCount().intValue());
    assertEquals(2, rec.getActualReplicaCount().intValue());
    assertEquals(1, rec.getReplicaDelta().intValue());

    // Missing Record - expect just a single missing record even though
    // it is mis-replicated too
    replicas.clear();
    when(placementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 5));
    status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, (long) 1234567,
            unhealthyContainerStateStatsMap);
    assertEquals(1, records.size());
    rec = records.get(0);
    assertEquals(UnHealthyContainerStates.MISSING.toString(),
        rec.getContainerState());
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.EMPTY_MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.OVER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.UNDER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MIS_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));

    logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    status =
        new ContainerHealthStatus(emptyContainer, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, (long) 345678,
            unhealthyContainerStateStatsMap);

    assertEquals(3, rec.getExpectedReplicaCount().intValue());
    assertEquals(0, rec.getActualReplicaCount().intValue());
    assertEquals(3, rec.getReplicaDelta().intValue());

    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.EMPTY_MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.OVER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.UNDER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MIS_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    unhealthyContainerStateStatsMap.clear();
  }

  @Test
  public void testRecordNotGeneratedIfAlreadyExists() {
    Map<UnHealthyContainerStates, Map<String, Long>>
        unhealthyContainerStateStatsMap =
        new HashMap<>();
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);
    Set<String> existingRec = new HashSet<>();

    // Over-replicated
    Set<ContainerReplica> replicas = generateReplicas(
        container, CLOSED, CLOSED, CLOSED, CLOSED, CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy,
            reconContainerMetadataManager, CONF);
    List<UnhealthyContainers> records =
        ContainerHealthTask.ContainerHealthRecords
            .generateUnhealthyRecords(status, existingRec, (long) 1234567,
                unhealthyContainerStateStatsMap);
    assertEquals(1, records.size());
    assertEquals(0, unhealthyContainerStateStatsMap.get(
        UnHealthyContainerStates.MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.EMPTY_MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.OVER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.UNDER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MIS_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));

    logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    // Missing
    replicas.clear();
    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, existingRec, (long) 1234567,
            unhealthyContainerStateStatsMap);
    assertEquals(1, records.size());
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.EMPTY_MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.OVER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.UNDER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MIS_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));

    logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
    initializeUnhealthyContainerStateStatsMap(unhealthyContainerStateStatsMap);

    // Under and Mis-Replicated
    replicas = generateReplicas(container, CLOSED, CLOSED);
    when(placementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 5));
    status = new ContainerHealthStatus(container, replicas, placementPolicy,
        reconContainerMetadataManager, CONF);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, existingRec, (long) 1234567,
            unhealthyContainerStateStatsMap);
    assertEquals(2, records.size());
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.EMPTY_MISSING)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(0, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.OVER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.UNDER_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));
    assertEquals(1, unhealthyContainerStateStatsMap.get(
            UnHealthyContainerStates.MIS_REPLICATED)
        .getOrDefault(CONTAINER_COUNT, 0L));

    logUnhealthyContainerStats(unhealthyContainerStateStatsMap);
    unhealthyContainerStateStatsMap.clear();
  }

  private UnhealthyContainers findRecordForState(
      List<UnhealthyContainers> recs, UnHealthyContainerStates state) {
    for (UnhealthyContainers r : recs) {
      if (r.getContainerState().equals(state.toString())) {
        return r;
      }
    }
    return null;
  }

  private UnhealthyContainersRecord missingRecord() {
    return new UnhealthyContainersRecord(container.containerID().getId(),
        UnHealthyContainerStates.MISSING.toString(), 10L,
        3, 0, 3, null);
  }

  private UnhealthyContainersRecord underReplicatedRecord() {
    return new UnhealthyContainersRecord(container.containerID().getId(),
        UnHealthyContainerStates.UNDER_REPLICATED.toString(),
        10L, 3, 1, 2, null);
  }

  private UnhealthyContainersRecord overReplicatedRecord() {
    return new UnhealthyContainersRecord(container.containerID().getId(),
        UnHealthyContainerStates.OVER_REPLICATED.toString(), 10L,
        3, 5, -2, null);
  }

  private UnhealthyContainersRecord misReplicatedRecord() {
    return new UnhealthyContainersRecord(container.containerID().getId(),
        UnHealthyContainerStates.MIS_REPLICATED.toString(), 10L,
        3, 1, 2, "should be on 1 more rack");
  }

  private Set<ContainerReplica> generateReplicas(ContainerInfo cont,
      ContainerReplicaProto.State...states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (ContainerReplicaProto.State s : states) {
      replicas.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(cont.containerID())
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
          .setChecksums(ContainerChecksums.of(1234L, 0L))
          .setContainerState(s)
          .build());
    }
    return replicas;
  }

  private Set<ContainerReplica> generateMismatchedReplicas(ContainerInfo cont,
                                                 ContainerReplicaProto.State...states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    long checksum = 1234L;
    for (ContainerReplicaProto.State s : states) {
      replicas.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(cont.containerID())
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
          .setContainerState(s)
          .setChecksums(ContainerChecksums.of(checksum, 0L))
          .build());
      checksum++;
    }
    return replicas;
  }

  private void initializeUnhealthyContainerStateStatsMap(
      Map<UnHealthyContainerStates, Map<String, Long>>
          unhealthyContainerStateStatsMap) {
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.MISSING, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.EMPTY_MISSING, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.UNDER_REPLICATED, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.OVER_REPLICATED, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.MIS_REPLICATED, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.NEGATIVE_SIZE, new HashMap<>());
    unhealthyContainerStateStatsMap.put(
        UnHealthyContainerStates.REPLICA_MISMATCH, new HashMap<>());
  }

  private void logUnhealthyContainerStats(
      Map<UnHealthyContainerStates, Map<String, Long>>
          unhealthyContainerStateStatsMap) {
    // If any EMPTY_MISSING containers, then it is possible that such
    // containers got stuck in the closing state which never got
    // any replicas created on the datanodes. In this case, we log it as
    // EMPTY_MISSING containers, but dont add it to the unhealthy container table.
    unhealthyContainerStateStatsMap.entrySet().forEach(stateEntry -> {
      UnHealthyContainerStates unhealthyContainerState = stateEntry.getKey();
      Map<String, Long> containerStateStatsMap = stateEntry.getValue();
      StringBuilder logMsgBuilder =
          new StringBuilder(unhealthyContainerState.toString());
      logMsgBuilder.append(" Container State Stats: \n\t");
      containerStateStatsMap.entrySet().forEach(statsEntry -> {
        logMsgBuilder.append(statsEntry.getKey());
        logMsgBuilder.append(" -> ");
        logMsgBuilder.append(statsEntry.getValue());
        logMsgBuilder.append(" , ");
      });
      LOG.info(logMsgBuilder.toString());
    });
  }
}
