/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.fsck;

import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;
import org.hadoop.ozone.recon.schema.tables.records.UnhealthyContainersRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test to validate the ContainerHealthTask Record Generator creates the correct
 * records to store in the database.
 */
public class TestContainerHealthTaskRecordGenerator {

  private PlacementPolicy placementPolicy;
  private ContainerInfo container;

  @Before
  public void setup() {
    placementPolicy = mock(PlacementPolicy.class);
    container = mock(ContainerInfo.class);
    when(container.getReplicationFactor())
        .thenReturn(HddsProtos.ReplicationFactor.THREE);
    when(container.containerID()).thenReturn(new ContainerID(123456));
    when(container.getContainerID()).thenReturn((long)123456);
    when(placementPolicy.validateContainerPlacement(
        Mockito.anyList(), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 1, 1));
  }

  @Test
  public void testMissingRecordRetained() {
    Set<ContainerReplica> replicas = new HashSet<>();
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    // Missing record should be retained
    assertTrue(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()));
    // Under / Over / Mis replicated should not be retained as if a container is
    // missing then it is not in any other category.
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, underReplicatedRecord()));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, overReplicatedRecord()));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, misReplicatedRecord()));

    replicas = generateReplicas(container, CLOSED, CLOSED, CLOSED);
    status = new ContainerHealthStatus(container, replicas, placementPolicy);
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()));
  }

  @Test
  public void testUnderReplicatedRecordRetainedAndUpdated() {
    // under replicated container
    Set<ContainerReplica> replicas =
        generateReplicas(container, CLOSED, CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);

    UnhealthyContainersRecord rec = underReplicatedRecord();
    assertTrue(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
    // The record actual count should be updated from 1 -> 2
    assertEquals(2, rec.getActualReplicaCount().intValue());
    assertEquals(1, rec.getReplicaDelta().intValue());

    // Missing / Over / Mis replicated should not be retained
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, overReplicatedRecord()));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, misReplicatedRecord()));

    // Container is now replicated OK - should be removed.
    replicas = generateReplicas(container, CLOSED, CLOSED, CLOSED);
    status = new ContainerHealthStatus(container, replicas, placementPolicy);
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
  }

  @Test
  public void testOverReplicatedRecordRetainedAndUpdated() {
    // under replicated container
    Set<ContainerReplica> replicas =
        generateReplicas(container, CLOSED, CLOSED, CLOSED, CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);

    UnhealthyContainersRecord rec = overReplicatedRecord();
    assertTrue(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
    // The record actual count should be updated from 5 -> 4
    assertEquals(4, rec.getActualReplicaCount().intValue());
    assertEquals(-1, rec.getReplicaDelta().intValue());

    // Missing / Over / Mis replicated should not be retained
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, underReplicatedRecord()));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, misReplicatedRecord()));

    // Container is now replicated OK - should be removed.
    replicas = generateReplicas(container, CLOSED, CLOSED, CLOSED);
    status = new ContainerHealthStatus(container, replicas, placementPolicy);
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
  }

  @Test
  public void testMisReplicatedRecordRetainedAndUpdated() {
    // under replicated container
    Set<ContainerReplica> replicas =
        generateReplicas(container, CLOSED, CLOSED, CLOSED);
    when(placementPolicy.validateContainerPlacement(
        Mockito.anyList(), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 3, 5));
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);

    UnhealthyContainersRecord rec = misReplicatedRecord();
    assertTrue(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
    // The record actual count should be updated from 1 -> 2
    assertEquals(2, rec.getActualReplicaCount().intValue());
    assertEquals(1, rec.getReplicaDelta().intValue());
    assertNotNull(rec.getReason());

    // Missing / Over / Mis replicated should not be retained
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, missingRecord()));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, underReplicatedRecord()));
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, overReplicatedRecord()));

    // Container is now placed OK - should be removed.
    when(placementPolicy.validateContainerPlacement(
        Mockito.anyList(), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(3, 3, 5));
    status = new ContainerHealthStatus(container, replicas, placementPolicy);
    assertFalse(ContainerHealthTask.ContainerHealthRecords
        .retainOrUpdateRecord(status, rec));
  }

  @Test
  public void testCorrectRecordsGenerated() {
    Set<ContainerReplica> replicas =
        generateReplicas(container, CLOSED, CLOSED, CLOSED);

    // HEALTHY container - no records generated.
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    List<UnhealthyContainers> records =
        ContainerHealthTask.ContainerHealthRecords
            .generateUnhealthyRecords(status, (long)1234567);
    assertEquals(0, records.size());

    // Over-replicated - expect 1 over replicated record
    replicas =
        generateReplicas(container, CLOSED, CLOSED, CLOSED, CLOSED, CLOSED);
    status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, (long)1234567);
    assertEquals(1, records.size());
    UnhealthyContainers rec = records.get(0);
    assertEquals(UnHealthyContainerStates.OVER_REPLICATED.toString(),
        rec.getContainerState());
    assertEquals(3, rec.getExpectedReplicaCount().intValue());
    assertEquals(5, rec.getActualReplicaCount().intValue());
    assertEquals(-2, rec.getReplicaDelta().intValue());

    // Under and Mis Replicated - expect 2 records - mis and under replicated
    replicas =
        generateReplicas(container, CLOSED, CLOSED);
    when(placementPolicy.validateContainerPlacement(
        Mockito.anyList(), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 5));
    status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, (long)1234567);
    assertEquals(2, records.size());

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
        Mockito.anyList(), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 5));
    status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, (long)1234567);
    assertEquals(1, records.size());
    rec = records.get(0);
    assertEquals(UnHealthyContainerStates.MISSING.toString(),
        rec.getContainerState());
    assertEquals(3, rec.getExpectedReplicaCount().intValue());
    assertEquals(0, rec.getActualReplicaCount().intValue());
    assertEquals(3, rec.getReplicaDelta().intValue());
  }

  @Test
  public void testRecordNotGeneratedIfAlreadyExists() {
    Set<String> existingRec = new HashSet<>();
    for (UnHealthyContainerStates s : UnHealthyContainerStates.values()) {
      existingRec.add(s.toString());
    }

    // Over-replicated
    Set<ContainerReplica> replicas = generateReplicas(
        container, CLOSED, CLOSED, CLOSED, CLOSED, CLOSED);
    ContainerHealthStatus status =
        new ContainerHealthStatus(container, replicas, placementPolicy);
    List<UnhealthyContainers> records =
        ContainerHealthTask.ContainerHealthRecords
            .generateUnhealthyRecords(status, existingRec, (long)1234567);
    assertEquals(0, records.size());

    // Missing
    replicas.clear();
    status = new ContainerHealthStatus(container, replicas, placementPolicy);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, existingRec, (long)1234567);
    assertEquals(0, records.size());

    // Under and Mis-Replicated
    replicas = generateReplicas(container, CLOSED, CLOSED);
    when(placementPolicy.validateContainerPlacement(
        Mockito.anyList(), Mockito.anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(1, 2, 5));
    status = new ContainerHealthStatus(container, replicas, placementPolicy);
    records = ContainerHealthTask.ContainerHealthRecords
        .generateUnhealthyRecords(status, existingRec, (long)1234567);
    assertEquals(0, records.size());
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
          .setContainerState(s)
          .build());
    }
    return replicas;
  }

}
