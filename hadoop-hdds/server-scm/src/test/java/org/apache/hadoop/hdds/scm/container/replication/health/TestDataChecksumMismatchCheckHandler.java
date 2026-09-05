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

package org.apache.hadoop.hdds.scm.container.replication.health;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.DATA_CHECKSUM_MISMATCH;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DataChecksumMismatchCheckHandler}.
 */
public class TestDataChecksumMismatchCheckHandler {

  private DataChecksumMismatchCheckHandler handler;

  @BeforeEach
  void setup() {
    handler = new DataChecksumMismatchCheckHandler();
  }

  @Test
  void testMismatchIsReportedAfterTwoScans() {
    ContainerInfo container = createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 10,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = createReplicasWithChecksums(container,
        new long[]{10, 10, 10}, new long[]{100, 200, 100});

    ReplicationManagerReport firstReport = runScan(container, replicas);
    assertEquals(0, firstReport.getStat(DATA_CHECKSUM_MISMATCH));
    assertFalse(handler.hasPersistentMismatch(container.containerID()));

    ReplicationManagerReport secondReport = runScan(container, replicas);
    assertEquals(1, secondReport.getStat(DATA_CHECKSUM_MISMATCH));
    assertThat(secondReport.getSample(DATA_CHECKSUM_MISMATCH))
        .containsExactly(container.containerID());
    assertThat(handler.hasPersistentMismatch(container.containerID())).isTrue();

    ReplicationManagerReport resolvedReport = runScan(container,
        createReplicasWithChecksums(container, new long[]{10, 10, 10},
            new long[]{100, 100, 100}));
    assertEquals(0, resolvedReport.getStat(DATA_CHECKSUM_MISMATCH));
    assertFalse(handler.hasPersistentMismatch(container.containerID()));
  }

  @Test
  void testOnlyClosedRatisContainersAreChecked() {
    ContainerInfo openRatis = createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 10,
        HddsProtos.LifeCycleState.OPEN);
    Set<ContainerReplica> replicas = createReplicasWithChecksums(openRatis,
        new long[]{10, 10, 10}, new long[]{100, 200, 100});
    runScan(openRatis, replicas);
    ReplicationManagerReport report = runScan(openRatis, replicas);
    assertEquals(0, report.getStat(DATA_CHECKSUM_MISMATCH));

    ContainerInfo closedEc = createContainerInfo(
        new ECReplicationConfig(3, 2), 10,
        HddsProtos.LifeCycleState.CLOSED);
    replicas = createReplicasWithChecksums(closedEc,
        new long[]{10, 10, 10}, new long[]{100, 200, 100});
    runScan(closedEc, replicas);
    report = runScan(closedEc, replicas);
    assertEquals(0, report.getStat(DATA_CHECKSUM_MISMATCH));
  }

  @Test
  void testIncompleteScanIsIgnored() {
    ContainerInfo container = createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 10,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = createReplicasWithChecksums(container,
        new long[]{10, 10, 10}, new long[]{100, 200, 100});

    handler.startScan();
    assertFalse(handler.handle(createRequest(container, replicas,
        new ReplicationManagerReport(10))));
    handler.abortScan();

    ReplicationManagerReport report = runScan(container, replicas);
    assertEquals(0, report.getStat(DATA_CHECKSUM_MISMATCH));
    assertFalse(handler.hasPersistentMismatch(container.containerID()));
  }

  @Test
  void testReadOnlyCheckDoesNotAffectDebounce() {
    ContainerInfo container = createContainerInfo(
        RatisReplicationConfig.getInstance(THREE), 10,
        HddsProtos.LifeCycleState.CLOSED);
    Set<ContainerReplica> replicas = createReplicasWithChecksums(container,
        new long[]{10, 10, 10}, new long[]{100, 200, 100});

    handler.startScan();
    ReplicationManagerReport readOnlyReport = new ReplicationManagerReport(10);
    assertFalse(handler.handle(createRequest(container, replicas,
        readOnlyReport, true)));
    handler.completeScan(readOnlyReport);

    ReplicationManagerReport report = runScan(container, replicas);
    assertEquals(0, report.getStat(DATA_CHECKSUM_MISMATCH));
    assertFalse(handler.hasPersistentMismatch(container.containerID()));
  }

  private ReplicationManagerReport runScan(ContainerInfo container,
      Set<ContainerReplica> replicas) {
    ReplicationManagerReport report = new ReplicationManagerReport(10);
    handler.startScan();
    assertFalse(handler.handle(createRequest(container, replicas, report)));
    handler.completeScan(report);
    return report;
  }

  private static ContainerCheckRequest createRequest(ContainerInfo container,
      Set<ContainerReplica> replicas, ReplicationManagerReport report) {
    return createRequest(container, replicas, report, false);
  }

  private static ContainerCheckRequest createRequest(ContainerInfo container,
      Set<ContainerReplica> replicas, ReplicationManagerReport report,
      boolean readOnly) {
    return new ContainerCheckRequest.Builder()
        .setContainerInfo(container)
        .setContainerReplicas(replicas)
        .setPendingOps(Collections.emptyList())
        .setMaintenanceRedundancy(2)
        .setReport(report)
        .setReplicationQueue(new ReplicationQueue())
        .setReadOnly(readOnly)
        .build();
  }

  private static Set<ContainerReplica> createReplicasWithChecksums(
      ContainerInfo container, long[] sequenceIds, long[] dataChecksums) {
    assertEquals(sequenceIds.length, dataChecksums.length);
    Set<ContainerReplica> replicas = new HashSet<>();
    for (int i = 0; i < sequenceIds.length; i++) {
      replicas.add(createContainerReplica(container.containerID(), i + 1,
          IN_SERVICE, ContainerReplicaProto.State.CLOSED, sequenceIds[i])
          .toBuilder()
          .setChecksums(ContainerChecksums.of(dataChecksums[i]))
          .build());
    }
    return replicas;
  }
}
