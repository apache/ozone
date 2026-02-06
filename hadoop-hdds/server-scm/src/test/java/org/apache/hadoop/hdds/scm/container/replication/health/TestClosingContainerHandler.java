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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.CLOSE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.QUASI_CLOSE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.TestContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link ClosingContainerHandler}.
 */
public class TestClosingContainerHandler {
  private final ReplicationManager.ReplicationManagerConfiguration rmConf =
      new OzoneConfiguration()
          .getObject(ReplicationManager.ReplicationManagerConfiguration.class);

  private ReplicationManager replicationManager;
  private ClosingContainerHandler subject;
  private static final ECReplicationConfig EC_REPLICATION_CONFIG =
      new ECReplicationConfig(3, 2);
  private static final RatisReplicationConfig RATIS_REPLICATION_CONFIG =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);

  private final TestClock clock = TestClock.newInstance();

  @BeforeEach
  public void setup() {
    replicationManager = mock(ReplicationManager.class);
    when(replicationManager.getConfig()).thenReturn(rmConf);
    subject = new ClosingContainerHandler(replicationManager, clock);
  }

  private static Stream<ReplicationConfig> replicationConfigs() {
    return Stream.of(RATIS_REPLICATION_CONFIG, EC_REPLICATION_CONFIG);
  }

  /**
   * If a container is not closing, it should not be handled by
   * ClosingContainerHandler. It should return false so the request can be
   * passed to the next handler in the chain.
   */
  @Test
  public void testNonClosingContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        EC_REPLICATION_CONFIG, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 1, 2, 3, 4, 5);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, false, 0);
  }

  @Test
  public void testNonClosingRatisContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RATIS_REPLICATION_CONFIG, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 0, 0, 0);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, false, 0);
  }

  /**
   * Close commands should not be sent for Unhealthy replicas.
   * @see
   * <a href="https://issues.apache.org/jira/browse/HDDS-5708">HDDS-5708</a>
   */
  @Test
  public void testUnhealthyReplicaIsNotClosed() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        EC_REPLICATION_CONFIG, 1, CLOSING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.UNHEALTHY, 1, 2, 3, 4);
    ContainerReplica openReplica = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 5,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN);
    containerReplicas.add(openReplica);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 1);
  }

  @Test
  public void testUnhealthyRatisReplicaIsNotClosed() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RATIS_REPLICATION_CONFIG, 1, CLOSING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.UNHEALTHY, 0, 0);
    ContainerReplica openReplica = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN);
    containerReplicas.add(openReplica);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 1);
  }

  @Test
  public void testClosingContainerStateIsNotUpdatedWhenThereAreReplicas() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RATIS_REPLICATION_CONFIG, 1, CLOSING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.OPEN, 0, 0);
    ContainerReplica openReplica = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN);
    containerReplicas.add(openReplica);

    ReplicationManagerReport report = new ReplicationManagerReport(rmConf.getContainerSampleLimit());

    ContainerCheckRequest.Builder builder = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas);
    ContainerCheckRequest request = builder.build();

    builder.setReadOnly(true);
    ContainerCheckRequest readRequest = builder.build();

    assertAndVerify(readRequest, true, 0);
    assertAndVerify(request, true, 3);
    report.getStats().forEach((k, v) -> assertEquals(0L, v));
  }

  @Test
  public void testClosingContainerStateIsUpdatedWhenThereAreNotReplicas() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RATIS_REPLICATION_CONFIG, 1, CLOSING, 1, 10);
    Set<ContainerReplica> containerReplicas = new HashSet<>();
    ReplicationManagerReport report = new ReplicationManagerReport(rmConf.getContainerSampleLimit());
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 0);
    report.getStats().forEach((k, v) -> {
      if (k.equals("MISSING")) {
        assertEquals(1L, v);
      } else {
        assertEquals(0L, v);
      }
    });
  }

  @Test
  public void testEmptyContainerInClosingState() throws InterruptedException {

    /*
     * Empty Container in CLOSING state should be CLOSED after
     * a timeout (ReplicationManager Interval * 5)
     */
    Duration replicationInterval = Duration.ofSeconds(1);
    rmConf.setInterval(replicationInterval);

    ContainerInfo containerInfo = TestContainerInfo.newBuilderForTest()
        .setReplicationConfig(RATIS_REPLICATION_CONFIG)
        .setContainerID(1)
        .setState(CLOSING)
        .setClock(clock)
        .build();
    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(new HashSet<>())
        .build();

    // not enough time has elapsed
    Duration notEnoughTime = replicationInterval.multipliedBy(3).plusMillis(1);
    clock.fastForward(notEnoughTime);
    assertAndVerify(request, true, 0);
    verify(replicationManager, never())
        .updateContainerState(containerInfo.containerID(), CLOSE);

    // wait time has elapsed (3x + 2x + a bit)
    Duration moreTime = replicationInterval.multipliedBy(2);
    clock.fastForward(moreTime);
    assertAndVerify(request, true, 0);
    verify(replicationManager, times(1))
        .updateContainerState(containerInfo.containerID(), CLOSE);
  }

  @Test
  public void testClosingRatisWithUnhealthyReplicas() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RATIS_REPLICATION_CONFIG, 1, CLOSING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.UNHEALTHY, 0, 0, 0);

    ReplicationManagerReport report = new ReplicationManagerReport(rmConf.getContainerSampleLimit());

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    subject.handle(request);

    verify(replicationManager, times(1))
        .updateContainerState(containerInfo.containerID(), QUASI_CLOSE);

    clearInvocations(replicationManager);

    // Now add an open container. This time, the container should not move to
    // quasi-closed, and a close should be sent for the open replica.
    containerReplicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.OPEN, 0));

    assertAndVerify(request, true, 1);
    verify(replicationManager, times(0))
        .updateContainerState(containerInfo.containerID(), QUASI_CLOSE);
  }

  @Test
  public void testClosingECWithUnhealthyReplicas() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        EC_REPLICATION_CONFIG, 1, CLOSING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.UNHEALTHY, 1, 2, 3, 4, 5);

    ReplicationManagerReport report = new ReplicationManagerReport(rmConf.getContainerSampleLimit());

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();
    subject.handle(request);

    verify(replicationManager, times(1))
        .updateContainerState(containerInfo.containerID(), CLOSE);

    clearInvocations(replicationManager);

    // Now add an open container. This time, the container should not move to
    // quasi-closed, and a close should be sent for the open replica.
    containerReplicas.addAll(ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.OPEN, 1));

    assertAndVerify(request, true, 1);
    verify(replicationManager, times(0))
        .updateContainerState(containerInfo.containerID(), CLOSE);
  }

  /**
   * Close commands should be sent for Open or Closing replicas.
   */
  @ParameterizedTest
  @MethodSource("replicationConfigs")
  public void testOpenOrClosingReplicasAreClosed(ReplicationConfig repConfig) {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        repConfig, 1, CLOSING);

    final int replicas = repConfig.getRequiredNodes();
    final int closing = replicas / 2;
    final boolean force = repConfig.getReplicationType() != RATIS;

    Set<ContainerReplica> containerReplicas = new HashSet<>();

    // Add CLOSING container replicas.
    // For EC, replica index will be in [1, closing].
    for (int i = 1; i <= closing; i++) {
      containerReplicas.add(ReplicationTestUtil.createContainerReplica(
          containerInfo.containerID(),
          repConfig.getReplicationType() == EC ? i : 0,
          HddsProtos.NodeOperationalState.IN_SERVICE,
          ContainerReplicaProto.State.CLOSING));
    }

    // Add OPEN container replicas.
    // For EC, replica index will be in [closing + 1, replicas].
    for (int i = closing + 1; i <= replicas; i++) {
      containerReplicas.add(ReplicationTestUtil.createContainerReplica(
          containerInfo.containerID(),
          repConfig.getReplicationType() == EC ? i : 0,
          HddsProtos.NodeOperationalState.IN_SERVICE,
          ContainerReplicaProto.State.OPEN));
    }

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    ArgumentCaptor<Boolean> forceCaptor =
        ArgumentCaptor.forClass(Boolean.class);
    assertTrue(subject.handle(request));
    verify(replicationManager, times(replicas))
        .sendCloseContainerReplicaCommand(any(ContainerInfo.class),
            any(DatanodeDetails.class), forceCaptor.capture());
    forceCaptor.getAllValues()
        .forEach(f -> assertEquals(force, f));
  }

  private void assertAndVerify(ContainerCheckRequest request,
      boolean assertion, int times) {
    assertEquals(assertion, subject.handle(request));
    verify(replicationManager, times(times))
        .sendCloseContainerReplicaCommand(any(ContainerInfo.class),
            any(DatanodeDetails.class), anyBoolean());
  }
}
