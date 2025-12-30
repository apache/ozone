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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EmptyContainerHandler}.
 */
public class TestEmptyContainerHandler {
  private ReplicationManager replicationManager;
  private ReplicationManager.ReplicationManagerConfiguration rmConf;
  private EmptyContainerHandler emptyContainerHandler;
  private ECReplicationConfig ecReplicationConfig;
  private RatisReplicationConfig ratisReplicationConfig;

  @BeforeEach
  public void setup()
      throws IOException, InvalidStateTransitionException, TimeoutException {
    ecReplicationConfig = new ECReplicationConfig(3, 2);
    ratisReplicationConfig = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE);
    replicationManager = mock(ReplicationManager.class);
    emptyContainerHandler =
        new EmptyContainerHandler(replicationManager);
    rmConf = mock(ReplicationManager.ReplicationManagerConfiguration.class);
  }

  /**
   * A container is considered empty if it has 0 key count. Handler should
   * return true for empty and CLOSED EC containers.
   */
  @Test
  public void testEmptyAndClosedECContainerReturnsTrue()
      throws IOException {
    long keyCount = 0L;
    long bytesUsed = 123L;
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSED, keyCount, bytesUsed);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, keyCount, bytesUsed,
            1, 2, 3, 4, 5);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();

    assertAndVerify(readRequest, true, 0, 1);
    assertAndVerify(request, true, 5, 1);
  }

  @Test
  public void testEmptyAndClosedRatisContainerReturnsTrue()
      throws IOException {
    long keyCount = 0L;
    long bytesUsed = 123L;
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED, keyCount, bytesUsed);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, keyCount, bytesUsed,
            0, 0, 0);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    ContainerCheckRequest readRequest = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .setReadOnly(true)
        .build();

    assertAndVerify(readRequest, true, 0, 1);
    assertAndVerify(request, true, 3, 1);
  }

  /**
   * Handler should return false when key count is 0 but the container is not
   * in CLOSED state.
   */
  @Test
  public void testEmptyAndNonClosedECContainerReturnsFalse()
      throws IOException {
    long keyCount = 0L;
    long bytesUsed = 123L;
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSING, keyCount, bytesUsed);

    // though key count is 0, the container and its replicas are not CLOSED
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.OPEN, keyCount, bytesUsed,
            1, 2, 3, 4, 5);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, false, 0, 0);
  }

  /**
   * This test exists to verify that the definition of an empty container is
   * 0 key count. Number of used bytes are not considered.
   */
  @Test
  public void testNonEmptyRatisContainerReturnsFalse()
      throws IOException {
    long keyCount = 5L;
    long bytesUsed = 123L;
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED, keyCount, bytesUsed);

    // though container and its replicas are CLOSED, key count is not 0
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, keyCount, bytesUsed,
            0, 0, 0);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, false, 0, 0);
  }

  /**
   * This test exists to verify that the definition of an empty container is
   * 0 key count. Number of used bytes are not considered.
   */
  @Test
  public void testEmptyContainerWithNoReplicas()
      throws IOException {
    long keyCount = 0L;
    long bytesUsed = 0L;
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED, keyCount, bytesUsed);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(Collections.emptySet())
        .build();

    assertAndVerify(request, true, 0, 1);
  }

  /**
   * Handler should return false when there is a non-empty replica.
   */
  @Test
  public void testEmptyECContainerWithNonEmptyReplicaReturnsFalse()
      throws IOException {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSED, 0L, 0L);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0L, 0L,
            1, 2, 3, 4);

    // add a non-empty replica
    DatanodeDetails mockDn = MockDatanodeDetails.randomDatanodeDetails();
    containerReplicas.add(
        ReplicationTestUtil.createContainerReplica(containerInfo.containerID(),
            5, HddsProtos.NodeOperationalState.IN_SERVICE,
            ContainerReplicaProto.State.CLOSED, 1L, 100L, mockDn,
            mockDn.getID()));

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport(rmConf.getContainerSampleLimit()))
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    // should return false because there is a non-empty replica
    assertAndVerify(request, false, 0, 0);
  }

  /**
   * Asserts that handler returns the specified assertion and delete command
   * to replicas is sent the specified number of times.
   * @param request ContainerCheckRequest object to pass to the handler
   * @param assertion true if expecting the handler to return true, else false
   * @param times number of times the delete command is expected to be sent
   * @param numEmptyExpected number of EMPTY and CLOSED containers expected
   *                         to be found in ReplicationManagerReport
   */
  private void assertAndVerify(ContainerCheckRequest request,
      boolean assertion, int times, long numEmptyExpected)
      throws IOException {
    assertEquals(assertion, emptyContainerHandler.handle(request));
    verify(replicationManager, times(times)).sendDeleteCommand(any(ContainerInfo.class), anyInt(),
        any(DatanodeDetails.class), eq(false));
    assertEquals(numEmptyExpected, request.getReport().getStat(ReplicationManagerReport.HealthState.EMPTY));

    if (times > 0) {
      verify(replicationManager, times(1)).updateContainerState(any(ContainerID.class),
          any(HddsProtos.LifeCycleEvent.class));
    }
  }

}
