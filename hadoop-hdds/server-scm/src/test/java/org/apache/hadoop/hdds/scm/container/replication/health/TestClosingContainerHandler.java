/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;

/**
 * Tests for {@link ClosingContainerHandler}.
 */
public class TestClosingContainerHandler {
  private ReplicationManager replicationManager;
  private ClosingContainerHandler closingContainerHandler;
  private ECReplicationConfig ecReplicationConfig;
  private RatisReplicationConfig ratisReplicationConfig;

  @BeforeEach
  public void setup() {
    ecReplicationConfig = new ECReplicationConfig(3, 2);
    ratisReplicationConfig = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE);
    replicationManager = Mockito.mock(ReplicationManager.class);
    closingContainerHandler = new ClosingContainerHandler(replicationManager);
  }

  /**
   * If a container is not closing, it should not be handled by
   * ClosingContainerHandler. It should return false so the request can be
   * passed to the next handler in the chain.
   */
  @Test
  public void testNonClosingContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 1, 2, 3, 4, 5);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, false, 0);
  }

  @Test
  public void testNonClosingRatisContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSED);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 0, 0, 0);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
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
        ecReplicationConfig, 1, CLOSING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.UNHEALTHY, 1, 2, 3, 4);
    ContainerReplica openReplica = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 5,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN);
    containerReplicas.add(openReplica);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 1);
  }

  @Test
  public void testUnhealthyRatisReplicaIsNotClosed() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.UNHEALTHY, 0, 0);
    ContainerReplica openReplica = ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN);
    containerReplicas.add(openReplica);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 1);
  }

  /**
   * Close commands should be sent for Open or Closing replicas.
   */
  @Test
  public void testOpenOrClosingReplicasAreClosed() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 1, 2);
    containerReplicas.add(ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 3,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN));

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 3);
  }

  @Test
  public void testOpenOrClosingRatisReplicasAreClosed() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ratisReplicationConfig, 1, CLOSING);
    Set<ContainerReplica> containerReplicas = ReplicationTestUtil
        .createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSING, 0, 0);
    containerReplicas.add(ReplicationTestUtil.createContainerReplica(
        containerInfo.containerID(), 0,
        HddsProtos.NodeOperationalState.IN_SERVICE,
        ContainerReplicaProto.State.OPEN));

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.EMPTY_LIST)
        .setReport(new ReplicationManagerReport())
        .setContainerInfo(containerInfo)
        .setContainerReplicas(containerReplicas)
        .build();

    assertAndVerify(request, true, 3);
  }


  private void assertAndVerify(ContainerCheckRequest request,
      boolean assertion, int times) {
    Assertions.assertEquals(assertion, closingContainerHandler.handle(request));
    Mockito.verify(replicationManager, Mockito.times(times))
        .sendCloseContainerReplicaCommand(Mockito.any(ContainerInfo.class),
            Mockito.any(DatanodeDetails.class), Mockito.anyBoolean());
  }
}
