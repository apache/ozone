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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerInfo;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createContainerReplica;

/**
 * Tests for {@link ClosedWithUnhealthyReplicasHandler}.
 */
public class TestClosedWithUnhealthyReplicasHandler {
  private ClosedWithUnhealthyReplicasHandler handler;
  private ReplicationManager replicationManager;
  private ECReplicationConfig ecReplicationConfig;
  private ContainerCheckRequest.Builder requestBuilder;

  @Before
  public void setup() {
    ecReplicationConfig = new ECReplicationConfig(3, 2);
    replicationManager = Mockito.mock(ReplicationManager.class);
    handler = new ClosedWithUnhealthyReplicasHandler(replicationManager);
    requestBuilder = new ContainerCheckRequest.Builder()
        .setPendingOps(Collections.emptyList())
        .setReport(new ReplicationManagerReport());
  }

  @Test
  public void testNonClosedContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        ecReplicationConfig, 1, CLOSING);
    Set<ContainerReplica> containerReplicas =
        ReplicationTestUtil.createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);
    containerReplicas.add(
        ReplicationTestUtil.createContainerReplica(containerInfo.containerID(),
            5, HddsProtos.NodeOperationalState.IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY));

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(containerReplicas)
        .setContainerInfo(containerInfo)
        .build();

    Assert.assertFalse(handler.handle(request));
  }

  @Test
  public void testRatisContainerReturnsFalse() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE), 1,
        CLOSED);
    Set<ContainerReplica> containerReplicas =
        ReplicationTestUtil.createReplicas(containerInfo.containerID(),
            ContainerReplicaProto.State.CLOSED, 0, 0, 0);
    containerReplicas.add(
        ReplicationTestUtil.createContainerReplica(containerInfo.containerID(),
            0, HddsProtos.NodeOperationalState.IN_SERVICE,
            ContainerReplicaProto.State.UNHEALTHY));

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(containerReplicas)
        .setContainerInfo(containerInfo)
        .build();

    Assert.assertFalse(handler.handle(request));
  }

  /**
   * A closed EC container that is not under, over, or mis replicated but
   * has some unhealthy replicas should be handled.
   */
  @Test
  public void
      testClosedWithSufficientReplicationDespiteUnhealthyReplicasReturnsTrue()
      throws NotLeaderException {
    ContainerInfo container =
        createContainerInfo(ecReplicationConfig, 1, CLOSED);

    // create 5 closed replicas and 1 unhealthy replica each for indices 2 and 5
    Set<ContainerReplica> containerReplicas =
        ReplicationTestUtil.createReplicas(container.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);
    ContainerReplica unhealthyIndex2 =
        createContainerReplica(container.containerID(), 2,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    ContainerReplica unhealthyIndex5 =
        createContainerReplica(container.containerID(), 5,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    containerReplicas.add(unhealthyIndex2);
    containerReplicas.add(unhealthyIndex5);

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(containerReplicas)
        .setContainerInfo(container)
        .build();

    Assert.assertTrue(handler.handle(request));
    Assert.assertEquals(1, request.getReport().getStat(
        ReplicationManagerReport.HealthState.UNHEALTHY));

    ArgumentCaptor<Integer> replicaIndexCaptor =
        ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(replicationManager, Mockito.times(2))
        .sendDeleteCommand(Mockito.eq(container), Mockito.anyInt(), Mockito.any(
            DatanodeDetails.class));
    // replica index that delete was sent for should either be 2 or 5
    replicaIndexCaptor.getAllValues()
        .forEach(index -> Assert.assertTrue(index == 2 || index == 5));
  }

  /**
   * Closed EC container that does not have unhealthy replicas should not be
   * handled.
   */
  @Test
  public void testClosedWithNoUnhealthyReplicasShouldReturnFalse() {
    ContainerInfo container =
        createContainerInfo(ecReplicationConfig, 1, CLOSED);

    Set<ContainerReplica> containerReplicas =
        ReplicationTestUtil.createReplicas(container.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 2, 3, 4, 5);

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(containerReplicas)
        .setContainerInfo(container)
        .build();

    Assert.assertFalse(handler.handle(request));
  }

  @Test
  public void testUnderReplicatedContainerReturnsFalse() {
    ContainerInfo container =
        createContainerInfo(ecReplicationConfig, 1, CLOSED);

    // make it under replicated by having only UNHEALTHY replica for index 2
    Set<ContainerReplica> containerReplicas =
        ReplicationTestUtil.createReplicas(container.containerID(),
            ContainerReplicaProto.State.CLOSED, 1, 3, 4, 5);
    ContainerReplica unhealthyIndex2 =
        createContainerReplica(container.containerID(), 2,
            IN_SERVICE, ContainerReplicaProto.State.UNHEALTHY);
    containerReplicas.add(unhealthyIndex2);

    ContainerCheckRequest request = requestBuilder
        .setContainerReplicas(containerReplicas)
        .setContainerInfo(container)
        .build();

    Assert.assertFalse(handler.handle(request));
  }
}
