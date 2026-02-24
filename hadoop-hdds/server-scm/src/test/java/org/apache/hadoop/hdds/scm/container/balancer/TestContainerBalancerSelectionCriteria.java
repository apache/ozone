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

package org.apache.hadoop.hdds.scm.container.balancer;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ContainerBalancerSelectionCriteria}.
 */
public class TestContainerBalancerSelectionCriteria {

  private ContainerBalancerSelectionCriteria criteria;
  private ContainerManager containerManager;
  private ReplicationManager replicationManager;
  private DatanodeDetails source;
  private ContainerInfo containerInfo;
  private ContainerID containerID;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ContainerBalancerConfiguration balancerConfiguration = conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * OzoneConsts.GB);

    NodeManager nodeManager = mock(NodeManager.class);
    containerManager = mock(ContainerManager.class);
    replicationManager = mock(ReplicationManager.class);
    FindSourceStrategy findSourceStrategy = mock(FindSourceStrategy.class);

    source = MockDatanodeDetails.randomDatanodeDetails();
    containerInfo = ReplicationTestUtil.createContainerInfo(RatisReplicationConfig.getInstance(THREE), 1L,
        HddsProtos.LifeCycleState.CLOSED, 1L, OzoneConsts.GB);
    containerID = containerInfo.containerID();

    Set<ContainerReplica> replicas = new HashSet<>();
    replicas.add(ReplicationTestUtil.createContainerReplica(containerID, 0, IN_SERVICE, CLOSED,
        1L, OzoneConsts.GB, source, source.getID()));

    when(containerManager.getContainer(containerID)).thenReturn(containerInfo);
    when(containerManager.getContainerReplicas(containerID)).thenReturn(replicas);
    when(replicationManager.isContainerReplicatingOrDeleting(containerID)).thenReturn(false);
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet()))
        .thenReturn(new ContainerHealthResult.HealthyResult(containerInfo));
    when(findSourceStrategy.canSizeLeaveSource(any(DatanodeDetails.class), anyLong())).thenReturn(true);

    criteria = new ContainerBalancerSelectionCriteria(balancerConfiguration, nodeManager, replicationManager,
        containerManager, findSourceStrategy, new HashMap<>());
  }

  @Test
  public void shouldExcludeUnderReplicatedContainer() {
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet())).thenReturn(
        new ContainerHealthResult.UnderReplicatedHealthResult(containerInfo, 1,
            false, false, false));

    assertTrue(criteria.shouldBeExcluded(containerID, source, 0L));
  }

  @Test
  public void shouldExcludeOverReplicatedContainer() {
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet())).thenReturn(
        new ContainerHealthResult.OverReplicatedHealthResult(containerInfo, 1, false));

    assertTrue(criteria.shouldBeExcluded(containerID, source, 0L));
  }

  @Test
  public void shouldExcludeMisReplicatedContainer() {
    when(replicationManager.getContainerReplicationHealth(eq(containerInfo), anySet())).thenReturn(
        new ContainerHealthResult.MisReplicatedHealthResult(containerInfo, false, "test"));

    assertTrue(criteria.shouldBeExcluded(containerID, source, 0L));
  }

  @Test
  public void shouldNotExcludeHealthyContainer() {
    assertFalse(criteria.shouldBeExcluded(containerID, source, 0L));
  }

  @Test
  public void shouldExcludeReplicatingContainer()
      throws Exception {
    when(replicationManager.isContainerReplicatingOrDeleting(containerID)).thenReturn(true);

    assertTrue(criteria.shouldBeExcluded(containerID, source, 0L));

    verify(containerManager, times(1)).getContainerReplicas(containerID);
    verify(replicationManager, times(1)).getContainerReplicationHealth(
        eq(containerInfo), anySet());
  }
}
