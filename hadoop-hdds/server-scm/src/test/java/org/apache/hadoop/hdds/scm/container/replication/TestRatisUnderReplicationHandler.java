/**
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

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.UnderReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationTestUtil.createReplicas;

/**
 * Tests for {@link RatisUnderReplicationHandler}.
 */
public class TestRatisUnderReplicationHandler {
  private ContainerInfo container;
  private NodeManager nodeManager;
  private OzoneConfiguration conf;
  private static final RatisReplicationConfig RATIS_REPLICATION_CONFIG =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private PlacementPolicy policy;

  @Before
  public void setup() throws NodeNotFoundException {
    container = ReplicationTestUtil.createContainer(
        HddsProtos.LifeCycleState.CLOSED, RATIS_REPLICATION_CONFIG);

    nodeManager = Mockito.mock(NodeManager.class);
    conf = SCMTestUtils.getConf();
    policy = ReplicationTestUtil
        .getSimpleTestPlacementPolicy(nodeManager, conf);

    /*
     Return NodeStatus with NodeOperationalState as specified in
     DatanodeDetails, and NodeState as HEALTHY.
     */
    Mockito.when(nodeManager.getNodeStatus(Mockito.any(DatanodeDetails.class)))
        .thenAnswer(invocationOnMock -> {
          DatanodeDetails dn = invocationOnMock.getArgument(0);
          return new NodeStatus(dn.getPersistedOpState(),
              HddsProtos.NodeState.HEALTHY);
        });
  }

  /**
   * When the container is under replicated even though there's a pending
   * add, the handler should create replication commands.
   */
  @Test
  public void testUnderReplicatedWithMissingReplicasAndPendingAdd()
      throws IOException {
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.CLOSED, 0);
    List<ContainerReplicaOp> pendingOps = ImmutableList.of(
        ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.ADD,
            MockDatanodeDetails.randomDatanodeDetails(), 0));

    testProcessing(replicas, pendingOps, getUnderReplicatedHealthResult(), 2,
        1);
  }

  /**
   * When the container is under replicated and unrecoverable (no replicas
   * exist), the handler will not create any commands.
   */
  @Test
  public void testUnderReplicatedAndUnrecoverable() throws IOException {
    testProcessing(Collections.emptySet(), Collections.emptyList(),
        getUnderReplicatedHealthResult(), 2, 0);
  }

  /**
   * The container is currently under replicated, but there's a pending add
   * that will make it sufficiently replicated. The handler should not create
   * any commands.
   */
  @Test
  public void testUnderReplicatedFixedByPendingAdd() throws IOException {
    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.CLOSED, 0, 0);
    List<ContainerReplicaOp> pendingOps = ImmutableList.of(
        ContainerReplicaOp.create(ContainerReplicaOp.PendingOpType.ADD,
            MockDatanodeDetails.randomDatanodeDetails(), 0));

    testProcessing(replicas, pendingOps, getUnderReplicatedHealthResult(), 2,
        0);
  }

  /**
   * The container is under-replicated because a DN is decommissioning. The
   * handler should create replication command.
   */
  @Test
  public void testUnderReplicatedBecauseOfDecommissioningReplica()
      throws IOException {
    Set<ContainerReplica> replicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 0), Pair.of(IN_SERVICE, 0),
            Pair.of(IN_SERVICE, 0));

    testProcessing(replicas, Collections.emptyList(),
        getUnderReplicatedHealthResult(), 2, 1);
  }

  /**
   * The container is under-replicated because a DN is entering maintenance
   * and the remaining number of replicas (CLOSED or QUASI_CLOSED replicas on
   * HEALTHY datanodes) are less than the minimum healthy required.
   */
  @Test
  public void testUnderReplicatedBecauseOfMaintenanceReplica()
      throws IOException {
    Set<ContainerReplica> replicas = ReplicationTestUtil
        .createReplicas(Pair.of(ENTERING_MAINTENANCE, 0),
            Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0));

    testProcessing(replicas, Collections.emptyList(),
        getUnderReplicatedHealthResult(), 3, 1);
  }

  /**
   * The container is sufficiently replicated because we have the minimum
   * healthy replicas required for a DN to enter maintenance.
   */
  @Test
  public void testSufficientlyReplicatedDespiteMaintenanceReplica()
      throws IOException {
    Set<ContainerReplica> replicas = ReplicationTestUtil
        .createReplicas(Pair.of(ENTERING_MAINTENANCE, 0),
            Pair.of(IN_SERVICE, 0), Pair.of(IN_SERVICE, 0));

    testProcessing(replicas, Collections.emptyList(),
        getUnderReplicatedHealthResult(), 2, 0);
  }

  /**
   * The handler should throw an exception when the placement policy is unable
   * to choose new targets for replication.
   */
  @Test
  public void testNoTargetsFoundBecauseOfPlacementPolicy() {
    policy = ReplicationTestUtil.getNoNodesTestPlacementPolicy(nodeManager,
        conf);
    RatisUnderReplicationHandler handler =
        new RatisUnderReplicationHandler(policy, conf, nodeManager);

    Set<ContainerReplica> replicas
        = createReplicas(container.containerID(), State.CLOSED, 0, 0);

    Assert.assertThrows(IOException.class,
        () -> handler.processAndCreateCommands(replicas,
            Collections.emptyList(), getUnderReplicatedHealthResult(), 2));
  }

  /**
   * Tests whether the specified expectNumCommands number of commands are
   * created by the handler.
   * @param replicas All replicas of the container
   * @param pendingOps Collection of pending ops
   * @param healthResult ContainerHealthResult that should be passed to the
   *                     handler
   * @param minHealthyForMaintenance the minimum number of healthy replicas
   *                                 required for a datanode to enter
   *                                 maintenance
   * @param expectNumCommands number of commands expected to be created by
   *                          the handler
   */
  private void testProcessing(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult healthResult,
      int minHealthyForMaintenance, int expectNumCommands) throws IOException {
    RatisUnderReplicationHandler handler =
        new RatisUnderReplicationHandler(policy, conf, nodeManager);

    Map<DatanodeDetails, SCMCommand<?>> commands =
        handler.processAndCreateCommands(replicas, pendingOps,
            healthResult, minHealthyForMaintenance);
    Assert.assertEquals(expectNumCommands, commands.size());
  }

  private UnderReplicatedHealthResult getUnderReplicatedHealthResult() {
    UnderReplicatedHealthResult healthResult =
        Mockito.mock(UnderReplicatedHealthResult.class);
    Mockito.when(healthResult.getContainerInfo()).thenReturn(container);
    return healthResult;
  }
}
