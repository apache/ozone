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
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.junit.Assert.assertThrows;

/**
 * Tests the ECUnderReplicationHandling functionality.
 */
public class TestECUnderReplicationHandler {
  private ECReplicationConfig repConfig;
  private ContainerInfo container;
  private NodeManager nodeManager;
  private OzoneConfiguration conf;
  private PlacementPolicy policy;
  private static final int DATA = 3;
  private static final int PARITY = 2;

  @BeforeEach
  public void setup() {
    nodeManager = new MockNodeManager(true, 10) {
      @Override
      public NodeStatus getNodeStatus(DatanodeDetails dd) {
        return new NodeStatus(
            dd.getPersistedOpState(), HddsProtos.NodeState.HEALTHY, 0);
      }
    };
    conf = SCMTestUtils.getConf();
    repConfig = new ECReplicationConfig(DATA, PARITY);
    container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    policy = ReplicationTestUtil
        .getSimpleTestPlacementPolicy(nodeManager, conf);
    NodeSchema[] schemas =
        new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
  }

  @Test
  public void testUnderReplicationWithMissingParityIndex5() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 0, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMissingIndex34() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 5));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(3, 4),
        availableReplicas, 0, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMissingIndex2345() throws IOException {
    Set<ContainerReplica> availableReplicas =
        ReplicationTestUtil.createReplicas(Pair.of(IN_SERVICE, 1));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(2, 3, 4, 5),
        availableReplicas, 0, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMissingIndex12345() throws IOException {
    Set<ContainerReplica> availableReplicas = new HashSet<>();
    testUnderReplicationWithMissingIndexes(ImmutableList.of(1, 2, 3, 4, 5),
        availableReplicas, 0, 0, policy);
  }

  @Test
  public void testUnderReplicationWithDecomIndex1() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    Map<DatanodeDetails, SCMCommand<?>> cmds =
        testUnderReplicationWithMissingIndexes(
            Lists.emptyList(), availableReplicas, 1, 0, policy);
    Assertions.assertEquals(1, cmds.size());
    // Check the replicate command has index 1 set
    ReplicateContainerCommand cmd = (ReplicateContainerCommand) cmds.values()
        .iterator().next();
    Assertions.assertEquals(1, cmd.getReplicaIndex());

  }

  @Test
  public void testUnderReplicationWithDecomIndex12() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(DECOMMISSIONING, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    testUnderReplicationWithMissingIndexes(Lists.emptyList(), availableReplicas,
        2, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMixedDecomAndMissingIndexes()
      throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(DECOMMISSIONING, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 2, 0, policy);
  }

  @Test
  public void testUnderReplicationWithMaintenanceIndex12() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_MAINTENANCE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    testUnderReplicationWithMissingIndexes(Lists.emptyList(), availableReplicas,
        0, 2, policy);
  }

  @Test
  public void testUnderReplicationWithMaintenanceAndMissingIndexes()
      throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_MAINTENANCE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 0, 2, policy);
  }

  @Test
  public void testUnderReplicationWithMissingDecomAndMaintenanceIndexes()
      throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_MAINTENANCE, 2), Pair.of(DECOMMISSIONING, 3),
            Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 1, 2, policy);
  }

  @Test
  public void testExceptionIfNoNodesFound() {
    PlacementPolicy noNodesPolicy = ReplicationTestUtil
        .getNoNodesTestPlacementPolicy(nodeManager, conf);
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(DECOMMISSIONING, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    assertThrows(SCMException.class, () ->
        testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
            availableReplicas, 2, 0, noNodesPolicy));
  }

  @Test
  public void testSameNodeIsNotReused() throws IOException {
    DatanodeDetails newDn = MockDatanodeDetails.randomDatanodeDetails();
    PlacementPolicy sameNodePolicy = ReplicationTestUtil
        .getSameNodeTestPlacementPolicy(nodeManager, conf, newDn);
    // Just have a missing index, this should return OK.
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    // Passing zero for decommIndexes, as we don't expect the decom command to
    // get created due to the placement policy returning an already used node
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, 0, 0, sameNodePolicy);

    // Now add a decommissioning index - we should get an exception as the
    // placement policy should throw. It will have used up the node for
    // reconstruction, and hence no nodes will be found to fix the decommission
    // index.
    Set<ContainerReplica> replicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4));
    assertThrows(SCMException.class, () ->
        testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
            replicas, 1, 0, sameNodePolicy));
  }

  @Test
  public void testUnderAndOverReplication() throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_MAINTENANCE, 1), Pair.of(IN_MAINTENANCE, 1),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    Map<DatanodeDetails, SCMCommand<?>> cmds =
        testUnderReplicationWithMissingIndexes(ImmutableList.of(2, 3),
            availableReplicas, 0, 0, policy);
    Assertions.assertEquals(1, cmds.size());
    ReconstructECContainersCommand cmd =
        (ReconstructECContainersCommand) cmds.values().iterator().next();
    // Ensure that all source nodes are IN_SERVICE, we should not have picked
    // the non in-service nodes for index 1.
    for (ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex s
        : cmd.getSources()) {
      Assertions.assertEquals(
          IN_SERVICE, s.getDnDetails().getPersistedOpState());
    }
  }

  public Map<DatanodeDetails, SCMCommand<?>>
      testUnderReplicationWithMissingIndexes(
      List<Integer> missingIndexes, Set<ContainerReplica> availableReplicas,
      int decomIndexes, int maintenanceIndexes,
      PlacementPolicy placementPolicy) throws IOException {
    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(placementPolicy, conf, nodeManager);
    ContainerHealthResult.UnderReplicatedHealthResult result =
        Mockito.mock(ContainerHealthResult.UnderReplicatedHealthResult.class);
    Mockito.when(result.isUnrecoverable()).thenReturn(false);
    Mockito.when(result.getContainerInfo()).thenReturn(container);

    int remainingMaintenanceRedundancy = 1;
    Map<DatanodeDetails, SCMCommand<?>> datanodeDetailsSCMCommandMap = ecURH
        .processAndCreateCommands(availableReplicas, ImmutableList.of(),
            result, remainingMaintenanceRedundancy);
    Set<Map.Entry<DatanodeDetails, SCMCommand<?>>> entries =
        datanodeDetailsSCMCommandMap.entrySet();
    int replicateCommand = 0;
    int reconstructCommand = 0;
    byte[] missingIndexesByteArr = new byte[missingIndexes.size()];
    for (int i = 0; i < missingIndexes.size(); i++) {
      missingIndexesByteArr[i] = missingIndexes.get(i).byteValue();
    }
    boolean shouldReconstructCommandExist =
        missingIndexes.size() > 0 && missingIndexes.size() <= repConfig
            .getParity();
    for (Map.Entry<DatanodeDetails, SCMCommand<?>> dnCommand : entries) {
      if (dnCommand.getValue() instanceof ReplicateContainerCommand) {
        replicateCommand++;
      } else if (dnCommand
          .getValue() instanceof ReconstructECContainersCommand) {
        if (shouldReconstructCommandExist) {
          Assertions.assertArrayEquals(missingIndexesByteArr,
              ((ReconstructECContainersCommand) dnCommand.getValue())
              .getMissingContainerIndexes());
        }
        reconstructCommand++;
      }
    }
    int maxMaintenance = PARITY - remainingMaintenanceRedundancy;
    int expectedMaintenanceCommands = Math.max(0,
        maintenanceIndexes - maxMaintenance);
    Assertions.assertEquals(decomIndexes + expectedMaintenanceCommands,
        replicateCommand);
    Assertions.assertEquals(shouldReconstructCommandExist ? 1 : 0,
        reconstructCommand);
    return datanodeDetailsSCMCommandMap;
  }
}
