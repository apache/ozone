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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;

/**
 * Tests the ECUnderReplicationHandling functionality.
 */
public class TestECUnderReplicationHandler {
  private ECReplicationConfig repConfig;
  private ContainerInfo container;
  private NodeManager nodeManager;
  private ReplicationManager replicationManager;
  private OzoneConfiguration conf;
  private PlacementPolicy policy;
  private static final int DATA = 3;
  private static final int PARITY = 2;
  private PlacementPolicy ecPlacementPolicy;

  @BeforeEach
  public void setup() {
    nodeManager = new MockNodeManager(true, 10) {
      @Override
      public NodeStatus getNodeStatus(DatanodeDetails dd) {
        return new NodeStatus(
            dd.getPersistedOpState(), HddsProtos.NodeState.HEALTHY, 0);
      }
    };
    replicationManager = Mockito.mock(ReplicationManager.class);
    conf = SCMTestUtils.getConf();
    repConfig = new ECReplicationConfig(DATA, PARITY);
    container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    policy = ReplicationTestUtil
            .getSimpleTestPlacementPolicy(nodeManager, conf);
    NodeSchema[] schemas =
        new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    ecPlacementPolicy = Mockito.mock(PlacementPolicy.class);
    Mockito.when(ecPlacementPolicy.validateContainerPlacement(
        anyList(), anyInt()))
        .thenReturn(new ContainerPlacementStatusDefault(2, 2, 3));
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
  public void testUnderReplicationWithInvalidPlacement()
          throws IOException {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
            .createReplicas(Pair.of(DECOMMISSIONING, 1),
                    Pair.of(DECOMMISSIONING, 2), Pair.of(IN_SERVICE, 3),
                    Pair.of(IN_SERVICE, 4));
    PlacementPolicy mockedPolicy = Mockito.spy(policy);
    ContainerPlacementStatus mockedContainerPlacementStatus =
            Mockito.mock(ContainerPlacementStatus.class);
    Mockito.when(mockedContainerPlacementStatus.isPolicySatisfied())
            .thenReturn(false);
    Mockito.when(mockedPolicy.validateContainerPlacement(anyList(),
            anyInt())).thenReturn(mockedContainerPlacementStatus);
    Mockito.when(mockedPolicy.validateContainerPlacement(anyList(),
            anyInt())).thenAnswer(invocationOnMock -> {
              Set<DatanodeDetails> dns =
                      new HashSet<>(invocationOnMock.getArgument(0));
              Assert.assertTrue(
                      availableReplicas.stream()
                      .map(ContainerReplica::getDatanodeDetails)
                      .filter(dn -> dn.getPersistedOpState() == IN_SERVICE)
                      .allMatch(dns::contains));
              return mockedContainerPlacementStatus;
            });
    testUnderReplicationWithMissingIndexes(Collections.emptyList(),
            availableReplicas, 0, 0, mockedPolicy);
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
  public void testUnderRepSentToOverRepHandlerIfNoNewNodes()
      throws IOException {
    PlacementPolicy noNodesPolicy = ReplicationTestUtil
        .getNoNodesTestPlacementPolicy(nodeManager, conf);

    ContainerReplica overRepReplica =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
            4, IN_SERVICE, CLOSED);
    ContainerReplica decomReplica =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
        5, DECOMMISSIONING, CLOSED);
    ContainerReplica maintReplica =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
            5, ENTERING_MAINTENANCE, CLOSED);

    List<ContainerReplica> replicasToAdd = new ArrayList<>();
    replicasToAdd.add(null);
    replicasToAdd.add(decomReplica);
    replicasToAdd.add(maintReplica);

    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(
            noNodesPolicy, conf, nodeManager, replicationManager);
    ContainerHealthResult.UnderReplicatedHealthResult underRep =
        new ContainerHealthResult.UnderReplicatedHealthResult(container,
            1, false, false, false);

    // The underRepHandler processes in stages. First missing indexes, then
    // decommission and then maintenance. If a stage cannot find new nodes and
    // there are no commands created yet, then we should either throw, or pass
    // control to the over rep handler if the container is also over replicated.
    // In this loop we first have the container under replicated with a missing
    // index, then with an under-rep index, and finally with a maintenance
    // index. In all cases, initially have it not over replicated, so it should
    // throw an exception, and then make it also over replicated, returning the
    // commands to fix the over replication instead.
    for (ContainerReplica toAdd : replicasToAdd) {
      Mockito.clearInvocations(replicationManager);
      Set<ContainerReplica> availableReplicas = ReplicationTestUtil
          .createReplicas(Pair.of(IN_SERVICE, 1),
              Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
              Pair.of(IN_SERVICE, 4));
      if (toAdd != null) {
        availableReplicas.add(toAdd);
      }

      Assert.assertThrows(SCMException.class,
          () -> ecURH.processAndCreateCommands(availableReplicas,
              Collections.emptyList(), underRep, 2));

      // Now adjust replicas so it is also over replicated. This time rather
      // than throwing it should call the OverRepHandler and return whatever it
      // returns, which in this case is a delete command for replica index 4.
      availableReplicas.add(overRepReplica);

      Map<DatanodeDetails, SCMCommand<?>> expectedDelete = new HashMap<>();
      expectedDelete.put(overRepReplica.getDatanodeDetails(),
          createDeleteContainerCommand(container, overRepReplica));

      Mockito.when(replicationManager.processOverReplicatedContainer(
          underRep)).thenReturn(expectedDelete);
      Map<DatanodeDetails, SCMCommand<?>> commands =
          ecURH.processAndCreateCommands(availableReplicas,
              Collections.emptyList(), underRep, 2);
      Mockito.verify(replicationManager, times(1))
          .processOverReplicatedContainer(underRep);
      Assertions.assertEquals(true, expectedDelete.equals(commands));
    }
  }

  @Test
  public void testUnderRepWithDecommissionAndNotEnoughNodes()
      throws IOException {
    DatanodeDetails newNode = MockDatanodeDetails.randomDatanodeDetails();
    PlacementPolicy sameNodePolicy = ReplicationTestUtil
        .getSameNodeTestPlacementPolicy(nodeManager, conf, newNode);
    
    ContainerReplica decomReplica =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
            5, DECOMMISSIONING, CLOSED);
    ContainerReplica maintReplica =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
            5, ENTERING_MAINTENANCE, CLOSED);

    List<ContainerReplica> replicasToAdd = new ArrayList<>();
    replicasToAdd.add(decomReplica);
    replicasToAdd.add(maintReplica);

    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(
            sameNodePolicy, conf, nodeManager, replicationManager);
    ContainerHealthResult.UnderReplicatedHealthResult underRep =
        new ContainerHealthResult.UnderReplicatedHealthResult(container,
            1, false, false, false);

    // The underRepHandler processes in stages. First missing indexes, then
    // decommission and then maintenance. If a stage fails due to no new nodes
    // available and there are some commands created by an earlier stage, we
    // should just return those created commands even if the container is over
    // replicated.
    for (ContainerReplica toAdd : replicasToAdd) {
      // The replicas are always over replicated with 2 index 4's.
      // Index 1 is missing, then we add in either a decommissioning or
      // entering_maintenance replica 5.
      Set<ContainerReplica> availableReplicas = ReplicationTestUtil
          .createReplicas(
              Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
              Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 4));
      if (toAdd != null) {
        availableReplicas.add(toAdd);
      }

      Map<DatanodeDetails, SCMCommand<?>> commands =
          ecURH.processAndCreateCommands(availableReplicas,
              Collections.emptyList(), underRep, 2);

      Mockito.verify(replicationManager, times(0))
          .processOverReplicatedContainer(underRep);
      Assertions.assertEquals(1, commands.size());
      Assertions.assertEquals(StorageContainerDatanodeProtocolProtos
              .SCMCommandProto.Type.reconstructECContainersCommand,
          commands.get(newNode).getType());
    }
  }

  @Test
  public void testUnderRepDueToDecomAndOverRep()
      throws IOException {
    PlacementPolicy sameNodePolicy = ReplicationTestUtil
        .getNoNodesTestPlacementPolicy(nodeManager, conf);
    // First it is under-replicated, due to decom index 5, but no mode will be
    // found. This will cause an exception to be thrown out, as the container is
    // not also over replicated.
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(DECOMMISSIONING, 5));

    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(
            sameNodePolicy, conf, nodeManager, replicationManager);

    ContainerHealthResult.UnderReplicatedHealthResult underRep =
        new ContainerHealthResult.UnderReplicatedHealthResult(container,
            1, true, false, false);

    Assert.assertThrows(SCMException.class,
        () -> ecURH.processAndCreateCommands(availableReplicas,
            Collections.emptyList(), underRep, 1));

    // Now adjust replicas so it is also over replicated. This time rather than
    // throwing it should call the OverRepHandler and return whatever it
    // returns, which in this case is a delete command for replica index 4.
    ContainerReplica overRepReplica =
        ReplicationTestUtil.createContainerReplica(container.containerID(),
            4, IN_SERVICE, CLOSED);
    availableReplicas.add(overRepReplica);

    Map<DatanodeDetails, SCMCommand<?>> expectedDelete = new HashMap<>();
    expectedDelete.put(overRepReplica.getDatanodeDetails(),
        createDeleteContainerCommand(container, overRepReplica));

    Mockito.when(replicationManager.processOverReplicatedContainer(
        underRep)).thenReturn(expectedDelete);
    Map<DatanodeDetails, SCMCommand<?>> commands =
        ecURH.processAndCreateCommands(availableReplicas,
            Collections.emptyList(), underRep, 1);
    Mockito.verify(replicationManager, times(1))
        .processOverReplicatedContainer(underRep);
    Assertions.assertEquals(true, expectedDelete.equals(commands));
  }

  @Test
  public void testMissingAndDecomIndexWithOnlyOneNewNodeAvailable()
      throws IOException {
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

    // Now add a decommissioning index - we will not get a replicate command
    // for it, as the placement policy will throw an exception as we catch
    // and just return the first reconstruction command. This will not goto
    // the over-rep handler as we have a command already created for the under
    // replication, even if the container is over replicated too.
    Set<ContainerReplica> replicas = ReplicationTestUtil
        .createReplicas(Pair.of(DECOMMISSIONING, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        replicas, 0, 0, sameNodePolicy);
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
        new ECUnderReplicationHandler(
            placementPolicy, conf, nodeManager, replicationManager);
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

  private DeleteContainerCommand createDeleteContainerCommand(
      ContainerInfo containerInfo, ContainerReplica replica) {
    DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(containerInfo.getContainerID(), true);
    deleteCommand.setReplicaIndex(replica.getReplicaIndex());
    return deleteCommand;
  }
}
