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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.TestContainerInfo;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.mockito.stubbing.Answer;

/**
 * Helper class to provide common methods used to test ReplicationManager.
 */
public final class ReplicationTestUtil {

  private ReplicationTestUtil() {
  }

  @SafeVarargs
  public static Set<ContainerReplica> createReplicas(ContainerID containerID,
      Pair<HddsProtos.NodeOperationalState, Integer>... nodes) {
    return createReplicas(containerID, CLOSED, nodes);
  }

  @SafeVarargs
  public static Set<ContainerReplica> createReplicas(ContainerID containerID,
      ContainerReplicaProto.State replicaState,
      Pair<HddsProtos.NodeOperationalState, Integer>... nodes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (Pair<HddsProtos.NodeOperationalState, Integer> p : nodes) {
      replicas.add(createContainerReplica(
          containerID, p.getRight(), p.getLeft(), replicaState));
    }
    return replicas;
  }

  public static Set<ContainerReplica> createReplicas(ContainerID containerID,
      int... indexes) {
    return createReplicas(containerID, CLOSED, indexes);
  }

  public static Set<ContainerReplica> createEmptyReplicas(ContainerID containerID,
      ContainerReplicaProto.State replicaState, int... indexes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (int i : indexes) {
      replicas.add(createEmptyContainerReplica(
          containerID, i, IN_SERVICE, replicaState));
    }
    return replicas;
  }

  public static Set<ContainerReplica> createReplicas(ContainerID containerID,
      ContainerReplicaProto.State replicaState, int... indexes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (int i : indexes) {
      replicas.add(createContainerReplica(
          containerID, i, IN_SERVICE, replicaState));
    }
    return replicas;
  }

  public static Set<ContainerReplica> createReplicas(ContainerID containerID,
      ContainerReplicaProto.State replicaState, long keyCount, long bytesUsed,
      int... indexes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (int i : indexes) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      replicas.add(createContainerReplica(containerID, i, IN_SERVICE,
          replicaState, keyCount, bytesUsed,
          dn, dn.getID()));
    }
    return replicas;
  }

  public static Set<ContainerReplica> createReplicasWithSameOrigin(
      ContainerID containerID, ContainerReplicaProto.State replicaState,
      int... indexes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    final DatanodeID originNodeId = DatanodeID.randomID();
    for (int i : indexes) {
      replicas.add(createContainerReplica(
          containerID, i, IN_SERVICE, replicaState, 123L, 1234L,
          MockDatanodeDetails.randomDatanodeDetails(), originNodeId));
    }
    return replicas;
  }

  public static ContainerReplica createEmptyContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState) {
    DatanodeDetails datanodeDetails
        = MockDatanodeDetails.randomDatanodeDetails();
    return createContainerReplica(containerID, replicaIndex, opState,
        replicaState, 0L, 0L,
        datanodeDetails, datanodeDetails.getID());
  }

  public static Set<ContainerReplica> createReplicasWithOriginAndOpState(
      ContainerID containerID, ContainerReplicaProto.State replicaState,
      Pair<DatanodeID, HddsProtos.NodeOperationalState>... nodes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (Pair<DatanodeID, HddsProtos.NodeOperationalState> i : nodes) {
      replicas.add(createContainerReplica(
          containerID, 0, i.getRight(), replicaState, 123L, 1234L,
          MockDatanodeDetails.randomDatanodeDetails(), i.getLeft()));
    }
    return replicas;
  }

  /**
   * Creates a single replica with a specific origin, operational state, replica state, and BCSID.
   */
  public static ContainerReplica createReplicaWithOriginAndSeqId(
      ContainerID containerID, DatanodeID originNodeId,
      HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState, long seqId) {
    return createContainerReplica(containerID, 0, opState, replicaState, 123L, 1234L,
        MockDatanodeDetails.randomDatanodeDetails(), originNodeId, seqId);
  }

  /**
   * Adds {@code count} replicas with the given origin, operational state, replica state, and BCSID.
   * to the provided set.
   */
  public static void addReplicasWithOriginAndSeqId(
      Set<ContainerReplica> replicas, ContainerID containerID,
      DatanodeID originNodeId, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState, long seqId, int count) {
    for (int i = 0; i < count; i++) {
      replicas.add(createReplicaWithOriginAndSeqId(containerID, originNodeId, opState, replicaState, seqId));
    }
  }

  public static ContainerReplica createContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState) {
    DatanodeDetails datanodeDetails
        = MockDatanodeDetails.randomDatanodeDetails();
    return createContainerReplica(containerID, replicaIndex, opState,
        replicaState, 123L, 1234L,
        datanodeDetails, datanodeDetails.getID());
  }

  public static ContainerReplica createContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState, long seqId) {
    DatanodeDetails datanodeDetails
        = MockDatanodeDetails.randomDatanodeDetails();
    return createContainerReplica(containerID, replicaIndex, opState,
        replicaState, 123L, 1234L,
        datanodeDetails, datanodeDetails.getID(), seqId);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static ContainerReplica createContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState, long keyCount, long bytesUsed,
      DatanodeDetails datanodeDetails, DatanodeID originNodeId) {
    ContainerReplica.ContainerReplicaBuilder builder
        = ContainerReplica.newBuilder();
    datanodeDetails.setPersistedOpState(opState);
    builder.setContainerID(containerID);
    builder.setReplicaIndex(replicaIndex);
    builder.setKeyCount(keyCount);
    builder.setBytesUsed(bytesUsed);
    builder.setContainerState(replicaState);
    builder.setDatanodeDetails(datanodeDetails);
    builder.setSequenceId(0);
    builder.setOriginNodeId(originNodeId);
    builder.setEmpty(keyCount == 0);
    return builder.build();
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static ContainerReplica createContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState, long keyCount, long bytesUsed,
      DatanodeDetails datanodeDetails, DatanodeID originNodeId, long seqId) {
    ContainerReplica.ContainerReplicaBuilder builder
        = ContainerReplica.newBuilder();
    datanodeDetails.setPersistedOpState(opState);
    builder.setContainerID(containerID);
    builder.setReplicaIndex(replicaIndex);
    builder.setKeyCount(keyCount);
    builder.setBytesUsed(bytesUsed);
    builder.setContainerState(replicaState);
    builder.setDatanodeDetails(datanodeDetails);
    builder.setSequenceId(seqId);
    builder.setOriginNodeId(originNodeId);
    return builder.build();
  }

  public static ContainerInfo createContainerInfo(ReplicationConfig repConfig) {
    return createContainerInfo(repConfig, 1, HddsProtos.LifeCycleState.CLOSED);
  }

  public static ContainerInfo createContainerInfo(
      ReplicationConfig replicationConfig, long containerID,
      HddsProtos.LifeCycleState containerState) {
    return TestContainerInfo.newBuilderForTest()
        .setContainerID(containerID)
        .setReplicationConfig(replicationConfig)
        .setState(containerState)
        .build();
  }

  public static ContainerInfo createContainerInfo(
      ReplicationConfig replicationConfig, long containerID,
      HddsProtos.LifeCycleState containerState, long sequenceID) {
    return TestContainerInfo.newBuilderForTest()
        .setContainerID(containerID)
        .setReplicationConfig(replicationConfig)
        .setState(containerState)
        .setSequenceId(sequenceID)
        .build();
  }

  public static ContainerInfo createContainerInfo(
      ReplicationConfig replicationConfig, long containerID,
      HddsProtos.LifeCycleState containerState, long keyCount, long bytesUsed) {
    return TestContainerInfo.newBuilderForTest()
        .setContainerID(containerID)
        .setReplicationConfig(replicationConfig)
        .setState(containerState)
        .setNumberOfKeys(keyCount)
        .setUsedBytes(bytesUsed)
        .build();
  }

  public static ContainerInfo createContainer(HddsProtos.LifeCycleState state,
      ReplicationConfig replicationConfig) {
    return TestContainerInfo.newBuilderForTest()
        .setState(state)
        .setReplicationConfig(replicationConfig)
        .build();
  }

  @SafeVarargs
  public static Set<ContainerReplica> createReplicas(
      Pair<HddsProtos.NodeOperationalState, Integer>... states) {
    return createReplicas(CLOSED,
        states);
  }

  @SafeVarargs
  public static Set<ContainerReplica> createReplicas(
      ContainerReplicaProto.State replicaState,
      Pair<HddsProtos.NodeOperationalState, Integer>... states) {
    Set<ContainerReplica> replica = new HashSet<>();
    for (Pair<HddsProtos.NodeOperationalState, Integer> s : states) {
      replica.add(createContainerReplica(ContainerID.valueOf(1), s.getRight(),
          s.getLeft(), replicaState));
    }
    return replica;
  }

  public static PlacementPolicy getSimpleTestPlacementPolicy(
      final NodeManager nodeManager, final OzoneConfiguration conf) {

    final Node rackNode = MockDatanodeDetails.randomDatanodeDetails();

    return new SCMCommonPlacementPolicy(nodeManager, conf) {
      @Override
      protected List<DatanodeDetails> chooseDatanodesInternal(
              List<DatanodeDetails> usedNodes,
              List<DatanodeDetails> excludedNodes,
              List<DatanodeDetails> favoredNodes, int nodesRequiredToChoose,
              long metadataSizeRequired, long dataSizeRequired) {
        List<DatanodeDetails> dns = new ArrayList<>();
        for (int i = 0; i < nodesRequiredToChoose; i++) {
          dns.add(MockDatanodeDetails.randomDatanodeDetails());
        }
        return dns;
      }

      @Override
      public DatanodeDetails chooseNode(List<DatanodeDetails> healthyNodes) {
        return null;
      }

      @Override
      protected Node getPlacementGroup(DatanodeDetails dn) {
        // Make it look like a single rack cluster
        return rackNode;
      }

      @Override
      public ContainerPlacementStatus
          validateContainerPlacement(List<DatanodeDetails> dns, int replicas) {
        return new ContainerPlacementStatusDefault(2, 2, 3);
      }
    };
  }

  public static PlacementPolicy getSameNodeTestPlacementPolicy(
      final NodeManager nodeManager, final OzoneConfiguration conf,
      DatanodeDetails nodeToReturn) {
    return new SCMCommonPlacementPolicy(nodeManager, conf) {
      @Override
      protected List<DatanodeDetails> chooseDatanodesInternal(
              List<DatanodeDetails> usedNodes,
              List<DatanodeDetails> excludedNodes,
              List<DatanodeDetails> favoredNodes, int nodesRequiredToChoose,
              long metadataSizeRequired, long dataSizeRequired)
              throws SCMException {
        long containerSize = (long) conf.getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
        assertEquals(HddsServerUtil.requiredReplicationSpace(containerSize), dataSizeRequired);
        if (nodesRequiredToChoose > 1) {
          throw new IllegalArgumentException("Only one node is allowed");
        }
        if (excludedNodes.contains(nodeToReturn)
            || usedNodes.contains(nodeToReturn)) {
          throw new SCMException("Insufficient Nodes available to choose",
              SCMException.ResultCodes.FAILED_TO_FIND_HEALTHY_NODES);
        }
        return Collections.singletonList(nodeToReturn);
      }

      @Override
      public DatanodeDetails chooseNode(List<DatanodeDetails> healthyNodes) {
        return null;
      }
    };
  }

  public static PlacementPolicy getNoNodesTestPlacementPolicy(
      final NodeManager nodeManager, final OzoneConfiguration conf) {
    return new SCMCommonPlacementPolicy(nodeManager, conf) {
      @Override
      protected List<DatanodeDetails> chooseDatanodesInternal(
              List<DatanodeDetails> usedNodes,
              List<DatanodeDetails> excludedNodes,
              List<DatanodeDetails> favoredNodes, int nodesRequiredToChoose,
              long metadataSizeRequired, long dataSizeRequired)
              throws SCMException {
        long containerSize = (long) conf.getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
        assertEquals(HddsServerUtil.requiredReplicationSpace(containerSize), dataSizeRequired);
        throw new SCMException("No nodes available",
                FAILED_TO_FIND_SUITABLE_NODE);
      }

      @Override
      public DatanodeDetails chooseNode(List<DatanodeDetails> healthyNodes) {
        return null;
      }
    };
  }

  /**
   * Placement policy that throws an exception when the number of requested
   * nodes is greater or equal to throwWhenThisOrMoreNodesRequested, otherwise
   * returns a random node.
   */
  public static PlacementPolicy getInsufficientNodesTestPlacementPolicy(
      final NodeManager nodeManager, final OzoneConfiguration conf,
      int throwWhenThisOrMoreNodesRequested) {
    return new SCMCommonPlacementPolicy(nodeManager, conf) {
      @Override
      protected List<DatanodeDetails> chooseDatanodesInternal(
          List<DatanodeDetails> usedNodes,
          List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes, int nodesRequiredToChoose,
          long metadataSizeRequired, long dataSizeRequired)
          throws SCMException {
        long containerSize = (long) conf.getStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
            ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
        assertEquals(HddsServerUtil.requiredReplicationSpace(containerSize), dataSizeRequired);
        if (nodesRequiredToChoose >= throwWhenThisOrMoreNodesRequested) {
          throw new SCMException("No nodes available",
              FAILED_TO_FIND_SUITABLE_NODE);
        }
        return Collections
            .singletonList(MockDatanodeDetails.randomDatanodeDetails());
      }

      @Override
      public DatanodeDetails chooseNode(List<DatanodeDetails> healthyNodes) {
        return null;
      }
    };
  }

  /**
   * Given a Mockito mock of ReplicationManager, this method will mock the
   * SendThrottledReplicationCommand method so that it adds the command created
   * to the commandsSent set.
   * @param mock Mock of ReplicationManager
   * @param commandsSent Set to add the command to rather than sending it.
   * @param throwOverloaded If the atomic boolean is true, throw a
   *                        CommandTargetOverloadedException and set the boolean
   *                        to false, instead of creating the replicate command.
   */
  public static void mockRMSendThrottleReplicateCommand(ReplicationManager mock,
      Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent,
      AtomicBoolean throwOverloaded)
      throws NotLeaderException, CommandTargetOverloadedException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      if (throwOverloaded.get()) {
        throwOverloaded.set(false);
        throw new CommandTargetOverloadedException("Overloaded");
      }
      List<DatanodeDetails> sources = invocationOnMock.getArgument(1);
      ContainerInfo containerInfo = invocationOnMock.getArgument(0);
      ReplicateContainerCommand command = ReplicateContainerCommand
          .toTarget(containerInfo.getContainerID(),
              invocationOnMock.getArgument(2));
      command.setReplicaIndex(invocationOnMock.getArgument(3));
      commandsSent.add(Pair.of(sources.get(0), command));
      return null;
    }).when(mock).sendThrottledReplicationCommand(
        any(ContainerInfo.class), anyList(), any(DatanodeDetails.class), anyInt());
  }

  /**
   * Given a Mockito mock of ReplicationManager, this method will mock the
   * SendThrottledReconstructionCommand method so that it adds the command
   * created to the commandsSent set.
   * @param mock Mock of ReplicationManager
   * @param commandsSent Set to add the command to rather than sending it.
   * @param throwOverloaded If the atomic boolean is true, throw a
   *                        CommandTargetOverloadedException and set the boolean
   *                        to false, instead of creating the replicate command.
   */
  public static void mockSendThrottledReconstructionCommand(
      ReplicationManager mock,
      Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent,
      AtomicBoolean throwOverloaded)
      throws NotLeaderException, CommandTargetOverloadedException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      if (throwOverloaded.get()) {
        throwOverloaded.set(false);
        throw new CommandTargetOverloadedException("Overloaded");
      }
      ReconstructECContainersCommand cmd = invocationOnMock.getArgument(1);
      commandsSent.add(Pair.of(cmd.getTargetDatanodes().get(0), cmd));
      return null;
    }).when(mock).sendThrottledReconstructionCommand(any(ContainerInfo.class), any());
  }

  /**
   * Given a Mockito mock of ReplicationManager, this method will mock the
   * sendDatanodeCommand method so that it adds the command created to the
   * commandsSent set.
   * @param mock Mock of ReplicationManager
   * @param commandsSent Set to add the command to rather than sending it.
   */
  public static void mockRMSendDatanodeCommand(ReplicationManager mock,
      Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent)
      throws NotLeaderException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      DatanodeDetails target = invocationOnMock.getArgument(2);
      SCMCommand<?> command = invocationOnMock.getArgument(0);
      commandsSent.add(Pair.of(target, command));
      return null;
    }).when(mock).sendDatanodeCommand(any(), any(), any());
  }

  /**
   * Given a Mockito mock of ReplicationManager, this method will mock the
   * sendDeleteCommand method so that it adds the command created to the
   * commandsSent set.
   * @param mock Mock of ReplicationManager
   * @param commandsSent Set to add the command to rather than sending it.
   */
  public static void mockRMSendDeleteCommand(ReplicationManager mock,
      Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent)
      throws NotLeaderException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      ContainerInfo containerInfo = invocationOnMock.getArgument(0);
      int replicaIndex = invocationOnMock.getArgument(1);
      DatanodeDetails target = invocationOnMock.getArgument(2);
      boolean forceDelete = invocationOnMock.getArgument(3);
      DeleteContainerCommand deleteCommand = new DeleteContainerCommand(
          containerInfo.getContainerID(), forceDelete);
      deleteCommand.setReplicaIndex(replicaIndex);
      commandsSent.add(Pair.of(target, deleteCommand));
      return null;
    }).when(mock).sendDeleteCommand(any(), anyInt(), any(), anyBoolean());
  }

  /**
   * Given a Mockito mock of ReplicationManager, this method will mock the
   * sendThrottledDeleteCommand method so that it adds the command created to
   * the commandsSent set.
   * @param mock Mock of ReplicationManager
   * @param commandsSent Set to add the command to rather than sending it.
   */
  public static void mockRMSendThrottledDeleteCommand(ReplicationManager mock,
                                                      Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent)
      throws NotLeaderException, CommandTargetOverloadedException {
    mockRMSendThrottledDeleteCommand(mock, commandsSent, new AtomicBoolean(false));
  }

  /**
   * Given a Mockito mock of ReplicationManager, this method will mock the
   * sendThrottledDeleteCommand method so that it adds the command created to
   * the commandsSent set.
   * @param mock Mock of ReplicationManager
   * @param commandsSent Set to add the command to rather than sending it.
   * @param throwOverloaded If the atomic boolean is true, throw a
   *                        CommandTargetOverloadedException and set the boolean
   *                        to false, instead of creating the replicate command.
   */
  public static void mockRMSendThrottledDeleteCommand(ReplicationManager mock,
      Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent, AtomicBoolean throwOverloaded)
      throws NotLeaderException, CommandTargetOverloadedException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      if (throwOverloaded.get()) {
        throwOverloaded.set(false);
        throw new CommandTargetOverloadedException("Overloaded");
      }
      ContainerInfo containerInfo = invocationOnMock.getArgument(0);
      int replicaIndex = invocationOnMock.getArgument(1);
      DatanodeDetails target = invocationOnMock.getArgument(2);
      boolean forceDelete = invocationOnMock.getArgument(3);
      DeleteContainerCommand deleteCommand = new DeleteContainerCommand(
          containerInfo.getContainerID(), forceDelete);
      deleteCommand.setReplicaIndex(replicaIndex);
      commandsSent.add(Pair.of(target, deleteCommand));
      return null;
    }).when(mock)
        .sendThrottledDeleteCommand(any(), anyInt(), any(), anyBoolean());
  }
}
