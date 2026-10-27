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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
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
  public static Set<ContainerReplica> createReplicas(ContainerID containerID, NodeOperationalStateAndIndex... nodes) {
    return createReplicas(containerID, CLOSED, nodes);
  }

  @SafeVarargs
  public static Set<ContainerReplica> createReplicas(ContainerID containerID,
      ContainerReplicaProto.State replicaState,
      NodeOperationalStateAndIndex... nodes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (NodeOperationalStateAndIndex p : nodes) {
      replicas.add(createContainerReplica(
          containerID, p.getIndex(), p.getState(), replicaState));
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
      NodeOperationalStateAndOrigin... nodes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (NodeOperationalStateAndOrigin i : nodes) {
      replicas.add(createContainerReplica(
          containerID, 0, i.getState(), replicaState, 123L, 1234L,
          MockDatanodeDetails.randomDatanodeDetails(), i.getOrigin()));
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
      NodeOperationalStateAndIndex... states) {
    return createReplicas(CLOSED,
        states);
  }

  @SafeVarargs
  public static Set<ContainerReplica> createReplicas(
      ContainerReplicaProto.State replicaState,
      NodeOperationalStateAndIndex... states) {
    Set<ContainerReplica> replica = new HashSet<>();
    for (NodeOperationalStateAndIndex s : states) {
      replica.add(createContainerReplica(ContainerID.valueOf(1), s.getIndex(),
          s.getState(), replicaState));
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
      Set<DatanodeCommand> commandsSent,
      AtomicBoolean throwOverloaded)
      throws NotLeaderException, CommandTargetOverloadedException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      if (throwOverloaded.get()) {
        throwOverloaded.set(false);
        throw new CommandTargetOverloadedException("Overloaded");
      }
      List<DatanodeDetails> sources = invocationOnMock.getArgument(1);
      ContainerInfo containerInfo = invocationOnMock.getArgument(0);
      ReplicateContainerCommand command = ContainerTestUtils
          .getReplicateContainerCommand(containerInfo.getContainerID(),
              invocationOnMock.getArgument(2));
      command.setReplicaIndex(invocationOnMock.getArgument(3));
      commandsSent.add(DatanodeCommand.of(sources.get(0), command));
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
      Set<DatanodeCommand> commandsSent,
      AtomicBoolean throwOverloaded)
      throws NotLeaderException, CommandTargetOverloadedException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      if (throwOverloaded.get()) {
        throwOverloaded.set(false);
        throw new CommandTargetOverloadedException("Overloaded");
      }
      ReconstructECContainersCommand cmd = invocationOnMock.getArgument(1);
      commandsSent.add(DatanodeCommand.of(cmd.getTargetDatanodes().get(0), cmd));
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
      Set<DatanodeCommand> commandsSent)
      throws NotLeaderException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      DatanodeDetails target = invocationOnMock.getArgument(2);
      SCMCommand<?> command = invocationOnMock.getArgument(0);
      commandsSent.add(DatanodeCommand.of(target, command));
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
      Set<DatanodeCommand> commandsSent)
      throws NotLeaderException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      ContainerInfo containerInfo = invocationOnMock.getArgument(0);
      int replicaIndex = invocationOnMock.getArgument(1);
      DatanodeDetails target = invocationOnMock.getArgument(2);
      boolean forceDelete = invocationOnMock.getArgument(3);
      DeleteContainerCommand deleteCommand = new DeleteContainerCommand(
          containerInfo.getContainerID(), forceDelete);
      deleteCommand.setReplicaIndex(replicaIndex);
      commandsSent.add(DatanodeCommand.of(target, deleteCommand));
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
                                                      Set<DatanodeCommand> commandsSent)
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
      Set<DatanodeCommand> commandsSent, AtomicBoolean throwOverloaded)
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
      commandsSent.add(DatanodeCommand.of(target, deleteCommand));
      return null;
    }).when(mock)
        .sendThrottledDeleteCommand(any(), anyInt(), any(), anyBoolean());
  }

  /**
   * Operational state of the node hosting a test replica, and the replica index.
   */
  public static final class NodeOperationalStateAndIndex {
    private final HddsProtos.NodeOperationalState state;
    private final int index;

    private NodeOperationalStateAndIndex(HddsProtos.NodeOperationalState state, int index) {
      this.state = state;
      this.index = index;
    }

    public static NodeOperationalStateAndIndex of(HddsProtos.NodeOperationalState state, int index) {
      return new NodeOperationalStateAndIndex(state, index);
    }

    public HddsProtos.NodeOperationalState getState() {
      return state;
    }

    public int getIndex() {
      return index;
    }
  }

  /**
   * Origin datanode of a test replica, and the operational state of its node.
   */
  public static final class NodeOperationalStateAndOrigin {
    private final DatanodeID origin;
    private final HddsProtos.NodeOperationalState state;

    private NodeOperationalStateAndOrigin(DatanodeID origin, HddsProtos.NodeOperationalState state) {
      this.origin = origin;
      this.state = state;
    }

    public static NodeOperationalStateAndOrigin of(DatanodeID origin, HddsProtos.NodeOperationalState state) {
      return new NodeOperationalStateAndOrigin(origin, state);
    }

    public DatanodeID getOrigin() {
      return origin;
    }

    public HddsProtos.NodeOperationalState getState() {
      return state;
    }
  }

  /**
   * A command sent to a datanode, as recorded by tests.
   */
  public static final class DatanodeCommand {
    private final DatanodeDetails datanode;
    private final SCMCommand<?> command;

    private DatanodeCommand(DatanodeDetails datanode, SCMCommand<?> command) {
      this.datanode = datanode;
      this.command = command;
    }

    public static DatanodeCommand of(DatanodeDetails datanode, SCMCommand<?> command) {
      return new DatanodeCommand(datanode, command);
    }

    public DatanodeDetails getDatanode() {
      return datanode;
    }

    public SCMCommand<?> getCommand() {
      return command;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DatanodeCommand)) {
        return false;
      }
      DatanodeCommand that = (DatanodeCommand) o;
      return Objects.equals(datanode, that.datanode) && Objects.equals(command, that.command);
    }

    @Override
    public int hashCode() {
      return Objects.hash(datanode, command);
    }

    @Override
    public String toString() {
      return "(" + datanode + ", " + command + ")";
    }
  }

  /**
   * A command sent to a datanode identified by its ID, as recorded by tests.
   */
  public static final class DatanodeIdCommand {
    private final DatanodeID datanodeId;
    private final SCMCommand<?> command;

    private DatanodeIdCommand(DatanodeID datanodeId, SCMCommand<?> command) {
      this.datanodeId = datanodeId;
      this.command = command;
    }

    public static DatanodeIdCommand of(DatanodeID datanodeId, SCMCommand<?> command) {
      return new DatanodeIdCommand(datanodeId, command);
    }

    public DatanodeID getDatanodeId() {
      return datanodeId;
    }

    public SCMCommand<?> getCommand() {
      return command;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DatanodeIdCommand)) {
        return false;
      }
      DatanodeIdCommand that = (DatanodeIdCommand) o;
      return Objects.equals(datanodeId, that.datanodeId) && Objects.equals(command, that.command);
    }

    @Override
    public int hashCode() {
      return Objects.hash(datanodeId, command);
    }

    @Override
    public String toString() {
      return "(" + datanodeId + ", " + command + ")";
    }
  }
}
