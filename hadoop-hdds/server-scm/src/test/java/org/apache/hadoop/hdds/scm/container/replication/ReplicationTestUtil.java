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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;

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
          dn, dn.getUuid()));
    }
    return replicas;
  }

  public static Set<ContainerReplica> createReplicasWithSameOrigin(
      ContainerID containerID, ContainerReplicaProto.State replicaState,
      int... indexes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    UUID originNodeId = MockDatanodeDetails.randomDatanodeDetails().getUuid();
    for (int i : indexes) {
      replicas.add(createContainerReplica(
          containerID, i, IN_SERVICE, replicaState, 123L, 1234L,
          MockDatanodeDetails.randomDatanodeDetails(), originNodeId));
    }
    return replicas;
  }

  public static ContainerReplica createContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState) {
    DatanodeDetails datanodeDetails
        = MockDatanodeDetails.randomDatanodeDetails();
    return createContainerReplica(containerID, replicaIndex, opState,
        replicaState, 123L, 1234L,
        datanodeDetails, datanodeDetails.getUuid());
  }

  public static ContainerReplica createContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState, long seqId) {
    DatanodeDetails datanodeDetails
        = MockDatanodeDetails.randomDatanodeDetails();
    return createContainerReplica(containerID, replicaIndex, opState,
        replicaState, 123L, 1234L,
        datanodeDetails, datanodeDetails.getUuid(), seqId);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static ContainerReplica createContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState, long keyCount, long bytesUsed,
      DatanodeDetails datanodeDetails, UUID originNodeId) {
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
    return builder.build();
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static ContainerReplica createContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState,
      ContainerReplicaProto.State replicaState, long keyCount, long bytesUsed,
      DatanodeDetails datanodeDetails, UUID originNodeId, long seqId) {
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
    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setContainerID(containerID);
    builder.setOwner("Ozone");
    builder.setPipelineID(PipelineID.randomId());
    builder.setReplicationConfig(replicationConfig);
    builder.setState(containerState);
    return builder.build();
  }

  public static ContainerInfo createContainerInfo(
      ReplicationConfig replicationConfig, long containerID,
      HddsProtos.LifeCycleState containerState, long keyCount, long bytesUsed) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setContainerID(containerID);
    builder.setOwner("Ozone");
    builder.setPipelineID(PipelineID.randomId());
    builder.setReplicationConfig(replicationConfig);
    builder.setState(containerState);
    builder.setNumberOfKeys(keyCount);
    builder.setUsedBytes(bytesUsed);
    return builder.build();
  }

  public static ContainerInfo createContainer(HddsProtos.LifeCycleState state,
      ReplicationConfig replicationConfig) {
    return new ContainerInfo.Builder()
        .setContainerID(1).setState(state)
        .setReplicationConfig(replicationConfig).build();
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
        if (nodesRequiredToChoose > 1) {
          throw new IllegalArgumentException("Only one node is allowed");
        }
        if (excludedNodes.contains(nodeToReturn)) {
          throw new SCMException("Insufficient Nodes available to choose",
              SCMException.ResultCodes.FAILED_TO_FIND_HEALTHY_NODES);
        }
        List<DatanodeDetails> dns = new ArrayList<>();
        dns.add(nodeToReturn);
        return dns;
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
   * Given a Mockito mock of ReplicationManager, this method will mock the
   * SendThrottledReplicationCommand method so that it adds the command created
   * to the commandsSent set.
   * @param mock Mock of ReplicationManager
   * @param commandsSent Set to add the command to rather than sending it.
   * @throws NotLeaderException
   * @throws CommandTargetOverloadedException
   */
  public static void mockRMSendThrottleReplicateCommand(ReplicationManager mock,
      Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent)
      throws NotLeaderException, CommandTargetOverloadedException {
    doAnswer((Answer<Void>) invocationOnMock -> {
      List<DatanodeDetails> sources = invocationOnMock.getArgument(1);
      ContainerInfo containerInfo = invocationOnMock.getArgument(0);
      ReplicateContainerCommand command = ReplicateContainerCommand
          .toTarget(containerInfo.getContainerID(),
              invocationOnMock.getArgument(2));
      command.setReplicaIndex(invocationOnMock.getArgument(3));
      commandsSent.add(Pair.of(sources.get(0), command));
      return null;
    }).when(mock).sendThrottledReplicationCommand(
        Mockito.any(ContainerInfo.class), Mockito.anyList(),
        Mockito.any(DatanodeDetails.class), anyInt());
  }

  /**
   * Given a Mockito mock of ReplicationManager, this method will mock the
   * sendDatanodeCommand method so that it adds the command created to the
   * commandsSent set.
   * @param mock Mock of ReplicationManager
   * @param commandsSent Set to add the command to rather than sending it.
   * @throws NotLeaderException
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
   * @throws NotLeaderException
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
   * @throws NotLeaderException
   */
  public static void mockRMSendThrottledDeleteCommand(ReplicationManager mock,
      Set<Pair<DatanodeDetails, SCMCommand<?>>> commandsSent)
      throws NotLeaderException, CommandTargetOverloadedException {
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
    }).when(mock)
        .sendThrottledDeleteCommand(any(), anyInt(), any(), anyBoolean());
  }
}
