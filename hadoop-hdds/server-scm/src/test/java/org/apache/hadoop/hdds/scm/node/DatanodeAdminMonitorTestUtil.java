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

package org.apache.hadoop.hdds.scm.node;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ECContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.RatisContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

/**
 * Helper class to provide common methods used to test DatanodeAdminMonitor
 * and NodeDecommissionMetrics for tracking decommission and maintenance mode
 * workflow progress.
 */
public final class DatanodeAdminMonitorTestUtil {
  private DatanodeAdminMonitorTestUtil() {
  }

  /**
   * Generate a new ContainerReplica with the given containerID and State.
   * @param containerID The ID the replica is associated with
   * @param nodeState The persistedOpState stored in datanodeDetails.
   * @param replicaState The state of the generated replica.
   * @param replicaIndex The replica Index for the replica.
   * @param datanodeDetails The datanode the replica is hosted on.
   * @return A containerReplica with the given ID and state
   */
  public static ContainerReplica generateReplica(
      ContainerID containerID,
      HddsProtos.NodeOperationalState nodeState,
      StorageContainerDatanodeProtocolProtos.ContainerReplicaProto
          .State replicaState,
      int replicaIndex,
      DatanodeDetails datanodeDetails) {
    datanodeDetails.setPersistedOpState(nodeState);
    return ContainerReplica.newBuilder()
        .setContainerState(replicaState)
        .setContainerID(containerID)
        .setSequenceId(1)
        .setDatanodeDetails(datanodeDetails)
        .setReplicaIndex(replicaIndex)
        .build();
  }

  /**
   * Create a ContainerReplicaCount object, including a container with the
   * requested ContainerID and state, along with a set of replicas of the given
   * states.
   * @param containerID The ID of the container to create an included
   * @param containerState The state of the container
   * @param states Create a replica for each of the given states.
   * @return A ContainerReplicaCount containing the generated container and
   *         replica set
   */
  public static ContainerReplicaCount generateReplicaCount(
      ContainerID containerID,
      HddsProtos.LifeCycleState containerState,
      HddsProtos.NodeOperationalState...states) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (HddsProtos.NodeOperationalState s : states) {
      replicas.add(generateReplica(containerID, s, CLOSED, 0,
          MockDatanodeDetails.randomDatanodeDetails()));
    }
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(containerID.getId())
        .setState(containerState)
        .build();

    return new RatisContainerReplicaCount(container, replicas, 0, 0, 3, 2);
  }

  /**
   * Create a ContainerReplicaCount object for an EC container, including a
   * container with the requested ContainerID and state, along with a set of
   * replicas of the given states.
   * @param containerID The ID of the container to create an included
   * @param repConfig The Replication Config for the container
   * @param containerState The state of the container
   * @param states Create a replica for each of the given states.
   * @return A ContainerReplicaCount containing the generated container and
   *         replica set
   */
  public static ContainerReplicaCount generateECReplicaCount(
      ContainerID containerID, ECReplicationConfig repConfig,
      HddsProtos.LifeCycleState containerState,
      Triple<HddsProtos.NodeOperationalState, DatanodeDetails,
          Integer>...states) {

    Set<ContainerReplica> replicas = new HashSet<>();
    for (Triple<HddsProtos.NodeOperationalState, DatanodeDetails, Integer> t
        : states) {
      replicas.add(generateReplica(containerID, t.getLeft(), CLOSED,
          t.getRight(), t.getMiddle()));
    }
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(containerID.getId())
        .setState(containerState)
        .setReplicationConfig(repConfig)
        .build();

    return new ECContainerReplicaCount(container, replicas,
        Collections.emptyList(), 1);
  }

  /**
   * The only interaction the DatanodeAdminMonitor has with the
   * ReplicationManager, is to request a ContainerReplicaCount object for each
   * container on nodes being deocmmissioned or moved to maintenance. This
   * method mocks that interface to return a ContainerReplicaCount with a
   * container in the given containerState and a set of replias in the given
   * replicaStates.
   * @param containerState
   * @param replicaStates
   * @throws ContainerNotFoundException
   */
  public static void mockGetContainerReplicaCount(
      ReplicationManager repManager,
      boolean underReplicated,
      HddsProtos.LifeCycleState containerState,
      HddsProtos.NodeOperationalState...replicaStates)
      throws ContainerNotFoundException {
    reset(repManager);
    mockReplicationManagerConfig(repManager);
    when(repManager.getContainerReplicaCount(
        any(ContainerID.class)))
        .thenAnswer(invocation ->
            generateReplicaCount((ContainerID)invocation.getArguments()[0],
                containerState, replicaStates));
    mockCheckContainerState(repManager, underReplicated);
  }

  /**
   * The only interaction the DatanodeAdminMonitor has with the
   * ReplicationManager, is to request a ContainerReplicaCount object for each
   * container on nodes being deocmmissioned or moved to maintenance. This
   * method mocks that interface to return a ContainerReplicaCount with a
   * container in the given containerState and a set of replias in the given
   * replicaStates.
   * @param containerState
   * @param replicaStates
   * @throws ContainerNotFoundException
   */
  public static void mockGetContainerReplicaCountForEC(
      ReplicationManager repManager,
      boolean underReplicated,
      HddsProtos.LifeCycleState containerState,
      ECReplicationConfig repConfig,
      Triple<HddsProtos.NodeOperationalState, DatanodeDetails,
          Integer>...replicaStates)
      throws ContainerNotFoundException {
    reset(repManager);
    mockReplicationManagerConfig(repManager);
    when(repManager.getContainerReplicaCount(
            any(ContainerID.class)))
        .thenAnswer(invocation ->
            generateECReplicaCount((ContainerID)invocation.getArguments()[0],
                repConfig, containerState, replicaStates));
    mockCheckContainerState(repManager, underReplicated);
  }

  static void mockCheckContainerState(ReplicationManager repManager, boolean underReplicated)
      throws ContainerNotFoundException {
    mockReplicationManagerConfig(repManager);
    when(repManager.checkContainerStatus(any(ContainerInfo.class),
            any(ReplicationManagerReport.class)))
        .then(invocation -> {
          ReplicationManagerReport report = invocation.getArgument(1);
          if (underReplicated) {
            report.increment(ContainerHealthState.UNDER_REPLICATED);
            return true;
          }
          return false;
        });
  }

  /**
   * Mocks the ReplicationManagerConfiguration to return default sample limit of 100.
   */
  static void mockReplicationManagerConfig(ReplicationManager repManager) {
    ReplicationManager.ReplicationManagerConfiguration rmConf =
        mock(ReplicationManager.ReplicationManagerConfiguration.class);
    when(repManager.getConfig()).thenReturn(rmConf);
    when(rmConf.getContainerSampleLimit()).thenReturn(100);
  }

  /**
   * This simple internal class is used to track and handle any DatanodeAdmin
   * events fired by the DatanodeAdminMonitor during tests.
   */
  public static class DatanodeAdminHandler implements
          EventHandler<DatanodeDetails> {
    private AtomicInteger invocation = new AtomicInteger(0);

    @Override
    public void onMessage(final DatanodeDetails dn,
                          final EventPublisher publisher) {
      invocation.incrementAndGet();
    }

    public int getInvocation() {
      return invocation.get();
    }
  }
}
