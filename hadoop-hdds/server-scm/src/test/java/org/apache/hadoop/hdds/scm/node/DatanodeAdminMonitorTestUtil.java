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
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.RatisContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.mockito.Mockito.reset;

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
   * @return A containerReplica with the given ID and state
   */
  public static ContainerReplica generateReplica(
      ContainerID containerID,
      HddsProtos.NodeOperationalState nodeState,
      StorageContainerDatanodeProtocolProtos.ContainerReplicaProto
          .State replicaState) {
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    dn.setPersistedOpState(nodeState);
    return ContainerReplica.newBuilder()
        .setContainerState(replicaState)
        .setContainerID(containerID)
        .setSequenceId(1)
        .setDatanodeDetails(dn)
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
      replicas.add(generateReplica(containerID, s, CLOSED));
    }
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(containerID.getId())
        .setState(containerState)
        .build();

    return new RatisContainerReplicaCount(container, replicas, 0, 0, 3, 2);
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
      HddsProtos.LifeCycleState containerState,
      HddsProtos.NodeOperationalState...replicaStates)
      throws ContainerNotFoundException {
    reset(repManager);
    Mockito.when(repManager.getContainerReplicaCount(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation ->
            generateReplicaCount((ContainerID)invocation.getArguments()[0],
                containerState, replicaStates));
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
