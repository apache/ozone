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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.TestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.TestUtils.getReplicas;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.container.TestReplicationManager.DatanodeCommandHandler;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.lock.LockManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test container replication behaviors under different container
 * placement policy.
 */
public class TestContainerReplicationUnderPlacementPolicy {

  private ReplicationManager replicationManager;
  private ContainerStateManager containerStateManager;
  private NodeManager nodeManager;
  private PlacementPolicy containerPlacementPolicy;
  private EventQueue eventQueue;
  private DatanodeCommandHandler datanodeCommandHandler;
  private Configuration conf;
  private ContainerManager containerManager;

  @Test
  public void testReplicationWithPlacementRandomPolicy()
      throws Exception {
    // In the test, we create 1 replica container and there still
    // needs at least two nodes to do remaining replica replication.

    // case1: provide only 1 healthy candidate node, it should
    // be failed to do replication.
    verifyReplicateCommandCount(1, 0, null);

    // case2: provide 2 healthy candidate node but one is chosen node
    // that already be used, it should be also failed since chosen node
    // will be excluded.
    List<DatanodeDetails> chosenNodes = new ArrayList<>();
    chosenNodes.add(randomDatanodeDetails());
    verifyReplicateCommandCount(2, 0, chosenNodes);

    // case3: provide other 2 healthy candidate node, it should
    // be succeed to do replication.
    verifyReplicateCommandCount(2, 2, null);
  }

  /**
   * Check if replication behavior succeed by counting
   * container replication command.
   * @param numProvidedNode Number of provided datanode.
   * @param expectedCount Expected replication command count.
   * @param chosenNodes Provided node.
   * @throws Exception
   */
  private void verifyReplicateCommandCount(int numProvidedNode,
      int expectedCount, List<DatanodeDetails> chosenNodes) throws Exception {
    List<DatanodeDetails> candidateNodes = new ArrayList<>();
    if (chosenNodes != null) {
      for (DatanodeDetails node : chosenNodes) {
        candidateNodes.add(node);
        numProvidedNode--;
      }
    }

    for (int i = 0; i < numProvidedNode; i++) {
      candidateNodes.add(randomDatanodeDetails());
    }

    conf = new OzoneConfiguration();
    containerManager =
        Mockito.mock(ContainerManager.class);
    eventQueue = new EventQueue();
    containerStateManager = new ContainerStateManager(conf);

    Mockito.when(containerManager.getContainerIDs())
        .thenAnswer(invocation -> containerStateManager.getAllContainerIDs());

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer((ContainerID)invocation.getArguments()[0]));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas((ContainerID)invocation.getArguments()[0]));

    datanodeCommandHandler = new DatanodeCommandHandler();
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, datanodeCommandHandler);

    SCMNodeMetric scmNodeMetric = Mockito.mock(SCMNodeMetric.class);
    Mockito.when(scmNodeMetric.get()).thenReturn(new SCMNodeStat(1, 0, 1));

    nodeManager =  Mockito.mock(NodeManager.class);
    Mockito.when(nodeManager.getNodes(HddsProtos.NodeState.HEALTHY))
        .thenReturn(candidateNodes);

    Mockito.when(nodeManager.getNodeStat(Mockito.any(DatanodeDetails.class)))
        .thenReturn(scmNodeMetric);

    containerPlacementPolicy = new SCMContainerPlacementRandom(nodeManager,
        conf, null, true, null);

    replicationManager = new ReplicationManager(
        new ReplicationManagerConfiguration(),
        containerManager,
        containerPlacementPolicy,
        eventQueue,
        new LockManager<>(conf));
    replicationManager.start();
    Thread.sleep(100L);

    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    containerStateManager.loadContainer(container);

    // One replica in CLOSED state
    DatanodeDetails node = (chosenNodes != null && !chosenNodes.isEmpty())
        ? chosenNodes.get(0): randomDatanodeDetails();
    final Set<ContainerReplica> replicas = getReplicas(id, State.CLOSED, node);

    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);

    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);
    Assert.assertEquals(expectedCount, currentReplicateCommandCount);

    containerStateManager.close();
    replicationManager.stop();
  }
}