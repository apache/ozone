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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.ALLOCATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for the ECPipelineProvider.
 */
public class TestECPipelineProvider {

  private PipelineProvider provider;
  private NodeManager nodeManager = mock(NodeManager.class);
  private PipelineStateManager stateManager =
      mock(PipelineStateManager.class);
  private PlacementPolicy placementPolicy = mock(PlacementPolicy.class);
  private long containerSizeBytes;

  @BeforeEach
  public void setup() throws IOException, NodeNotFoundException {
    OzoneConfiguration conf = new OzoneConfiguration();
    provider = new ECPipelineProvider(
        nodeManager, stateManager, conf, placementPolicy);
    this.containerSizeBytes = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);
    // Placement policy will always return EC number of random nodes.
    when(placementPolicy.chooseDatanodes(anyList(),
        anyList(), anyInt(), anyLong(),
        anyLong()))
        .thenAnswer(invocation -> {
          List<DatanodeDetails> dns = new ArrayList<>();
          for (int i = 0; i < (int) invocation.getArguments()[2]; i++) {
            dns.add(MockDatanodeDetails.randomDatanodeDetails());
          }
          return dns;
        });

    when(nodeManager.getNodeStatus(any()))
        .thenReturn(NodeStatus.inServiceHealthy());
  }

  @Test
  public void testSimplePipelineCanBeCreatedWithIndexes() throws IOException {
    ECReplicationConfig ecConf = new ECReplicationConfig(3, 2);
    Pipeline pipeline = provider.create(ecConf);
    assertEquals(EC, pipeline.getType());
    assertEquals(ecConf.getData() + ecConf.getParity(), pipeline.getNodes().size());
    assertEquals(ALLOCATED, pipeline.getPipelineState());
    List<DatanodeDetails> dns = pipeline.getNodes();
    for (int i = 0; i < ecConf.getRequiredNodes(); i++) {
      // EC DN indexes are numbered starting from 1 to N.
      assertEquals(i + 1, pipeline.getReplicaIndex(dns.get(i)));
    }
  }

  @Test
  public void testPipelineForReadCanBeCreated() {
    ECReplicationConfig ecConf = new ECReplicationConfig(3, 2);

    Set<ContainerReplica> replicas = createContainerReplicas(4);
    Pipeline pipeline = provider.createForRead(ecConf, replicas);

    assertEquals(EC, pipeline.getType());
    assertEquals(4, pipeline.getNodes().size());
    assertEquals(ALLOCATED, pipeline.getPipelineState());
    for (ContainerReplica r : replicas) {
      assertEquals(r.getReplicaIndex(),
          pipeline.getReplicaIndex(r.getDatanodeDetails()));
    }
  }

  @Test
  void omitsDeadNodes() throws NodeNotFoundException {
    ECReplicationConfig ecConf = new ECReplicationConfig(3, 2);
    Set<ContainerReplica> replicas = createContainerReplicas(5);

    Iterator<ContainerReplica> iterator = replicas.iterator();
    DatanodeDetails dead = iterator.next().getDatanodeDetails();
    when(nodeManager.getNodeStatus(dead))
        .thenReturn(NodeStatus.inServiceDead());
    DatanodeDetails dead2 = iterator.next().getDatanodeDetails();
    when(nodeManager.getNodeStatus(dead2))
        .thenReturn(NodeStatus.valueOf(IN_MAINTENANCE, DEAD));
    DatanodeDetails dead3 = iterator.next().getDatanodeDetails();
    when(nodeManager.getNodeStatus(dead3))
        .thenReturn(NodeStatus.valueOf(DECOMMISSIONED, DEAD));
    Set<DatanodeDetails> deadNodes = ImmutableSet.of(dead, dead2, dead3);

    Pipeline pipeline = provider.createForRead(ecConf, replicas);

    List<DatanodeDetails> nodes = pipeline.getNodes();
    assertEquals(replicas.size() - deadNodes.size(), nodes.size());
    for (DatanodeDetails d : deadNodes) {
      assertThat(nodes).doesNotContain(d);
    }
  }

  @Test
  void sortsHealthyNodesFirst() throws NodeNotFoundException {
    ECReplicationConfig ecConf = new ECReplicationConfig(3, 2);
    Set<ContainerReplica> replicas = new HashSet<>();
    Set<DatanodeDetails> healthyNodes = new HashSet<>();
    Set<DatanodeDetails> staleNodes = new HashSet<>();
    Set<DatanodeDetails> decomNodes = new HashSet<>();
    for (ContainerReplica replica : createContainerReplicas(5)) {
      replicas.add(replica);
      healthyNodes.add(replica.getDatanodeDetails());

      DatanodeDetails decomNode = MockDatanodeDetails.randomDatanodeDetails();
      replicas.add(replica.toBuilder().setDatanodeDetails(decomNode).build());
      when(nodeManager.getNodeStatus(decomNode))
          .thenReturn(NodeStatus.valueOf(DECOMMISSIONING, HEALTHY));
      decomNodes.add(decomNode);

      DatanodeDetails staleNode = MockDatanodeDetails.randomDatanodeDetails();
      replicas.add(replica.toBuilder().setDatanodeDetails(staleNode).build());
      when(nodeManager.getNodeStatus(staleNode))
          .thenReturn(NodeStatus.inServiceStale());
      staleNodes.add(staleNode);
    }

    Pipeline pipeline = provider.createForRead(ecConf, replicas);

    List<DatanodeDetails> nodes = pipeline.getNodes();
    assertEquals(replicas.size(), nodes.size());
    assertEquals(healthyNodes, new HashSet<>(nodes.subList(0, 5)));
    assertEquals(decomNodes, new HashSet<>(nodes.subList(5, 10)));
    assertEquals(staleNodes, new HashSet<>(nodes.subList(10, 15)));
  }

  @Test
  public void testExcludedAndFavoredNodesPassedToPlacementPolicy()
      throws IOException {
    ECReplicationConfig ecConf = new ECReplicationConfig(3, 2);

    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    excludedNodes.add(MockDatanodeDetails.randomDatanodeDetails());

    List<DatanodeDetails> favoredNodes = new ArrayList<>();
    favoredNodes.add(MockDatanodeDetails.randomDatanodeDetails());

    Pipeline pipeline = provider.create(ecConf, excludedNodes, favoredNodes);
    assertEquals(EC, pipeline.getType());
    assertEquals(ecConf.getData() + ecConf.getParity(), pipeline.getNodes().size());

    verify(placementPolicy).chooseDatanodes(excludedNodes, favoredNodes,
        ecConf.getRequiredNodes(), 0, containerSizeBytes);
  }

  private Set<ContainerReplica> createContainerReplicas(int number) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (int i = 0; i < number; i++) {
      ContainerReplica r = ContainerReplica.newBuilder()
          .setBytesUsed(1)
          .setContainerID(ContainerID.valueOf(1))
          .setContainerState(StorageContainerDatanodeProtocolProtos
              .ContainerReplicaProto.State.CLOSED)
          .setKeyCount(1)
          .setOriginNodeId(DatanodeID.randomID())
          .setSequenceId(1)
          .setReplicaIndex(i + 1)
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
          .build();
      replicas.add(r);
    }
    return replicas;
  }

}
