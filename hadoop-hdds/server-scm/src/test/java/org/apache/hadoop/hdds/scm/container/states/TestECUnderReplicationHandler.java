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
package org.apache.hadoop.hdds.scm.container.states;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.container.replication.ECUnderReplicationHandler;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;

/**
 * Tests the ECUnderReplicationHandling functionality.
 */
public class TestECUnderReplicationHandler {
  private ECReplicationConfig repConfig;
  private ContainerInfo container;
  private NodeManager nodeManager;
  private OzoneConfiguration conf;
  private NetworkTopology cluster;
  private PlacementPolicy policy;
  private SCMContainerPlacementMetrics metrics;

  @BeforeEach
  public void setup() {
    nodeManager = new MockNodeManager(true, 10) {
      @Override
      public NodeStatus getNodeStatus(DatanodeDetails dd)
          throws NodeNotFoundException {
        return NodeStatus.inServiceHealthy();
      }
    };
    conf = SCMTestUtils.getConf();
    repConfig = new ECReplicationConfig(3, 2);
    container = createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    // create placement policy instances
    cluster = new NetworkTopologyImpl(NodeSchemaManager.getInstance());
    policy =
        new SCMContainerPlacementRackScatter(nodeManager, conf, cluster, true,
            metrics) {
          @Override
          public List<DatanodeDetails> chooseDatanodes(
              List<DatanodeDetails> excludedNodes,
              List<DatanodeDetails> favoredNodes, int nodesRequiredToChoose,
              long metadataSizeRequired, long dataSizeRequired)
              throws SCMException {
            List<DatanodeDetails> dns = new ArrayList<>();
            for (int i = 0; i < nodesRequiredToChoose; i++) {
              dns.add(MockDatanodeDetails.randomDatanodeDetails());
            }
            return dns;
          }
        };
    NodeSchema[] schemas =
        new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);

  }

  @BeforeEach
  public void init() {
    metrics = SCMContainerPlacementMetrics.create();
  }

  @Test
  public void testUnderReplicationWithMissingParityIndex5() {
    Set<ContainerReplica> availableReplicas =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, false);
  }

  @Test
  public void testUnderReplicationWithMissingIndex34() {
    Set<ContainerReplica> availableReplicas =
        registerNodes(Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 5));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(3, 4),
        availableReplicas, false);
  }

  @Test
  public void testUnderReplicationWithMissingIndex2345() {
    Set<ContainerReplica> availableReplicas =
        registerNodes(Pair.of(IN_SERVICE, 1));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(2, 3, 4, 5),
        availableReplicas, false);
  }

  @Test
  public void testUnderReplicationWithMissingIndex12345() {
    Set<ContainerReplica> availableReplicas = new HashSet<>();
    testUnderReplicationWithMissingIndexes(ImmutableList.of(1, 2, 3, 4, 5),
        availableReplicas, false);
  }

  @Test
  public void testUnderReplicationWithDecomIndex1() {
    Set<ContainerReplica> availableReplicas =
        registerNodes(Pair.of(DECOMMISSIONING, 1), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    testUnderReplicationWithMissingIndexes(Lists.emptyList(), availableReplicas,
        true);
  }

  @Test
  public void testUnderReplicationWithDecomIndex12() {
    Set<ContainerReplica> availableReplicas =
        registerNodes(Pair.of(DECOMMISSIONING, 1), Pair.of(DECOMMISSIONING, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5));
    testUnderReplicationWithMissingIndexes(Lists.emptyList(), availableReplicas,
        true);
  }

  @Test
  public void testUnderReplicationWithMixedDecomAndMissingIndexes() {
    Set<ContainerReplica> availableReplicas =
        registerNodes(Pair.of(DECOMMISSIONING, 1), Pair.of(DECOMMISSIONING, 2),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 4));
    testUnderReplicationWithMissingIndexes(ImmutableList.of(5),
        availableReplicas, false);
  }

  public void testUnderReplicationWithMissingIndexes(
      List<Integer> missingIndexes, Set<ContainerReplica> availableReplicas,
      boolean decom) {
    ECUnderReplicationHandler ecURH =
        new ECUnderReplicationHandler(policy, conf, nodeManager);
    ContainerReplicaPendingOps pendingOpsMock =
        Mockito.mock(ContainerReplicaPendingOps.class);
    ContainerHealthResult.UnderReplicatedHealthResult result =
        Mockito.mock(ContainerHealthResult.UnderReplicatedHealthResult.class);
    Mockito.when(result.underReplicatedDueToDecommission()).thenReturn(decom);
    Mockito.when(result.isUnrecoverable()).thenReturn(false);
    Mockito.when(result.getContainerInfo()).thenReturn(container);

    Map<DatanodeDetails, SCMCommand> datanodeDetailsSCMCommandMap = ecURH
        .processAndGetCommands(availableReplicas, pendingOpsMock,
            result, 1);
    if (!decom) {
      if (missingIndexes.size() <= repConfig.getParity()) {
        Assertions.assertEquals(1, datanodeDetailsSCMCommandMap.size());
        Map.Entry<DatanodeDetails, SCMCommand> dnVsCommand =
            datanodeDetailsSCMCommandMap.entrySet().iterator().next();
        Assert.assertTrue(
            dnVsCommand.getValue() instanceof ReconstructECContainersCommand);
        byte[] missingIndexesByteArr = new byte[missingIndexes.size()];
        for (int i = 0; i < missingIndexes.size(); i++) {
          missingIndexesByteArr[i] = (byte) (int) missingIndexes.get(i);
        }
        Assert.assertArrayEquals(missingIndexesByteArr,
            ((ReconstructECContainersCommand) dnVsCommand.getValue())
                .getMissingContainerIndexes());
      } else {
        Assertions.assertNull(datanodeDetailsSCMCommandMap);
      }
    } else {
      int numDecomIndexes = 0;
      for (ContainerReplica repl : availableReplicas) {
        if (repl.getDatanodeDetails()
            .getPersistedOpState() == DECOMMISSIONING) {
          numDecomIndexes++;
        }
      }
      Assertions
          .assertEquals(numDecomIndexes, datanodeDetailsSCMCommandMap.size());
      Set<Map.Entry<DatanodeDetails, SCMCommand>> entries =
          datanodeDetailsSCMCommandMap.entrySet();
      for (Map.Entry<DatanodeDetails, SCMCommand> dnCommand : entries) {
        Assert.assertTrue(
            dnCommand.getValue() instanceof ReplicateContainerCommand);
      }
    }
  }

  private Set<ContainerReplica> registerNodes(
      Pair<HddsProtos.NodeOperationalState, Integer>... states) {
    Set<ContainerReplica> replica = new HashSet<>();
    for (Pair<HddsProtos.NodeOperationalState, Integer> s : states) {
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      dn.setPersistedOpState(s.getLeft());
      replica.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(ContainerID.valueOf(1)).setContainerState(CLOSED)
          .setDatanodeDetails(dn).setOriginNodeId(dn.getUuid()).setSequenceId(1)
          .setReplicaIndex(s.getRight()).build());
    }
    return replica;
  }

  private ContainerInfo createContainer(HddsProtos.LifeCycleState state,
      ReplicationConfig replicationConfig) {

    return new ContainerInfo.Builder()
        .setContainerID(ContainerID.valueOf(1).getId()).setState(state)
        .setReplicationConfig(replicationConfig).build();
  }
}
