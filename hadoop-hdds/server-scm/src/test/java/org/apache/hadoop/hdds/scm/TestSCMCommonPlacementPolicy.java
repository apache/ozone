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

package org.apache.hadoop.hdds.scm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;

/**
 * Test functions of SCMCommonPlacementPolicy.
 */
public class TestSCMCommonPlacementPolicy {

  private NodeManager nodeManager;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setup() {
    nodeManager = new MockNodeManager(true, 10);
    conf = SCMTestUtils.getConf();
  }

  @Test
  public void testGetResultSet() throws SCMException {
    DummyPlacementPolicy dummyPlacementPolicy =
        new DummyPlacementPolicy(nodeManager, conf, 5);
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> result = dummyPlacementPolicy.getResultSet(3, list);
    Set<DatanodeDetails> resultSet = new HashSet<>(result);
    Assertions.assertNotEquals(1, resultSet.size());
  }

  private Set<ContainerReplica> testReplicasToFixMisreplication(
          List<ContainerReplica> replicas,
          DummyPlacementPolicy placementPolicy,
          int expectedNumberOfReplicasToCopy,
          Map<Node, Integer> expectedNumberOfCopyOperationFromRack) {
    return testReplicasToFixMisreplication(replicas.stream().distinct().collect(
            Collectors.toMap(Function.identity(), r -> true)), placementPolicy,
            expectedNumberOfReplicasToCopy,
            expectedNumberOfCopyOperationFromRack);
  }

  private Set<ContainerReplica> testReplicasToFixMisreplication(
          Map<ContainerReplica, Boolean> replicas,
          DummyPlacementPolicy placementPolicy,
          int expectedNumberOfReplicasToCopy,
          Map<Node, Integer> expectedNumberOfCopyOperationFromRack) {
    Set<ContainerReplica> replicasToCopy = placementPolicy
            .replicasToCopyToFixMisreplication(replicas);
    Assertions.assertEquals(expectedNumberOfReplicasToCopy,
            replicasToCopy.size());
    Map<Node, Long> rackCopyMap =
            replicasToCopy.stream().collect(Collectors.groupingBy(
                    replica -> placementPolicy
                            .getPlacementGroup(replica.getDatanodeDetails()),
            Collectors.counting()));
    Set<Node> racks = replicas.keySet().stream()
            .map(ContainerReplica::getDatanodeDetails)
            .map(placementPolicy::getPlacementGroup)
            .collect(Collectors.toSet());
    for (Node rack: racks) {
      Assertions.assertEquals(
              expectedNumberOfCopyOperationFromRack.getOrDefault(rack, 0),
              rackCopyMap.getOrDefault(rack, 0L).intValue());
    }
    return replicasToCopy;
  }

  @Test
  public void testReplicasToFixMisreplicationWithOneMisreplication() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 5);
    List<Node> racks = dummyPlacementPolicy.racks;
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns = Stream.of(0, 1, 2, 3, 5)
                    .map(list::get).collect(Collectors.toList());
    List<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(new ContainerID(1),
                    CLOSED, 0, 0, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 1,
            ImmutableMap.of(racks.get(0), 1));
  }

  @Test
  public void testReplicasToFixMisreplicationWithTwoMisreplication() {
    DummyPlacementPolicy dummyPlacementPolicy = new DummyPlacementPolicy(
            nodeManager, conf,
            GenericTestUtils.getReverseMap(
                    ImmutableMap.of(0, ImmutableList.of(0, 1, 5),
                            1, ImmutableList.of(6),
                            2, ImmutableList.of(2, 7),
                            3, ImmutableList.of(3, 8),
                            4, ImmutableList.of(4, 9))), 5);
    List<Node> racks = dummyPlacementPolicy.racks;
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns = Stream.of(0, 1, 2, 3, 5)
                    .map(list::get).collect(Collectors.toList());
    List<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(new ContainerID(1),
                    CLOSED, 0, 0, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 2,
            ImmutableMap.of(racks.get(0), 2));
  }

  @Test
  public void testReplicasToFixMisreplicationWithThreeMisreplication() {
    DummyPlacementPolicy dummyPlacementPolicy = new DummyPlacementPolicy(
            nodeManager, conf,
            GenericTestUtils.getReverseMap(
                    ImmutableMap.of(0, ImmutableList.of(0, 1, 2, 5),
                            1, ImmutableList.of(6),
                            2, ImmutableList.of(7),
                            3, ImmutableList.of(3, 8),
                            4, ImmutableList.of(4, 9))), 5);
    List<Node> racks = dummyPlacementPolicy.racks;
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns = Stream.of(0, 1, 2, 3, 5)
                    .map(list::get).collect(Collectors.toList());
    List<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(new ContainerID(1),
                    CLOSED, 0, 0, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 3,
            ImmutableMap.of(racks.get(0), 3));
  }

  @Test
  public void
      testReplicasToFixMisreplicationWithThreeMisreplicationOnDifferentRack() {
    DummyPlacementPolicy dummyPlacementPolicy = new DummyPlacementPolicy(
            nodeManager, conf,
            GenericTestUtils.getReverseMap(
                    ImmutableMap.of(0, ImmutableList.of(0, 1, 2, 5),
                            1, ImmutableList.of(6),
                            2, ImmutableList.of(7),
                            3, ImmutableList.of(3, 4, 8),
                            4, ImmutableList.of(9))), 5);
    List<Node> racks = dummyPlacementPolicy.racks;
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns = Stream.of(0, 1, 2, 3, 4)
                    .map(list::get).collect(Collectors.toList());
    //Creating Replicas without replica Index
    List<ContainerReplica> replicas = HddsTestUtils
            .getReplicas(new ContainerID(1), CLOSED, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 3,
            ImmutableMap.of(racks.get(0), 2, racks.get(3), 1));
  }

  @Test
  public void
      testReplicasToFixMisreplicationWithReplicationFactorLessThanNumberOfRack(
  ) {
    DummyPlacementPolicy dummyPlacementPolicy = new DummyPlacementPolicy(
            nodeManager, conf,
            GenericTestUtils.getReverseMap(
                    ImmutableMap.of(0, ImmutableList.of(0, 1, 5),
                            1, ImmutableList.of(6),
                            2, ImmutableList.of(2, 7),
                            3, ImmutableList.of(3, 4, 8),
                            4, ImmutableList.of(9))), 5);
    List<Node> racks = dummyPlacementPolicy.racks;
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns = Stream.of(0, 1, 3, 4)
                    .map(list::get).collect(Collectors.toList());
    //Creating Replicas without replica Index for replicas < number of racks
    List<ContainerReplica> replicas = HddsTestUtils
            .getReplicas(new ContainerID(1), CLOSED, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 2,
            ImmutableMap.of(racks.get(0), 1, racks.get(3), 1));
  }

  @Test
  public void
      testReplicasToFixMisreplicationWithReplicationFactorMoreThanNumberOfRack(
  ) {
    DummyPlacementPolicy dummyPlacementPolicy = new DummyPlacementPolicy(
            nodeManager, conf,
            GenericTestUtils.getReverseMap(
                    ImmutableMap.of(0, ImmutableList.of(0, 1, 2, 5),
                            1, ImmutableList.of(6),
                            2, ImmutableList.of(7),
                            3, ImmutableList.of(3, 4, 8),
                            4, ImmutableList.of(9))), 5);
    List<Node> racks = dummyPlacementPolicy.racks;
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns = Stream.of(0, 1, 2, 3, 4, 6)
                    .map(list::get).collect(Collectors.toList());
    //Creating Replicas without replica Index for replicas >number of racks
    List<ContainerReplica> replicas = HddsTestUtils
            .getReplicas(new ContainerID(1), CLOSED, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 2,
            ImmutableMap.of(racks.get(0), 1, racks.get(3), 1));
  }

  @Test
  public void testReplicasToFixMisreplicationMaxReplicaPerRack() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 2);
    List<Node> racks = dummyPlacementPolicy.racks;
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns = Stream.of(0, 2, 4, 6, 8)
                    .map(list::get).collect(Collectors.toList());
    List<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(new ContainerID(1),
                    CLOSED, 0, 0, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 2,
            ImmutableMap.of(racks.get(0), 2));
  }

  @Test
  public void
      testReplicasToFixMisreplicationMaxReplicaPerRackWithUncopyableReplicas() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 2);
    List<Node> racks = dummyPlacementPolicy.racks;
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns = Stream.of(0, 2, 4, 6, 8)
            .map(list::get).collect(Collectors.toList());
    List<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(new ContainerID(1),
                    CLOSED, 0, 0, 0, replicaDns);
    Map<ContainerReplica, Boolean> replicaMap = replicas.stream().distinct()
            .collect(Collectors.toMap(Function.identity(), r -> false));
    replicaMap.put(replicas.get(0), true);
    Assertions.assertEquals(testReplicasToFixMisreplication(replicaMap,
            dummyPlacementPolicy, 1,
            ImmutableMap.of(racks.get(0), 1)),
            Sets.newHashSet(replicas.get(0)));
  }

  @Test
  public void testReplicasWithoutMisreplication() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 5);
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns = Stream.of(0, 1, 2, 3, 4)
                    .map(list::get).collect(Collectors.toList());
    Map<ContainerReplica, Boolean> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(new ContainerID(1),
                    CLOSED, 0, 0, 0, replicaDns)
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), r -> true));
    Set<ContainerReplica> replicasToCopy = dummyPlacementPolicy
            .replicasToCopyToFixMisreplication(replicas);
    Assertions.assertEquals(0, replicasToCopy.size());
  }



  private static class DummyPlacementPolicy extends SCMCommonPlacementPolicy {
    private Map<DatanodeDetails, Node> rackMap;
    private List<Node> racks;
    private int rackCnt;

    DummyPlacementPolicy(NodeManager nodeManager, ConfigurationSource conf,
        int rackCnt) {
      this(nodeManager, conf,
           IntStream.range(0, nodeManager.getAllNodes().size()).boxed()
           .collect(Collectors.toMap(Function.identity(),
                   idx -> idx % rackCnt)), rackCnt);
    }

    DummyPlacementPolicy(NodeManager nodeManager, ConfigurationSource conf,
            Map<Integer, Integer> datanodeRackMap, int rackCnt) {
      super(nodeManager, conf);
      this.rackCnt = rackCnt;
      this.racks = IntStream.range(0, rackCnt)
      .mapToObj(i -> Mockito.mock(Node.class)).collect(Collectors.toList());
      List<DatanodeDetails> datanodeDetails = nodeManager.getAllNodes();
      rackMap = datanodeRackMap.entrySet().stream()
              .collect(Collectors.toMap(
                      entry -> datanodeDetails.get(entry.getKey()),
                      entry -> racks.get(entry.getValue())));
    }



    @Override
    public DatanodeDetails chooseNode(List<DatanodeDetails> healthyNodes) {
      return healthyNodes.get(0);
    }

    @Override
    public Node getPlacementGroup(DatanodeDetails dn) {
      return rackMap.get(dn);
    }

    @Override
    protected int getRequiredRackCount(int numReplicas) {
      return Math.min(numReplicas, rackCnt);
    }
  }
}
