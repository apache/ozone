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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto.DISK;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Test functions of SCMCommonPlacementPolicy.
 */
public class TestSCMCommonPlacementPolicy {

  private MockNodeManager nodeManager;
  private OzoneConfiguration conf;

  @BeforeEach
  void setup(@TempDir File testDir) {
    nodeManager = new MockNodeManager(true, 10);
    conf = SCMTestUtils.getConf(testDir);
  }

  @Test
  public void testGetResultSet() throws SCMException {
    DummyPlacementPolicy dummyPlacementPolicy =
        new DummyPlacementPolicy(nodeManager, conf, 5);
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> result = dummyPlacementPolicy.getResultSet(3, list);
    Set<DatanodeDetails> resultSet = new HashSet<>(result);
    assertNotEquals(1, resultSet.size());
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
    assertEquals(expectedNumberOfReplicasToCopy,
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
      assertEquals(
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
            HddsTestUtils.getReplicasWithReplicaIndex(ContainerID.valueOf(1),
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
            HddsTestUtils.getReplicasWithReplicaIndex(ContainerID.valueOf(1),
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
            HddsTestUtils.getReplicasWithReplicaIndex(ContainerID.valueOf(1),
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
            .getReplicas(ContainerID.valueOf(1), CLOSED, 0, replicaDns);
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
            .getReplicas(ContainerID.valueOf(1), CLOSED, 0, replicaDns);
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
            .getReplicas(ContainerID.valueOf(1), CLOSED, 0, replicaDns);
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
            HddsTestUtils.getReplicasWithReplicaIndex(ContainerID.valueOf(1),
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
            HddsTestUtils.getReplicasWithReplicaIndex(ContainerID.valueOf(1),
                    CLOSED, 0, 0, 0, replicaDns);
    Map<ContainerReplica, Boolean> replicaMap = replicas.stream().distinct()
            .collect(Collectors.toMap(Function.identity(), r -> false));
    replicaMap.put(replicas.get(0), true);
    assertEquals(testReplicasToFixMisreplication(replicaMap,
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
            HddsTestUtils.getReplicasWithReplicaIndex(ContainerID.valueOf(1),
                    CLOSED, 0, 0, 0, replicaDns)
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), r -> true));
    Set<ContainerReplica> replicasToCopy = dummyPlacementPolicy
            .replicasToCopyToFixMisreplication(replicas);
    assertEquals(0, replicasToCopy.size());
  }

  @Test
  public void testReplicasToRemoveWithOneOverreplication() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 5);
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    Set<ContainerReplica> replicas = Sets.newHashSet(
            HddsTestUtils.getReplicasWithReplicaIndex(
                    ContainerID.valueOf(1), CLOSED, 0, 0, 0, list.subList(1, 6)));
    ContainerReplica replica = ContainerReplica.newBuilder()
            .setContainerID(ContainerID.valueOf(1))
            .setContainerState(CLOSED)
            .setReplicaIndex(1)
            .setDatanodeDetails(list.get(7)).build();
    replicas.add(replica);

    Set<ContainerReplica> replicasToRemove = dummyPlacementPolicy
            .replicasToRemoveToFixOverreplication(replicas, 1);
    assertEquals(1, replicasToRemove.size());
    assertEquals(replicasToRemove.toArray()[0], replica);
  }

  @Test
  public void testReplicasToRemoveWithTwoOverreplication() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 5);
    List<DatanodeDetails> list = nodeManager.getAllNodes();

    Set<ContainerReplica> replicas = Sets.newHashSet(
            HddsTestUtils.getReplicasWithReplicaIndex(
                    ContainerID.valueOf(1), CLOSED, 0, 0, 0, list.subList(1, 6)));

    Set<ContainerReplica> replicasToBeRemoved = Sets.newHashSet(
            HddsTestUtils.getReplicasWithReplicaIndex(
                    ContainerID.valueOf(1), CLOSED, 0, 0, 0, list.subList(7, 9)));
    replicas.addAll(replicasToBeRemoved);

    Set<ContainerReplica> replicasToRemove = dummyPlacementPolicy
            .replicasToRemoveToFixOverreplication(replicas, 1);
    assertEquals(2, replicasToRemove.size());
    assertEquals(replicasToRemove, replicasToBeRemoved);
  }

  @Test
  public void testReplicasToRemoveWith2CountPerUniqueReplica() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 3);
    List<DatanodeDetails> list = nodeManager.getAllNodes();

    Set<ContainerReplica> replicas = Sets.newHashSet(
            HddsTestUtils.getReplicasWithReplicaIndex(
                    ContainerID.valueOf(1), CLOSED, 0, 0, 0, list.subList(0, 3)));
    replicas.addAll(HddsTestUtils.getReplicasWithReplicaIndex(
            ContainerID.valueOf(1), CLOSED, 0, 0, 0, list.subList(3, 6)));
    Set<ContainerReplica> replicasToBeRemoved = Sets.newHashSet(
            HddsTestUtils.getReplicaBuilder(ContainerID.valueOf(1), CLOSED, 0, 0, 0,
                    list.get(7).getID(), list.get(7))
                    .setReplicaIndex(1).build(),
            HddsTestUtils.getReplicaBuilder(ContainerID.valueOf(1), CLOSED, 0, 0, 0,
                    list.get(8).getID(), list.get(8)).setReplicaIndex(1)
                    .build());
    replicas.addAll(replicasToBeRemoved);

    Set<ContainerReplica> replicasToRemove = dummyPlacementPolicy
            .replicasToRemoveToFixOverreplication(replicas, 2);
    assertEquals(2, replicasToRemove.size());
    assertEquals(replicasToRemove, replicasToBeRemoved);
  }

  @Test
  public void testReplicasToRemoveWithoutReplicaIndex() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 3);
    List<DatanodeDetails> list = nodeManager.getAllNodes();

    Set<ContainerReplica> replicas = Sets.newHashSet(HddsTestUtils.getReplicas(
                    ContainerID.valueOf(1), CLOSED, 0, list.subList(0, 5)));

    Set<ContainerReplica> replicasToRemove = dummyPlacementPolicy
            .replicasToRemoveToFixOverreplication(replicas, 3);
    assertEquals(2, replicasToRemove.size());
    Set<Node> racksToBeRemoved = Arrays.asList(0, 1).stream()
            .map(dummyPlacementPolicy.racks::get).collect(Collectors.toSet());
    assertEquals(replicasToRemove.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .map(dummyPlacementPolicy::getPlacementGroup)
            .collect(Collectors.toSet()), racksToBeRemoved);
  }

  @Test
  public void testReplicasToRemoveWithOverreplicationWithinSameRack() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 3);
    List<DatanodeDetails> list = nodeManager.getAllNodes();

    Set<ContainerReplica> replicas = Sets.newHashSet(
            HddsTestUtils.getReplicasWithReplicaIndex(
                    ContainerID.valueOf(1), CLOSED, 0, 0, 0, list.subList(1, 6)));

    ContainerReplica replica1 = ContainerReplica.newBuilder()
            .setContainerID(ContainerID.valueOf(1))
            .setContainerState(CLOSED)
            .setReplicaIndex(1)
            .setDatanodeDetails(list.get(6)).build();
    replicas.add(replica1);
    ContainerReplica replica2 = ContainerReplica.newBuilder()
            .setContainerID(ContainerID.valueOf(1))
            .setContainerState(CLOSED)
            .setReplicaIndex(1)
            .setDatanodeDetails(list.get(0)).build();
    replicas.add(replica2);

    Set<ContainerReplica> replicasToRemove = dummyPlacementPolicy
            .replicasToRemoveToFixOverreplication(replicas, 1);
    Map<Node, Long> removedReplicasRackCntMap = replicasToRemove.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .map(dummyPlacementPolicy::getPlacementGroup)
            .collect(Collectors.groupingBy(Function.identity(),
                    Collectors.counting()));
    assertEquals(2, replicasToRemove.size());
    assertThat(Sets.newHashSet(1L, 2L)).contains(
            removedReplicasRackCntMap.get(dummyPlacementPolicy.racks.get(0)));
    assertEquals(
            removedReplicasRackCntMap.get(dummyPlacementPolicy.racks.get(1)),
            removedReplicasRackCntMap.get(dummyPlacementPolicy.racks.get(0))
                    == 2 ? 0 : 1);
  }

  @Test
  public void testReplicasToRemoveWithNoOverreplication() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 5);
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    Set<ContainerReplica> replicas = Sets.newHashSet(
            HddsTestUtils.getReplicasWithReplicaIndex(
                    ContainerID.valueOf(1), CLOSED, 0, 0, 0, list.subList(1, 6)));

    Set<ContainerReplica> replicasToRemove = dummyPlacementPolicy
            .replicasToRemoveToFixOverreplication(replicas, 1);
    assertEquals(replicasToRemove.size(), 0);
  }

  @Test
  public void testIdentityUsedNodesWhenUsedNotPassed() throws SCMException {
    AtomicBoolean usedNodesIdentity = new AtomicBoolean(true);
    DummyPlacementPolicy dummyPlacementPolicy =
        new DummyPlacementPolicy(nodeManager, conf, 5) {
          @Override
          protected List<DatanodeDetails> chooseDatanodesInternal(
              List<DatanodeDetails> usedNodes,
              List<DatanodeDetails> excludedNodes,
              List<DatanodeDetails> favoredNodes,
              int nodesRequired, long metadataSizeRequired,
              long dataSizeRequired) {
            usedNodesIdentity.set(usedNodesPassed(usedNodes));
            return null;
          }
        };
    dummyPlacementPolicy.chooseDatanodes(null, null, 1, 1, 1);
    assertFalse(usedNodesIdentity.get());
    dummyPlacementPolicy.chooseDatanodes(null, null, null, 1, 1, 1);
    assertTrue(usedNodesIdentity.get());
  }

  @Test
  public void testDatanodeIsInvalidInCaseOfIncreasingCommittedBytes() {
    NodeManager nodeMngr = mock(NodeManager.class);
    final DatanodeID datanodeID = DatanodeID.of(UUID.randomUUID());
    DummyPlacementPolicy placementPolicy =
        new DummyPlacementPolicy(nodeMngr, conf, 1);
    DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);
    when(datanodeDetails.getID()).thenReturn(datanodeID);

    DatanodeInfo datanodeInfo = mock(DatanodeInfo.class);
    NodeStatus nodeStatus = mock(NodeStatus.class);
    when(nodeStatus.isNodeWritable()).thenReturn(true);
    when(datanodeInfo.getNodeStatus()).thenReturn(nodeStatus);
    when(nodeMngr.getNode(eq(datanodeID))).thenReturn(datanodeInfo);

    // capacity = 200000, used = 90000, remaining = 101000, committed = 500
    StorageContainerDatanodeProtocolProtos.StorageReportProto storageReport1 =
        HddsTestUtils.createStorageReport(DatanodeID.randomID(), "/data/hdds",
                200000, 90000, 101000, DISK).toBuilder()
            .setCommitted(500)
            .setFreeSpaceToSpare(10000)
            .build();
    // capacity = 200000, used = 90000, remaining = 101000, committed = 1000
    StorageContainerDatanodeProtocolProtos.StorageReportProto storageReport2 =
        HddsTestUtils.createStorageReport(DatanodeID.randomID(), "/data/hdds",
                200000, 90000, 101000, DISK).toBuilder()
            .setCommitted(1000)
            .setFreeSpaceToSpare(100000)
            .build();
    StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto
        metaReport = HddsTestUtils.createMetadataStorageReport("/data/metadata",
          200);
    when(datanodeInfo.getStorageReports())
        .thenReturn(Collections.singletonList(storageReport1))
        .thenReturn(Collections.singletonList(storageReport2));
    when(datanodeInfo.getMetadataStorageReports())
        .thenReturn(Collections.singletonList(metaReport));


    // 500 committed bytes:
    //
    //   101000       500
    //     |           |
    // (remaining - committed) > Math.max(4000, freeSpaceToSpare)
    //                                                    |
    //                                                  100000
    //
    // Summary: 101000 - 500 > 100000 == true
    assertTrue(placementPolicy.isValidNode(datanodeDetails, 100, 4000));

    // 1000 committed bytes:
    // Summary: 101000 - 1000 > 100000 == false
    assertFalse(placementPolicy.isValidNode(datanodeDetails, 100, 4000));
  }

  /**
   * Tests that the placement validation logic is able to figure out a dead maintenance node's rack using
   * {@link DatanodeDetails#getNetworkLocation()}. So when there are three datanodes, two on one rack and the dead +
   * maintenance one on another rack (for a ratis container), the placement is valid. It is expected that the
   * maintenance node will return to the cluster later.
   */
  @Test
  public void testValidatePlacementWithDeadMaintenanceNode() throws NodeNotFoundException {
    DatanodeDetails maintenanceDn = MockDatanodeDetails.randomDatanodeDetails();
    // create 4 Datanodes: 2 in-service healthy + 1 extra in-service healthy + 1 dead and in-maintenance
    List<DatanodeDetails> allNodes = ImmutableList.of(MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails(), MockDatanodeDetails.randomDatanodeDetails(), maintenanceDn);
    Map<Integer, Integer> datanodeRackMap = new HashMap<>();
    // dead, in-maintenance dn does not get any rack to simulate that it was removed from topology on dying
    datanodeRackMap.put(0, 0); // dn0 on rack 0
    datanodeRackMap.put(1, 0); // dn1 on rack 1
    datanodeRackMap.put(2, 1); // dn2 (extra) on rack 2
    NodeManager mockNodeManager = Mockito.mock(NodeManager.class);
    when(mockNodeManager.getNodeStatus(any(DatanodeDetails.class))).thenAnswer(invocation -> {
      DatanodeDetails dn = invocation.getArgument(0);
      if (dn.equals(maintenanceDn)) {
        return NodeStatus.valueOf(HddsProtos.NodeOperationalState.IN_MAINTENANCE, HddsProtos.NodeState.DEAD);
      }
      return NodeStatus.inServiceHealthy();
    });
    when(mockNodeManager.getAllNodes()).thenAnswer(inv -> allNodes);

    NetworkTopology topology = mock(NetworkTopology.class);
    when(topology.getMaxLevel()).thenReturn(3); // leaf level
    when(topology.getNumOfNodes(anyInt())).thenReturn(2); // total racks in the cluster
    when(mockNodeManager.getClusterNetworkTopologyMap()).thenReturn(topology);

    DummyPlacementPolicy placementPolicy = new DummyPlacementPolicy(mockNodeManager, conf, datanodeRackMap, 2);
    ContainerPlacementStatus placementStatus = placementPolicy.validateContainerPlacement(
        ImmutableList.of(allNodes.get(0), allNodes.get(1), allNodes.get(3)), 3);
    assertTrue(placementStatus.isPolicySatisfied());
  }

  private static class DummyPlacementPolicy extends SCMCommonPlacementPolicy {
    private Map<DatanodeDetails, Node> rackMap;
    private List<Node> racks;
    private int rackCnt;

    /**
     * Creates Dummy Placement Policy with dn index to rack Mapping
     * in round robin fashion (rack Index = dn Index % total number of racks).
     * @param nodeManager
     * @param conf
     * @param rackCnt
     */
    DummyPlacementPolicy(NodeManager nodeManager, ConfigurationSource conf,
        int rackCnt) {
      this(nodeManager, conf,
           IntStream.range(0, nodeManager.getAllNodeCount()).boxed()
           .collect(Collectors.toMap(Function.identity(),
                   idx -> idx % rackCnt)), rackCnt);
    }

    /**
     * Creates Dummy Placement Policy with dn index -> rack index mapping.
     * @param nodeManager
     * @param conf
     * @param rackCnt
     */
    DummyPlacementPolicy(NodeManager nodeManager, ConfigurationSource conf,
            Map<Integer, Integer> datanodeRackMap, int rackCnt) {
      super(nodeManager, conf);
      this.rackCnt = rackCnt;
      this.racks = IntStream.range(0, rackCnt)
      .mapToObj(i -> {
        Node node = mock(Node.class);
        when(node.getNetworkFullPath()).thenReturn(String.valueOf(i));
        return node;
      }).collect(Collectors.toList());
      final List<? extends DatanodeDetails> datanodeDetails = nodeManager.getAllNodes();
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
    protected Node getPlacementGroup(DatanodeDetails dn) {
      return rackMap.get(dn);
    }

    @Override
    protected int getRequiredRackCount(int numReplicas, int excludedRackCount) {
      return Math.min(numReplicas, rackCnt);
    }
  }
}
