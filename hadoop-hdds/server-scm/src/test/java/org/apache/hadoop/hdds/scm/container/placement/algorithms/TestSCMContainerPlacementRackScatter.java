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

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_FIND_HEALTHY_NODES;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test for the scm container rack aware placement.
 */
public class TestSCMContainerPlacementRackScatter {
  private NetworkTopology cluster;
  private OzoneConfiguration conf;
  private NodeManager nodeManager;
  private final List<DatanodeDetails> datanodes = new ArrayList<>();
  private final List<DatanodeInfo> dnInfos = new ArrayList<>();
  // policy with fallback capability
  private SCMContainerPlacementRackScatter policy;
  // node storage capacity
  private static final long STORAGE_CAPACITY = 100L;
  private SCMContainerPlacementMetrics metrics;
  private static final int NODE_PER_RACK = 5;

  private static IntStream numDatanodes() {
    return IntStream.concat(IntStream.rangeClosed(3, 15),
        IntStream.of(20, 25, 30));
  }

  private void updateStorageInDatanode(int dnIndex, long used, long remaining) {
    StorageReportProto storage = HddsTestUtils.createStorageReport(
            dnInfos.get(dnIndex).getID(),
            "/data1-" + dnInfos.get(dnIndex).getID(),
            STORAGE_CAPACITY, used, remaining, null);
    dnInfos.get(dnIndex).updateStorageReports(
            new ArrayList<>(Arrays.asList(storage)));
  }

  private void setup(int datanodeCount) {
    setup(datanodeCount, NODE_PER_RACK);
  }

  /**
   * Sets up cluster in a way such that there's one datanode per rack, except
   * the last rack, which has datanodesInLastRackCount number of datanodes.
   * @param rackCount number of racks this cluster should have, including the
   * last rack
   * @param datanodesInLastRackCount number of datanodes that should be in
   * the last rack
   */
  private void setupOneDatanodePerRackWithExtraInLastRack(int rackCount,
      int datanodesInLastRackCount) {
    setupConfiguration();

    String rack = "/rack";
    String hostname = "node";
    // add a datanode per rack to each rack except the last one
    for (int i = 0; i < rackCount - 1; i++) {
      DatanodeDetails datanodeDetails =
          MockDatanodeDetails.createDatanodeDetails(
              hostname + i, rack + i);
      setupDatanode(datanodeDetails);
    }

    // add datanodesInLastRackCount number of datanodes to the last rack
    for (int i = 0; i < datanodesInLastRackCount; i++) {
      DatanodeDetails datanodeDetails =
          MockDatanodeDetails.createDatanodeDetails(
              hostname + (rackCount - 1 + i), rack + (rackCount - 1));
      setupDatanode(datanodeDetails);
    }

    createMocksAndUpdateStorageReports(
        rackCount - 1 + datanodesInLastRackCount);
  }

  private void setupConfiguration() {
    //initialize network topology instance
    conf = new OzoneConfiguration();
    // We are using small units here
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        1, StorageUnit.BYTES);
    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    cluster = new NetworkTopologyImpl(NodeSchemaManager.getInstance());
  }

  private void setup(int datanodeCount, int nodesPerRack) {
    setupConfiguration();
    // build datanodes, and network topology
    String rack = "/rack";
    String hostname = "node";
    for (int i = 0; i < datanodeCount; i++) {
      // Totally 6 racks, each has 5 datanodes
      DatanodeDetails datanodeDetails =
          MockDatanodeDetails.createDatanodeDetails(
              hostname + i, rack + (i / nodesPerRack));
      setupDatanode(datanodeDetails);
    }

    createMocksAndUpdateStorageReports(datanodeCount);
  }

  /**
   * Adds the datanode to class level data structures and Network Topology.
   * Creates a DatanodeInfo object and storage reports for it.
   * @param datanodeDetails the datanode to setup
   */
  private void setupDatanode(DatanodeDetails datanodeDetails) {
    datanodes.add(datanodeDetails);
    cluster.add(datanodeDetails);
    DatanodeInfo datanodeInfo = new DatanodeInfo(
        datanodeDetails, NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());

    StorageReportProto storage1 = HddsTestUtils.createStorageReport(
        datanodeInfo.getID(), "/data1-" + datanodeInfo.getID(),
        STORAGE_CAPACITY, 0, 100L, null);
    MetadataStorageReportProto metaStorage1 =
        HddsTestUtils.createMetadataStorageReport(
            "/metadata1-" + datanodeInfo.getID(),
            STORAGE_CAPACITY, 0, 100L, null);
    datanodeInfo.updateStorageReports(
        new ArrayList<>(Collections.singletonList(storage1)));
    datanodeInfo.updateMetaDataStorageReports(
        new ArrayList<>(Collections.singletonList(metaStorage1)));
    dnInfos.add(datanodeInfo);
  }

  private void createMocksAndUpdateStorageReports(int datanodeCount) {
    if (datanodeCount > 4) {
      StorageReportProto storage2 = HddsTestUtils.createStorageReport(
          dnInfos.get(2).getID(),
          "/data1-" + datanodes.get(2).getID(),
          STORAGE_CAPACITY, 90L, 10L, null);
      dnInfos.get(2).updateStorageReports(
          new ArrayList<>(Arrays.asList(storage2)));
      StorageReportProto storage3 = HddsTestUtils.createStorageReport(
          dnInfos.get(3).getID(),
          "/data1-" + dnInfos.get(3).getID(),
          STORAGE_CAPACITY, 80L, 20L, null);
      dnInfos.get(3).updateStorageReports(
          new ArrayList<>(Arrays.asList(storage3)));
      StorageReportProto storage4 = HddsTestUtils.createStorageReport(
          dnInfos.get(4).getID(),
          "/data1-" + dnInfos.get(4).getID(),
          STORAGE_CAPACITY, 70L, 30L, null);
      dnInfos.get(4).updateStorageReports(
          new ArrayList<>(Arrays.asList(storage4)));
    } else if (datanodeCount > 3) {
      StorageReportProto storage2 = HddsTestUtils.createStorageReport(
          dnInfos.get(2).getID(),
          "/data1-" + dnInfos.get(2).getID(),
          STORAGE_CAPACITY, 90L, 10L, null);
      dnInfos.get(2).updateStorageReports(
          new ArrayList<>(Arrays.asList(storage2)));
      StorageReportProto storage3 = HddsTestUtils.createStorageReport(
          dnInfos.get(3).getID(),
          "/data1-" + dnInfos.get(3).getID(),
          STORAGE_CAPACITY, 80L, 20L, null);
      dnInfos.get(3).updateStorageReports(
          new ArrayList<>(Arrays.asList(storage3)));
    } else if (datanodeCount > 2) {
      StorageReportProto storage2 = HddsTestUtils.createStorageReport(
          dnInfos.get(2).getID(),
          "/data1-" + dnInfos.get(2).getID(),
          STORAGE_CAPACITY, 84L, 16L, null);
      dnInfos.get(2).updateStorageReports(
          new ArrayList<>(Arrays.asList(storage2)));
    }

    // create mock node manager
    nodeManager = mock(NodeManager.class);
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(new ArrayList<>(datanodes));
    for (DatanodeInfo dn: dnInfos) {
      when(nodeManager.getNode(dn.getID()))
          .thenReturn(dn);
    }
    when(nodeManager.getClusterNetworkTopologyMap())
        .thenReturn(cluster);

    // create placement policy instances
    policy = new SCMContainerPlacementRackScatter(
        nodeManager, conf, cluster, true, metrics);
  }

  @BeforeEach
  public void init() {
    metrics = SCMContainerPlacementMetrics.create();
  }

  @AfterEach
  public void teardown() {
    metrics.unRegister();
  }

  @ParameterizedTest
  @MethodSource("numDatanodes")
  public void chooseNodeWithNoExcludedNodes(int datanodeCount)
      throws SCMException {
    setup(datanodeCount);
    int rackLevel = cluster.getMaxLevel() - 1;
    int rackNum = cluster.getNumOfNodes(rackLevel);

    // test choose new datanodes for new pipeline cases
    // 1 replica
    int nodeNum = 1;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(null, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());

    // 2 replicas
    nodeNum = 2;
    datanodeDetails = policy.chooseDatanodes(null, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertTrue(!cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)) || (datanodeCount <= NODE_PER_RACK));

    //  3 replicas
    nodeNum = 3;
    if (datanodeCount > nodeNum) {
      datanodeDetails = policy.chooseDatanodes(null, null, nodeNum, 0, 15);
      assertEquals(nodeNum, datanodeDetails.size());
      assertEquals(getRackSize(datanodeDetails),
          Math.min(nodeNum, rackNum));
    }

    //  5 replicas
    nodeNum = 5;
    if (datanodeCount > nodeNum) {
      assumeTrue(datanodeCount >= NODE_PER_RACK);
      if (datanodeCount == 6) {
        int finalNodeNum = nodeNum;
        SCMException e = assertThrows(SCMException.class,
                () -> policy.chooseDatanodes(null, null, finalNodeNum, 0, 15));
        assertEquals(FAILED_TO_FIND_HEALTHY_NODES, e.getResult());
      } else {
        datanodeDetails = policy.chooseDatanodes(null, null, nodeNum, 0, 15);
        assertEquals(nodeNum, datanodeDetails.size());
        assertEquals(getRackSize(datanodeDetails), Math.min(nodeNum, rackNum));
      }
    }

    //  10 replicas
    nodeNum = 10;
    if (datanodeCount > nodeNum) {
      assumeTrue(datanodeCount > 2 * NODE_PER_RACK);
      if (datanodeCount == 11) {
        int finalNodeNum = nodeNum;
        SCMException e = assertThrows(SCMException.class,
                () -> policy.chooseDatanodes(null, null, finalNodeNum, 0, 15));
        assertEquals(FAILED_TO_FIND_HEALTHY_NODES, e.getResult());
      } else {
        datanodeDetails = policy.chooseDatanodes(null, null, nodeNum, 0, 15);
        assertEquals(nodeNum, datanodeDetails.size());
        assertEquals(getRackSize(datanodeDetails), Math.min(nodeNum, rackNum));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("numDatanodes")
  public void chooseNodeWithExcludedNodes(int datanodeCount)
      throws SCMException {
    // test choose new datanodes for under replicated pipeline
    // 3 replicas, two existing datanodes on same rack
    assumeTrue(datanodeCount > NODE_PER_RACK);
    setup(datanodeCount);
    int rackLevel = cluster.getMaxLevel() - 1;
    int rackNum = cluster.getNumOfNodes(rackLevel);
    int totalNum;
    int nodeNum = 1;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();

    excludedNodes.add(datanodes.get(0));
    excludedNodes.add(datanodes.get(1));
    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        excludedNodes.get(0)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        excludedNodes.get(1)));

    // 3 replicas, one existing datanode
    nodeNum = 2;
    totalNum = 3;
    excludedNodes.clear();
    excludedNodes.add(datanodes.get(0));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertEquals(getRackSize(datanodeDetails, excludedNodes),
        Math.min(totalNum, rackNum));

    // 3 replicas, two existing datanodes on different rack
    nodeNum = 1;
    totalNum = 3;
    excludedNodes.clear();
    excludedNodes.add(datanodes.get(0));
    excludedNodes.add(datanodes.get(5));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertEquals(getRackSize(datanodeDetails, excludedNodes),
        Math.min(totalNum, rackNum));

    // 5 replicas, one existing datanode
    nodeNum = 4;
    totalNum = 5;
    excludedNodes.clear();
    excludedNodes.add(datanodes.get(0));
    if (datanodeCount == 6) {
      int finalNodeNum = nodeNum;
      SCMException e = assertThrows(SCMException.class,
              () -> policy.chooseDatanodes(excludedNodes, null,
                      finalNodeNum, 0, 15));
      assertEquals(FAILED_TO_FIND_HEALTHY_NODES, e.getResult());
    } else {
      datanodeDetails = policy.chooseDatanodes(
              excludedNodes, null, nodeNum, 0, 15);
      assertEquals(nodeNum, datanodeDetails.size());
      assertEquals(getRackSize(datanodeDetails, excludedNodes),
              Math.min(totalNum, rackNum));
    }


    // 5 replicas, two existing datanodes on different rack
    nodeNum = 3;
    totalNum = 5;
    excludedNodes.clear();
    excludedNodes.add(datanodes.get(0));
    excludedNodes.add(datanodes.get(5));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertEquals(getRackSize(datanodeDetails, excludedNodes),
        Math.min(totalNum, rackNum));
  }

  @ParameterizedTest
  @MethodSource("numDatanodes")
  public void chooseNodeWithFavoredNodes(int datanodeCount)
      throws SCMException {
    setup(datanodeCount);
    int nodeNum = 1;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    List<DatanodeDetails> favoredNodes = new ArrayList<>();

    // no excludedNodes, only favoredNodes
    favoredNodes.add(datanodes.get(0));
    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(
        excludedNodes, favoredNodes, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertEquals(datanodeDetails.get(0).getNetworkFullPath(),
        favoredNodes.get(0).getNetworkFullPath());

    // no overlap between excludedNodes and favoredNodes, favoredNodes can be
    // chosen.
    excludedNodes.clear();
    favoredNodes.clear();
    excludedNodes.add(datanodes.get(0));
    favoredNodes.add(datanodes.get(1));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, favoredNodes, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertEquals(datanodeDetails.get(0).getNetworkFullPath(),
        favoredNodes.get(0).getNetworkFullPath());

    // there is overlap between excludedNodes and favoredNodes, favoredNodes
    // should not be chosen.
    excludedNodes.clear();
    favoredNodes.clear();
    excludedNodes.add(datanodes.get(0));
    favoredNodes.add(datanodes.get(0));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, favoredNodes, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertNotEquals(datanodeDetails.get(0).getNetworkFullPath(),
        favoredNodes.get(0).getNetworkFullPath());
  }

  @ParameterizedTest
  @MethodSource("numDatanodes")
  public void testNoInfiniteLoop(int datanodeCount) {
    setup(datanodeCount);
    int nodeNum = 1;
    // request storage space larger than node capability
    Exception e =
        assertThrows(Exception.class,
            () -> policy.chooseDatanodes(null, null, nodeNum, STORAGE_CAPACITY + 0, 15),
            "Storage requested exceeds capacity, this call should fail");
    assertEquals("SCMException", e.getClass().getSimpleName());

    // get metrics
    long totalRequest = metrics.getDatanodeRequestCount();
    long successCount = metrics.getDatanodeChooseSuccessCount();
    long tryCount = metrics.getDatanodeChooseAttemptCount();
    long compromiseCount = metrics.getDatanodeChooseFallbackCount();

    assertEquals(totalRequest, nodeNum);
    assertEquals(successCount, 0);
    assertThat(tryCount).withFailMessage("Not enough try").isGreaterThanOrEqualTo(nodeNum);
    assertEquals(compromiseCount, 0);
  }

  @ParameterizedTest
  @MethodSource("numDatanodes")
  public void testDatanodeWithDefaultNetworkLocation(int datanodeCount)
      throws SCMException {
    setup(datanodeCount);
    String hostname = "node";
    List<DatanodeInfo> dnInfoList = new ArrayList<>();
    List<DatanodeDetails> dataList = new ArrayList<>();
    NetworkTopology clusterMap =
        new NetworkTopologyImpl(NodeSchemaManager.getInstance());
    for (int i = 0; i < 30; i++) {
      // Totally 6 racks, each has 5 datanodes
      DatanodeDetails dn = MockDatanodeDetails.createDatanodeDetails(
          hostname + i, null);
      DatanodeInfo dnInfo = new DatanodeInfo(
          dn, NodeStatus.inServiceHealthy(),
          UpgradeUtils.defaultLayoutVersionProto());

      StorageReportProto storage1 = HddsTestUtils.createStorageReport(
          dnInfo.getID(), "/data1-" + dnInfo.getID(),
          STORAGE_CAPACITY, 0, 100L, null);
      MetadataStorageReportProto metaStorage1 =
          HddsTestUtils.createMetadataStorageReport(
          "/metadata1-" + dnInfo.getID(),
          STORAGE_CAPACITY, 0, 100L, null);
      dnInfo.updateStorageReports(
          new ArrayList<>(Arrays.asList(storage1)));
      dnInfo.updateMetaDataStorageReports(
          new ArrayList<>(Arrays.asList(metaStorage1)));

      dataList.add(dn);
      clusterMap.add(dn);
      dnInfoList.add(dnInfo);
    }
    assertEquals(dataList.size(), StringUtils.countMatches(
        clusterMap.toString(), NetConstants.DEFAULT_RACK));
    for (DatanodeInfo dn: dnInfoList) {
      when(nodeManager.getNode(dn.getID()))
          .thenReturn(dn);
    }

    // choose nodes to host 5 replica
    int nodeNum = 5;
    SCMContainerPlacementRackScatter newPolicy =
        new SCMContainerPlacementRackScatter(nodeManager, conf, clusterMap,
            true, metrics);
    List<DatanodeDetails> datanodeDetails =
        newPolicy.chooseDatanodes(null, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertEquals(1, getRackSize(datanodeDetails));
  }

  @ParameterizedTest
  @ValueSource(ints = {15, 20, 25, 30})
  public void testValidateContainerPlacement(int datanodeCount) {
    // Only run this test for the full set of DNs. 5 DNs per rack on 6 racks.
    assumeTrue(datanodeCount >= 15);
    setup(datanodeCount);
    List<DatanodeDetails> dns = new ArrayList<>();
    // First 5 node are on the same rack
    dns.add(datanodes.get(0));
    dns.add(datanodes.get(1));
    dns.add(datanodes.get(2));
    ContainerPlacementStatus stat = policy.validateContainerPlacement(dns, 3);
    assertFalse(stat.isPolicySatisfied());
    assertEquals(2, stat.misReplicationCount());

    // Pick a new list which spans 2 racks
    dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    dns.add(datanodes.get(1));
    dns.add(datanodes.get(5)); // This is on second rack
    stat = policy.validateContainerPlacement(dns, 3);
    assertFalse(stat.isPolicySatisfied());
    assertEquals(1, stat.misReplicationCount());

    // Pick single DN, expecting 3 replica. Policy is not met.
    dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    stat = policy.validateContainerPlacement(dns, 3);
    assertFalse(stat.isPolicySatisfied());
    assertEquals(2, stat.misReplicationCount());

    // Pick single DN, expecting 1 replica. Policy is met.
    dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    stat = policy.validateContainerPlacement(dns, 1);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());
  }

  @ParameterizedTest
  @MethodSource("org.apache.hadoop.hdds.scm.node.NodeStatus#outOfServiceStates")
  public void testOverReplicationAndOutOfServiceNodes(HddsProtos.NodeOperationalState state) {
    setup(9, 3);
    //    9 datanodes, 3 per rack.
    //    /rack0/node0  -> IN_SERVICE - used
    //    /rack0/node1
    //    /rack0/node2
    //    /rack1/node3  -> IN_SERVICE - used
    //    /rack1/node4
    //    /rack1/node5
    //    /rack2/node6  -> IN_SERVICE - used
    //    /rack2/node7
    //    /rack2/node8
    List<DatanodeDetails> dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    dns.add(datanodes.get(3));
    dns.add(datanodes.get(6));

    ContainerPlacementStatus status = policy.validateContainerPlacement(dns, 3);
    assertTrue(status.isPolicySatisfied());
    assertEquals(3, status.actualPlacementCount());
    assertEquals(3, status.expectedPlacementCount());
    assertEquals(0, status.misReplicationCount());
    assertNull(status.misReplicatedReason());

    //    /rack0/node0  -> IN_SERVICE - used
    //    /rack0/node1  -> OFFLINE    - used
    //    /rack0/node2
    //    /rack1/node3  -> IN_SERVICE - used
    //    /rack1/node4  -> OFFLINE    - used
    //    /rack1/node5
    //    /rack2/node6  -> IN_SERVICE - used
    //    /rack2/node7
    //    /rack2/node8
    datanodes.get(1).setPersistedOpState(state);
    datanodes.get(4).setPersistedOpState(state);
    dns.add(datanodes.get(1));
    dns.add(datanodes.get(4));

    status = policy.validateContainerPlacement(dns, 3);
    assertTrue(status.isPolicySatisfied());
    assertEquals(3, status.actualPlacementCount());
    assertEquals(3, status.expectedPlacementCount());
    assertEquals(0, status.misReplicationCount());
    assertNull(status.misReplicatedReason());

    //    /rack0/node0  -> IN_SERVICE - used
    //    /rack0/node1  -> OFFLINE    - used
    //    /rack0/node2  -> IN_SERVICE - used
    //    /rack1/node3  -> IN_SERVICE - used
    //    /rack1/node4  -> OFFLINE    - used
    //    /rack1/node5
    //    /rack2/node6  -> IN_SERVICE - used
    //    /rack2/node7
    //    /rack2/node8
    dns.add(datanodes.get(2));

    status = policy.validateContainerPlacement(dns, 3);
    assertTrue(status.isPolicySatisfied());
    assertEquals(3, status.actualPlacementCount());
    assertEquals(3, status.expectedPlacementCount());
    assertEquals(0, status.misReplicationCount());
    assertNull(status.misReplicatedReason());
  }

  public List<DatanodeDetails> getDatanodes(List<Integer> dnIndexes) {
    return dnIndexes.stream().map(datanodes::get).collect(Collectors.toList());
  }

  private void assertPlacementPolicySatisfied(List<DatanodeDetails> usedDns,
      List<DatanodeDetails> additionalNodes,
      List<DatanodeDetails> excludedNodes, int requiredNodes,
      boolean isSatisfied, int misReplication) {
    assertFalse(excludedNodes.stream().anyMatch(additionalNodes::contains));
    ContainerPlacementStatus stat = policy.validateContainerPlacement(
            Stream.of(usedDns, additionalNodes)
                    .flatMap(List::stream).collect(Collectors.toList()),
            requiredNodes);
    assertEquals(isSatisfied, stat.isPolicySatisfied());
    assertEquals(misReplication, stat.misReplicationCount());
  }

  @Test
  public void testPipelineProviderRackScatter() throws SCMException {
    setup(3, 1);
    conf.set(OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementRackScatter.class.getCanonicalName());
    List<DatanodeDetails> usedDns = new ArrayList<>();
    List<DatanodeDetails> excludedDns = new ArrayList<>();
    List<DatanodeDetails> additionalNodes = policy.chooseDatanodes(usedDns,
        excludedDns, null, 3, 0, 5);
    assertPlacementPolicySatisfied(usedDns, additionalNodes, excludedDns, 3,
        true, 0);
  }

  // Test for pipeline provider placement when number of racks less than
  // number of node required and nodes cannot be scattered.  In this case
  // the placement spreads the nodes as much as possible.  In one case
  // 3 nodes required and 2 racks placing 2 in one 1 in another.  When
  // only 1 rack placing all nodes in same rack.
  @Test
  public void testPipelineProviderRackScatterFallback() throws SCMException {
    setup(3, 2);
    conf.set(OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementRackScatter.class.getCanonicalName());
    List<DatanodeDetails> usedDns = new ArrayList<>();
    List<DatanodeDetails> excludedDns = new ArrayList<>();
    List<DatanodeDetails> additionalNodes = policy.chooseDatanodes(usedDns,
        excludedDns, null, 3, 0, 5);
    assertPlacementPolicySatisfied(usedDns, additionalNodes, excludedDns, 3,
        true, 0);

    setup(3, 3);
    additionalNodes = policy.chooseDatanodes(usedDns,
        excludedDns, null, 3, 0, 5);
    assertPlacementPolicySatisfied(usedDns, additionalNodes, excludedDns, 3,
        true, 0);
  }

  // add test for pipeline engagement

  @Test
  public void testValidChooseNodesWithUsedNodes() throws SCMException {
    setup(5, 2);
    List<DatanodeDetails> usedDns = getDatanodes(Lists.newArrayList(0, 1));
    List<DatanodeDetails> excludedDns = getDatanodes(Lists.newArrayList(2));
    List<DatanodeDetails> additionalNodes = policy.chooseDatanodes(usedDns,
            excludedDns, null, 2, 0, 5);
    assertPlacementPolicySatisfied(usedDns, additionalNodes, excludedDns, 4,
            true, 0);
  }

  /**
   * The expectation is that one datanode should be chosen, even though the
   * placement policy ideally requires two more racks.
   * @see <a href="https://issues.apache.org/jira/browse/HDDS-9011">...</a>
   */
  @Test
  public void shouldChooseNodeIfNodesRequiredLessThanAdditionalRacksRequired()
      throws SCMException {
    setup(5, 2);
    List<DatanodeDetails> usedDns = getDatanodes(Lists.newArrayList(0, 1));
    List<DatanodeDetails> excludedDns = getDatanodes(Lists.newArrayList(2));

    List<DatanodeDetails> chosenNodes =
        policy.chooseDatanodes(usedDns, excludedDns,
            null, 1, 0, 5);
    assertEquals(1, chosenNodes.size());
    /*
    The chosen node should be node4 from the third rack because we prefer to
    choose from racks that don't have used or excluded nodes.
     */
    assertEquals(datanodes.get(4), chosenNodes.get(0));
  }

  /**
   * Scenario:
   * rack0 -> node0
   * rack1 -> node1
   * rack2 -> node2
   * rack3 -> node3
   * rack4 -> node4
   * rack5 -> node5, node6
   * <p>
   * node0, node1, node5, node6 are used nodes. node2 is excluded. We are
   * asking the placement policy for one more node. Expectation is that an
   * SCMException should be thrown if fallback is false because we're asking
   * for one more node while placement policy requires two more racks. Else,
   * either node3 or node4 should be returned.
   */
  @Test
  public void shouldChooseNodeWhenOneNodeRequiredAndTwoRacksRequired()
      throws SCMException {
    setupOneDatanodePerRackWithExtraInLastRack(6, 2);
    List<DatanodeDetails> usedDns = getDatanodes(Lists.newArrayList(0, 1, 5,
        5));
    List<DatanodeDetails> excludedDns = getDatanodes(Lists.newArrayList(2));

    List<DatanodeDetails> chosenNode =
        policy.chooseDatanodes(usedDns, excludedDns,
            null, 1, 0, 5);
    assertEquals(1, chosenNode.size());
    assertTrue(chosenNode.get(0).equals(datanodes.get(3)) ||
        chosenNode.get(0).equals(datanodes.get(4)));
  }

  @Test
  public void testChooseNodesWithInsufficientNodesAvailable() {
    setup(5, 2);
    List<DatanodeDetails> usedDns = getDatanodes(Lists.newArrayList(0, 1));
    List<DatanodeDetails> excludedDns = getDatanodes(Lists.newArrayList(2));
    SCMException exception = assertThrows(SCMException.class, () ->
        policy.chooseDatanodes(usedDns, excludedDns,
            null, 3, 0, 5));
    assertThat(exception.getMessage())
        .matches("^No enough datanodes to choose.*");
    assertEquals(SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE,
        exception.getResult());
  }

  /**
   * Simulate a scenario with three racks and two datanodes per rack.
   * Two new nodes are required and the only available ones are on the same
   * rack. The expectation is that both these nodes on the same rack should be
   * chosen, since max replicas allowed per rack in this situation is 2.
   */
  @Test
  public void chooseNodesOnTheSameRackWhenInSufficientRacks()
      throws SCMException {
    setup(6, 2);
    List<DatanodeDetails> usedDns = getDatanodes(Lists.newArrayList(0, 1));
    updateStorageInDatanode(4, 99, 1);
    List<DatanodeDetails> excludedDns = getDatanodes(Lists.newArrayList(5));

    List<DatanodeDetails> chosenDatanodes =
        policy.chooseDatanodes(usedDns, excludedDns, null, 2, 0, 5);

    assertEquals(2, chosenDatanodes.size());
    for (DatanodeDetails dn : chosenDatanodes) {
      assertTrue(
          dn.equals(datanodes.get(2)) || dn.equals(datanodes.get(3)));
    }
  }

  @Test
  public void testValidateContainerPlacementSingleRackCluster() {
    final int datanodeCount = 5;
    setup(datanodeCount);

    // All nodes are on the same rack in this test, and the cluster only has
    // one rack.
    List<DatanodeDetails> dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    dns.add(datanodes.get(1));
    dns.add(datanodes.get(2));
    ContainerPlacementStatus stat = policy.validateContainerPlacement(dns, 3);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());

    // Single DN - policy met as cluster only has one rack.
    dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    stat = policy.validateContainerPlacement(dns, 3);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());

    // Single DN - only 1 replica expected
    dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    stat = policy.validateContainerPlacement(dns, 1);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());
  }

  @Test
  public void testExcludedNodesOverlapsOutOfServiceNodes() throws SCMException {
    final int datanodeCount = 6;
    setup(datanodeCount);

    // DN 5 is out of service
    dnInfos.get(5).setNodeStatus(NodeStatus.valueOf(DECOMMISSIONED, HEALTHY));

    // SCM should have detected that DN 5 is dead
    cluster.remove(datanodes.get(5));

    // Here we still have 5 DNs, so pick 5 should be possible
    int nodeNum = 5;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    // The DN 5 is out of service,
    // but the client already has it in the excludeList.
    // So there is an overlap.
    excludedNodes.add(datanodes.get(5));

    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 0, 5);
    assertEquals(nodeNum, datanodeDetails.size());
  }

  @Test
  public void testAllNodesOnRackExcludedReducesRackCount()
      throws SCMException {
    setup(10, 2);
    // Here we have the following nodes / racks. Note that rack 4 is all
    // excluded, so this test ensures we still get a node returned on the
    // reduced set of racks.
    // /rack0/node0 - used
    // /rack0/node1 - free
    // /rack1/node2 - used
    // /rack1/node3 - free
    // /rack2/node4 - used
    // /rack2/node5 - free
    // /rack3/node6 - used
    // /rack3/node7 - free
    // /rack4/node8 - excluded
    // /rack4/node9 - excluded

    List<DatanodeDetails> usedDns =
        getDatanodes(Lists.newArrayList(0, 2, 4, 6));
    List<DatanodeDetails> excludedDns = getDatanodes(Lists.newArrayList(8, 9));

    List<DatanodeDetails> chosenNodes =
        policy.chooseDatanodes(usedDns, excludedDns,
            null, 1, 0, 5);
    assertEquals(1, chosenNodes.size());
  }

  @Test
  public void testAllNodesOnRackExcludedReducesRackCount2()
      throws SCMException {
    setup(5, 2);
    // Here we have a setup like this, which is like a Ratis container with a
    // decommissioning node on rack 2. This makes rack 2 excluded, so we should
    // get a node returned on rack 0 or 1 instead.
    // /rack0/node0 - used
    // /rack0/node1 - free
    // /rack1/node2 - used
    // /rack1/node3 - free
    // /rack2/node4 - excluded (eg decommissioning)
    List<DatanodeDetails> usedDns = getDatanodes(Lists.newArrayList(0, 2));
    List<DatanodeDetails> excludedDns = getDatanodes(Lists.newArrayList(4));

    List<DatanodeDetails> chosenNodes =
        policy.chooseDatanodes(usedDns, excludedDns,
            null, 1, 0, 5);
    assertEquals(1, chosenNodes.size());
  }

  private int getRackSize(List<DatanodeDetails>... datanodeDetails) {
    Set<Node> racks = new HashSet<>();
    for (List<DatanodeDetails> list : datanodeDetails) {
      for (DatanodeDetails dn : list) {
        racks.add(cluster.getAncestor(dn, 1));
      }
    }
    return racks.size();
  }
}
