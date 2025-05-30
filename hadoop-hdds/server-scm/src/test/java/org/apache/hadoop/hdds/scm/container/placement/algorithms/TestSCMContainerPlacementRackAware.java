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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
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
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomUtils;
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
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test for the scm container rack aware placement.
 */
public class TestSCMContainerPlacementRackAware {
  private NetworkTopology cluster;
  private OzoneConfiguration conf;
  private NodeManager nodeManager;
  private final List<DatanodeDetails> datanodes = new ArrayList<>();
  private final List<DatanodeInfo> dnInfos = new ArrayList<>();
  // policy with fallback capability
  private SCMContainerPlacementRackAware policy;
  // policy prohibit fallback
  private SCMContainerPlacementRackAware policyNoFallback;
  // node storage capacity
  private static final long STORAGE_CAPACITY = 100L;
  private SCMContainerPlacementMetrics metrics;
  private static final int NODE_PER_RACK = 5;

  private static IntStream numDatanodes() {
    return IntStream.rangeClosed(3, 15);
  }

  private void setup(int datanodeCount) {
    //initialize network topology instance
    conf = new OzoneConfiguration();
    // We are using small units here
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        1, StorageUnit.BYTES);
    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    cluster = new NetworkTopologyImpl(NodeSchemaManager.getInstance());

    // build datanodes, and network topology
    String rack = "/rack";
    String hostname = "node";
    for (int i = 0; i < datanodeCount; i++) {
      // Totally 3 racks, each has 5 datanodes
      DatanodeDetails datanodeDetails =
          MockDatanodeDetails.createDatanodeDetails(
          hostname + i, rack + (i / NODE_PER_RACK));
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
          new ArrayList<>(Arrays.asList(storage1)));
      datanodeInfo.updateMetaDataStorageReports(
          new ArrayList<>(Arrays.asList(metaStorage1)));
      dnInfos.add(datanodeInfo);
    }

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
    policy = new SCMContainerPlacementRackAware(
        nodeManager, conf, cluster, true, metrics);
    policyNoFallback = new SCMContainerPlacementRackAware(
        nodeManager, conf, cluster, false, metrics);
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
    assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)) || (datanodeCount % NODE_PER_RACK == 1));

    //  3 replicas
    nodeNum = 3;
    datanodeDetails = policy.chooseDatanodes(null, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    // requires at least 2 racks for following statement
    assumeTrue(datanodeCount > NODE_PER_RACK &&
        datanodeCount % NODE_PER_RACK > 1);
    assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));

    //  4 replicas
    nodeNum = 4;
    datanodeDetails = policy.chooseDatanodes(null, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    // requires at least 2 racks and enough datanodes for following statement
    assumeTrue(datanodeCount > NODE_PER_RACK + 1);
    assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }

  @ParameterizedTest
  @MethodSource("numDatanodes")
  public void chooseNodeWithExcludedNodes(int datanodeCount)
      throws SCMException {
    // test choose new datanodes for under replicated pipeline
    // 3 replicas, two existing datanodes on same rack
    assumeTrue(datanodeCount > NODE_PER_RACK);
    setup(datanodeCount);
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
    excludedNodes.clear();
    excludedNodes.add(datanodes.get(0));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertTrue(cluster.isSameParent(
        datanodeDetails.get(0), excludedNodes.get(0)) ||
        cluster.isSameParent(datanodeDetails.get(0), excludedNodes.get(1)));

    // 3 replicas, two existing datanodes on different rack
    nodeNum = 1;
    excludedNodes.clear();
    excludedNodes.add(datanodes.get(0));
    excludedNodes.add(datanodes.get(5));
    datanodeDetails = policy.chooseDatanodes(
        excludedNodes, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertTrue(cluster.isSameParent(
        datanodeDetails.get(0), excludedNodes.get(0)) ||
        cluster.isSameParent(datanodeDetails.get(0), excludedNodes.get(1)));
  }

  @ParameterizedTest
  @ValueSource(ints = {NODE_PER_RACK + 1, 2 * NODE_PER_RACK + 1})
  public void testSingleNodeRack(int datanodeCount) throws SCMException {
    // make sure there is a single node rack
    assumeTrue(datanodeCount % NODE_PER_RACK == 1);
    setup(datanodeCount);
    List<DatanodeDetails> excludeNodes = new ArrayList<>();
    excludeNodes.add(datanodes.get(datanodeCount - 1));
    excludeNodes.add(datanodes.get(0));
    List<DatanodeDetails> chooseDatanodes =
        policy.chooseDatanodes(excludeNodes, null, 1, 0, 0);
    assertEquals(1, chooseDatanodes.size());
    // the selected node should be on the same rack as the second exclude node
    assertTrue(
        cluster.isSameParent(chooseDatanodes.get(0), excludeNodes.get(1)),
        chooseDatanodes.get(0).toString());
  }

  @ParameterizedTest
  @ValueSource(ints = {12, 13, 14})
  public void testFallback(int datanodeCount) throws SCMException {
    // 5 replicas. there are only 3 racks. policy with fallback should
    // allocate the 5th datanode though it will break the rack rule(first
    // 2 replicas on same rack, others on different racks).
    assumeTrue(datanodeCount > NODE_PER_RACK * 2 &&
        (datanodeCount % NODE_PER_RACK > 1));
    setup(datanodeCount);
    int nodeNum = 5;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(null, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(3)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(2),
        datanodeDetails.get(3)));

    // get metrics
    long totalRequest = metrics.getDatanodeRequestCount();
    long successCount = metrics.getDatanodeChooseSuccessCount();
    long tryCount = metrics.getDatanodeChooseAttemptCount();
    long compromiseCount = metrics.getDatanodeChooseFallbackCount();

    // verify metrics
    assertEquals(totalRequest, nodeNum);
    assertEquals(successCount, nodeNum);
    assertThat(tryCount).isGreaterThan(nodeNum);
    assertThat(compromiseCount).isGreaterThanOrEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(ints = {11, 12, 13, 14, 15})
  public void testNoFallback(int datanodeCount) {
    assumeTrue(datanodeCount > (NODE_PER_RACK * 2) &&
        (datanodeCount <= NODE_PER_RACK * 3));
    setup(datanodeCount);
    // 5 replicas. there are only 3 racks. policy prohibit fallback should fail.
    int nodeNum = 5;
    Exception e =
        assertThrows(Exception.class,
            () -> policyNoFallback.chooseDatanodes(null, null, nodeNum, 0, 15),
            "Fallback prohibited, this call should fail");
    assertEquals("SCMException", e.getClass().getSimpleName());

    // get metrics
    long totalRequest = metrics.getDatanodeRequestCount();
    long successCount = metrics.getDatanodeChooseSuccessCount();
    long tryCount = metrics.getDatanodeChooseAttemptCount();
    long compromiseCount = metrics.getDatanodeChooseFallbackCount();

    assertEquals(nodeNum, totalRequest);
    assertThat(successCount).withFailMessage("Not enough success count")
        .isGreaterThanOrEqualTo(1);
    assertThat(tryCount).withFailMessage("Not enough try count")
        .isGreaterThanOrEqualTo(1);
    assertEquals(0, compromiseCount);
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

    // no overlap between excludedNodes and favoredNodes, favoredNodes can been
    // chosen.
    excludedNodes.clear();
    favoredNodes.clear();
    excludedNodes.add(datanodes.get(0));
    favoredNodes.add(datanodes.get(2));
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
    for (int i = 0; i < 15; i++) {
      // Totally 3 racks, each has 5 datanodes
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

    // choose nodes to host 3 replica
    int nodeNum = 3;
    SCMContainerPlacementRackAware newPolicy =
        new SCMContainerPlacementRackAware(nodeManager, conf, clusterMap, true,
            metrics);
    List<DatanodeDetails> datanodeDetails =
        newPolicy.chooseDatanodes(null, null, nodeNum, 0, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    assertTrue(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }

  @Test
  public void testvalidateContainerPlacement() {
    // Only run this test for the full set of DNs. 5 DNs per rack on 3 racks.
    final int datanodeCount = 15;
    setup(datanodeCount);
    List<DatanodeDetails> dns = new ArrayList<>();
    // First 5 node are on the same rack
    dns.add(datanodes.get(0));
    dns.add(datanodes.get(1));
    dns.add(datanodes.get(2));
    ContainerPlacementStatus stat = policy.validateContainerPlacement(dns, 3);
    assertFalse(stat.isPolicySatisfied());
    assertEquals(1, stat.misReplicationCount());

    // Pick a new list which spans 2 racks
    dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    dns.add(datanodes.get(1));
    dns.add(datanodes.get(5)); // This is on second rack
    stat = policy.validateContainerPlacement(dns, 3);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());

    // Pick single DN, expecting 3 replica. Policy is not met.
    dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    stat = policy.validateContainerPlacement(dns, 3);
    assertFalse(stat.isPolicySatisfied());
    assertEquals(1, stat.misReplicationCount());

    // Pick single DN, expecting 1 replica. Policy is met.
    dns = new ArrayList<>();
    dns.add(datanodes.get(0));
    stat = policy.validateContainerPlacement(dns, 1);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());
  }

  @Test
  public void testvalidateContainerPlacementSingleRackCluster() {
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

  @ParameterizedTest
  @MethodSource("org.apache.hadoop.hdds.scm.node.NodeStatus#outOfServiceStates")
  public void testOverReplicationAndOutOfServiceNodes(HddsProtos.NodeOperationalState state) {
    setup(7);
    //    7 datanodes, all nodes are used.
    //    /rack0/node0  -> IN_SERVICE
    //    /rack0/node1  -> IN_SERVICE
    //    /rack0/node2  -> OFFLINE
    //    /rack0/node3  -> OFFLINE
    //    /rack0/node4  -> OFFLINE
    //    /rack1/node5  -> IN_SERVICE
    //    /rack1/node6  -> OFFLINE
    datanodes.get(2).setPersistedOpState(state);
    datanodes.get(3).setPersistedOpState(state);
    datanodes.get(4).setPersistedOpState(state);
    datanodes.get(6).setPersistedOpState(state);
    List<DatanodeDetails> dns = new ArrayList<>(datanodes);

    ContainerPlacementStatus status = policy.validateContainerPlacement(dns, 3);
    assertTrue(status.isPolicySatisfied());
    assertEquals(2, status.actualPlacementCount());
    assertEquals(2, status.expectedPlacementCount());
    assertEquals(0, status.misReplicationCount());
    assertNull(status.misReplicatedReason());

    //    /rack0/node0  -> IN_SERVICE
    //    /rack0/node1  -> IN_SERVICE
    //    /rack0/node2  -> OFFLINE > IN_SERVICE
    //    /rack0/node3  -> OFFLINE
    //    /rack0/node4  -> OFFLINE
    //    /rack1/node5  -> IN_SERVICE
    //    /rack1/node6  -> OFFLINE > IN_SERVICE
    datanodes.get(2).setPersistedOpState(IN_SERVICE);
    datanodes.get(6).setPersistedOpState(IN_SERVICE);
    dns = new ArrayList<>(datanodes);

    status = policy.validateContainerPlacement(dns, 3);
    assertTrue(status.isPolicySatisfied());
    assertEquals(2, status.actualPlacementCount());
    assertEquals(2, status.expectedPlacementCount());
    assertEquals(0, status.misReplicationCount());
    assertNull(status.misReplicatedReason());
  }

  @ParameterizedTest
  @MethodSource("numDatanodes")
  public void testOutOfServiceNodesNotSelected(int datanodeCount) {
    setup(datanodeCount);
    // Set all the nodes to out of service
    for (DatanodeInfo dn : dnInfos) {
      dn.setNodeStatus(NodeStatus.valueOf(DECOMMISSIONED, HEALTHY));
    }

    for (int i = 0; i < 10; i++) {
      // Set a random DN to in_service and ensure it is always picked
      int index = RandomUtils.secure().randomInt(0, dnInfos.size());
      dnInfos.get(index).setNodeStatus(NodeStatus.inServiceHealthy());
      try {
        List<DatanodeDetails> datanodeDetails =
            policy.chooseDatanodes(null, null, 1, 0, 0);
        assertEquals(dnInfos.get(index), datanodeDetails.get(0));
      } catch (SCMException e) {
        // If we get SCMException: No satisfied datanode to meet the ... this is
        // ok, as there is only 1 IN_SERVICE node and with the retry logic we
        // may never find it.
      }
      dnInfos.get(index).setNodeStatus(NodeStatus.valueOf(DECOMMISSIONED, HEALTHY));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {NODE_PER_RACK + 1, 2 * NODE_PER_RACK + 1})
  public void chooseNodeWithUsedNodesMultipleRack(int datanodeCount)
      throws SCMException {
    assumeTrue(datanodeCount > NODE_PER_RACK);
    setup(datanodeCount);
    int nodeNum = 1;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    List<DatanodeDetails> usedNodes = new ArrayList<>();

    // 2 replicas, two existing datanodes on same rack
    usedNodes.add(datanodes.get(0));
    usedNodes.add(datanodes.get(1));

    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(usedNodes,
        excludedNodes, null, nodeNum, 0, 5);
    assertEquals(nodeNum, datanodeDetails.size());

    // New DN should be on different rack than DN0 & DN1
    assertTrue(!cluster.isSameParent(
        datanodes.get(0), datanodeDetails.get(0)) &&
        !cluster.isSameParent(datanodes.get(1), datanodeDetails.get(0)));

    // 2 replicas, two existing datanodes on different rack
    usedNodes.clear();
    // 1st Replica on rack0
    usedNodes.add(datanodes.get(0));
    // 2nd Replica on rack1
    usedNodes.add(datanodes.get(5));

    datanodeDetails = policy.chooseDatanodes(usedNodes,
        excludedNodes, null, nodeNum, 0, 5);
    assertEquals(nodeNum, datanodeDetails.size());

    // New replica should be either on rack0 or rack1
    assertTrue(cluster.isSameParent(
        datanodes.get(0), datanodeDetails.get(0)) ||
        cluster.isSameParent(datanodes.get(5), datanodeDetails.get(0)));
  }

  @Test
  public void chooseSingleNodeRackWithUsedAndExcludeNodes()
      throws SCMException {
    int datanodeCount = 5;
    setup(datanodeCount);
    int nodeNum = 1;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    List<DatanodeDetails> usedNodes = new ArrayList<>();

    // 2 replicas, two existing datanodes on same rack
    usedNodes.add(datanodes.get(0));
    usedNodes.add(datanodes.get(1));
    excludedNodes.add(datanodes.get(2));

    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(usedNodes,
        excludedNodes, null, nodeNum, 0, 5);
    assertEquals(nodeNum, datanodeDetails.size());

    assertTrue(cluster.isSameParent(datanodes.get(0), datanodeDetails.get(0)));
    assertTrue(cluster.isSameParent(datanodes.get(1), datanodeDetails.get(0)));
    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(0));

    // Required 2 DN for 2 replica
    nodeNum = 2;
    // One replica exist
    usedNodes.clear();
    // 1st Replica on rack0
    usedNodes.add(datanodes.get(0));

    datanodeDetails = policy.chooseDatanodes(usedNodes,
        excludedNodes, null, nodeNum, 0, 5);
    assertEquals(nodeNum, datanodeDetails.size());

    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(0));
    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(1));

    nodeNum = 3;
    // No replica exist
    usedNodes.clear();

    datanodeDetails = policy.chooseDatanodes(usedNodes,
        excludedNodes, null, nodeNum, 0, 5);
    assertEquals(nodeNum, datanodeDetails.size());

    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(0));
    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(1));
  }

  @ParameterizedTest
  @MethodSource("numDatanodes")
  public void chooseNodeWithUsedAndExcludeNodesMultipleRack(int datanodeCount)
      throws SCMException {
    assumeTrue(datanodeCount > NODE_PER_RACK);
    setup(datanodeCount);
    int nodeNum = 2;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    List<DatanodeDetails> usedNodes = new ArrayList<>();

    // 1 replica
    usedNodes.add(datanodes.get(0));
    // 1 exclude node
    excludedNodes.add(datanodes.get(1));

    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(usedNodes,
        excludedNodes, null, nodeNum, 0, 5);

    assertEquals(nodeNum, datanodeDetails.size());

    // Exclude node should not be returned
    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(0));
    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(1));

    usedNodes.clear();
    excludedNodes.clear();
    // 1 replica
    // Multiple exclude nodes
    usedNodes.add(datanodes.get(0));
    excludedNodes.add(datanodes.get(1));
    excludedNodes.add(datanodes.get(2));

    datanodeDetails = policy.chooseDatanodes(usedNodes,
        excludedNodes, null, nodeNum, 0, 5);

    assertEquals(nodeNum, datanodeDetails.size());

    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(0));
    assertNotEquals(excludedNodes.get(1), datanodeDetails.get(0));

    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(1));
    assertNotEquals(excludedNodes.get(1), datanodeDetails.get(1));
  }

  @ParameterizedTest
  @MethodSource("numDatanodes")
  public void chooseNodeWithOnlyExcludeAndNoUsedNodes(int datanodeCount)
      throws SCMException {
    assumeTrue(datanodeCount > NODE_PER_RACK);
    setup(datanodeCount);
    int nodeNum = 3;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    // 1 exclude node
    excludedNodes.add(datanodes.get(1));

    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(null,
        excludedNodes, null, nodeNum, 0, 5);

    assertEquals(nodeNum, datanodeDetails.size());

    // Exclude node should not be returned
    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(0));
    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(1));

    excludedNodes.clear();
    // Multiple exclude nodes
    excludedNodes.add(datanodes.get(1));
    excludedNodes.add(datanodes.get(2));

    datanodeDetails = policy.chooseDatanodes(null,
        excludedNodes, null, nodeNum, 0, 5);

    assertEquals(nodeNum, datanodeDetails.size());

    // Exclude node should not be returned
    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(0));
    assertNotEquals(excludedNodes.get(0), datanodeDetails.get(1));

    assertNotEquals(excludedNodes.get(1), datanodeDetails.get(0));
    assertNotEquals(excludedNodes.get(1), datanodeDetails.get(1));
  }

  @ParameterizedTest
  @ValueSource(ints = {11, 12, 13, 14, 15})
  public void testNoFallbackWithUsedNodes(int datanodeCount) {
    assumeTrue(datanodeCount > (NODE_PER_RACK * 2) &&
        (datanodeCount <= NODE_PER_RACK * 3));
    setup(datanodeCount);

    List<DatanodeDetails> usedNodes = new ArrayList<>();
    usedNodes.add(datanodes.get(0));

    // 5 replicas. there are only 3 racks. policy prohibit fallback should fail.
    int nodeNum = 5;
    Exception e =
        assertThrows(Exception.class,
            () -> policyNoFallback.chooseDatanodes(usedNodes, null, null, nodeNum, 0, 15),
            "Fallback prohibited, this call should fail");
    assertEquals("SCMException", e.getClass().getSimpleName());

    // get metrics
    long totalRequest = metrics.getDatanodeRequestCount();
    long successCount = metrics.getDatanodeChooseSuccessCount();
    long tryCount = metrics.getDatanodeChooseAttemptCount();
    long compromiseCount = metrics.getDatanodeChooseFallbackCount();

    assertEquals(nodeNum, totalRequest);
    assertThat(successCount).withFailMessage("Not enough success count")
        .isGreaterThanOrEqualTo(1);
    assertThat(tryCount).withFailMessage("Not enough try count")
        .isGreaterThanOrEqualTo(1);
    assertEquals(0, compromiseCount);
  }

  @Test
  public void chooseNodeWithUsedAndFavouredNodesMultipleRack()
      throws SCMException {
    int datanodeCount = 12;
    setup(datanodeCount);
    int nodeNum = 1;
    List<DatanodeDetails> usedNodes = new ArrayList<>();
    List<DatanodeDetails> favouredNodes = new ArrayList<>();

    // 2 replica
    usedNodes.add(datanodes.get(0));
    usedNodes.add(datanodes.get(1));
    // 1 favoured node
    favouredNodes.add(datanodes.get(2));

    List<DatanodeDetails> datanodeDetails = policy.chooseDatanodes(usedNodes,
        null, favouredNodes, nodeNum, 0, 5);

    assertEquals(nodeNum, datanodeDetails.size());
    // Favoured node should not be returned,
    // Returned node should be on the different rack than the favoured node.
    assertFalse(cluster.isSameParent(
        favouredNodes.get(0), datanodeDetails.get(0)));

    favouredNodes.clear();
    // 1 favoured node
    favouredNodes.add(datanodes.get(6));

    datanodeDetails = policy.chooseDatanodes(usedNodes,
        null, favouredNodes, nodeNum, 0, 5);

    assertEquals(nodeNum, datanodeDetails.size());

    // Favoured node should be returned,
    // as favoured node is in the different rack as used nodes.
    assertEquals(favouredNodes.get(0), datanodeDetails.get(0));

  }

  @Test
  public void testSourceDatanodeIsNotChosenAsTarget() {
    setup(2);
    List<DatanodeDetails> usedNodes = new ArrayList<>();
    usedNodes.add(datanodes.get(0));
    dnInfos.get(1).setNodeStatus(NodeStatus.inServiceHealthyReadOnly());

    assertThrows(SCMException.class,
            () -> policy.chooseDatanodes(usedNodes, null, null, 1, 0, 0),
            "No target datanode, this call should fail");
  }
}
