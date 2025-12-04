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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for PipelinePlacementFactory.
 */
public class TestPipelinePlacementFactory {
  private OzoneConfiguration conf;
  private NodeManager nodeManager;
  private PipelineStateManager stateManager;
  private NetworkTopologyImpl cluster;
  private final List<DatanodeDetails> datanodes = new ArrayList<>();
  private final List<DatanodeInfo> dnInfos = new ArrayList<>();

  private static final long STORAGE_CAPACITY = 100L;

  @BeforeEach
  public void setup(@TempDir File testDir) {
    //initialize ozone config for tests
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
  }

  private void setupRacks(int datanodeCount, int nodesPerRack,
                          boolean firstRackLessNode)
      throws Exception {
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
      DatanodeDetails datanodeDetails =
          MockDatanodeDetails.createDatanodeDetails(
              hostname + i, firstRackLessNode ?
                  rack + ((i + 1) / nodesPerRack) : rack + (i / nodesPerRack));

      datanodes.add(datanodeDetails);
      cluster.add(datanodeDetails);
      DatanodeInfo datanodeInfo = new DatanodeInfo(
          datanodeDetails, NodeStatus.inServiceHealthy(),
          UpgradeUtils.defaultLayoutVersionProto());

      StorageContainerDatanodeProtocolProtos.StorageReportProto storage1 =
          HddsTestUtils.createStorageReport(
          datanodeInfo.getID(), "/data1-" + datanodeInfo.getID(),
          STORAGE_CAPACITY, 0, 100L, null);
      StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto
          metaStorage1 =
          HddsTestUtils.createMetadataStorageReport(
              "/metadata1-" + datanodeInfo.getID(),
              STORAGE_CAPACITY, 0, 100L, null);
      datanodeInfo.updateStorageReports(
          new ArrayList<>(Arrays.asList(storage1)));
      datanodeInfo.updateMetaDataStorageReports(
          new ArrayList<>(Arrays.asList(metaStorage1)));
      dnInfos.add(datanodeInfo);
    }
    NodeManager nodeManagerBase = new MockNodeManager(cluster, datanodes,
        false, 10);
    nodeManager = spy(nodeManagerBase);
    for (DatanodeInfo dn: dnInfos) {
      when(nodeManager.getNode(dn.getID()))
          .thenReturn(dn);
    }

    DBStore dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);

    stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
  }

  @Test
  public void testDefaultPolicy() {
    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(null, null, conf);
    assertSame(PipelinePlacementPolicy.class, policy.getClass());
  }

  @Test
  public void testRackScatterPolicy() throws Exception {
    conf.set(OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementRackScatter.class.getCanonicalName());
    // for this test, rack setup does not matter, just
    // need a non-null NetworkTopologyMap within the nodeManager
    setupRacks(6, 3, false);
    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);
    assertSame(SCMContainerPlacementRackScatter.class,
        policy.getClass());
  }

  // test default rack aware pipeline provider placement - 3 racks
  // pipeline created with 1 node on one rack and other 2 nodes
  // on separate rack
  @Test
  public void testDefaultPipelineProviderRackPlacement() throws Exception {
    setupRacks(6, 2, false);
    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);

    int nodeNum = 3;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(null, null, nodeNum, 15, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }

  // test rack scatter pipeline provider placement - 3 racks
  // pipeline created with node on each rack
  @Test
  public void testRackScatterPipelineProviderRackPlacement() throws Exception {
    conf.set(OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementRackScatter.class.getCanonicalName());

    setupRacks(6, 2, false);
    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);

    int nodeNum = 3;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    List<DatanodeDetails> favoredNodes = new ArrayList<>();
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(excludedNodes, excludedNodes, favoredNodes,
            nodeNum, 15, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }

  @Test
  public void testPipelineProviderRackPlacementAnchorChange()
      throws Exception {
    // rack0: Node0
    // rack1: Node1, Node2
    // rack2: Node3, Node4
    // rack3: Node5
    setupRacks(6, 2, true);

    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);

    int nodeNum = 3;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(null, null, nodeNum, 15, 15);
    assertEquals(nodeNum, datanodeDetails.size());

    // First anchor will be Node0, Since there is no more node available
    // on rack0, Anchor will change to Node1 and then Node2 will be selected
    // which is same rack as Node1.
    assertTrue(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
  }

  @Test
  public void testPipelineProviderRackPlacementWithUsedNodes()
      throws Exception {
    // 3 nodes per rack
    setupRacks(9, 3, false);

    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);
    List<DatanodeDetails> usedNodes = new ArrayList<>();

    // 1 Used node
    usedNodes.add(datanodes.get(0));
    int nodeNum = 2;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(usedNodes, null, null, nodeNum, 15, 15);
    assertEquals(nodeNum, datanodeDetails.size());

    assertTrue(cluster.isSameParent(usedNodes.get(0),
        datanodeDetails.get(0)) || cluster.isSameParent(usedNodes.get(0),
        datanodeDetails.get(1)));

    // 2 usedNodes in same rack
    usedNodes.clear();
    usedNodes.add(datanodes.get(0));
    usedNodes.add(datanodes.get(1));

    nodeNum = 1;
    datanodeDetails =
        policy.chooseDatanodes(usedNodes, null, null, nodeNum, 15, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    // Node return by policy should have different parent as node0 and node1
    assertFalse(cluster.isSameParent(usedNodes.get(0),
        datanodeDetails.get(0)) && cluster.isSameParent(usedNodes.get(1),
        datanodeDetails.get(0)));

    // 2 usedNodes in different rack
    usedNodes.clear();
    usedNodes.add(datanodes.get(0));
    usedNodes.add(datanodes.get(3));

    nodeNum = 1;
    datanodeDetails =
        policy.chooseDatanodes(usedNodes, null, null, nodeNum, 15, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    // Node return by policy should have same parent as node0 or node3
    assertTrue(cluster.isSameParent(usedNodes.get(0),
        datanodeDetails.get(0)) || cluster.isSameParent(usedNodes.get(3),
        datanodeDetails.get(0)));

    // 0 usedNode
    usedNodes.clear();
    nodeNum = 3;
    datanodeDetails =
        policy.chooseDatanodes(usedNodes, null, null, nodeNum, 15, 15);
    assertEquals(nodeNum, datanodeDetails.size());
  }

  @Test
  public void testPipelineProviderRackPlacementWithUsedAndExcludeNodes()
      throws Exception {
    // 2 nodes per rack
    setupRacks(8, 2, false);

    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);
    List<DatanodeDetails> usedNodes = new ArrayList<>();
    List<DatanodeDetails> excludeNodes = new ArrayList<>();

    // 1 Used node
    usedNodes.add(datanodes.get(0));
    excludeNodes.add(datanodes.get(2));
    excludeNodes.add(datanodes.get(3));
    int nodeNum = 2;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(usedNodes, excludeNodes, null, nodeNum, 15, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    // policy should not return any of excluded node
    assertNotSame(datanodeDetails.get(0).getID(), excludeNodes.get(0).getID());
    assertNotSame(datanodeDetails.get(0).getID(), excludeNodes.get(1).getID());
    assertNotSame(datanodeDetails.get(1).getID(), excludeNodes.get(0).getID());
    assertNotSame(datanodeDetails.get(1).getID(), excludeNodes.get(1).getID());
    // One of the node should be in same rack as Node0
    assertTrue(cluster.isSameParent(usedNodes.get(0),
        datanodeDetails.get(0)) || cluster.isSameParent(usedNodes.get(0),
        datanodeDetails.get(1)));

    // 2 Used node
    usedNodes.clear();
    usedNodes.add(datanodes.get(0));
    usedNodes.add(datanodes.get(4));
    excludeNodes.add(datanodes.get(1));
    excludeNodes.add(datanodes.get(2));
    nodeNum = 1;
    datanodeDetails =
        policy.chooseDatanodes(usedNodes, excludeNodes, null, nodeNum, 15, 15);
    assertEquals(nodeNum, datanodeDetails.size());
    // policy should not return any of excluded node
    assertNotSame(datanodeDetails.get(0).getID(), excludeNodes.get(0).getID());
    assertNotSame(datanodeDetails.get(0).getID(), excludeNodes.get(1).getID());
    // Since rack0 has not enough node (node0 is used and node1 is excluded),
    // Anchor will change to node4(rack2) and will return another node
    // from rack2
    assertTrue(cluster.isSameParent(usedNodes.get(1),
        datanodeDetails.get(0)));
  }
}
