/*
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

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.TestContainerManagerImpl;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;

import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersions;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.junit.Assert.assertFalse;

/**
 * Test for PipelinePlacementPolicy.
 */
public class TestPipelinePlacementPolicy {
  private MockNodeManager nodeManager;
  private PipelineStateManager stateManager;
  private OzoneConfiguration conf;
  private PipelinePlacementPolicy placementPolicy;
  private NetworkTopologyImpl cluster;
  private static final int PIPELINE_PLACEMENT_MAX_NODES_COUNT = 10;
  private static final int PIPELINE_LOAD_LIMIT = 5;
  private File testDir;
  private DBStore dbStore;
  private SCMHAManager scmhaManager;

  private List<DatanodeDetails> nodesWithOutRackAwareness = new ArrayList<>();
  private List<DatanodeDetails> nodesWithRackAwareness = new ArrayList<>();

  @Before
  public void init() throws Exception {
    cluster = initTopology();
    // start with nodes with rack awareness.
    nodeManager = new MockNodeManager(cluster, getNodesWithRackAwareness(),
        false, PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    conf = SCMTestUtils.getConf();
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, PIPELINE_LOAD_LIMIT);
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        10, StorageUnit.MB);
    nodeManager.setNumPipelinePerDatanode(PIPELINE_LOAD_LIMIT);
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    scmhaManager = SCMHAManagerStub.getInstance(true);
    stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
    placementPolicy = new PipelinePlacementPolicy(
        nodeManager, stateManager, conf);
  }

  @After
  public void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  private NetworkTopologyImpl initTopology() {
    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    NetworkTopologyImpl topology =
        new NetworkTopologyImpl(NodeSchemaManager.getInstance());
    return topology;
  }

  private List<DatanodeDetails> getNodesWithRackAwareness() {
    List<DatanodeDetails> datanodes = new ArrayList<>();
    int iter = 0;
    int delimiter = NODES.length;
    while (iter < PIPELINE_PLACEMENT_MAX_NODES_COUNT) {
      DatanodeDetails datanode = overwriteLocationInNode(
          getNodesWithoutRackAwareness(), NODES[iter % delimiter]);
      nodesWithRackAwareness.add(datanode);
      datanodes.add(datanode);
      iter++;
    }
    return datanodes;
  }

  private DatanodeDetails getNodesWithoutRackAwareness() {
    DatanodeDetails node = MockDatanodeDetails.randomDatanodeDetails();
    nodesWithOutRackAwareness.add(node);
    return node;
  }

  @Test
  public void testChooseNodeBasedOnNetworkTopology() {
    DatanodeDetails anchor = placementPolicy.chooseNode(nodesWithRackAwareness);
    // anchor should be removed from healthyNodes after being chosen.
    Assert.assertFalse(nodesWithRackAwareness.contains(anchor));

    List<DatanodeDetails> excludedNodes =
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    excludedNodes.add(anchor);
    DatanodeDetails nextNode = placementPolicy.chooseNodeBasedOnSameRack(
        nodesWithRackAwareness, excludedNodes,
        nodeManager.getClusterNetworkTopologyMap(), anchor);
    //DatanodeDetails nextNode = placementPolicy.chooseNodeFromNetworkTopology(
    //    nodeManager.getClusterNetworkTopologyMap(), anchor, excludedNodes);
    Assert.assertFalse(excludedNodes.contains(nextNode));
    // next node should not be the same as anchor.
    Assert.assertTrue(anchor.getUuid() != nextNode.getUuid());
    // next node should be on the same rack based on topology.
    Assert.assertEquals(anchor.getNetworkLocation(),
        nextNode.getNetworkLocation());
  }

  @Test
  public void testChooseNodeWithSingleNodeRack() throws IOException {
    // There is only one node on 3 racks altogether.
    List<DatanodeDetails> datanodes = new ArrayList<>();
    for (Node node : SINGLE_NODE_RACK) {
      DatanodeDetails datanode = overwriteLocationInNode(
          MockDatanodeDetails.randomDatanodeDetails(), node);
      datanodes.add(datanode);
    }
    MockNodeManager localNodeManager = new MockNodeManager(initTopology(),
        datanodes, false, datanodes.size());

    PipelineStateManager tempPipelineStateManager = PipelineStateManagerImpl
        .newBuilder().setNodeManager(localNodeManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();

    PipelinePlacementPolicy localPlacementPolicy = new PipelinePlacementPolicy(
        localNodeManager, tempPipelineStateManager, conf);
    int nodesRequired = HddsProtos.ReplicationFactor.THREE.getNumber();
    List<DatanodeDetails> results = localPlacementPolicy.chooseDatanodes(
        new ArrayList<>(datanodes.size()),
        new ArrayList<>(datanodes.size()),
        nodesRequired, 0, 0);

    Assert.assertEquals(nodesRequired, results.size());
    // 3 nodes should be on different racks.
    Assert.assertNotEquals(results.get(0).getNetworkLocation(),
        results.get(1).getNetworkLocation());
    Assert.assertNotEquals(results.get(0).getNetworkLocation(),
        results.get(2).getNetworkLocation());
    Assert.assertNotEquals(results.get(1).getNetworkLocation(),
        results.get(2).getNetworkLocation());
  }

  @Test
  public void testChooseNodeNotEnoughSpace() throws IOException {
    // There is only one node on 3 racks altogether.
    List<DatanodeDetails> datanodes = new ArrayList<>();
    for (Node node : SINGLE_NODE_RACK) {
      DatanodeDetails datanode = overwriteLocationInNode(
          MockDatanodeDetails.randomDatanodeDetails(), node);
      datanodes.add(datanode);
    }
    MockNodeManager localNodeManager = new MockNodeManager(initTopology(),
        datanodes, false, datanodes.size());

    PipelineStateManager tempPipelineStateManager = PipelineStateManagerImpl
        .newBuilder().setNodeManager(localNodeManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();

    PipelinePlacementPolicy localPlacementPolicy = new PipelinePlacementPolicy(
        localNodeManager, tempPipelineStateManager, conf);
    int nodesRequired = HddsProtos.ReplicationFactor.THREE.getNumber();

    String expectedMessageSubstring = "Unable to find enough nodes that meet " +
        "the space requirement";
    try {
      // A huge container size
      localPlacementPolicy.chooseDatanodes(new ArrayList<>(datanodes.size()),
          new ArrayList<>(datanodes.size()), nodesRequired,
          0, 10 * OzoneConsts.TB);
      Assert.fail("SCMException should have been thrown.");
    } catch (SCMException ex) {
      Assert.assertTrue(ex.getMessage().contains(expectedMessageSubstring));
    }

    try {
      // a huge free space min configured
      localPlacementPolicy.chooseDatanodes(new ArrayList<>(datanodes.size()),
          new ArrayList<>(datanodes.size()), nodesRequired, 10 * OzoneConsts.TB,
          0);
      Assert.fail("SCMException should have been thrown.");
    } catch (SCMException ex) {
      Assert.assertTrue(ex.getMessage().contains(expectedMessageSubstring));
    }
  }

  @Test
  public void testPickLowestLoadAnchor() throws IOException {
    List<DatanodeDetails> healthyNodes = nodeManager
        .getNodes(NodeStatus.inServiceHealthy());

    int maxPipelineCount = PIPELINE_LOAD_LIMIT * healthyNodes.size()
        / HddsProtos.ReplicationFactor.THREE.getNumber();
    for (int i = 0; i < maxPipelineCount; i++) {
      try {
        List<DatanodeDetails> nodes = placementPolicy.chooseDatanodes(null,
            null, HddsProtos.ReplicationFactor.THREE.getNumber(), 0, 0);

        Pipeline pipeline = Pipeline.newBuilder()
            .setId(PipelineID.randomId())
            .setState(Pipeline.PipelineState.ALLOCATED)
            .setReplicationConfig(RatisReplicationConfig.getInstance(
                ReplicationFactor.THREE))
            .setNodes(nodes)
            .build();
        HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
            ClientVersions.CURRENT_VERSION);
        nodeManager.addPipeline(pipeline);
        stateManager.addPipeline(pipelineProto);
      } catch (SCMException e) {
        throw e;
        //break;
      }
    }

    // Every node should be evenly used.
    int averageLoadOnNode = maxPipelineCount *
        HddsProtos.ReplicationFactor.THREE.getNumber() / healthyNodes.size();
    for (DatanodeDetails node : healthyNodes) {
      Assert.assertTrue(nodeManager.getPipelinesCount(node)
          >= averageLoadOnNode);
    }
    
    // Should max out pipeline usage.
    Assert.assertEquals(maxPipelineCount,
        stateManager
            .getPipelines(RatisReplicationConfig
                .getInstance(ReplicationFactor.THREE))
            .size());
  }

  @Test
  public void testChooseNodeBasedOnRackAwareness() {
    List<DatanodeDetails> healthyNodes = overWriteLocationInNodes(
        nodeManager.getNodes(NodeStatus.inServiceHealthy()));
    DatanodeDetails anchor = placementPolicy.chooseNode(healthyNodes);
    NetworkTopology topologyWithDifRacks =
        createNetworkTopologyOnDifRacks();
    DatanodeDetails nextNode = placementPolicy.chooseNodeBasedOnRackAwareness(
        healthyNodes, new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        topologyWithDifRacks, anchor);
    Assert.assertNotNull(nextNode);
    // next node should be on a different rack.
    Assert.assertNotEquals(anchor.getNetworkLocation(),
        nextNode.getNetworkLocation());
  }

  @Test
  public void testFallBackPickNodes() {
    List<DatanodeDetails> healthyNodes = overWriteLocationInNodes(
        nodeManager.getNodes(NodeStatus.inServiceHealthy()));
    DatanodeDetails node;

    // test no nodes are excluded
    node = placementPolicy.fallBackPickNodes(healthyNodes, null);
    Assert.assertNotNull(node);

    // when input nodeSet are all excluded.
    List<DatanodeDetails> exclude = healthyNodes;
    node = placementPolicy.fallBackPickNodes(healthyNodes, exclude);
    Assert.assertNull(node);

  }

  @Test
  public void testRackAwarenessNotEnabledWithFallBack() throws SCMException {
    DatanodeDetails anchor = placementPolicy
        .chooseNode(nodesWithOutRackAwareness);
    DatanodeDetails randomNode = placementPolicy
        .chooseNode(nodesWithOutRackAwareness);
    // rack awareness is not enabled.
    Assert.assertTrue(anchor.getNetworkLocation().equals(
        randomNode.getNetworkLocation()));

    NetworkTopology topology =
        new NetworkTopologyImpl(new OzoneConfiguration());
    DatanodeDetails nextNode = placementPolicy.chooseNodeBasedOnRackAwareness(
        nodesWithOutRackAwareness, new ArrayList<>(
            PIPELINE_PLACEMENT_MAX_NODES_COUNT), topology, anchor);
    // RackAwareness should not be able to choose any node.
    Assert.assertNull(nextNode);

    // PlacementPolicy should still be able to pick a set of 3 nodes.
    int numOfNodes = HddsProtos.ReplicationFactor.THREE.getNumber();
    List<DatanodeDetails> results = placementPolicy
        .getResultSet(numOfNodes, nodesWithOutRackAwareness);
    
    Assert.assertEquals(numOfNodes, results.size());
    // All nodes are on same rack.
    Assert.assertEquals(results.get(0).getNetworkLocation(),
        results.get(1).getNetworkLocation());
    Assert.assertEquals(results.get(0).getNetworkLocation(),
        results.get(2).getNetworkLocation());
  }

  private static final Node[] NODES = new NodeImpl[] {
      new NodeImpl("h1", "/r1", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h2", "/r1", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h3", "/r2", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h4", "/r2", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h5", "/r3", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h6", "/r3", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h7", "/r4", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h8", "/r4", NetConstants.NODE_COST_DEFAULT),
  };

  // 3 racks with single node.
  private static final Node[] SINGLE_NODE_RACK = new NodeImpl[] {
      new NodeImpl("h1", "/r1", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h2", "/r2", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h3", "/r3", NetConstants.NODE_COST_DEFAULT)
  };

  private NetworkTopology createNetworkTopologyOnDifRacks() {
    NetworkTopology topology =
        new NetworkTopologyImpl(new OzoneConfiguration());
    for (Node n : NODES) {
      topology.add(n);
    }
    return topology;
  }

  private DatanodeDetails overwriteLocationInNode(
      DatanodeDetails datanode, Node node) {
    DatanodeDetails result = DatanodeDetails.newBuilder()
        .setUuid(datanode.getUuid())
        .setHostName(datanode.getHostName())
        .setIpAddress(datanode.getIpAddress())
        .addPort(datanode.getPort(DatanodeDetails.Port.Name.STANDALONE))
        .addPort(datanode.getPort(DatanodeDetails.Port.Name.RATIS))
        .addPort(datanode.getPort(DatanodeDetails.Port.Name.REST))
        .setNetworkLocation(node.getNetworkLocation()).build();
    return result;
  }

  private List<DatanodeDetails> overWriteLocationInNodes(
      List<DatanodeDetails> datanodes) {
    List<DatanodeDetails> results = new ArrayList<>(datanodes.size());
    for (int i = 0; i < datanodes.size(); i++) {
      DatanodeDetails datanode = overwriteLocationInNode(
          datanodes.get(i), NODES[i]);
      results.add(datanode);
    }
    return results;
  }

  @Test
  public void testHeavyNodeShouldBeExcludedWithMinorityHeavy()
      throws IOException {
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(NodeStatus.inServiceHealthy());
    int nodesRequired = HddsProtos.ReplicationFactor.THREE.getNumber();
    // only minority of healthy NODES are heavily engaged in pipelines.
    int minorityHeavy = healthyNodes.size() / 2 - 1;
    List<DatanodeDetails> pickedNodes1 = placementPolicy.chooseDatanodes(
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        nodesRequired, 0, 0);
    // modify node to pipeline mapping.
    insertHeavyNodesIntoNodeManager(healthyNodes, minorityHeavy);
    // NODES should be sufficient.
    Assert.assertEquals(nodesRequired, pickedNodes1.size());
    // make sure pipeline placement policy won't select duplicated NODES.
    Assert.assertTrue(checkDuplicateNodesUUID(pickedNodes1));

    // majority of healthy NODES are heavily engaged in pipelines.
    int majorityHeavy = healthyNodes.size() / 2 + 2;
    insertHeavyNodesIntoNodeManager(healthyNodes, majorityHeavy);
    boolean thrown = false;
    List<DatanodeDetails> pickedNodes2 = null;
    try {
      pickedNodes2 = placementPolicy.chooseDatanodes(
          new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
          new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
          nodesRequired, 0, 0);
    } catch (SCMException e) {
      Assert.assertFalse(thrown);
      thrown = true;
    }
    // NODES should NOT be sufficient and exception should be thrown.
    Assert.assertNull(pickedNodes2);
    Assert.assertTrue(thrown);
  }

  @Test
  public void testHeavyNodeShouldBeExcludedWithMajorityHeavy()
      throws IOException {
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(NodeStatus.inServiceHealthy());
    int nodesRequired = HddsProtos.ReplicationFactor.THREE.getNumber();
    // majority of healthy NODES are heavily engaged in pipelines.
    int majorityHeavy = healthyNodes.size() / 2 + 2;
    insertHeavyNodesIntoNodeManager(healthyNodes, majorityHeavy);
    boolean thrown = false;
    List<DatanodeDetails> pickedNodes2 = null;
    try {
      pickedNodes2 = placementPolicy.chooseDatanodes(
          new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
          new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
          nodesRequired, 0, 0);
    } catch (SCMException e) {
      Assert.assertFalse(thrown);
      thrown = true;
    }
    // NODES should NOT be sufficient and exception should be thrown.
    Assert.assertNull(pickedNodes2);
    Assert.assertTrue(thrown);
  }

  @Test
  public void testValidatePlacementPolicyOK() {
    cluster = initTopology();
    nodeManager = new MockNodeManager(cluster, getNodesWithRackAwareness(),
        false, PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    placementPolicy = new PipelinePlacementPolicy(
        nodeManager, stateManager, conf);

    List<DatanodeDetails> dns = new ArrayList<>();
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host1", "/rack1"));
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host2", "/rack1"));
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host3", "/rack2"));
    for (DatanodeDetails dn : dns) {
      cluster.add(dn);
    }
    ContainerPlacementStatus status =
        placementPolicy.validateContainerPlacement(dns, 3);
    assertTrue(status.isPolicySatisfied());
    assertEquals(0, status.misReplicationCount());


    List<DatanodeDetails> subSet = new ArrayList<>();
    // Cut it down to two nodes, two racks
    subSet.add(dns.get(0));
    subSet.add(dns.get(2));
    status = placementPolicy.validateContainerPlacement(subSet, 3);
    assertTrue(status.isPolicySatisfied());
    assertEquals(0, status.misReplicationCount());

    // Cut it down to two nodes, one racks
    subSet = new ArrayList<>();
    subSet.add(dns.get(0));
    subSet.add(dns.get(1));
    status = placementPolicy.validateContainerPlacement(subSet, 3);
    assertFalse(status.isPolicySatisfied());
    assertEquals(1, status.misReplicationCount());

    // One node, but only one replica
    subSet = new ArrayList<>();
    subSet.add(dns.get(0));
    status = placementPolicy.validateContainerPlacement(subSet, 1);
    assertTrue(status.isPolicySatisfied());
  }

  @Test
  public void testValidatePlacementPolicySingleRackInCluster() {
    cluster = initTopology();
    nodeManager = new MockNodeManager(cluster, new ArrayList<>(),
        false, PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    placementPolicy = new PipelinePlacementPolicy(
        nodeManager, stateManager, conf);

    List<DatanodeDetails> dns = new ArrayList<>();
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host1", "/rack1"));
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host2", "/rack1"));
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host3", "/rack1"));
    for (DatanodeDetails dn : dns) {
      cluster.add(dn);
    }
    ContainerPlacementStatus status =
        placementPolicy.validateContainerPlacement(dns, 3);
    assertTrue(status.isPolicySatisfied());
    assertEquals(0, status.misReplicationCount());
  }

  @Test
  public void test3NodesInSameRackReturnedWhenOnlyOneHealthyRackIsPresent()
      throws Exception {
    List<DatanodeDetails> dns = setupSkewedRacks();

    int nodesRequired = HddsProtos.ReplicationFactor.THREE.getNumber();
    // Set the only node on rack1 stale. This makes the cluster effectively a
    // single rack.
    nodeManager.setNodeState(dns.get(0), HddsProtos.NodeState.STALE);

    // As there is only 1 rack alive, the 3 DNs on /rack2 should be returned
    List<DatanodeDetails> pickedDns =  placementPolicy.chooseDatanodes(
        new ArrayList<>(), new ArrayList<>(), nodesRequired, 0, 0);

    assertEquals(3, pickedDns.size());
    assertTrue(pickedDns.contains(dns.get(1)));
    assertTrue(pickedDns.contains(dns.get(2)));
    assertTrue(pickedDns.contains(dns.get(3)));
  }

  @Rule
  public ExpectedException thrownExp = ExpectedException.none();

  @Test
  public void testExceptionIsThrownWhenRackAwarePipelineCanNotBeCreated()
      throws Exception {
    thrownExp.expect(SCMException.class);
    thrownExp.expectMessage(PipelinePlacementPolicy.MULTIPLE_RACK_PIPELINE_MSG);

    List<DatanodeDetails> dns = setupSkewedRacks();

    // Set the first node to its pipeline limit. This means there are only
    // 3 hosts on a single rack available for new pipelines
    insertHeavyNodesIntoNodeManager(dns, 1);
    int nodesRequired = HddsProtos.ReplicationFactor.THREE.getNumber();

    placementPolicy.chooseDatanodes(
        new ArrayList<>(), new ArrayList<>(), nodesRequired, 0, 0);
  }

  @Test
  public void testExceptionThrownRackAwarePipelineCanNotBeCreatedExcludedNode()
      throws Exception {
    thrownExp.expect(SCMException.class);
    thrownExp.expectMessage(PipelinePlacementPolicy.MULTIPLE_RACK_PIPELINE_MSG);

    List<DatanodeDetails> dns = setupSkewedRacks();

    // Set the first node to its pipeline limit. This means there are only
    // 3 hosts on a single rack available for new pipelines
    insertHeavyNodesIntoNodeManager(dns, 1);
    int nodesRequired = HddsProtos.ReplicationFactor.THREE.getNumber();

    List<DatanodeDetails> excluded = new ArrayList<>();
    excluded.add(dns.get(0));
    placementPolicy.chooseDatanodes(
        excluded, new ArrayList<>(), nodesRequired, 0, 0);
  }

  private List<DatanodeDetails> setupSkewedRacks() {
    cluster = initTopology();

    List<DatanodeDetails> dns = new ArrayList<>();
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host1", "/rack1"));
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host2", "/rack2"));
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host3", "/rack2"));
    dns.add(MockDatanodeDetails
        .createDatanodeDetails("host4", "/rack2"));

    nodeManager = new MockNodeManager(cluster, dns,
        false, PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    placementPolicy = new PipelinePlacementPolicy(
        nodeManager, stateManager, conf);
    return dns;
  }

  private boolean checkDuplicateNodesUUID(List<DatanodeDetails> nodes) {
    HashSet<UUID> uuids = nodes.stream().
        map(DatanodeDetails::getUuid).
        collect(Collectors.toCollection(HashSet::new));
    return uuids.size() == nodes.size();
  }

  private Set<PipelineID> mockPipelineIDs(int count) {
    Set<PipelineID> pipelineIDs = new HashSet<>(count);
    for (int i = 0; i < count; i++) {
      pipelineIDs.add(PipelineID.randomId());
    }
    return pipelineIDs;
  }

  private void insertHeavyNodesIntoNodeManager(
      List<DatanodeDetails> nodes, int heavyNodeCount) throws IOException {
    if (nodes == null) {
      throw new SCMException("",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    int considerHeavyCount =
        conf.getInt(
            ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
            ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT) + 1;

    for (DatanodeDetails node : nodes) {
      if (heavyNodeCount > 0) {
        int pipelineCount = 0;
        List<DatanodeDetails> dnList = new ArrayList<>();
        dnList.add(node);
        dnList.add(MockDatanodeDetails.randomDatanodeDetails());
        dnList.add(MockDatanodeDetails.randomDatanodeDetails());
        Pipeline pipeline;
        HddsProtos.Pipeline pipelineProto;

        while (pipelineCount < considerHeavyCount) {
          pipeline = Pipeline.newBuilder()
              .setId(PipelineID.randomId())
              .setState(Pipeline.PipelineState.OPEN)
              .setReplicationConfig(ReplicationConfig
                  .fromProtoTypeAndFactor(RATIS, THREE))
              .setNodes(dnList)
              .build();

          pipelineProto = pipeline.getProtobufMessage(
              ClientVersions.CURRENT_VERSION);
          nodeManager.addPipeline(pipeline);
          stateManager.addPipeline(pipelineProto);
          pipelineCount++;
        }
        heavyNodeCount--;
      }
    }
  }

  @Test
  public void testCurrentRatisThreePipelineCount()
      throws IOException {
    List<DatanodeDetails> healthyNodes = nodeManager
        .getNodes(NodeStatus.inServiceHealthy());
    int pipelineCount;

    // Check datanode with one STANDALONE/ONE pipeline
    List<DatanodeDetails> standaloneOneDn = new ArrayList<>();
    standaloneOneDn.add(healthyNodes.get(0));
    createPipelineWithReplicationConfig(standaloneOneDn, STAND_ALONE, ONE);

    pipelineCount
        = placementPolicy.currentRatisThreePipelineCount(healthyNodes.get(0));
    assertEquals(pipelineCount, 0);

    // Check datanode with one RATIS/ONE pipeline
    List<DatanodeDetails> ratisOneDn = new ArrayList<>();
    ratisOneDn.add(healthyNodes.get(1));
    createPipelineWithReplicationConfig(ratisOneDn, RATIS, ONE);

    pipelineCount
        = placementPolicy.currentRatisThreePipelineCount(healthyNodes.get(1));
    assertEquals(pipelineCount, 0);

    // Check datanode with one RATIS/THREE pipeline
    List<DatanodeDetails> ratisThreeDn = new ArrayList<>();
    ratisThreeDn.add(healthyNodes.get(2));
    ratisThreeDn.add(healthyNodes.get(3));
    ratisThreeDn.add(healthyNodes.get(4));
    createPipelineWithReplicationConfig(ratisThreeDn, RATIS, THREE);

    pipelineCount
        = placementPolicy.currentRatisThreePipelineCount(healthyNodes.get(2));
    assertEquals(pipelineCount, 1);

    // Check datanode with one RATIS/ONE and one STANDALONE/ONE pipeline
    standaloneOneDn = new ArrayList<>();
    standaloneOneDn.add(healthyNodes.get(1));
    createPipelineWithReplicationConfig(standaloneOneDn, STAND_ALONE, ONE);

    pipelineCount
        = placementPolicy.currentRatisThreePipelineCount(healthyNodes.get(1));
    assertEquals(pipelineCount, 0);

    // Check datanode with one RATIS/ONE and one STANDALONE/ONE pipeline and
    // two RATIS/THREE pipelines
    ratisThreeDn = new ArrayList<>();
    ratisThreeDn.add(healthyNodes.get(1));
    ratisThreeDn.add(healthyNodes.get(3));
    ratisThreeDn.add(healthyNodes.get(4));
    createPipelineWithReplicationConfig(ratisThreeDn, RATIS, THREE);
    createPipelineWithReplicationConfig(ratisThreeDn, RATIS, THREE);

    pipelineCount
        = placementPolicy.currentRatisThreePipelineCount(healthyNodes.get(1));
    assertEquals(pipelineCount, 2);
  }

  private void createPipelineWithReplicationConfig(List<DatanodeDetails> dnList,
                                                   HddsProtos.ReplicationType
                                                       replicationType,
                                                   ReplicationFactor
                                                       replicationFactor)
      throws IOException {
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(Pipeline.PipelineState.OPEN)
        .setReplicationConfig(ReplicationConfig
            .fromProtoTypeAndFactor(replicationType, replicationFactor))
        .setNodes(dnList)
        .build();

    HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(
        ClientVersions.CURRENT_VERSION);
    nodeManager.addPipeline(pipeline);
    stateManager.addPipeline(pipelineProto);
  }
}
