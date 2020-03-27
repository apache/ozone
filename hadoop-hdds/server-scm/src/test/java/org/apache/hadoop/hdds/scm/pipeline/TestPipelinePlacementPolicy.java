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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.*;
import org.apache.hadoop.hdds.scm.node.states.Node2PipelineMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;

/**
 * Test for PipelinePlacementPolicy.
 */
public class TestPipelinePlacementPolicy {
  private MockNodeManager nodeManager;
  private OzoneConfiguration conf;
  private PipelinePlacementPolicy placementPolicy;
  private NetworkTopologyImpl cluster;
  private static final int PIPELINE_PLACEMENT_MAX_NODES_COUNT = 10;

  private List<DatanodeDetails> nodesWithOutRackAwareness = new ArrayList<>();
  private List<DatanodeDetails> nodesWithRackAwareness = new ArrayList<>();

  @Before
  public void init() throws Exception {
    cluster = initTopology();
    // start with nodes with rack awareness.
    nodeManager = new MockNodeManager(cluster, getNodesWithRackAwareness(),
        false, PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    conf = new OzoneConfiguration();
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 5);
    placementPolicy = new PipelinePlacementPolicy(
        nodeManager, new PipelineStateManager(), conf);
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
    for (Node node : NODES) {
      DatanodeDetails datanode = overwriteLocationInNode(
          getNodesWithoutRackAwareness(), node);
      nodesWithRackAwareness.add(datanode);
      datanodes.add(datanode);
    }
    return datanodes;
  }

  private DatanodeDetails getNodesWithoutRackAwareness() {
    DatanodeDetails node = MockDatanodeDetails.randomDatanodeDetails();
    nodesWithOutRackAwareness.add(node);
    return node;
  }

  @Test
  public void testChooseNodeBasedOnNetworkTopology() throws SCMException {
    DatanodeDetails anchor = placementPolicy.chooseNode(nodesWithRackAwareness);
    // anchor should be removed from healthyNodes after being chosen.
    Assert.assertFalse(nodesWithRackAwareness.contains(anchor));

    List<DatanodeDetails> excludedNodes =
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT);
    excludedNodes.add(anchor);
    DatanodeDetails nextNode = placementPolicy.chooseNodeFromNetworkTopology(
        nodeManager.getClusterNetworkTopologyMap(), anchor, excludedNodes);
    Assert.assertFalse(excludedNodes.contains(nextNode));
    // next node should not be the same as anchor.
    Assert.assertTrue(anchor.getUuid() != nextNode.getUuid());
    // next node should be on the same rack based on topology.
    Assert.assertEquals(anchor.getNetworkLocation(),
        nextNode.getNetworkLocation());
  }

  @Test
  public void testChooseNodeWithSingleNodeRack() throws SCMException {
    // There is only one node on 3 racks altogether.
    List<DatanodeDetails> datanodes = new ArrayList<>();
    for (Node node : SINGLE_NODE_RACK) {
      DatanodeDetails datanode = overwriteLocationInNode(
          MockDatanodeDetails.randomDatanodeDetails(), node);
      datanodes.add(datanode);
    }
    MockNodeManager localNodeManager = new MockNodeManager(initTopology(),
        datanodes, false, datanodes.size());
    PipelinePlacementPolicy localPlacementPolicy = new PipelinePlacementPolicy(
        localNodeManager, new PipelineStateManager(), conf);
    int nodesRequired = HddsProtos.ReplicationFactor.THREE.getNumber();
    List<DatanodeDetails> results = localPlacementPolicy.chooseDatanodes(
        new ArrayList<>(datanodes.size()),
        new ArrayList<>(datanodes.size()),
        nodesRequired, 0);

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
  public void testChooseNodeBasedOnRackAwareness() {
    List<DatanodeDetails> healthyNodes = overWriteLocationInNodes(
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY));
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
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY));
    DatanodeDetails node;
    try {
      node = placementPolicy.fallBackPickNodes(healthyNodes, null);
      Assert.assertNotNull(node);
    } catch (SCMException e) {
      Assert.fail("Should not reach here.");
    }

    // when input nodeSet are all excluded.
    List<DatanodeDetails> exclude = healthyNodes;
    try {
      node = placementPolicy.fallBackPickNodes(healthyNodes, exclude);
      Assert.assertNull(node);
    } catch (SCMException e) {
      Assert.assertEquals(SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE,
          e.getResult());
    } catch (Exception ex) {
      Assert.fail("Should not reach here.");
    }
  }

  @Test
  public void testRackAwarenessNotEnabledWithFallBack() throws SCMException{
    DatanodeDetails anchor = placementPolicy
        .chooseNode(nodesWithOutRackAwareness);
    DatanodeDetails randomNode = placementPolicy
        .chooseNode(nodesWithOutRackAwareness);
    // rack awareness is not enabled.
    Assert.assertTrue(anchor.getNetworkLocation().equals(
        randomNode.getNetworkLocation()));

    NetworkTopology topology = new NetworkTopologyImpl(new Configuration());
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

  private final static Node[] NODES = new NodeImpl[] {
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
  private final static Node[] SINGLE_NODE_RACK = new NodeImpl[] {
      new NodeImpl("h1", "/r1", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h2", "/r2", NetConstants.NODE_COST_DEFAULT),
      new NodeImpl("h3", "/r3", NetConstants.NODE_COST_DEFAULT)
  };

  private NetworkTopology createNetworkTopologyOnDifRacks() {
    NetworkTopology topology = new NetworkTopologyImpl(new Configuration());
    for (Node n : NODES) {
      topology.add(n);
    }
    return topology;
  }

  private DatanodeDetails overwriteLocationInNode(
      DatanodeDetails datanode, Node node) {
    DatanodeDetails result = DatanodeDetails.newBuilder()
        .setUuid(datanode.getUuidString())
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
  public void testHeavyNodeShouldBeExcluded() throws SCMException{
    List<DatanodeDetails> healthyNodes =
        nodeManager.getNodes(HddsProtos.NodeState.HEALTHY);
    int nodesRequired = HddsProtos.ReplicationFactor.THREE.getNumber();
    // only minority of healthy NODES are heavily engaged in pipelines.
    int minorityHeavy = healthyNodes.size()/2 - 1;
    List<DatanodeDetails> pickedNodes1 = placementPolicy.chooseDatanodes(
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
        nodesRequired, 0);
    // modify node to pipeline mapping.
    insertHeavyNodesIntoNodeManager(healthyNodes, minorityHeavy);
    // NODES should be sufficient.
    Assert.assertEquals(nodesRequired, pickedNodes1.size());
    // make sure pipeline placement policy won't select duplicated NODES.
    Assert.assertTrue(checkDuplicateNodesUUID(pickedNodes1));

    // majority of healthy NODES are heavily engaged in pipelines.
    int majorityHeavy = healthyNodes.size()/2 + 2;
    insertHeavyNodesIntoNodeManager(healthyNodes, majorityHeavy);
    boolean thrown = false;
    List<DatanodeDetails> pickedNodes2 = null;
    try {
      pickedNodes2 = placementPolicy.chooseDatanodes(
          new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
          new ArrayList<>(PIPELINE_PLACEMENT_MAX_NODES_COUNT),
          nodesRequired, 0);
    } catch (SCMException e) {
      Assert.assertFalse(thrown);
      thrown = true;
    }
    // NODES should NOT be sufficient and exception should be thrown.
    Assert.assertNull(pickedNodes2);
    Assert.assertTrue(thrown);
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
      List<DatanodeDetails> nodes, int heavyNodeCount) throws SCMException{
    if (nodes == null) {
      throw new SCMException("",
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }

    int considerHeavyCount =
        conf.getInt(
            ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
            ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT) + 1;

    Node2PipelineMap mockMap = new Node2PipelineMap();
    for (DatanodeDetails node : nodes) {
      // mock heavy node
      if (heavyNodeCount > 0) {
        mockMap.insertNewDatanode(
            node.getUuid(), mockPipelineIDs(considerHeavyCount));
        heavyNodeCount--;
      } else {
        mockMap.insertNewDatanode(node.getUuid(), mockPipelineIDs(1));
      }
    }
    nodeManager.setNode2PipelineMap(mockMap);
  }
}
