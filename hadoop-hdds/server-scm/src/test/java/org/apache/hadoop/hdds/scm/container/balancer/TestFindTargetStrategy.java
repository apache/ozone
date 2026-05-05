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

package org.apache.hadoop.hdds.scm.container.balancer;

import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.NODEGROUP_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.junit.jupiter.api.Test;

/**
 * Tests for all the implementations of FindTargetStrategy.
 */
public class TestFindTargetStrategy {
  /**
   * Checks whether FindTargetGreedyByUsage always choose target
   * for a given source by Usage.
   */
  @Test
  public void testFindTargetGreedyByUsage() {
    FindTargetGreedyByUsageInfo findTargetStrategyByUsageInfo =
        new FindTargetGreedyByUsageInfo(null, null, null);
    List<DatanodeUsageInfo> overUtilizedDatanodes = new ArrayList<>();

    //create three datanodes with different usageinfo
    DatanodeUsageInfo dui1 = new DatanodeUsageInfo(MockDatanodeDetails
        .randomDatanodeDetails(), new SCMNodeStat(100, 0, 40, 0, 30, 0));
    DatanodeUsageInfo dui2 = new DatanodeUsageInfo(MockDatanodeDetails
        .randomDatanodeDetails(), new SCMNodeStat(100, 0, 60, 0, 30, 0));
    DatanodeUsageInfo dui3 = new DatanodeUsageInfo(MockDatanodeDetails
        .randomDatanodeDetails(), new SCMNodeStat(100, 0, 80, 0, 30, 0));

    //insert in ascending order
    overUtilizedDatanodes.add(dui1);
    overUtilizedDatanodes.add(dui2);
    overUtilizedDatanodes.add(dui3);
    findTargetStrategyByUsageInfo.reInitialize(
        overUtilizedDatanodes, null, null);

    //no need to set the datanode usage for source.
    findTargetStrategyByUsageInfo.sortTargetForSource(
        MockDatanodeDetails.randomDatanodeDetails());

    Collection<DatanodeUsageInfo> potentialTargets =
        findTargetStrategyByUsageInfo.getPotentialTargets();

    Object[] sortedPotentialTargetArray = potentialTargets.toArray();

    assertEquals(3, sortedPotentialTargetArray.length);

    //make sure after sorting target for source, the potentialTargets is
    //sorted in descending order of usage
    assertEquals(((DatanodeUsageInfo)sortedPotentialTargetArray[0]).getDatanodeDetails(), dui3.getDatanodeDetails());
    assertEquals(((DatanodeUsageInfo)sortedPotentialTargetArray[1]).getDatanodeDetails(), dui2.getDatanodeDetails());
    assertEquals(((DatanodeUsageInfo)sortedPotentialTargetArray[2]).getDatanodeDetails(), dui1.getDatanodeDetails());

  }

  /**
   * Tests {@link FindTargetStrategy#resetPotentialTargets(Collection)}.
   */
  @Test
  public void testResetPotentialTargets() {
    // create three datanodes with different usage infos
    DatanodeUsageInfo dui1 = new DatanodeUsageInfo(MockDatanodeDetails
        .randomDatanodeDetails(), new SCMNodeStat(100, 30, 70, 0, 50, 0));
    DatanodeUsageInfo dui2 = new DatanodeUsageInfo(MockDatanodeDetails
        .randomDatanodeDetails(), new SCMNodeStat(100, 20, 80, 0, 60, 0));
    DatanodeUsageInfo dui3 = new DatanodeUsageInfo(MockDatanodeDetails
        .randomDatanodeDetails(), new SCMNodeStat(100, 10, 90, 0, 70, 0));

    List<DatanodeUsageInfo> potentialTargets = new ArrayList<>();
    potentialTargets.add(dui1);
    potentialTargets.add(dui2);
    potentialTargets.add(dui3);
    MockNodeManager mockNodeManager = new MockNodeManager(potentialTargets);

    FindTargetGreedyByUsageInfo findTargetGreedyByUsageInfo =
        new FindTargetGreedyByUsageInfo(null, null, mockNodeManager);
    findTargetGreedyByUsageInfo.reInitialize(potentialTargets, null, null);

    // now, reset potential targets to only the first datanode
    List<DatanodeDetails> newPotentialTargets = new ArrayList<>(1);
    newPotentialTargets.add(dui1.getDatanodeDetails());
    findTargetGreedyByUsageInfo.resetPotentialTargets(newPotentialTargets);
    assertEquals(1, findTargetGreedyByUsageInfo.getPotentialTargets().size());
    assertEquals(dui1, findTargetGreedyByUsageInfo.getPotentialTargets().iterator().next());
  }

  /**
   * Checks whether FindTargetGreedyByNetworkTopology always choose target
   * for a given source by network topology distance.
   */
  @Test
  public void testFindTargetGreedyByNetworkTopology() {
    // network topology with default cost
    List<NodeSchema> schemas = new ArrayList<>();
    schemas.add(ROOT_SCHEMA);
    schemas.add(RACK_SCHEMA);
    schemas.add(NODEGROUP_SCHEMA);
    schemas.add(LEAF_SCHEMA);

    NodeSchemaManager manager = NodeSchemaManager.getInstance();
    manager.init(schemas.toArray(new NodeSchema[0]), true);
    NetworkTopology newCluster =
        new NetworkTopologyImpl(manager);

    DatanodeDetails source =
        MockDatanodeDetails.createDatanodeDetails("1.1.1.1", "/r1/ng1");
    //create one target in the same rack and same node group
    DatanodeDetails target1 =
        MockDatanodeDetails.createDatanodeDetails("2.2.2.2", "/r1/ng1");
    //create tree targets in the same rack but different node group
    DatanodeDetails target2 =
        MockDatanodeDetails.createDatanodeDetails("3.3.3.3", "/r1/ng2");
    DatanodeDetails target3 =
        MockDatanodeDetails.createDatanodeDetails("4.4.4.4", "/r1/ng2");
    DatanodeDetails target4 =
        MockDatanodeDetails.createDatanodeDetails("5.5.5.5", "/r1/ng2");
    //create one target in different rack
    DatanodeDetails target5 =
        MockDatanodeDetails.createDatanodeDetails("6.6.6.6", "/r2/ng1");

    //add all datanode to cluster map
    newCluster.add(source);
    newCluster.add(target1);
    newCluster.add(target2);
    newCluster.add(target3);
    newCluster.add(target4);
    newCluster.add(target5);

    //make sure targets have different network topology distance to source
    assertEquals(2, newCluster.getDistanceCost(source, target1));
    assertEquals(4, newCluster.getDistanceCost(source, target2));
    assertEquals(4, newCluster.getDistanceCost(source, target3));
    assertEquals(4, newCluster.getDistanceCost(source, target4));
    assertEquals(6, newCluster.getDistanceCost(source, target5));



    //insert in ascending order of network topology distance
    List<DatanodeUsageInfo> overUtilizedDatanodes = new ArrayList<>();
    //set the farthest target with the lowest usage info
    overUtilizedDatanodes.add(
        new DatanodeUsageInfo(target5, new SCMNodeStat(100, 0, 90, 0, 80, 0)));
    //set the tree targets, which have the same network topology distance
    //to source , with different usage info
    overUtilizedDatanodes.add(
        new DatanodeUsageInfo(target2, new SCMNodeStat(100, 0, 20, 0, 10, 0)));
    overUtilizedDatanodes.add(
        new DatanodeUsageInfo(target3, new SCMNodeStat(100, 0, 40, 0, 30, 0)));
    overUtilizedDatanodes.add(
        new DatanodeUsageInfo(target4, new SCMNodeStat(100, 0, 60, 0, 50, 0)));
    //set the nearest target with the highest usage info
    overUtilizedDatanodes.add(
        new DatanodeUsageInfo(target1, new SCMNodeStat(100, 0, 10, 0, 5, 0)));


    FindTargetGreedyByNetworkTopology findTargetGreedyByNetworkTopology =
        new FindTargetGreedyByNetworkTopology(
            null, null, null, newCluster);

    findTargetGreedyByNetworkTopology.reInitialize(
        overUtilizedDatanodes, null, null);

    findTargetGreedyByNetworkTopology.sortTargetForSource(source);

    Collection<DatanodeUsageInfo> potentialTargets =
        findTargetGreedyByNetworkTopology.getPotentialTargets();

    Object[] sortedPotentialTargetArray = potentialTargets.toArray();
    assertEquals(5, sortedPotentialTargetArray.length);

    // although target1 has the highest usage, it has the nearest network
    // topology distance to source, so it should be at the head of the
    // sorted PotentialTargetArray
    assertEquals(((DatanodeUsageInfo)sortedPotentialTargetArray[0]).getDatanodeDetails(), target1);

    // these targets have same network topology distance to source,
    // so they should be sorted by usage
    assertEquals(((DatanodeUsageInfo)sortedPotentialTargetArray[1]).getDatanodeDetails(), target4);
    assertEquals(((DatanodeUsageInfo)sortedPotentialTargetArray[2]).getDatanodeDetails(), target3);
    assertEquals(((DatanodeUsageInfo)sortedPotentialTargetArray[3]).getDatanodeDetails(), target2);

    //target5 has the lowest usage , but it has the farthest distance to source
    //so it should be at the tail of the sorted PotentialTargetArray
    assertEquals(((DatanodeUsageInfo)sortedPotentialTargetArray[4]).getDatanodeDetails(), target5);
  }
}
