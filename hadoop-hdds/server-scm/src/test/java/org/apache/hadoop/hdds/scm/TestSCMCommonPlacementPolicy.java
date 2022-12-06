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

import com.google.common.collect.Maps;
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
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
    List<DatanodeDetails> list =
        nodeManager.getNodes(NodeStatus.inServiceHealthy());
    List<DatanodeDetails> result = dummyPlacementPolicy.getResultSet(3, list);
    Set<DatanodeDetails> resultSet = new HashSet<>(result);
    Assertions.assertNotEquals(1, resultSet.size());
  }

  private void testReplicasToFixMisreplication(
          List<ContainerReplica> replicas,
          DummyPlacementPolicy placementPolicy,
          int expectedNumberOfReplicasToCopy,
          Map<Node, Integer> expectedNumberOfCopyOperationFromRack) {
    Set<ContainerReplica> replicasToCopy = placementPolicy
            .replicasToCopyToFixMisreplication(Sets.newHashSet(replicas));
    Assertions.assertEquals(expectedNumberOfReplicasToCopy,
            replicasToCopy.size());
    Map<Node, Long> rackCopyMap =
            replicasToCopy.stream().collect(Collectors.groupingBy(
                    replica -> placementPolicy
                            .getPlacementGroup(replica.getDatanodeDetails()),
            Collectors.counting()));
    Set<Node> racks = replicas.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .map(placementPolicy::getPlacementGroup)
            .collect(Collectors.toSet());
    for (Node rack: racks) {
      Assertions.assertEquals(
              expectedNumberOfCopyOperationFromRack.getOrDefault(rack, 0),
              rackCopyMap.getOrDefault(rack, 0L).intValue());
    }
  }

  @Test
  public void testReplicasToFixMisreplication() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 5);
    List<Node> racks = dummyPlacementPolicy.racks;
    List<DatanodeDetails> list = nodeManager.getAllNodes();
    List<DatanodeDetails> replicaDns =
            Stream.of(0, 1, 2, 3, 5)
                    .map(list::get).collect(Collectors.toList());
    List<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(new ContainerID(1),
                    CLOSED, 0, 0, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 1,
            ImmutableMap.of(racks.get(0), 1));
    //Changing Rack of Dn 1 to move to rack 0
    dummyPlacementPolicy.rackMap.put(list.get(1),
            dummyPlacementPolicy.getPlacementGroup(list.get(0)));
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 2,
            ImmutableMap.of(racks.get(0), 2));
    //Changing Rack of Dn 2 to move to rack 0
    dummyPlacementPolicy.rackMap.put(list.get(2),
            dummyPlacementPolicy.getPlacementGroup(list.get(0)));
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 3,
            ImmutableMap.of(racks.get(0), 3));
    //Changing Rack of Dn 4 to move to rack 3
    dummyPlacementPolicy.rackMap.put(list.get(4),
            dummyPlacementPolicy.getPlacementGroup(list.get(3)));
    replicaDns =
            Stream.of(0, 1, 2, 3, 4)
                    .map(list::get).collect(Collectors.toList());
    //Creating Replicas without replica Index
    replicas = HddsTestUtils.getReplicas(new ContainerID(1),
            CLOSED, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 3,
            ImmutableMap.of(racks.get(0), 2, racks.get(3), 1));
    //Creating Replicas without replica Index for replicas < number of racks
    replicaDns =
            Stream.of(0, 1, 3, 4)
                    .map(list::get).collect(Collectors.toList());
    replicas = HddsTestUtils.getReplicas(new ContainerID(1),
            CLOSED, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 2,
            ImmutableMap.of(racks.get(0), 1, racks.get(3), 1));

    //Creating Replicas without replica Index for replicas > number of racks
    replicaDns =
            Stream.of(0, 1, 2, 3, 4, 6)
                    .map(list::get).collect(Collectors.toList());

    replicas = HddsTestUtils.getReplicas(new ContainerID(1),
            CLOSED, 0, replicaDns);

    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 2,
            ImmutableMap.of(racks.get(0), 1, racks.get(3), 1));
    dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 2);
    racks = dummyPlacementPolicy.racks;
    replicaDns = Stream.of(0, 2, 4, 6, 8)
            .map(list::get).collect(Collectors.toList());
    replicas = HddsTestUtils.getReplicas(new ContainerID(1),
            CLOSED, 0, replicaDns);
    testReplicasToFixMisreplication(replicas, dummyPlacementPolicy, 2,
            ImmutableMap.of(racks.get(0), 2));

  }

  @Test
  public void testReplicasWithoutMisreplication() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf, 5);
    List<DatanodeDetails> list =
            nodeManager.getNodes(NodeStatus.inServiceHealthy());
    List<DatanodeDetails> replicaDns =
            Stream.of(0, 1, 2, 3, 4)
                    .map(list::get).collect(Collectors.toList());


    List<ContainerReplica> replicas =
            HddsTestUtils.getReplicasWithReplicaIndex(new ContainerID(1),
                    CLOSED, 0, 0, 0, replicaDns);

    Set<ContainerReplica> replicasToCopy = dummyPlacementPolicy
            .replicasToCopyToFixMisreplication(Sets.newHashSet(replicas));
    Assertions.assertEquals(0, replicasToCopy.size());
  }



  private static class DummyPlacementPolicy extends SCMCommonPlacementPolicy {
    private Map<DatanodeDetails, Node> rackMap;
    private List<Node> racks;
    private int rackCnt;

    DummyPlacementPolicy(
        NodeManager nodeManager,
        ConfigurationSource conf,
        int rackCnt) {
      super(nodeManager, conf);
      rackMap = new HashMap<>();
      List<DatanodeDetails> datanodeDetails = nodeManager.getAllNodes();
      this.rackCnt = Math.min(rackCnt, datanodeDetails.size());
      this.racks = new ArrayList<>(this.rackCnt);
      for (int r = 0; r < this.rackCnt; r++) {
        racks.add(Mockito.mock(Node.class));
      }
      for (int idx = 0; idx < datanodeDetails.size(); idx++) {
        rackMap.put(datanodeDetails.get(idx), racks.get(idx % this.rackCnt));
      }
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
