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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        new DummyPlacementPolicy(nodeManager, conf);
    List<DatanodeDetails> list =
        nodeManager.getNodes(NodeStatus.inServiceHealthy());
    List<DatanodeDetails> result = dummyPlacementPolicy.getResultSet(3, list);
    Set<DatanodeDetails> resultSet = new HashSet<>(result);
    Assertions.assertNotEquals(1, resultSet.size());
  }

  @Test
  public void testReplicasToCopy() {
    DummyPlacementPolicy dummyPlacementPolicy =
            new DummyPlacementPolicy(nodeManager, conf);
    List<DatanodeDetails> list =
            nodeManager.getNodes(NodeStatus.inServiceHealthy());
    List<ContainerReplica> replicas =
            IntStream.range(1, 5).mapToObj(i ->
                    ContainerReplica.newBuilder()
                            .setContainerID(new ContainerID(1))
                            .setContainerState(CLOSED)
                            .setReplicaIndex(i)
            .setDatanodeDetails(list.get(i - 1)).build())
                    .collect(Collectors.toList());
    replicas.add(ContainerReplica.newBuilder()
            .setContainerID(new ContainerID(1))
            .setContainerState(CLOSED)
            .setReplicaIndex(5)
            .setDatanodeDetails(list.get(5)).build());

    Set<ContainerReplica> replicasToCopy =
            dummyPlacementPolicy.replicasToCopy(Sets.newHashSet(replicas),
            1, 5);
    Assertions.assertTrue(replicasToCopy.stream().findFirst()
            .map(replica -> Arrays.asList(replicas.get(0), replicas.get(4))
                    .contains(replica)).orElse(false));
    Assertions.assertEquals(1, replicasToCopy.size());
  }

  private static class DummyPlacementPolicy extends
          SCMCommonPlacementPolicy {
    private Map<DatanodeDetails, Node> dns;

    DummyPlacementPolicy(
        NodeManager nodeManager,
        ConfigurationSource conf) {
      super(nodeManager, conf);
      dns = new HashMap<>();
      List<DatanodeDetails> datanodeDetails =
              nodeManager.getNodes(NodeStatus.inServiceHealthy());
      for (int idx = 0; idx < datanodeDetails.size(); idx++) {
        dns.put(datanodeDetails.get(idx), datanodeDetails.get(idx % 5));
      }
    }

    @Override
    public DatanodeDetails chooseNode(List<DatanodeDetails> healthyNodes) {
      return healthyNodes.get(0);
    }

    @Override
    public Node getPlacementGroup(DatanodeDetails dn) {
      return dns.get(dn);
    }

    @Override
    protected int getRequiredRackCount(int numReplicas) {
      return 5;
    }
  }
}
