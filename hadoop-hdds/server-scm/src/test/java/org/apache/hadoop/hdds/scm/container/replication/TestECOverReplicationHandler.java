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
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.replication.health.ECReplicationCheckHandler;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;

/**
 * Tests the ECOverReplicationHandling functionality.
 */
public class TestECOverReplicationHandler {
  private ECReplicationConfig repConfig;
  private ContainerInfo container;
  private NodeManager nodeManager;
  private OzoneConfiguration conf;
  private PlacementPolicy policy;
  private ECReplicationCheckHandler replicationCheck;

  @BeforeEach
  public void setup() {
    nodeManager = new MockNodeManager(true, 10) {
      @Override
      public NodeStatus getNodeStatus(DatanodeDetails dd)
          throws NodeNotFoundException {
        return NodeStatus.inServiceHealthy();
      }
    };
    conf = SCMTestUtils.getConf();
    repConfig = new ECReplicationConfig(3, 2);
    container = ReplicationTestUtil
        .createContainer(HddsProtos.LifeCycleState.CLOSED, repConfig);
    policy = ReplicationTestUtil
        .getSimpleTestPlacementPolicy(nodeManager, conf);
    NodeSchema[] schemas =
        new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    replicationCheck = new ECReplicationCheckHandler();
  }

  @Test
  public void testNoOverReplication() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));
    testOverReplicationWithIndexes(availableReplicas, Collections.emptyMap());
  }

  @Test
  public void testOverReplicationWithOneSameIndexes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 5));

    testOverReplicationWithIndexes(availableReplicas,
        //num of index 1 is 3, but it should be 1, so 2 excess
        new ImmutableMap.Builder<Integer, Integer>().put(1, 2).build());
  }

  @Test
  public void testOverReplicationWithMultiSameIndexes() {
    Set<ContainerReplica> availableReplicas = ReplicationTestUtil
        .createReplicas(Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 1), Pair.of(IN_SERVICE, 1),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 2),
            Pair.of(IN_SERVICE, 2), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 3), Pair.of(IN_SERVICE, 3),
            Pair.of(IN_SERVICE, 4), Pair.of(IN_SERVICE, 4),
            Pair.of(IN_SERVICE, 5), Pair.of(IN_SERVICE, 5));

    testOverReplicationWithIndexes(availableReplicas,
        //num of index 1 is 3, but it should be 1, so 2 excess
        new ImmutableMap.Builder<Integer, Integer>()
            .put(1, 2).put(2, 2).put(3, 2).put(4, 1)
            .put(5, 1).build());
  }

  private void testOverReplicationWithIndexes(
      Set<ContainerReplica> availableReplicas,
      Map<Integer, Integer> index2excessNum) {
    ECOverReplicationHandler ecORH =
        new ECOverReplicationHandler(replicationCheck, policy, nodeManager);
    ContainerHealthResult.OverReplicatedHealthResult result =
        Mockito.mock(ContainerHealthResult.OverReplicatedHealthResult.class);
    Mockito.when(result.getContainerInfo()).thenReturn(container);

    Map<DatanodeDetails, SCMCommand<?>> commands = ecORH
        .processAndCreateCommands(availableReplicas, ImmutableList.of(),
            result, 1);

    // total commands send out should be equal to the sum of all
    // the excess nums
    int totalDeleteCommandNum =
        index2excessNum.values().stream().reduce(0, Integer::sum);
    Assert.assertEquals(totalDeleteCommandNum, commands.size());

    // Each command should have a non-zero replica index
    commands.forEach((datanode, command) -> Assert.assertNotEquals(0,
        ((DeleteContainerCommand)command).getReplicaIndex()));

    // command num of each index should be equal to the excess num
    // of this index
    Map<DatanodeDetails, Integer> datanodeDetails2Index =
        availableReplicas.stream().collect(Collectors.toMap(
            ContainerReplica::getDatanodeDetails,
            ContainerReplica::getReplicaIndex));
    Map<Integer, Integer> index2commandNum = new HashMap<>();
    commands.keySet().forEach(dd ->
        index2commandNum.merge(datanodeDetails2Index.get(dd), 1, Integer::sum)
    );

    index2commandNum.keySet().forEach(i -> {
      Assert.assertTrue(index2excessNum.containsKey(i));
      Assert.assertEquals(index2commandNum.get(i), index2excessNum.get(i));
    });
  }
}
