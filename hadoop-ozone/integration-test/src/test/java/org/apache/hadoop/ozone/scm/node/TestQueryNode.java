/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.node;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_NODE_REPORT_INTERVAL;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.
    NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.
    NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.
    NodeOperationalState.IN_MAINTENANCE;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.Assert.assertEquals;

/**
 * Test Query Node Operation.
 */
@Ignore
public class TestQueryNode {
  private static int numOfDatanodes = 5;
  private MiniOzoneCluster cluster;

  private ContainerOperationClient scmClient;

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final int interval = 100;

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        interval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 3);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .setTotalPipelineNumLimit(numOfDatanodes + numOfDatanodes/2)
        .build();
    cluster.waitForClusterToBeReady();
    scmClient = new ContainerOperationClient(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testHealthyNodesCount() throws Exception {
    List<HddsProtos.Node> nodes = scmClient.queryNode(null, HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "");
    assertEquals("Expected  live nodes", numOfDatanodes,
        nodes.size());
  }

  @Test(timeout = 10 * 1000L)
  public void testStaleNodesCount() throws Exception {
    cluster.shutdownHddsDatanode(0);
    cluster.shutdownHddsDatanode(1);

    GenericTestUtils.waitFor(() ->
            cluster.getStorageContainerManager().getNodeCount(STALE) == 2,
        100, 4 * 1000);

    int nodeCount = scmClient.queryNode(null, STALE,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals("Mismatch of expected nodes count", 2, nodeCount);

    GenericTestUtils.waitFor(() ->
            cluster.getStorageContainerManager().getNodeCount(DEAD) == 2,
        100, 4 * 1000);

    // Assert that we don't find any stale nodes.
    nodeCount = scmClient.queryNode(null, STALE,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals("Mismatch of expected nodes count", 0, nodeCount);

    // Assert that we find the expected number of dead nodes.
    nodeCount = scmClient.queryNode(null, DEAD,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals("Mismatch of expected nodes count", 2, nodeCount);
  }

  @Test
  public void testNodeOperationalStates() throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    NodeManager nm = scm.getScmNodeManager();

    // Set one node to be something other than IN_SERVICE
    DatanodeDetails node = nm.getAllNodes().get(0);
    nm.setNodeOperationalState(node, DECOMMISSIONING);

    // All nodes should be returned as they are all in service
    int nodeCount = scmClient.queryNode(IN_SERVICE, HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(numOfDatanodes - 1, nodeCount);

    // null acts as wildcard for opState
    nodeCount = scmClient.queryNode(null, HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(numOfDatanodes, nodeCount);

    // null acts as wildcard for nodeState
    nodeCount = scmClient.queryNode(IN_SERVICE, null,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(numOfDatanodes - 1, nodeCount);

    // Both null - should return all nodes
    nodeCount = scmClient.queryNode(null, null,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(numOfDatanodes, nodeCount);

    // No node should be returned
    nodeCount = scmClient.queryNode(IN_MAINTENANCE, HEALTHY,
        HddsProtos.QueryScope.CLUSTER, "").size();
    assertEquals(0, nodeCount);

    // Test all operational states by looping over them all and setting the
    // state manually.
    node = nm.getAllNodes().get(0);
    for (HddsProtos.NodeOperationalState s :
        HddsProtos.NodeOperationalState.values()) {
      nm.setNodeOperationalState(node, s);
      nodeCount = scmClient.queryNode(s, HEALTHY,
          HddsProtos.QueryScope.CLUSTER, "").size();
      if (s == IN_SERVICE) {
        assertEquals(5, nodeCount);
      } else {
        assertEquals(1, nodeCount);
      }
    }
  }
}
