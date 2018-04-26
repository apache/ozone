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
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.client.ContainerOperationClient;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.INVALID;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.Assert.assertEquals;

/**
 * Test Query Node Operation.
 */
public class TestQueryNode {
  private static int numOfDatanodes = 5;
  private MiniOzoneClassicCluster cluster;

  private ContainerOperationClient scmClient;

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final int interval = 100;

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        interval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);

    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
        .build();
    cluster.waitOzoneReady();
    scmClient = new ContainerOperationClient(cluster
        .createStorageContainerLocationClient(),
        new XceiverClientManager(conf));
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testHealthyNodesCount() throws Exception {
    HddsProtos.NodePool pool = scmClient.queryNode(
        EnumSet.of(HEALTHY),
        HddsProtos.QueryScope.CLUSTER, "");
    assertEquals("Expected  live nodes", numOfDatanodes,
        pool.getNodesCount());
  }

  @Test(timeout = 10 * 1000L)
  public void testStaleNodesCount() throws Exception {
    cluster.shutdownDataNode(0);
    cluster.shutdownDataNode(1);

    GenericTestUtils.waitFor(() ->
            cluster.getStorageContainerManager().getNodeCount(STALE) == 2,
        100, 4 * 1000);

    int nodeCount = scmClient.queryNode(EnumSet.of(STALE),
        HddsProtos.QueryScope.CLUSTER, "").getNodesCount();
    assertEquals("Mismatch of expected nodes count", 2, nodeCount);

    GenericTestUtils.waitFor(() ->
            cluster.getStorageContainerManager().getNodeCount(DEAD) == 2,
        100, 4 * 1000);

    // Assert that we don't find any stale nodes.
    nodeCount = scmClient.queryNode(EnumSet.of(STALE),
        HddsProtos.QueryScope.CLUSTER, "").getNodesCount();
    assertEquals("Mismatch of expected nodes count", 0, nodeCount);

    // Assert that we find the expected number of dead nodes.
    nodeCount = scmClient.queryNode(EnumSet.of(DEAD),
        HddsProtos.QueryScope.CLUSTER, "").getNodesCount();
    assertEquals("Mismatch of expected nodes count", 2, nodeCount);
  }
}
