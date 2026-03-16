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

package org.apache.hadoop.ozone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that MiniOzoneCluster correctly supports datanode topology
 * configuration via the {@link MiniOzoneCluster.Builder#setTopology} API.
 */
@Timeout(value = 300)
class TestMiniOzoneClusterTopology {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMiniOzoneClusterTopology.class);

  /**
   * Verify that datanodes are assigned to the correct racks when
   * topology is configured via {@code setTopology()}.
   * Uses 6 datanodes split across 2 racks (3 per rack).
   */
  @Test
  void testDatanodesHaveCorrectRackAssignment() throws Exception {
    int numDatanodes = 6;
    String[] racks = {
        "/rack0", "/rack0", "/rack0",
        "/rack1", "/rack1", "/rack1"
    };

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL,
        100, TimeUnit.MILLISECONDS);

    try (MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numDatanodes)
        .setTopology(racks)
        .build()) {
      cluster.waitForClusterToBeReady();

      NodeManager nodeManager =
          cluster.getStorageContainerManager().getScmNodeManager();
      List<DatanodeDetails> healthyNodes =
          nodeManager.getNodes(NodeStatus.inServiceHealthy());
      assertEquals(numDatanodes, healthyNodes.size(),
          "All datanodes should be healthy");

      // Verify each datanode has a non-default network location
      Set<String> seenRacks = new HashSet<>();
      for (DatanodeDetails dn : healthyNodes) {
        String location = dn.getNetworkLocation();
        assertNotNull(location, "Network location should not be null for "
            + dn.getHostName());
        LOG.info("Datanode {} (hostname={}) -> rack={}",
            dn.getUuidString(), dn.getHostName(), location);
        seenRacks.add(location);
      }

      // Verify both racks are represented
      assertTrue(seenRacks.contains("/rack0"),
          "Should have datanodes in /rack0");
      assertTrue(seenRacks.contains("/rack1"),
          "Should have datanodes in /rack1");
      assertEquals(2, seenRacks.size(),
          "Should have exactly 2 distinct racks");

      // Count datanodes per rack
      long rack0Count = healthyNodes.stream()
          .filter(dn -> "/rack0".equals(dn.getNetworkLocation()))
          .count();
      long rack1Count = healthyNodes.stream()
          .filter(dn -> "/rack1".equals(dn.getNetworkLocation()))
          .count();
      assertEquals(3, rack0Count, "Should have 3 datanodes in /rack0");
      assertEquals(3, rack1Count, "Should have 3 datanodes in /rack1");
    }
  }

  /**
   * Verify topology works with 3 datanodes across 2 racks
   * (minimum viable configuration for Ratis pipeline formation).
   */
  @Test
  void testThreeNodesAcrossTwoRacks() throws Exception {
    String[] racks = {"/rack-a", "/rack-a", "/rack-b"};

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL,
        100, TimeUnit.MILLISECONDS);

    try (MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setTopology(racks)
        .build()) {
      cluster.waitForClusterToBeReady();

      NodeManager nodeManager =
          cluster.getStorageContainerManager().getScmNodeManager();
      List<DatanodeDetails> healthyNodes =
          nodeManager.getNodes(NodeStatus.inServiceHealthy());
      assertEquals(3, healthyNodes.size());

      Set<String> locations = new HashSet<>();
      for (DatanodeDetails dn : healthyNodes) {
        locations.add(dn.getNetworkLocation());
        LOG.info("Datanode {} -> {}", dn.getHostName(),
            dn.getNetworkLocation());
      }

      assertTrue(locations.contains("/rack-a"),
          "Should have datanodes in /rack-a");
      assertTrue(locations.contains("/rack-b"),
          "Should have a datanode in /rack-b");
      assertEquals(2, locations.size(),
          "Should have exactly 2 distinct rack locations");
    }
  }
}
