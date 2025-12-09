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

package org.apache.hadoop.ozone.om.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration test for 'ozone admin om snapshot defrag' command.
 * Tests that the defrag command can be successfully triggered on any OM
 * (leader or follower) in an HA cluster.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSnapshotDefragAdmin {

  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient client;
  private static String omServiceId;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    // Enable snapshot defrag service
    conf.setInt(OMConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL, 7200);
    conf.setInt(OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK, 1);

    omServiceId = "om-service-test-defrag";
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(3)
        .build();

    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  @AfterAll
  public static void cleanup() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests triggering snapshot defrag on the OM leader.
   */
  @Test
  public void testDefragOnLeader() throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    String leaderId = leader.getOMNodeId();

    executeDefragCommand(leaderId, false);
  }

  /**
   * Tests triggering snapshot defrag on an OM follower.
   */
  @Test
  public void testDefragOnFollower() throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    List<OzoneManager> allOMs = cluster.getOzoneManagersList();

    // Find a follower OM
    OzoneManager follower = null;
    for (OzoneManager om : allOMs) {
      if (!om.getOMNodeId().equals(leader.getOMNodeId())) {
        follower = om;
        break;
      }
    }

    assertNotNull(follower, "Should have at least one follower OM");
    executeDefragCommand(follower.getOMNodeId(), false);
  }

  /**
   * Tests triggering snapshot defrag on all OMs in the cluster.
   */
  @Test
  public void testDefragOnAllOMs() throws Exception {
    List<OzoneManager> allOMs = cluster.getOzoneManagersList();

    assertEquals(3, allOMs.size(), "Expected 3 OMs in the cluster");

    // Test defrag on each OM
    for (OzoneManager om : allOMs) {
      String omNodeId = om.getOMNodeId();
      executeDefragCommand(omNodeId, false);
    }
  }

  /**
   * Tests triggering snapshot defrag with --no-wait option.
   */
  @Test
  public void testDefragWithNoWait() throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    String leaderId = leader.getOMNodeId();

    executeDefragCommand(leaderId, true);
  }

  /**
   * Tests triggering snapshot defrag on a follower with --no-wait option.
   */
  @Test
  public void testDefragOnFollowerWithNoWait() throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    List<OzoneManager> allOMs = cluster.getOzoneManagersList();

    // Find a follower OM
    OzoneManager follower = null;
    for (OzoneManager om : allOMs) {
      if (!om.getOMNodeId().equals(leader.getOMNodeId())) {
        follower = om;
        break;
      }
    }

    assertNotNull(follower, "Should have at least one follower OM");
    executeDefragCommand(follower.getOMNodeId(), true);
  }

  /**
   * Helper method to execute the defrag command on a specific OM node.
   *
   * @param nodeId the OM node ID to target
   * @param noWait whether to use the --no-wait option
   */
  private void executeDefragCommand(String nodeId, boolean noWait) throws Exception {
    OzoneAdmin ozoneAdmin = new OzoneAdmin();
    ozoneAdmin.getOzoneConf().addResource(cluster.getConf());

    // Capture output to verify command execution
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8.name());
    PrintStream oldOut = System.out;
    System.setOut(ps);

    try {
      String[] args;
      if (noWait) {
        args = new String[]{
            "om",
            "snapshot",
            "defrag",
            "--service-id", omServiceId,
            "--node-id", nodeId,
            "--no-wait"
        };
      } else {
        args = new String[]{
            "om",
            "snapshot",
            "defrag",
            "--service-id", omServiceId,
            "--node-id", nodeId
        };
      }

      int exitCode = ozoneAdmin.execute(args);
      System.out.flush();
      String output = baos.toString(StandardCharsets.UTF_8.name());

      // Verify successful execution
      assertEquals(0, exitCode,
          "Command should execute successfully on OM " + nodeId);
      assertTrue(output.contains("Triggering Snapshot Defrag Service"),
          "Output should indicate defrag service is being triggered");

      if (noWait) {
        assertTrue(output.contains("triggered successfully") &&
                output.contains("background"),
            "Output should indicate task triggered in background: " + output);
      } else {
        assertTrue(output.contains("completed successfully") ||
                output.contains("failed") ||
                output.contains("interrupted"),
            "Output should indicate completion status: " + output);
      }
    } finally {
      System.setOut(oldOut);
      ps.close();
    }
  }
}

