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

package org.apache.hadoop.ozone.scm.node;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests disk balancer operations during
 * decommission and maintenance of DNs.
 * TODO: HDDS-13598 - Rewrite tests for direct client-to-DN DiskBalancer communication
 */
@Unhealthy("Tests need to be rewritten for direct client-to-DN communication")
@Timeout(300)
public class TestDiskBalancerDuringDecommissionAndMaintenance {

  private static MiniOzoneCluster cluster;
  private static ScmClient scmClient;

  @BeforeAll
  public static void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY, true);
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    conf.setTimeDuration("hdds.datanode.disk.balancer.service.interval", 2, TimeUnit.SECONDS);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();

    scmClient = new ContainerOperationClient(conf);

    for (DatanodeDetails dn : cluster.getStorageContainerManager()
        .getScmNodeManager().getAllNodes()) {
      ((DatanodeInfo) dn).updateStorageReports(
          HddsTestUtils.getRandomNodeReport(20, 1).getStorageReportList());
    }
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @AfterEach
  public void stopDiskBalancer() throws IOException, InterruptedException, TimeoutException {
    // TODO: HDDS-13598 - Implement stopDiskBalancer via direct client-to-DN communication
  }

  @Test
  public void testDiskBalancerWithDecommissionAndMaintenanceNodes()
      throws IOException, InterruptedException, TimeoutException {
    // TODO: HDDS-13598 - Test that verifies:
    //  1. DiskBalancer report and status exclude DNs in DECOMMISSIONING state
    //  2. DiskBalancer report and status exclude DNs in ENTERING_MAINTENANCE state
    //  3. DiskBalancerService automatically stops when DN enters DECOMMISSIONING state
    //  4. DiskBalancerService automatically stops when DN enters ENTERING_MAINTENANCE state
    //  5. DiskBalancer report and status include recommissioned DNs
    //  6. DiskBalancerService automatically resumes when DN returns to IN_SERVICE state
    //  Use direct client-to-DN communication for all DiskBalancer operations
  }

  @Test
  public void testStopDiskBalancerOnDecommissioningNode() throws Exception {
    // TODO: HDDS-13598 - Test that verifies:
    //  1. Start DiskBalancer on a specific DN using direct client-to-DN RPC
    //  2. Verify DiskBalancer status shows RUNNING via direct client-to-DN query
    //  3. Decommission the DN and verify DiskBalancerService automatically pauses
    //  4. Recommission the DN and verify DiskBalancerService automatically resumes
    //     (since it was running before decommission, not explicitly stopped)
    //  Use direct client-to-DN communication for start/status operations
  }

  @Test
  public void testStartDiskBalancerOnDecommissioningNode() throws Exception {
    // TODO: HDDS-13598 - Test that verifies:
    //  1. Verify DiskBalancer status shows STOPPED on a specific DN via direct client-to-DN query
    //  2. Decommission the DN (while DiskBalancer is stopped)
    //  3. Recommission the DN and verify DiskBalancer status remains STOPPED
    //     (since no explicit start command was sent, it should not auto-resume)
    //  This tests that only explicitly started DiskBalancers auto-resume after recommission
    //  Use direct client-to-DN communication for status operations
  }
}

