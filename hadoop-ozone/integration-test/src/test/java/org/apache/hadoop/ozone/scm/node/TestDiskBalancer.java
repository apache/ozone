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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests disk balancer operations.
 * TODO: HDDS-13598 - Rewrite tests for direct client-to-DN DiskBalancer communication
 */
@Unhealthy("Tests need to be rewritten for direct client-to-DN communication")
@Timeout(300)
public class TestDiskBalancer {

  private static ScmClient storageClient;
  private static MiniOzoneCluster cluster;

  @BeforeAll
  public static void setup() throws Exception {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    ozoneConf.setBoolean(HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY, true);
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    ozoneConf.setTimeDuration("hdds.datanode.disk.balancer.service.interval", 3, TimeUnit.SECONDS);
    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(3).build();
    storageClient = new ContainerOperationClient(ozoneConf);
    cluster.waitForClusterToBeReady();

    for (DatanodeDetails dn: cluster.getStorageContainerManager()
        .getScmNodeManager().getAllNodes()) {
      ((DatanodeInfo) dn).updateStorageReports(
          HddsTestUtils.getRandomNodeReport(3, 1).getStorageReportList());
    }
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDatanodeDiskBalancerReport() throws IOException {
    // TODO: HDDS-13598 - Test that verifies:
    //  1. Query DiskBalancer report from multiple DNs using direct client-to-DN RPC
    //  2. Verify report contains volume density information for each DN
    //  3. Verify reports are sorted by volume density (highest imbalance first)
    //  Use direct client-to-DN communication to get reports from specific DNs
  }

  @Test
  public void testDiskBalancerStopAfterEven() throws IOException,
      InterruptedException, TimeoutException {
    // TODO: HDDS-13598 - Test that verifies:
    //  1. Start DiskBalancer on a DN using direct client-to-DN RPC
    //  2. Update DiskBalancer to set stopAfterDiskEven=true using direct client-to-DN RPC
    //  3. Wait for some time and query status to verify the RUNNING status changes to STOPPED
  }

  @Test
  public void testDatanodeDiskBalancerStatus() throws IOException, InterruptedException, TimeoutException {
    // TODO: HDDS-13598 - Test that verifies:
    //  1. Start DiskBalancer on all DNs using direct client-to-DN RPC
    //  2. Query status from all DNs and verify all show RUNNING status
    //  3. Decommission one DN and verify its status query returns appropriate state
    //  4. Query status from remaining IN_SERVICE DNs and verify they still show RUNNING
    //  5. Recommission the DN and verify its status can be queried again
    //  Use direct client-to-DN communication for start/status operations on specific DNs
  }
}
