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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.getDNHostAndPort;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachOpState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.DiskBalancerManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerService;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests disk balancer operations.
 */
@Timeout(300)
public class TestDiskBalancer {

  private static ScmClient storageClient;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;
  private static DiskBalancerManager diskBalancerManager;

  @BeforeAll
  public static void setup() throws Exception {
    ozoneConf = new OzoneConfiguration();
    ozoneConf.setBoolean(HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY, true);
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    ozoneConf.setTimeDuration("hdds.datanode.disk.balancer.service.interval", 3, TimeUnit.SECONDS);
    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(3).build();
    storageClient = new ContainerOperationClient(ozoneConf);
    cluster.waitForClusterToBeReady();
    diskBalancerManager = cluster.getStorageContainerManager()
        .getDiskBalancerManager();

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
    List<HddsProtos.DatanodeDiskBalancerInfoProto> reportProtoList =
        storageClient.getDiskBalancerReport(2);

    assertEquals(2, reportProtoList.size());
    assertTrue(
        reportProtoList.get(0).getCurrentVolumeDensitySum()
        >= reportProtoList.get(1).getCurrentVolumeDensitySum());
  }

  @Test
  public void testDiskBalancerStopAfterEven() throws IOException,
      InterruptedException, TimeoutException {
    //capture LOG for DiskBalancerManager and DiskBalancerService
    LogCapturer logCapturer = LogCapturer.captureLogs(DiskBalancerManager.LOG);
    LogCapturer dnLogCapturer = LogCapturer.captureLogs(DiskBalancerService.class);

    // Start DiskBalancer on all datanodes
    diskBalancerManager.startDiskBalancer(
        10.0, // threshold
        10L,  // bandwidth in MB
        5,    // parallel threads
        true, // stopAfterDiskEven
        null); // apply to all datanodes

    // verify logs for all DNs has started
    String logs = logCapturer.getOutput();
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      String uuid = dn.getDatanodeDetails().getUuidString();
      assertTrue(logs.contains("Sending diskBalancerCommand: opType=START") &&
              logs.contains(uuid));
    }

    // Wait up to 5 seconds for all DNs to log the stop message
    GenericTestUtils.waitFor(() -> {
      String dnLogs = dnLogCapturer.getOutput();
      long count = Arrays.stream(dnLogs.split("\n"))
          .filter(line -> line.contains("Disk balancer is stopped due to disk even as" +
              " the property StopAfterDiskEven is set to true"))
          .count();
      return count >= cluster.getHddsDatanodes().size();
    }, 100, 10000); // check every 100ms, timeout after 10s
  }

  @Test
  public void testDatanodeDiskBalancerStatus() throws IOException, InterruptedException, TimeoutException {
    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    DatanodeDetails toDecommission = dns.get(0).getDatanodeDetails();

    diskBalancerManager.startDiskBalancer(
        10.0, // threshold
        10L,  // bandwidth in MB
        5,    // parallel threads
        true, // stopAfterDiskEven
        null);

    //all DNs IN_SERVICE, so disk balancer status for all should be present
    List<HddsProtos.DatanodeDiskBalancerInfoProto> statusProtoList =
        diskBalancerManager.getDiskBalancerStatus(null,
            null,
            ClientVersion.CURRENT_VERSION);
    assertEquals(3, statusProtoList.size());

    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();

    // Decommission the first DN
    storageClient.decommissionNodes(Arrays.asList(
        getDNHostAndPort(toDecommission)), false);
    waitForDnToReachOpState(nm, toDecommission, DECOMMISSIONING);

    //one DN is in DECOMMISSIONING state, so disk balancer status for it should not be present
    statusProtoList = diskBalancerManager.getDiskBalancerStatus(null,
        null,
        ClientVersion.CURRENT_VERSION);
    assertEquals(2, statusProtoList.size());

    // Check status for the decommissioned DN should not be present
    statusProtoList = diskBalancerManager.getDiskBalancerStatus(
        Collections.singletonList(getDNHostAndPort(toDecommission)),
        null,
        ClientVersion.CURRENT_VERSION);
    assertEquals(0, statusProtoList.size());

    storageClient.recommissionNodes(Arrays.asList(
        getDNHostAndPort(toDecommission)));
    waitForDnToReachOpState(nm, toDecommission, IN_SERVICE);

    // Check status for the recommissioned DN should now be present
    statusProtoList = diskBalancerManager.getDiskBalancerStatus(
        Collections.singletonList(getDNHostAndPort(toDecommission)),
        null,
        ClientVersion.CURRENT_VERSION);
    assertEquals(1, statusProtoList.size());
  }
}
