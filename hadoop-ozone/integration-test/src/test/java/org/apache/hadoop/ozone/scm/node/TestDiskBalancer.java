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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerConfigurationProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.hdds.protocolPB.DiskBalancerProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests disk balancer operations using direct client-to-DN communication.
 */
@Timeout(300)
public class TestDiskBalancer {

  private static ScmClient storageClient;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;

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

  /**
   * Helper method to create a DiskBalancerProtocol proxy for a datanode.
   */
  private DiskBalancerProtocol getDiskBalancerProxy(DatanodeDetails dn) throws IOException {
    InetSocketAddress nodeAddr = new java.net.InetSocketAddress(
        dn.getIpAddress(), dn.getPort(Port.Name.CLIENT_RPC).getValue());
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    return new DiskBalancerProtocolClientSideTranslatorPB(nodeAddr, user, ozoneConf);
  }

  @Test
  public void testDatanodeDiskBalancerReport() throws IOException {
    // Query DiskBalancer report from multiple DNs using direct client-to-DN RPC
    List<DatanodeDetails> datanodes = new ArrayList<>(
        cluster.getStorageContainerManager().getScmNodeManager().getAllNodes());
    List<DatanodeDiskBalancerInfoProto> reportProtoList = new ArrayList<>();

    for (DatanodeDetails dn : datanodes) {
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        DatanodeDiskBalancerInfoProto report = proxy.getDiskBalancerInfo();
        assertNotNull(report);
        assertTrue(report.hasCurrentVolumeDensitySum());
        reportProtoList.add(report);
      }
    }

    assertEquals(datanodes.size(), reportProtoList.size());
    
    // Verify that we got reports from all datanodes with volume density information
    // Note: The sorting by volume density happens in the CLI display logic,
    // not in the individual datanode responses. Each datanode returns its own report.
    for (DatanodeDiskBalancerInfoProto report : reportProtoList) {
      assertTrue(report.hasCurrentVolumeDensitySum());
      assertNotNull(report.getNode());
    }
  }

  @Test
  public void testDiskBalancerStopAfterEven() throws IOException,
      InterruptedException, TimeoutException {
    // Start DiskBalancer on a DN using direct client-to-DN RPC with stopAfterDiskEven=true
    DatanodeDetails dn = cluster.getStorageContainerManager()
        .getScmNodeManager().getAllNodes().get(0);

    DiskBalancerConfigurationProto configProto = DiskBalancerConfigurationProto.newBuilder()
        .setThreshold(10.0)
        .setDiskBandwidthInMB(10L)
        .setParallelThread(5)
        .setStopAfterDiskEven(true)
        .build();

    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
      proxy.startDiskBalancer(configProto);

      // Query status to verify it's RUNNING
      DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
      assertEquals(DiskBalancerRunningStatus.RUNNING, status.getRunningStatus());

      // Wait for some time and query status to verify the RUNNING status changes to STOPPED
      // This happens when disks become even
      GenericTestUtils.waitFor(
          () -> {
            try {
              DatanodeDiskBalancerInfoProto currentStatus = proxy.getDiskBalancerInfo();
              return currentStatus.getRunningStatus() == DiskBalancerRunningStatus.STOPPED;
            } catch (IOException e) {
              return false;
            }
          },
          500, 30000); // Check every 500ms, timeout after 30s
    }
  }

  @Test
  public void testDatanodeDiskBalancerStatus() throws IOException, InterruptedException, TimeoutException {
    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();
    List<DatanodeDetails> allDatanodes = new ArrayList<>(nm.getAllNodes());
    DatanodeDetails toDecommission = allDatanodes.get(0);

    // Start DiskBalancer on all DNs using direct client-to-DN RPC
    DiskBalancerConfigurationProto configProto = DiskBalancerConfigurationProto.newBuilder()
        .setThreshold(10.0)
        .setDiskBandwidthInMB(10L)
        .setParallelThread(5)
        .setStopAfterDiskEven(false)
        .build();

    for (DatanodeDetails dn : allDatanodes) {
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        proxy.startDiskBalancer(configProto);
      }
    }

    // Query status from all DNs and verify all show RUNNING status
    List<DatanodeDiskBalancerInfoProto> statusProtoList = new ArrayList<>();
    for (DatanodeDetails dn : allDatanodes) {
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
        assertEquals(DiskBalancerRunningStatus.RUNNING, status.getRunningStatus());
        statusProtoList.add(status);
      }
    }
    assertEquals(3, statusProtoList.size());

    // Decommission one DN
    storageClient.decommissionNodes(
        Collections.singletonList(getDNHostAndPort(toDecommission)), false);
    waitForDnToReachOpState(nm, toDecommission, DECOMMISSIONING);

    // Verify that the decommissioned DN's DiskBalancer status changed to PAUSED
    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(toDecommission)) {
      GenericTestUtils.waitFor(
          () -> {
            try {
              DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
              return status.getRunningStatus() == DiskBalancerRunningStatus.PAUSED;
            } catch (IOException e) {
              return false;
            }
          },
          500, 30000); // Check every 500ms, timeout after 30s

      DatanodeDiskBalancerInfoProto pausedStatus = proxy.getDiskBalancerInfo();
      assertEquals(DiskBalancerRunningStatus.PAUSED, pausedStatus.getRunningStatus(),
          "DiskBalancer status should be PAUSED after decommissioning");
    }

    // Query status from remaining IN_SERVICE DNs and verify they still show RUNNING
    List<DatanodeDetails> inServiceDatanodes = nm.getNodes(IN_SERVICE, HddsProtos.NodeState.HEALTHY);
    statusProtoList.clear();
    for (DatanodeDetails dn : inServiceDatanodes) {
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
        assertEquals(DiskBalancerRunningStatus.RUNNING, status.getRunningStatus());
        statusProtoList.add(status);
      }
    }
    assertEquals(2, statusProtoList.size());

    // Recommission the DN
    storageClient.recommissionNodes(
        Collections.singletonList(getDNHostAndPort(toDecommission)));
    waitForDnToReachOpState(nm, toDecommission, IN_SERVICE);

    // Wait for DiskBalancerService to resume from PAUSED to RUNNING after recommissioning
    // The service automatically pauses when decommissioned and resumes when recommissioned
    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(toDecommission)) {
      GenericTestUtils.waitFor(
          () -> {
            try {
              DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
              return status.getRunningStatus() == DiskBalancerRunningStatus.RUNNING;
            } catch (IOException e) {
              return false;
            }
          },
          500, 30000); // Check every 500ms, timeout after 30s
    }
  }
}
