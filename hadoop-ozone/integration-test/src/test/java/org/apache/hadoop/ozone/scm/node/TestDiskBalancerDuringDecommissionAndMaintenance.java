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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.getDNHostAndPort;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachOpState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests disk balancer operations during
 * decommission and maintenance of DNs using direct client-to-DN communication.
 */
@Timeout(300)
public class TestDiskBalancerDuringDecommissionAndMaintenance {

  private static MiniOzoneCluster cluster;
  private static ScmClient scmClient;
  private static OzoneConfiguration conf;

  @BeforeAll
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
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
    // Stop disk balancer on all DNs after each test
    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();
    List<DatanodeDetails> allDatanodes = new ArrayList<>(nm.getAllNodes());
    
    for (DatanodeDetails dn : allDatanodes) {
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        proxy.stopDiskBalancer();
      } catch (IOException e) {
      }
    }

    // Verify that all DNs have stopped DiskBalancerService
    for (DatanodeDetails dn : allDatanodes) {
      GenericTestUtils.waitFor(() -> {
        try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
          DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
          return status.getRunningStatus() == DiskBalancerRunningStatus.STOPPED;
        } catch (IOException e) {
          return false;
        }
      }, 100, 5000);
    }
  }

  /**
   * Helper method to create a DiskBalancerProtocol proxy for a datanode.
   */
  private DiskBalancerProtocol getDiskBalancerProxy(DatanodeDetails dn) throws IOException {
    InetSocketAddress nodeAddr = new InetSocketAddress(
        dn.getIpAddress(), dn.getPort(Port.Name.CLIENT_RPC).getValue());
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    return new DiskBalancerProtocolClientSideTranslatorPB(nodeAddr, user, conf);
  }

  /**
   * Helper method to get all IN_SERVICE datanodes.
   */
  private List<DatanodeDetails> getInServiceDatanodes(NodeManager nm) {
    return nm.getNodes(IN_SERVICE, HddsProtos.NodeState.HEALTHY);
  }

  /**
   * Helper method to query DiskBalancer info from all IN_SERVICE datanodes.
   * Similar to --in-service-datanodes option in CLI.
   */
  private <T> List<T> queryAllInServiceDatanodes(
      DiskBalancerQuery<T> query) throws IOException {
    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();
    List<DatanodeDetails> inServiceDatanodes = getInServiceDatanodes(nm);
    List<T> results = new ArrayList<>();
    
    for (DatanodeDetails dn : inServiceDatanodes) {
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        T result = query.query(proxy, dn);
        if (result != null) {
          results.add(result);
        }
      }
    }
    
    return results;
  }

  /**
   * Functional interface for DiskBalancer queries.
   */
  @FunctionalInterface
  private interface DiskBalancerQuery<T> {
    T query(DiskBalancerProtocol proxy, DatanodeDetails dn) throws IOException;
  }

  @Test
  public void testDiskBalancerWithDecommissionAndMaintenanceNodes()
      throws IOException, InterruptedException, TimeoutException {
    LogCapturer dnStateChangeLog = LogCapturer.captureLogs(
        DiskBalancerService.class);

    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();
    List<DatanodeDetails> allDatanodes = new ArrayList<>(nm.getAllNodes());
    DatanodeDetails dnToDecommission = allDatanodes.get(0);
    DatanodeDetails dnToMaintenance = allDatanodes.get(1);

    // Start disk balancer on all DNs using direct client-to-DN RPC
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

    // Decommission DN1
    scmClient.decommissionNodes(
        Arrays.asList(getDNHostAndPort(dnToDecommission)), false);
    waitForDnToReachOpState(nm, dnToDecommission, DECOMMISSIONING);

    // Start maintenance on DN2
    scmClient.startMaintenanceNodes(
        Arrays.asList(getDNHostAndPort(dnToMaintenance)), 0, false);
    waitForDnToReachOpState(nm, dnToMaintenance, ENTERING_MAINTENANCE);

    // Get diskBalancer report from IN_SERVICE nodes only
    List<DatanodeDiskBalancerInfoProto> reportProtoList = queryAllInServiceDatanodes(
        (proxy, dn) -> proxy.getDiskBalancerInfo());

    // Get diskBalancer status from IN_SERVICE nodes only
    List<DatanodeDiskBalancerInfoProto> statusProtoList = queryAllInServiceDatanodes(
        (proxy, dn) -> proxy.getDiskBalancerInfo());

    // Verify that decommissioning and maintenance DN is not included
    // in DiskBalancer report and status (since we only queried IN_SERVICE nodes)
    boolean isDecommissionedDnInReport = reportProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(dnToDecommission.getUuid().toString()));
    boolean isMaintenanceDnInReport = reportProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(dnToMaintenance.getUuid().toString()));
    boolean isDecommissionedDnInStatus = statusProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(dnToDecommission.getUuid().toString()));
    boolean isMaintenanceDnInStatus = statusProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(dnToMaintenance.getUuid().toString()));

    // Assert that the decommissioned DN is not present in both report and status
    assertFalse(isDecommissionedDnInReport);
    assertFalse(isMaintenanceDnInReport);
    assertFalse(isDecommissionedDnInStatus);
    assertFalse(isMaintenanceDnInStatus);

    // Verify using logs that DiskBalancerService is stopped
    // on DN with state is DECOMMISSIONING or ENTERING_MAINTENANCE
    GenericTestUtils.waitFor(() -> {
      String dnLogs = dnStateChangeLog.getOutput();
      return dnLogs.contains("Stopping DiskBalancerService as Node state changed to DECOMMISSIONING.")
          && dnLogs.contains("Stopping DiskBalancerService as Node state changed to ENTERING_MAINTENANCE.");
    }, 100, 5000);

    // Recommission DN1
    scmClient.recommissionNodes(
        Arrays.asList(getDNHostAndPort(dnToDecommission)));
    waitForDnToReachOpState(nm, dnToDecommission, IN_SERVICE);

    DatanodeDetails recommissionedDn = dnToDecommission;

    // Verify that recommissioned DN is included in DiskBalancer report and status
    reportProtoList = queryAllInServiceDatanodes(
        (proxy, dn) -> proxy.getDiskBalancerInfo());
    statusProtoList = queryAllInServiceDatanodes(
        (proxy, dn) -> proxy.getDiskBalancerInfo());

    boolean isRecommissionedDnInReport = reportProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(recommissionedDn.getUuid().toString()));
    boolean isRecommissionedDnInStatus = statusProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(recommissionedDn.getUuid().toString()));

    // Verify that the recommissioned DN is included in both report and status
    assertTrue(isRecommissionedDnInReport);
    assertTrue(isRecommissionedDnInStatus);

    // Verify using logs when DN is recommissioned
    // if the DN was previously in stopped state it will not be resumed
    // otherwise it will be resumed
    GenericTestUtils.waitFor(() -> {
      String dnLogs = dnStateChangeLog.getOutput();
      return dnLogs.contains("Resuming DiskBalancerService to running state as Node state changed to IN_SERVICE.");
    }, 100, 5000);
  }

  @Test
  public void testStopDiskBalancerOnDecommissioningNode() throws Exception {
    LogCapturer serviceLog = LogCapturer.captureLogs(DiskBalancerService.class);
    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();
    List<DatanodeDetails> allDatanodes = new ArrayList<>(nm.getAllNodes());
    DatanodeDetails dn = allDatanodes.get(3);
    List<String> dnAddressList = Collections.singletonList(getDNHostAndPort(dn));

    // Start disk balancer on this specific DN using direct client-to-DN RPC
    DiskBalancerConfigurationProto configProto = DiskBalancerConfigurationProto.newBuilder()
        .setThreshold(10.0)
        .setDiskBandwidthInMB(10L)
        .setParallelThread(1)
        .setStopAfterDiskEven(false)
        .build();

    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
      proxy.startDiskBalancer(configProto);
    }

    // Verify diskBalancer is running
    GenericTestUtils.waitFor(() -> {
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
        return status != null && status.getRunningStatus() == DiskBalancerRunningStatus.RUNNING;
      } catch (IOException e) {
        return false;
      }
    }, 100, 5000);

    // Decommission the DN
    scmClient.decommissionNodes(dnAddressList, false);
    waitForDnToReachOpState(nm, dn, DECOMMISSIONING);

    // Verify DiskBalancerService on DN automatically paused
    final String expectedLogForPause =
        "Stopping DiskBalancerService as Node state changed to DECOMMISSIONING.";
    GenericTestUtils.waitFor(() -> serviceLog.getOutput().contains(expectedLogForPause),
        100, 5000);

    // Verify status is PAUSED
    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
      DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
      assertEquals(DiskBalancerRunningStatus.PAUSED, status.getRunningStatus());
    }

    // Attempt to stop disk balancer on the decommissioning DN
    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
      proxy.stopDiskBalancer();
    }

    // Verify disk balancer is now explicitly stopped (operationalState becomes STOPPED)
    GenericTestUtils.waitFor(() -> {
      String logs = serviceLog.getOutput();
      return logs.contains("DiskBalancer operational state changing from PAUSED to STOPPED");
    }, 100, 5000);

    // Verify status is STOPPED
    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
      DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
      assertEquals(DiskBalancerRunningStatus.STOPPED, status.getRunningStatus());
    }

    // Recommission the node
    scmClient.recommissionNodes(dnAddressList);
    waitForDnToReachOpState(nm, dn, IN_SERVICE);

    // Verify it does not automatically restart (since it was explicitly stopped)
    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
      GenericTestUtils.waitFor(() -> {
        try {
          DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
          return status.getRunningStatus() == DiskBalancerRunningStatus.STOPPED;
        } catch (IOException e) {
          return false;
        }
      }, 100, 5000);
    }
  }

  @Test
  public void testStartDiskBalancerOnDecommissioningNode() throws Exception {
    LogCapturer serviceLog = LogCapturer.captureLogs(DiskBalancerService.class);
    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();
    List<DatanodeDetails> allDatanodes = new ArrayList<>(nm.getAllNodes());
    DatanodeDetails dn = allDatanodes.get(4);
    List<String> dnAddressList = Collections.singletonList(getDNHostAndPort(dn));

    // Verify diskBalancer is in stopped state initially
    GenericTestUtils.waitFor(() -> {
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
        return status != null && status.getRunningStatus() == DiskBalancerRunningStatus.STOPPED;
      } catch (IOException e) {
        return false;
      }
    }, 100, 5000);

    // Decommission the DN
    scmClient.decommissionNodes(dnAddressList, false);
    waitForDnToReachOpState(nm, dn, DECOMMISSIONING);

    // Wait for persisted state to be updated on the datanode
    // This ensures that when startDiskBalancer is called, it sees DECOMMISSIONING state
    GenericTestUtils.waitFor(() -> {
      return dn.getPersistedOpState() == DECOMMISSIONING;
    }, 100, 5000);

    // Attempt to start disk balancer on the decommissioning DN
    DiskBalancerConfigurationProto configProto = DiskBalancerConfigurationProto.newBuilder()
        .setThreshold(10.0)
        .setDiskBandwidthInMB(10L)
        .setParallelThread(1)
        .setStopAfterDiskEven(false)
        .build();

    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
      proxy.startDiskBalancer(configProto);
    }

    // Verify disk balancer goes to PAUSED (not RUNNING) because node is decommissioning
    // Check both log message and actual status
    GenericTestUtils.waitFor(() -> {
      String logs = serviceLog.getOutput();
      boolean logFound = logs.contains("Cannot start DiskBalancer as node is in" +
          " DECOMMISSIONING state. Pausing instead.");
      if (logFound) {
        return true;
      }
      // Also check if status is PAUSED
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
        return status.getRunningStatus() == DiskBalancerRunningStatus.PAUSED;
      } catch (IOException e) {
        return false;
      }
    }, 100, 5000);

    // Verify status is PAUSED
    try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
      DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
      assertEquals(DiskBalancerRunningStatus.PAUSED, status.getRunningStatus());
    }

    // Recommission the node
    scmClient.recommissionNodes(dnAddressList);
    waitForDnToReachOpState(nm, dn, IN_SERVICE);

    // Verify it automatically restarts (since it was explicitly started)
    GenericTestUtils.waitFor(() -> {
      try (DiskBalancerProtocol proxy = getDiskBalancerProxy(dn)) {
        DatanodeDiskBalancerInfoProto status = proxy.getDiskBalancerInfo();
        return status != null && status.getRunningStatus() == DiskBalancerRunningStatus.RUNNING;
      } catch (IOException e) {
        return false;
      }
    }, 100, 5000);
  }
}
