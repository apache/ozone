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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests disk balancer operations during
 * decommission and maintenance of DNs.
 */
@Timeout(300)
public class TestDiskBalancerDuringDecommissionAndMaintenance {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static DiskBalancerManager diskBalancerManager;
  private static ScmClient scmClient;

  @BeforeAll
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    conf.setTimeDuration("hdds.datanode.disk.balancer.service.interval", 2, TimeUnit.SECONDS);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();

    diskBalancerManager = cluster.getStorageContainerManager().getDiskBalancerManager();
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
    // Stop disk balancer after each test
    diskBalancerManager.stopDiskBalancer(Optional.empty());
    // Verify that all DNs have stopped DiskBalancerService
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      GenericTestUtils.waitFor(() -> {
        return !dn.getDatanodeStateMachine().getContainer().getDiskBalancerInfo().isShouldRun();
      }, 100, 5000);
    }
  }

  @Test
  public void testDiskBalancerWithDecommissionAndMaintenanceNodes()
      throws IOException, InterruptedException, TimeoutException {
    LogCapturer dnStateChangeLog = LogCapturer.captureLogs(
        DiskBalancerService.class);

    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    DatanodeDetails dnToDecommission = dns.get(0).getDatanodeDetails();
    DatanodeDetails dnToMaintenance = dns.get(1).getDatanodeDetails();

    // Start disk balancer on all DNs
    diskBalancerManager.startDiskBalancer(
        Optional.of(10.0),
        Optional.of(10L),
        Optional.of(5),
        Optional.of(false),
        Optional.empty());

    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();

    // Decommission DN1
    scmClient.decommissionNodes(
        Arrays.asList(getDNHostAndPort(dnToDecommission)), false);
    waitForDnToReachOpState(nm, dnToDecommission, DECOMMISSIONING);

    // Start maintenance on DN2
    scmClient.startMaintenanceNodes(
        Arrays.asList(getDNHostAndPort(dnToMaintenance)), 0, false);
    waitForDnToReachOpState(nm, dnToMaintenance, ENTERING_MAINTENANCE);

    //get diskBalancer report
    List<HddsProtos.DatanodeDiskBalancerInfoProto> reportProtoList =
        diskBalancerManager.getDiskBalancerReport(5,
            ClientVersion.CURRENT_VERSION);

    //get diskBalancer status
    List<HddsProtos.DatanodeDiskBalancerInfoProto> statusProtoList =
        diskBalancerManager.getDiskBalancerStatus(Optional.empty(),
            Optional.empty(),
            ClientVersion.CURRENT_VERSION);

    // Verify that decommissioning and maintenance DN is not
    // included in DiskBalancer report and status
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

    // verify using logs that DiskBalancerService is stopped
    // on DN with state is DECOMMISSIONING or ENTERING_MAINTENANCE
    GenericTestUtils.waitFor(() -> {
      String dnLogs = dnStateChangeLog.getOutput();
      return
          dnLogs.contains("Stopping DiskBalancerService as Node state changed to DECOMMISSIONING.")
              && dnLogs.contains("Stopping DiskBalancerService as Node state changed to ENTERING_MAINTENANCE.");
    }, 100, 5000);

    // Recommission DN1
    scmClient.recommissionNodes(
        Arrays.asList(getDNHostAndPort(dnToDecommission)));
    waitForDnToReachOpState(nm, dnToDecommission, IN_SERVICE);

    DatanodeDetails recommissionedDn = dnToDecommission;

    // Verify that recommissioned DN is included in DiskBalancer report and status
    reportProtoList = diskBalancerManager.getDiskBalancerReport(5,
        ClientVersion.CURRENT_VERSION);
    statusProtoList = diskBalancerManager.getDiskBalancerStatus(Optional.empty(),
        Optional.empty(),
        ClientVersion.CURRENT_VERSION);

    boolean isRecommissionedDnInReport = reportProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(recommissionedDn.getUuid().toString()));
    boolean isRecommissionedDnInStatus = statusProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(recommissionedDn.getUuid().toString()));

    // Verify that the recommissioned DN is included in both report and status
    assertTrue(isRecommissionedDnInReport);
    assertTrue(isRecommissionedDnInStatus);

    //Verify using logs when DN is recommissioned
    //if the DN was previously in stopped state it will not be resumed
    //otherwise it will be resumed
    GenericTestUtils.waitFor(() -> {
      String dnLogs = dnStateChangeLog.getOutput();
      return dnLogs.contains("Resuming DiskBalancerService to running state as Node state changed to IN_SERVICE.");
    }, 100, 5000);
  }

  @Test
  public void testStopDiskBalancerOnDecommissioningNode() throws Exception {
    LogCapturer serviceLog = LogCapturer.captureLogs(DiskBalancerService.class);
    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    DatanodeDetails dn = dns.get(3).getDatanodeDetails();
    List<String> dnAddressList = Collections.singletonList(getDNHostAndPort(dn));
    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();


    // Start disk balancer on this specific DN
    diskBalancerManager.startDiskBalancer(
        Optional.of(10.0),
        Optional.of(10L),
        Optional.of(1),
        Optional.of(false),
        Optional.of(dnAddressList));

    // Verify diskBalancer is running
    GenericTestUtils.waitFor(() -> {
      try {
        HddsProtos.DatanodeDiskBalancerInfoProto status =
            diskBalancerManager.getDiskBalancerStatus(Optional.of(dnAddressList),
                Optional.empty(),
                ClientVersion.CURRENT_VERSION).stream().findFirst().orElse(null);
        return status != null && status.getRunningStatus() == HddsProtos.DiskBalancerRunningStatus.RUNNING;
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

    // Attempt to stop disk balancer on the decommissioning DN
    diskBalancerManager.stopDiskBalancer(Optional.of(dnAddressList));

    // Verify disk balancer is now explicitly stopped (operationalState becomes STOPPED)
    final String expectedLogForStop =
        "DiskBalancer operational state changing from PAUSED_BY_NODE_STATE to STOPPED";
    GenericTestUtils.waitFor(() -> serviceLog.getOutput().contains(expectedLogForStop),
        100, 5000);

    //Recommission the node
    scmClient.recommissionNodes(dnAddressList);
    waitForDnToReachOpState(nm, dn, IN_SERVICE);

    // Verify it does not automatically restart (since it was explicitly stopped)
    HddsProtos.DatanodeDiskBalancerInfoProto statusAfterRecommission =
        diskBalancerManager.getDiskBalancerStatus(Optional.of(dnAddressList),
            Optional.empty(),
            ClientVersion.CURRENT_VERSION).stream().findFirst().orElse(null);
    assertEquals(HddsProtos.DiskBalancerRunningStatus.STOPPED, statusAfterRecommission.getRunningStatus());
  }

  @Test
  public void testStartDiskBalancerOnDecommissioningNode() throws Exception {
    LogCapturer serviceLog = LogCapturer.captureLogs(DiskBalancerService.class);
    LogCapturer supervisorLog = LogCapturer.captureLogs(ReplicationSupervisor.class);

    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    DatanodeDetails dn = dns.get(4).getDatanodeDetails();
    List<String> dnAddressList = Collections.singletonList(getDNHostAndPort(dn));
    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();

    // Verify diskBalancer is stopped
    GenericTestUtils.waitFor(() -> {
      try {
        HddsProtos.DatanodeDiskBalancerInfoProto status =
            diskBalancerManager.getDiskBalancerStatus(Optional.of(dnAddressList),
                Optional.empty(),
                ClientVersion.CURRENT_VERSION).stream().findFirst().orElse(null);
        return status != null && status.getRunningStatus() == HddsProtos.DiskBalancerRunningStatus.STOPPED;
      } catch (IOException e) {
        return false;
      }
    }, 100, 5000);

    // Decommission the DN
    scmClient.decommissionNodes(dnAddressList, false);
    waitForDnToReachOpState(nm, dn, DECOMMISSIONING);

    final String nodeStateChangeLogs =
        "Node state updated to DECOMMISSIONING, scaling executor pool size to 20";
    GenericTestUtils.waitFor(() -> supervisorLog.getOutput().contains(nodeStateChangeLogs),
        100, 5000);

    // Attempt to start disk balancer on the decommissioning DN
    diskBalancerManager.startDiskBalancer(
        Optional.of(10.0),
        Optional.of(10L),
        Optional.of(1),
        Optional.of(false),
        Optional.of(dnAddressList));

    // Verify disk balancer goes to PAUSED_BY_NODE_STATE
    final String expectedLogForPause =
        "DiskBalancer operational state changing from STOPPED to PAUSED_BY_NODE_STATE";
    GenericTestUtils.waitFor(() -> serviceLog.getOutput().contains(expectedLogForPause),
        100, 5000);

    //Recommission the node
    scmClient.recommissionNodes(dnAddressList);
    waitForDnToReachOpState(nm, dn, IN_SERVICE);

    // Verify it automatically restart (since it was explicitly started)
    GenericTestUtils.waitFor(() -> {
      try {
        HddsProtos.DatanodeDiskBalancerInfoProto status =
            diskBalancerManager.getDiskBalancerStatus(Optional.of(dnAddressList),
                Optional.empty(),
                ClientVersion.CURRENT_VERSION).stream().findFirst().orElse(null);
        return status != null && status.getRunningStatus() == HddsProtos.DiskBalancerRunningStatus.RUNNING;
      } catch (IOException e) {
        return false;
      }
    }, 100, 5000);
  }
}

