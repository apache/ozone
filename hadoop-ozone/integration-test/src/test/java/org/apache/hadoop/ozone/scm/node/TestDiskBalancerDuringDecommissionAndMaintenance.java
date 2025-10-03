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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
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
  private static DiskBalancerManager diskBalancerManager;
  private static ScmClient scmClient;

  @BeforeAll
  public static void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY, true);
    conf.setStrings(HddsConfigKeys.HDDS_DISK_BALANCER_REPORT_INTERVAL, "2s");
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
    diskBalancerManager.stopDiskBalancer(null);
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
        10.0,
        10L,
        5,
        false,
        null);

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
        diskBalancerManager.getDiskBalancerStatus(null,
            null,
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
    statusProtoList = diskBalancerManager.getDiskBalancerStatus(null,
        null,
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
        10.0,
        10L,
        1,
        false,
        dnAddressList);

    // Verify diskBalancer is running
    GenericTestUtils.waitFor(() -> {
      try {
        HddsProtos.DatanodeDiskBalancerInfoProto status =
            diskBalancerManager.getDiskBalancerStatus(dnAddressList, null,
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
    List<DatanodeAdminError> dnErrors = diskBalancerManager.stopDiskBalancer(dnAddressList);

    // Verify disk balancer stop command is not sent to decommissioning DN
    assertEquals(1, dnErrors.size());
    assertTrue(dnErrors.get(0).getError()
        .contains("Datanode is not in an optimal state for disk balancing"));

    //Recommission the node
    scmClient.recommissionNodes(dnAddressList);
    waitForDnToReachOpState(nm, dn, IN_SERVICE);

    // Verify it automatically resumes (since explicit stop command was not sent)
    GenericTestUtils.waitFor(() -> {
      String dnLogs = serviceLog.getOutput();
      return dnLogs.contains("Resuming DiskBalancerService to running state as Node state changed to IN_SERVICE.");
    }, 100, 5000);
  }

  @Test
  public void testStartDiskBalancerOnDecommissioningNode() throws Exception {
    LogCapturer supervisorLog = LogCapturer.captureLogs(ReplicationSupervisor.class);

    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    DatanodeDetails dn = dns.get(4).getDatanodeDetails();
    List<String> dnAddressList = Collections.singletonList(getDNHostAndPort(dn));
    NodeManager nm = cluster.getStorageContainerManager().getScmNodeManager();

    // Verify diskBalancer is stopped
    GenericTestUtils.waitFor(() -> {
      try {
        HddsProtos.DatanodeDiskBalancerInfoProto status =
            diskBalancerManager.getDiskBalancerStatus(dnAddressList, null,
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
    List<DatanodeAdminError> dnErrors = diskBalancerManager.startDiskBalancer(
        10.0, 10L, 1, false, dnAddressList);

    // Verify disk balancer start command is not sent to decommissioning DN
    assertEquals(1, dnErrors.size());
    assertTrue(dnErrors.get(0).getError()
        .contains("Datanode is not in an optimal state for disk balancing"));

    //Recommission the node
    scmClient.recommissionNodes(dnAddressList);
    waitForDnToReachOpState(nm, dn, IN_SERVICE);

    // Verify it does not automatically resume (since it was explicit
    // start command was not sent to decommissioning DN)
    GenericTestUtils.waitFor(() -> {
      try {
        HddsProtos.DatanodeDiskBalancerInfoProto status =
            diskBalancerManager.getDiskBalancerStatus(dnAddressList, null,
                ClientVersion.CURRENT_VERSION).stream().findFirst().orElse(null);
        return status != null && status.getRunningStatus() == HddsProtos.DiskBalancerRunningStatus.STOPPED;
      } catch (IOException e) {
        return false;
      }
    }, 100, 5000);
  }
}

