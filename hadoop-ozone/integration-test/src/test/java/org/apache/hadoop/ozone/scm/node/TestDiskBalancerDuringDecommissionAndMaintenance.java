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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.getDNHostAndPort;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachOpState;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
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
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.SetNodeOperationalStateCommandHandler;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
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
    final int interval = 100;

    conf = new OzoneConfiguration();
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        interval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
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

  @Test
  public void testDiskBalancerWithDecommissionAndMaintenanceNodes()
      throws IOException, InterruptedException, TimeoutException {
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

    LogCapturer dnStateChangeLog = LogCapturer.captureLogs(
        SetNodeOperationalStateCommandHandler.class);

    LogCapturer dnStopLog = LogCapturer.captureLogs(DatanodeStateMachine.class);

    // verify using logs that DiskBalancerService is stopped
    // on DN with state is DECOMMISSIONING or ENTERING_MAINTENANCE
    GenericTestUtils.waitFor(() -> {
      String dnLogs = dnStateChangeLog.getOutput();
      String dnStopLogs = dnStopLog.getOutput();

      return
          dnLogs.contains("Node state changed to DECOMMISSIONING." +
          " Stopping DiskBalancerService.") && dnLogs.contains("Node state changed to ENTERING_MAINTENANCE." +
          " Stopping DiskBalancerService.") && dnStopLogs.contains("Stopping DiskBalancerService as " +
          "the DN state is set to DECOMMISSIONING/MAINTENANCE.");
    }, 100, 5000); // Poll every 100ms, timeout after 5 seconds

    // Recommission DN1
    scmClient.recommissionNodes(
        Arrays.asList(getDNHostAndPort(dnToDecommission)));
    waitForDnToReachOpState(nm, dnToDecommission, IN_SERVICE);

    // Verify that recommissioned DN is included in DiskBalancer report and status
    reportProtoList = diskBalancerManager.getDiskBalancerReport(5,
        ClientVersion.CURRENT_VERSION);
    statusProtoList = diskBalancerManager.getDiskBalancerStatus(Optional.empty(),
        Optional.empty(),
        ClientVersion.CURRENT_VERSION);

    boolean isRecommissionedDnInReport = reportProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(dnToDecommission.getUuid().toString()));
    boolean isRecommissionedDnInStatus = statusProtoList.stream()
        .anyMatch(proto -> proto.getNode().getUuid().
            equals(dnToDecommission.getUuid().toString()));

    // Verify that the recommissioned DN is included in both report and status
    assertTrue(isRecommissionedDnInReport);
    assertTrue(isRecommissionedDnInStatus);

    // verify using logs that DiskBalancerService has
    // resume when DN is recommissioned
    GenericTestUtils.waitFor(() -> {
      String dnLogs = dnStateChangeLog.getOutput();
      String dnStopLogs = dnStopLog.getOutput();

      return dnLogs.contains("Node state changed to IN_SERVICE. Resuming DiskBalancerService.")
              && dnStopLogs.contains("Resuming DiskBalancerService as the DN state is changed to IN_SERVICE.");
    }, 100, 5000); // Poll every 100ms, timeout after 5 seconds
  }
}

