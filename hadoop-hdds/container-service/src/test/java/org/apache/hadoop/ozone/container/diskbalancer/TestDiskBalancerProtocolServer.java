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

package org.apache.hadoop.ozone.container.diskbalancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.GetDiskBalancerInfoRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerConfigurationProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerProtocolServer.PrivilegedOperation;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DiskBalancerProtocolServer}.
 */
class TestDiskBalancerProtocolServer {
  private static final GetDiskBalancerInfoRequestProto REQUEST_WITH_OLD_CLIENT_VERSION
      = GetDiskBalancerInfoRequestProto.newBuilder().setClientVersion(1).build();
  private static final double TEST_THRESHOLD = 10.0;
  private static final long TEST_BANDWIDTH = 20L;
  private static final int TEST_THREADS = 5;
  private static final boolean TEST_STOP_AFTER_DISK_EVEN = true;
  private static final double TEST_VOLUME_DENSITY = 15.5;

  private DatanodeStateMachine datanodeStateMachine;
  private DiskBalancerService diskBalancerService;
  private DiskBalancerInfo diskBalancerInfo;
  private PrivilegedOperation denyAdminChecker;
  private DiskBalancerProtocolServer server;

  @BeforeEach
  void setup() throws IOException {
    datanodeStateMachine = mock(DatanodeStateMachine.class);
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(datanodeStateMachine.getContainer()).thenReturn(ozoneContainer);
    diskBalancerService = mock(DiskBalancerService.class);
    when(ozoneContainer.getDiskBalancerService()).thenReturn(diskBalancerService);
    
    // Create DiskBalancerInfo with test data
    diskBalancerInfo = new DiskBalancerInfo(
        DiskBalancerRunningStatus.STOPPED,
        TEST_THRESHOLD,
        TEST_BANDWIDTH,
        TEST_THREADS,
        TEST_STOP_AFTER_DISK_EVEN,
        DiskBalancerVersion.DEFAULT_VERSION,
        0L, // successCount
        0L, // failureCount
        0L, // bytesToMove
        0L, // balancedBytes
        TEST_VOLUME_DENSITY
    );
    when(ozoneContainer.getDiskBalancerInfo()).thenReturn(diskBalancerInfo);

    // Create datanode details
    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("test-host")
        .setIpAddress("127.0.0.1")
        .build();
    when(datanodeStateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);

    // Create admin checkers
    PrivilegedOperation acceptAdminChecker = operation -> { };
    denyAdminChecker = operation -> {
      throw new IOException("Access denied for operation: " + operation);
    };
    
    // Create server with ACCEPT admin checker by default
    server = new DiskBalancerProtocolServer(datanodeStateMachine, acceptAdminChecker);
  }

  @Test
  void testGetDiskBalancerInfoReport() throws IOException {
    // Test REPORT type - should only return volume density
    DatanodeDiskBalancerInfoProto report = server.getDiskBalancerInfo(REQUEST_WITH_OLD_CLIENT_VERSION);
    
    assertNotNull(report);
    assertNotNull(report.getNode());
    assertEquals(TEST_VOLUME_DENSITY, report.getCurrentVolumeDensitySum());
  }

  @Test
  void testGetDiskBalancerInfoStatus() throws IOException {
    // Set operational state to RUNNING
    diskBalancerInfo.setOperationalState(DiskBalancerRunningStatus.RUNNING);
    diskBalancerInfo.setSuccessCount(10);
    diskBalancerInfo.setFailureCount(2);
    diskBalancerInfo.setBytesToMove(1000000);
    diskBalancerInfo.setBalancedBytes(500000);
    
    // Test STATUS type - should return full status information
    DatanodeDiskBalancerInfoProto status = server.getDiskBalancerInfo(REQUEST_WITH_OLD_CLIENT_VERSION);
    assertNotNull(status);
    assertNotNull(status.getNode());
    assertEquals(DiskBalancerRunningStatus.RUNNING, status.getRunningStatus());
    assertTrue(status.hasDiskBalancerConf());
    
    // Verify configuration
    DiskBalancerConfigurationProto conf = status.getDiskBalancerConf();
    assertEquals(TEST_THRESHOLD, conf.getThreshold());
    assertEquals(TEST_BANDWIDTH, conf.getDiskBandwidthInMB());
    assertEquals(TEST_THREADS, conf.getParallelThread());
    assertEquals(TEST_STOP_AFTER_DISK_EVEN, conf.getStopAfterDiskEven());
    assertEquals(10, status.getSuccessMoveCount());
    assertEquals(2, status.getFailureMoveCount());
    assertEquals(1000000, status.getBytesToMove());
    assertEquals(500000, status.getBytesMoved());
  }

  @Test
  void testStartDiskBalancer() throws IOException {
    // Verify initial state is STOPPED
    assertEquals(DiskBalancerRunningStatus.STOPPED, diskBalancerInfo.getOperationalState());
    
    // Start DiskBalancer without configuration
    server.startDiskBalancer(null);
    
    // Verify state changed to RUNNING
    assertEquals(DiskBalancerRunningStatus.RUNNING, diskBalancerInfo.getOperationalState());
    
    // Verify service was refreshed
    verify(diskBalancerService, times(1)).refresh(diskBalancerInfo);
  }

  @Test
  void testStartDiskBalancerWithConfiguration() throws IOException {
    // Create configuration with different values
    DiskBalancerConfigurationProto config = DiskBalancerConfigurationProto.newBuilder()
        .setThreshold(5.0)
        .setDiskBandwidthInMB(50L)
        .setParallelThread(10)
        .setStopAfterDiskEven(false)
        .build();
    
    // Start DiskBalancer with configuration
    server.startDiskBalancer(config);
    
    // Verify state changed to RUNNING
    assertEquals(DiskBalancerRunningStatus.RUNNING, diskBalancerInfo.getOperationalState());
    
    // Verify configuration was updated
    assertEquals(5.0, diskBalancerInfo.getThreshold(), 0.001);
    assertEquals(50L, diskBalancerInfo.getBandwidthInMB());
    assertEquals(10, diskBalancerInfo.getParallelThread());
    assertFalse(diskBalancerInfo.isStopAfterDiskEven());
    
    // Verify service was refreshed
    verify(diskBalancerService, times(1)).refresh(diskBalancerInfo);
  }

  @Test
  void testStopDiskBalancer() throws IOException {
    //initial start DiskBalancer and verify state is RUNNING
    server.startDiskBalancer(null);
    assertEquals(DiskBalancerRunningStatus.RUNNING, diskBalancerInfo.getOperationalState());

    // Stop DiskBalancer and verify state changed to STOPPED
    server.stopDiskBalancer();
    assertEquals(DiskBalancerRunningStatus.STOPPED, diskBalancerInfo.getOperationalState());
    
    // Verify service was refreshed
    verify(diskBalancerService, times(2)).refresh(diskBalancerInfo);
  }

  @Test
  void testUpdateDiskBalancerConfiguration() throws IOException {
    // Create configuration with updated values
    DiskBalancerConfigurationProto config = DiskBalancerConfigurationProto.newBuilder()
        .setThreshold(15.0)
        .setDiskBandwidthInMB(100L)
        .setParallelThread(20)
        .setStopAfterDiskEven(false)
        .build();
    
    // Update configuration
    server.updateDiskBalancerConfiguration(config);
    
    // Verify configuration was updated
    assertEquals(15.0, diskBalancerInfo.getThreshold(), 0.001);
    assertEquals(100L, diskBalancerInfo.getBandwidthInMB());
    assertEquals(20, diskBalancerInfo.getParallelThread());
    assertFalse(diskBalancerInfo.isStopAfterDiskEven());
    
    // Verify service was refreshed
    verify(diskBalancerService, times(1)).refresh(diskBalancerInfo);
  }

  @Test
  void testStartRequiresAdmin() {
    // Create server with DENY admin checker
    DiskBalancerProtocolServer serverWithDeny = 
        new DiskBalancerProtocolServer(datanodeStateMachine, denyAdminChecker);
    
    // Verify start requires admin privilege
    IOException exception = assertThrows(IOException.class, () -> 
        serverWithDeny.startDiskBalancer(null));
    assertEquals("Access denied for operation: startDiskBalancer", exception.getMessage());
  }

  @Test
  void testStopRequiresAdmin() {
    // Create server with DENY admin checker
    DiskBalancerProtocolServer serverWithDeny = 
        new DiskBalancerProtocolServer(datanodeStateMachine, denyAdminChecker);
    
    // Verify stop requires admin privilege
    IOException exception = assertThrows(IOException.class, () -> 
        serverWithDeny.stopDiskBalancer());
    assertEquals("Access denied for operation: stopDiskBalancer", exception.getMessage());
  }

  @Test
  void testUpdateRequiresAdmin() {
    // Create server with DENY admin checker
    DiskBalancerProtocolServer serverWithDeny = 
        new DiskBalancerProtocolServer(datanodeStateMachine, denyAdminChecker);
    
    DiskBalancerConfigurationProto config = DiskBalancerConfigurationProto.newBuilder()
        .setThreshold(5.0)
        .setDiskBandwidthInMB(50L)
        .setParallelThread(10)
        .setStopAfterDiskEven(false)
        .build();
    
    // Verify update requires admin privilege
    IOException exception = assertThrows(IOException.class, () -> 
        serverWithDeny.updateDiskBalancerConfiguration(config));
    assertEquals("Access denied for operation: updateDiskBalancerConfiguration", 
        exception.getMessage());
  }

  @Test
  void testStartDiskBalancerWhenAlreadyRunning() throws IOException {
    // Start DiskBalancer first
    server.startDiskBalancer(null);
    assertEquals(DiskBalancerRunningStatus.RUNNING, diskBalancerInfo.getOperationalState());
    
    // Verify service was refreshed once (from the first start)
    verify(diskBalancerService, times(1)).refresh(diskBalancerInfo);
    
    // Try to start again - should log warning and return early without exception
    server.startDiskBalancer(null);
    
    // Verify state is still RUNNING (unchanged)
    assertEquals(DiskBalancerRunningStatus.RUNNING, diskBalancerInfo.getOperationalState());
    
    // Verify service was not refreshed again (still only once)
    verify(diskBalancerService, times(1)).refresh(diskBalancerInfo);
  }
}

