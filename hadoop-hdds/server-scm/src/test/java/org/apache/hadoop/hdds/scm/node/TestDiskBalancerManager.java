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

package org.apache.hadoop.hdds.scm.node;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.DiskBalancerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.ClientVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for the DiskBalancer manager.
 */

public class TestDiskBalancerManager {

  private DiskBalancerManager diskBalancerManager;
  private NodeManager nodeManager;
  private DiskBalancerReportHandler diskBalancerReportHandler;
  private Random random;
  @TempDir
  private File testDir;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY, true);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    nodeManager = new MockNodeManager(true, 3);
    diskBalancerManager = new DiskBalancerManager(conf, new EventQueue(),
        SCMContext.emptyContext(), nodeManager);
    diskBalancerReportHandler =
        new DiskBalancerReportHandler(diskBalancerManager);
    random = new Random();
  }

  @Test
  public void testDatanodeDiskBalancerReport() throws IOException {
    // Populate disk balancer reports for all datanodes to avoid Double.NaN comparison issues.
    for (DatanodeDetails dn : nodeManager.getAllNodes()) {
      diskBalancerReportHandler.onMessage(
          new DiskBalancerReportFromDatanode(dn, generateRandomReport()), null);
    }

    List<HddsProtos.DatanodeDiskBalancerInfoProto> reportProtoList =
        diskBalancerManager.getDiskBalancerReport(2,
            ClientVersion.CURRENT_VERSION);

    Assertions.assertEquals(2, reportProtoList.size());
    Assertions.assertTrue(
        reportProtoList.get(0).getCurrentVolumeDensitySum()
            >= reportProtoList.get(1).getCurrentVolumeDensitySum());
  }

  @Test
  public void testDatanodeDiskBalancerStatus() throws IOException {
    diskBalancerManager.addRunningDatanode(nodeManager.getAllNodes().get(0));
    diskBalancerManager.addRunningDatanode(nodeManager.getAllNodes().get(1));

    // Simulate users asking all status of 3 datanodes
    List<String> dns = nodeManager.getAllNodes().stream().map(
        DatanodeDetails::getIpAddress).collect(
        Collectors.toList());

    List<HddsProtos.DatanodeDiskBalancerInfoProto> statusProtoList =
        diskBalancerManager.getDiskBalancerStatus(dns, null,
            ClientVersion.CURRENT_VERSION);

    Assertions.assertEquals(3, statusProtoList.size());

    // Simulate users asking status of 1 datanodes
    dns = nodeManager.getAllNodes().stream().map(
        DatanodeDetails::getIpAddress).limit(1).collect(
        Collectors.toList());

    statusProtoList =
        diskBalancerManager.getDiskBalancerStatus(dns, null,
            ClientVersion.CURRENT_VERSION);

    Assertions.assertEquals(1, statusProtoList.size());
  }

  @Test
  public void testHandleDiskBalancerReportFromDatanode() {
    for (DatanodeDetails dn: nodeManager.getAllNodes()) {
      diskBalancerReportHandler.onMessage(
          new DiskBalancerReportFromDatanode(dn, generateRandomReport()), null);
    }

    Assertions.assertEquals(3, diskBalancerManager.getStatusMap().size());
  }

  private DiskBalancerReportProto generateRandomReport() {
    return DiskBalancerReportProto.newBuilder()
        .setIsRunning(random.nextBoolean())
        .setBalancedBytes(random.nextInt(10000))
        .setVolumeDataDensity(random.nextDouble())
        .setDiskBalancerConf(
            HddsProtos.DiskBalancerConfigurationProto.newBuilder()
                .setThreshold(random.nextInt(99))
                .setParallelThread(random.nextInt(4) + 1)
                .setDiskBandwidthInMB(random.nextInt(99) + 1)
                .build())
        .build();
  }
}
