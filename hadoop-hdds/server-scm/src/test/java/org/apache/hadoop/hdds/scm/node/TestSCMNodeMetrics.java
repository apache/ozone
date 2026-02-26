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

import static java.lang.Thread.sleep;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getLongCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify the metrics exposed by SCMNodeManager.
 */
public class TestSCMNodeMetrics {

  private static SCMNodeManager nodeManager;

  private static DatanodeDetails registeredDatanode;

  @BeforeAll
  public static void setup() throws Exception {

    OzoneConfiguration source = new OzoneConfiguration();
    EventQueue publisher = new EventQueue();
    SCMStorageConfig config =
        new SCMStorageConfig(NodeType.DATANODE, new File("/tmp"), "storage");
    HDDSLayoutVersionManager versionManager = mock(HDDSLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion()).thenReturn(maxLayoutVersion());
    when(versionManager.getSoftwareLayoutVersion()).thenReturn(maxLayoutVersion());
    nodeManager = new SCMNodeManager(source, config, publisher,
        new NetworkTopologyImpl(source), SCMContext.emptyContext(),
            versionManager);

    registeredDatanode = DatanodeDetails.newBuilder()
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .setUuid(UUID.randomUUID())
        .build();

    nodeManager.register(registeredDatanode, createNodeReport(),
        PipelineReportsProto.newBuilder().build());

  }

  @AfterAll
  public static void teardown() throws IOException {
    nodeManager.close();
  }

  /**
   * Verifies heartbeat processing count.
   *
   */
  @Test
  public void testHBProcessing() throws InterruptedException {
    long hbProcessed = getCounter("NumHBProcessed");
    createNodeReport();
    nodeManager.processHeartbeat(registeredDatanode);
    assertEquals(hbProcessed + 1, getCounter("NumHBProcessed"),
        "NumHBProcessed");
  }

  /**
   * Verifies heartbeat processing failure count.
   */
  @Test
  public void testHBProcessingFailure() {
    long hbProcessedFailed = getCounter("NumHBProcessingFailed");
    nodeManager.processHeartbeat(MockDatanodeDetails.randomDatanodeDetails());
    assertEquals(hbProcessedFailed + 1, getCounter("NumHBProcessingFailed"),
        "NumHBProcessingFailed");
  }

  /**
   * Verifies node report processing count.
   *
   * @throws InterruptedException
   */
  @Test
  public void testNodeReportProcessing() throws InterruptedException {

    long nrProcessed = getCounter("NumNodeReportProcessed");

    StorageReportProto storageReport =
        HddsTestUtils.createStorageReport(registeredDatanode.getID(), "/tmp",
            100, 10, 90, null);
    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();

    nodeManager.processNodeReport(registeredDatanode, nodeReport);
    assertEquals(nrProcessed + 1, getCounter("NumNodeReportProcessed"),
        "NumNodeReportProcessed");
  }

  /**
   * Verifies node report processing failure count.
   */
  @Test
  public void testNodeReportProcessingFailure() {

    long nrProcessed = getCounter("NumNodeReportProcessingFailed");
    DatanodeDetails randomDatanode =
        MockDatanodeDetails.randomDatanodeDetails();

    StorageReportProto storageReport = HddsTestUtils.createStorageReport(
        randomDatanode.getID(), "/tmp", 100, 10, 90, null);

    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();

    nodeManager.processNodeReport(randomDatanode, nodeReport);
    assertEquals(nrProcessed + 1, getCounter("NumNodeReportProcessingFailed"),
        "NumNodeReportProcessingFailed");
  }

  /**
   * Verify that datanode aggregated state and capacity metrics are
   * reported.
   */
  @Test
  public void testNodeCountAndInfoMetricsReported() throws Exception {

    StorageReportProto storageReport = HddsTestUtils.createStorageReport(
        registeredDatanode.getID(), "/tmp", 100, 10, 90, null)
        .toBuilder()
        .setFsCapacity(200)
        .setFsAvailable(150)
        .build();
    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();

    nodeManager.processNodeReport(registeredDatanode, nodeReport);

    MetricsRecordBuilder metricsSource = getMetrics(SCMNodeMetrics.SOURCE_NAME);

    assertGauge("InServiceHealthyNodes", 1,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("InServiceStaleNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("InServiceDeadNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissioningHealthyNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissioningStaleNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissioningDeadNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedHealthyNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedStaleNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedDeadNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("EnteringMaintenanceHealthyNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("EnteringMaintenanceStaleNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("EnteringMaintenanceDeadNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("InMaintenanceHealthyNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("InMaintenanceStaleNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("InMaintenanceDeadNodes", 0,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("MaintenanceDiskCapacity", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("MaintenanceDiskUsed", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("MaintenanceDiskRemaining", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("MaintenanceSSDCapacity", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("MaintenanceSSDUsed", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("MaintenanceSSDRemaining", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedDiskCapacity", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedDiskUsed", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedDiskRemaining", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedSSDCapacity", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedSSDUsed", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DecommissionedSSDRemaining", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("AllNodes", 1,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    // The DN has no metadata volumes, so hasEnoughSpace() returns false indicating the DN is out of space.
    assertGauge("NonWritableNodes", 1,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("TotalOzoneCapacity", 100L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("TotalOzoneUsed", 10L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("TotalFilesystemCapacity", 200L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("TotalFilesystemUsed", 50L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("TotalFilesystemAvailable", 150L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    nodeManager.processHeartbeat(registeredDatanode);
    sleep(4000);
    metricsSource = getMetrics(SCMNodeMetrics.SOURCE_NAME);
    assertGauge("InServiceHealthyNodes", 1, metricsSource);

  }

  private long getCounter(String metricName) {
    return getLongCounter(metricName, getMetrics(SCMNodeMetrics.SOURCE_NAME));
  }

  private static NodeReportProto createNodeReport() {
    return NodeReportProto.newBuilder()
        .addStorageReport(
            StorageReportProto.newBuilder()
                .setCapacity(1)
                .setStorageUuid(UUID.randomUUID().toString())
                .setStorageLocation("/tmp")
                .build())
        .build();
  }
}
