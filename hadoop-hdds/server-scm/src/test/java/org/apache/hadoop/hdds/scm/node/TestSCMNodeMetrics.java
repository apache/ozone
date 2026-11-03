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

import static org.apache.hadoop.hdds.scm.upgrade.ScmUpgradeTestUtils.mockVersionManager;
import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getLongCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
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
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify the metrics exposed by SCMNodeManager.
 */
public class TestSCMNodeMetrics {

  private static SCMNodeManager nodeManager;
  private static SCMNodeMetrics metrics;

  private static DatanodeDetails registeredDatanode;

  @BeforeAll
  public static void setup() throws Exception {

    OzoneConfiguration source = new OzoneConfiguration();
    EventQueue publisher = new EventQueue();
    SCMStorageConfig config =
        new SCMStorageConfig(NodeType.DATANODE, new File("/tmp"), "storage");
    ScmVersionManager versionManager = mockVersionManager();
    nodeManager = new SCMNodeManager(source, config, publisher,
        new NetworkTopologyImpl(source), SCMContext.emptyContext(),
            versionManager);

    Field metricsField = SCMNodeManager.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metrics = (SCMNodeMetrics) metricsField.get(nodeManager);

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

    MetricsRecordBuilder metricsSource = getMetrics(metrics);

    assertGauge("InServiceHealthyNodes", 1,
        getMetrics(metrics));
    assertGauge("InServiceStaleNodes", 0,
        getMetrics(metrics));
    assertGauge("InServiceDeadNodes", 0,
        getMetrics(metrics));
    assertGauge("DecommissioningHealthyNodes", 0,
        getMetrics(metrics));
    assertGauge("DecommissioningStaleNodes", 0,
        getMetrics(metrics));
    assertGauge("DecommissioningDeadNodes", 0,
        getMetrics(metrics));
    assertGauge("DecommissionedHealthyNodes", 0,
        getMetrics(metrics));
    assertGauge("DecommissionedStaleNodes", 0,
        getMetrics(metrics));
    assertGauge("DecommissionedDeadNodes", 0,
        getMetrics(metrics));
    assertGauge("EnteringMaintenanceHealthyNodes", 0,
        getMetrics(metrics));
    assertGauge("EnteringMaintenanceStaleNodes", 0,
        getMetrics(metrics));
    assertGauge("EnteringMaintenanceDeadNodes", 0,
        getMetrics(metrics));
    assertGauge("InMaintenanceHealthyNodes", 0,
        getMetrics(metrics));
    assertGauge("InMaintenanceStaleNodes", 0,
        getMetrics(metrics));
    assertGauge("InMaintenanceDeadNodes", 0,
        getMetrics(metrics));
    assertGauge("MaintenanceDiskCapacity", 0L,
        getMetrics(metrics));
    assertGauge("MaintenanceDiskUsed", 0L,
        getMetrics(metrics));
    assertGauge("MaintenanceDiskRemaining", 0L,
        getMetrics(metrics));
    assertGauge("MaintenanceSSDCapacity", 0L,
        getMetrics(metrics));
    assertGauge("MaintenanceSSDUsed", 0L,
        getMetrics(metrics));
    assertGauge("MaintenanceSSDRemaining", 0L,
        getMetrics(metrics));
    assertGauge("DecommissionedDiskCapacity", 0L,
        getMetrics(metrics));
    assertGauge("DecommissionedDiskUsed", 0L,
        getMetrics(metrics));
    assertGauge("DecommissionedDiskRemaining", 0L,
        getMetrics(metrics));
    assertGauge("DecommissionedSSDCapacity", 0L,
        getMetrics(metrics));
    assertGauge("DecommissionedSSDUsed", 0L,
        getMetrics(metrics));
    assertGauge("DecommissionedSSDRemaining", 0L,
        getMetrics(metrics));
    assertGauge("AllNodes", 1,
        getMetrics(metrics));
    // The DN has no metadata volumes, so hasEnoughSpace() returns false indicating the DN is out of space.
    assertGauge("NonWritableNodes", 1,
        getMetrics(metrics));
    assertGauge("TotalOzoneCapacity", 100L,
        getMetrics(metrics));
    assertGauge("TotalOzoneUsed", 10L,
        getMetrics(metrics));
    assertGauge("TotalFilesystemCapacity", 200L,
        getMetrics(metrics));
    assertGauge("TotalFilesystemUsed", 50L,
        getMetrics(metrics));
    assertGauge("TotalFilesystemAvailable", 150L,
        getMetrics(metrics));
    nodeManager.processHeartbeat(registeredDatanode);
    GenericTestUtils.waitFor(
        () -> nodeManager.getNodeCount(NodeStatus.inServiceHealthy()) == 1, 100, 5000);
    metricsSource = getMetrics(metrics);
    assertGauge("InServiceHealthyNodes", 1, metricsSource);

  }

  private long getCounter(String metricName) {
    return getLongCounter(metricName, getMetrics(metrics));
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
