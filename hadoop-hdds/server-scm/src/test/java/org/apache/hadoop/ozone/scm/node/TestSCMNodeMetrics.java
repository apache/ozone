/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.scm.node;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeMetrics;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import static java.lang.Thread.sleep;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
    HDDSLayoutVersionManager versionManager =
        Mockito.mock(HDDSLayoutVersionManager.class);
    Mockito.when(versionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    Mockito.when(versionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
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

    LayoutVersionManager versionManager = nodeManager.getLayoutVersionManager();
    LayoutVersionProto layoutInfo = LayoutVersionProto.newBuilder()
        .setSoftwareLayoutVersion(versionManager.getSoftwareLayoutVersion())
        .setMetadataLayoutVersion(versionManager.getMetadataLayoutVersion())
        .build();
    nodeManager.processHeartbeat(registeredDatanode, layoutInfo);

    assertEquals(hbProcessed + 1, getCounter("NumHBProcessed"),
        "NumHBProcessed");
  }

  /**
   * Verifies heartbeat processing failure count.
   */
  @Test
  public void testHBProcessingFailure() {

    long hbProcessedFailed = getCounter("NumHBProcessingFailed");

    LayoutVersionManager versionManager = nodeManager.getLayoutVersionManager();
    LayoutVersionProto layoutInfo = LayoutVersionProto.newBuilder()
        .setSoftwareLayoutVersion(versionManager.getSoftwareLayoutVersion())
        .setMetadataLayoutVersion(versionManager.getMetadataLayoutVersion())
        .build();
    nodeManager.processHeartbeat(MockDatanodeDetails
        .randomDatanodeDetails(), layoutInfo);

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
        HddsTestUtils.createStorageReport(registeredDatanode.getUuid(), "/tmp",
            100, 10, 90, null);
    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();

    nodeManager.processNodeReport(registeredDatanode, nodeReport);
    Assertions.assertEquals(nrProcessed + 1,
        getCounter("NumNodeReportProcessed"),
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
        randomDatanode.getUuid(), "/tmp", 100, 10, 90, null);

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
        registeredDatanode.getUuid(), "/tmp", 100, 10, 90, null);
    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();

    nodeManager.processNodeReport(registeredDatanode, nodeReport);

    MetricsRecordBuilder metricsSource = getMetrics(SCMNodeMetrics.SOURCE_NAME);

    assertGauge("InServiceHealthyNodes", 1,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("InServiceHealthyReadonlyNodes", 0,
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

    LayoutVersionManager versionManager = nodeManager.getLayoutVersionManager();
    LayoutVersionProto layoutInfo = LayoutVersionProto.newBuilder()
        .setSoftwareLayoutVersion(versionManager.getSoftwareLayoutVersion())
        .setMetadataLayoutVersion(versionManager.getMetadataLayoutVersion())
        .build();
    nodeManager.processHeartbeat(registeredDatanode, layoutInfo);
    sleep(4000);
    metricsSource = getMetrics(SCMNodeMetrics.SOURCE_NAME);
    assertGauge("InServiceHealthyReadonlyNodes", 0, metricsSource);
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
