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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeMetrics;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test cases to verify the metrics exposed by SCMNodeManager.
 */
public class TestSCMNodeMetrics {

  private static SCMNodeManager nodeManager;

  private static DatanodeDetails registeredDatanode;

  @BeforeClass
  public static void setup() throws Exception {

    OzoneConfiguration source = new OzoneConfiguration();
    EventQueue publisher = new EventQueue();
    SCMStorageConfig config =
        new SCMStorageConfig(NodeType.DATANODE, new File("/tmp"), "storage");
    nodeManager = new SCMNodeManager(source, config, publisher,
        new NetworkTopologyImpl(source));

    registeredDatanode = DatanodeDetails.newBuilder()
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .setUuid(UUID.randomUUID())
        .build();

    nodeManager.register(registeredDatanode, createNodeReport(),
        PipelineReportsProto.newBuilder().build());

  }

  @AfterClass
  public static void teardown() throws IOException {
    nodeManager.close();
  }

  /**
   * Verifies heartbeat processing count.
   *
   * @throws InterruptedException
   */
  @Test
  public void testHBProcessing() throws InterruptedException {
    long hbProcessed = getCounter("NumHBProcessed");

    NodeReportProto nodeReport = createNodeReport();

    nodeManager.processHeartbeat(registeredDatanode);

    assertEquals("NumHBProcessed", hbProcessed + 1,
        getCounter("NumHBProcessed"));
  }

  /**
   * Verifies heartbeat processing failure count.
   */
  @Test
  public void testHBProcessingFailure() {

    long hbProcessedFailed = getCounter("NumHBProcessingFailed");

    nodeManager.processHeartbeat(MockDatanodeDetails
        .randomDatanodeDetails());

    assertEquals("NumHBProcessingFailed", hbProcessedFailed + 1,
        getCounter("NumHBProcessingFailed"));
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
        TestUtils.createStorageReport(registeredDatanode.getUuid(), "/tmp", 100,
            10, 90,
            null);
    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();

    nodeManager.processNodeReport(registeredDatanode, nodeReport);
    Assert.assertEquals("NumNodeReportProcessed", nrProcessed + 1,
        getCounter("NumNodeReportProcessed"));
  }

  /**
   * Verifies node report processing failure count.
   */
  @Test
  public void testNodeReportProcessingFailure() {

    long nrProcessed = getCounter("NumNodeReportProcessingFailed");
    DatanodeDetails randomDatanode =
        MockDatanodeDetails.randomDatanodeDetails();

    StorageReportProto storageReport = TestUtils.createStorageReport(
        randomDatanode.getUuid(), "/tmp", 100, 10, 90, null);

    NodeReportProto nodeReport = NodeReportProto.newBuilder()
        .addStorageReport(storageReport).build();

    nodeManager.processNodeReport(randomDatanode, nodeReport);
    assertEquals("NumNodeReportProcessingFailed", nrProcessed + 1,
        getCounter("NumNodeReportProcessingFailed"));
  }

  /**
   * Verify that datanode aggregated state and capacity metrics are
   * reported.
   */
  @Test
  public void testNodeCountAndInfoMetricsReported() throws Exception {

    StorageReportProto storageReport = TestUtils.createStorageReport(
        registeredDatanode.getUuid(), "/tmp", 100, 10, 90, null);
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
    assertGauge("DiskCapacity", 100L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DiskUsed", 10L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("DiskRemaining", 90L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("SSDCapacity", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("SSDUsed", 0L,
        getMetrics(SCMNodeMetrics.class.getSimpleName()));
    assertGauge("SSDRemaining", 0L,
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
