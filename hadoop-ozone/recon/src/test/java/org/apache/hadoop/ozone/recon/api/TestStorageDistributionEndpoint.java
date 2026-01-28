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

package org.apache.hadoop.ozone.recon.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsServiceResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The TestStorageDistributionEndpoint class contains unit tests for verifying
 * the functionality of the {@link StorageDistributionEndpoint} class.
 *
 */
public class TestStorageDistributionEndpoint {
  private DataNodeMetricsService dataNodeMetricsService;
  private StorageDistributionEndpoint storageDistributionEndpoint;
  private ReconNodeManager nodeManager = mock(ReconNodeManager.class);

  @BeforeEach
  public void setup() {
    ReconGlobalMetricsService reconGlobalMetricsService = mock(ReconGlobalMetricsService.class);
    dataNodeMetricsService = mock(DataNodeMetricsService.class);
    NSSummaryEndpoint nssummaryEndpoint = mock(NSSummaryEndpoint.class);
    OzoneStorageContainerManager reconSCM = mock(OzoneStorageContainerManager.class);
    when(reconSCM.getScmNodeManager()).thenReturn(nodeManager);
    ReconGlobalStatsManager reconGlobalStatsManager = mock(ReconGlobalStatsManager.class);
    storageDistributionEndpoint = new StorageDistributionEndpoint(reconSCM,
        nssummaryEndpoint,
        reconGlobalStatsManager,
        reconGlobalMetricsService,
        dataNodeMetricsService);
  }

  @Test
  public void testDownloadReturnsAcceptedWhenCollectionInProgress() {
    DataNodeMetricsServiceResponse metricsResponse = DataNodeMetricsServiceResponse.newBuilder()
        .setStatus(DataNodeMetricsService.MetricCollectionStatus.IN_PROGRESS)
        .build();
    when(dataNodeMetricsService.getCollectedMetrics(null)).thenReturn(metricsResponse);
    Response response = storageDistributionEndpoint.downloadDataNodeDistribution();

    assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    assertEquals("application/json", response.getMediaType().toString());
    assertEquals(metricsResponse, response.getEntity());
  }

  @Test
  public void testDownloadReturnsServerErrorWhenMetricsMissing() {
    DataNodeMetricsServiceResponse metricsResponse = DataNodeMetricsServiceResponse.newBuilder()
        .setStatus(DataNodeMetricsService.MetricCollectionStatus.FINISHED)
        .build();
    when(dataNodeMetricsService.getCollectedMetrics(null)).thenReturn(metricsResponse);
    Response response = storageDistributionEndpoint.downloadDataNodeDistribution();

    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    assertEquals("Metrics data is missing despite FINISHED status.", response.getEntity());
    assertEquals("text/plain", response.getMediaType().toString());
  }

  @Test
  public void testDownloadReturnsCsvWithMetrics() throws Exception {
    // given
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();

    String dataNode1 = "dn1";
    String dataNode2 = "dn2";

    List<DatanodePendingDeletionMetrics> pendingDeletionMetrics = Arrays.asList(
        new DatanodePendingDeletionMetrics(dataNode1, uuid1.toString(), 10L),
        new DatanodePendingDeletionMetrics(dataNode2, uuid2.toString(), 20L)
    );

    DataNodeMetricsServiceResponse metricsResponse =
        DataNodeMetricsServiceResponse.newBuilder()
            .setStatus(DataNodeMetricsService.MetricCollectionStatus.FINISHED)
            .setPendingDeletion(pendingDeletionMetrics)
            .build();

    when(dataNodeMetricsService.getCollectedMetrics(null))
        .thenReturn(metricsResponse);

    mockDatanodeStorageReports(pendingDeletionMetrics);
    mockNodeManagerStats(uuid1, uuid2);

    Response response = storageDistributionEndpoint.downloadDataNodeDistribution();

    // then
    assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    assertEquals("text/csv", response.getMediaType().toString());
    assertEquals(
        "attachment; filename=\"datanode_storage_and_pending_deletion_stats.csv\"",
        response.getHeaderString("Content-Disposition")
    );

    String csv = readCsv(response);

    assertTrue(csv.contains(
        "HostName,Datanode UUID,Capacity,Used Space,Remaining Space," +
            "Committed Space,Reserved Space,Minimum Free Space,Pending Block Size"
    ));
    assertTrue(csv.contains(dataNode1 + "," + uuid1 + ",100,10,10,10,5,5,10"));
    assertTrue(csv.contains(dataNode2 + "," + uuid2 + ",100,10,10,10,5,5,20"));
  }

  private void mockDatanodeStorageReports(
      List<DatanodePendingDeletionMetrics> metrics) {

    List<DatanodeStorageReport> reports = metrics.stream()
        .map(m -> DatanodeStorageReport.newBuilder()
            .setDatanodeUuid(m.getDatanodeUuid())
            .setHostName(m.getHostName())
            .build())
        .collect(Collectors.toList());

    when(storageDistributionEndpoint.collectDatanodeReports())
        .thenReturn(reports);
  }

  private void mockNodeManagerStats(UUID uuid1, UUID uuid2) {
    DatanodeDetails dn1 = DatanodeDetails.newBuilder()
        .setUuid(uuid1)
        .setHostName("dn1")
        .build();

    DatanodeDetails dn2 = DatanodeDetails.newBuilder()
        .setUuid(uuid2)
        .setHostName("dn3")
        .build();

    List<DatanodeInfo> datanodes = Arrays.asList(
        new DatanodeInfo(dn1, null, null),
        new DatanodeInfo(dn2, null, null)
    );

    when(nodeManager.getAllNodes()).thenReturn(datanodes);
    when(nodeManager.getNodeStat(dn1))
        .thenReturn(new SCMNodeMetric(100, 10, 10, 10, 5, 5));
    when(nodeManager.getNodeStat(dn2))
        .thenReturn(new SCMNodeMetric(100, 10, 10, 10, 5, 5));
  }

  private String readCsv(Response response) throws Exception {
    StreamingOutput output = assertInstanceOf(StreamingOutput.class, response.getEntity());
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    output.write(outputStream);
    return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
  }
}
