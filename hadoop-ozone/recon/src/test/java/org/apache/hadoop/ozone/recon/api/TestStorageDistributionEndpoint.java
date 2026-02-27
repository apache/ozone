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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsServiceResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.StorageCapacityDistributionResponse;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The TestStorageDistributionEndpoint class contains unit tests for verifying
 * the functionality of the {@link StorageDistributionEndpoint} class.
 *
 */
public class TestStorageDistributionEndpoint {
  private static final int EXPECTED_GLOBAL_TOTAL_KEYS = 14;
  private static final long PENDING_DELETION_SIZE = 10L;
  private static final long FS_CAPACITY = 1000L;
  private static final long FS_USED = 500L;
  private static final long FS_AVAILABLE = 500L;
  private static final long RESERVED = 100L;
  private static final long MIN_FREE_SPACE = 100L;
  private static final long COMMITTED = 20L;
  private static final long PENDING_DIRECTORY_SIZE = 120L;
  private static final long PENDING_KEY_SIZE = 30L;
  private static final long OPEN_KEY_BYTES = 200L;
  private static final long OPEN_MPU_KEY_BYTES = 20L;
  private static final long NON_OZONE_USED_SPACE_BYTES = 100L;
  private static final long EXPECTED_COMMITTED_KEY_BYTES = 300L;
  private static final long GLOBAL_STAT_KEY_COUNT = 7L;
  private static final long OZONE_CAPACITY = FS_CAPACITY - RESERVED;
  private static final long OZONE_USED = FS_USED - NON_OZONE_USED_SPACE_BYTES;
  private static final long OZONE_REMAINING = FS_AVAILABLE - MIN_FREE_SPACE;
  private static final String CSV_SEPARATOR = ",";
  private static final String APPLICATION_JSON = "application/json";
  private static final String TEXT_PLAIN = "text/plain";
  private static final String TEXT_CSV = "text/csv";
  private static final String CONTENT_DISPOSITION = "Content-Disposition";
  private static final String DOWNLOAD_CONTENT_DISPOSITION =
      "attachment; filename=\"datanode_storage_and_pending_deletion_stats.csv\"";
  private static final String METRICS_MISSING_ERROR =
      "Metrics data is missing despite FINISHED status.";
  private static final String ROOT_PATH = "/";
  private static final String HOSTNAME_PREFIX = "datanode-";
  private static final String PENDING_DIRECTORY_SIZE_KEY = "pendingDirectorySize";
  private static final String PENDING_KEY_SIZE_KEY = "pendingKeySize";
  private static final String TOTAL_REPLICATED_DATA_SIZE_KEY = "totalReplicatedDataSize";

  private DataNodeMetricsService dataNodeMetricsService;
  private StorageDistributionEndpoint storageDistributionEndpoint;
  private ReconNodeManager nodeManager;
  private ReconGlobalMetricsService reconGlobalMetricsService;
  private NSSummaryEndpoint nssummaryEndpoint;
  private ReconGlobalStatsManager reconGlobalStatsManager;

  @BeforeEach
  public void setup() {
    reconGlobalMetricsService = mock(ReconGlobalMetricsService.class);
    nodeManager = mock(ReconNodeManager.class);
    dataNodeMetricsService = mock(DataNodeMetricsService.class);
    nssummaryEndpoint = mock(NSSummaryEndpoint.class);
    OzoneStorageContainerManager reconSCM = mock(OzoneStorageContainerManager.class);
    when(reconSCM.getScmNodeManager()).thenReturn(nodeManager);
    reconGlobalStatsManager = mock(ReconGlobalStatsManager.class);
    storageDistributionEndpoint = new StorageDistributionEndpoint(reconSCM,
        nssummaryEndpoint,
        reconGlobalStatsManager,
        reconGlobalMetricsService,
        dataNodeMetricsService);
  }

  @Test
  public void testStorageDistributionApiReturnsSuccess() throws Exception {
    mockStorageDistributionData(3);
    Response response = storageDistributionEndpoint.getStorageDistribution();
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse distributionResponse = assertInstanceOf(
        StorageCapacityDistributionResponse.class, response.getEntity());

    long totalNameSpace = PENDING_KEY_SIZE + PENDING_DIRECTORY_SIZE + OPEN_KEY_BYTES +
        OPEN_MPU_KEY_BYTES + EXPECTED_COMMITTED_KEY_BYTES;

    assertEquals(OZONE_USED * 3, distributionResponse.getGlobalStorage().getTotalUsedSpace());
    assertEquals(OZONE_REMAINING * 3, distributionResponse.getGlobalStorage().getTotalFreeSpace());
    assertEquals(OZONE_CAPACITY * 3, distributionResponse.getGlobalStorage().getTotalCapacity());
    assertEquals(totalNameSpace, distributionResponse.getGlobalNamespace().getTotalUsedSpace());
    assertEquals(EXPECTED_GLOBAL_TOTAL_KEYS, distributionResponse.getGlobalNamespace().getTotalKeys());
    assertEquals(OPEN_KEY_BYTES,
        distributionResponse.getUsedSpaceBreakDown().getOpenKeyBytes().getOpenKeyAndFileBytes());
    assertEquals(OPEN_MPU_KEY_BYTES,
        distributionResponse.getUsedSpaceBreakDown().getOpenKeyBytes().getMultipartOpenKeyBytes());
    assertEquals(OPEN_KEY_BYTES + OPEN_MPU_KEY_BYTES,
        distributionResponse.getUsedSpaceBreakDown().getOpenKeyBytes().getTotalOpenKeyBytes());
    assertEquals(EXPECTED_COMMITTED_KEY_BYTES,
        distributionResponse.getUsedSpaceBreakDown().getCommittedKeyBytes());
    assertEquals(COMMITTED * 3,
        distributionResponse.getUsedSpaceBreakDown().getPreAllocatedContainerBytes());
    for (int i = 0; i < 3; i++) {
      DatanodeStorageReport report = distributionResponse.getDataNodeUsage().get(i);
      assertEquals(OZONE_CAPACITY, report.getCapacity());
      assertEquals(OZONE_USED, report.getUsed());
      assertEquals(OZONE_REMAINING, report.getRemaining());
      assertEquals(COMMITTED, report.getCommitted());
      assertEquals(RESERVED, report.getReserved());
      assertEquals(MIN_FREE_SPACE, report.getMinimumFreeSpace());
      assertEquals(report.getHostName(), HOSTNAME_PREFIX + i);
      assertNotNull(report.getDatanodeUuid());
    }
  }

  @Test
  public void testDownloadReturnsAcceptedWhenCollectionInProgress() {
    DataNodeMetricsServiceResponse metricsResponse = DataNodeMetricsServiceResponse.newBuilder()
        .setStatus(DataNodeMetricsService.MetricCollectionStatus.IN_PROGRESS)
        .build();
    when(dataNodeMetricsService.getCollectedMetrics(null)).thenReturn(metricsResponse);
    Response response = storageDistributionEndpoint.downloadDataNodeStorageDistribution();

    assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    assertEquals(APPLICATION_JSON, response.getMediaType().toString());
    assertEquals(metricsResponse, response.getEntity());
  }

  @Test
  public void testDownloadReturnsServerErrorWhenMetricsMissing() {
    DataNodeMetricsServiceResponse metricsResponse = DataNodeMetricsServiceResponse.newBuilder()
        .setStatus(DataNodeMetricsService.MetricCollectionStatus.FINISHED)
        .build();
    when(dataNodeMetricsService.getCollectedMetrics(null)).thenReturn(metricsResponse);
    Response response = storageDistributionEndpoint.downloadDataNodeStorageDistribution();

    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    assertEquals(METRICS_MISSING_ERROR, response.getEntity());
    assertEquals(TEXT_PLAIN, response.getMediaType().toString());
  }

  @Test
  public void testDownloadReturnsCsvWithMetrics() throws Exception {

    List<String> csvRows = mockStorageDistributionData(3);
    Response response = storageDistributionEndpoint.downloadDataNodeStorageDistribution();

    // then
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(TEXT_CSV, response.getMediaType().toString());
    assertEquals(DOWNLOAD_CONTENT_DISPOSITION, response.getHeaderString(CONTENT_DISPOSITION));
    String csv = readCsv(response);
    for (String row : csvRows) {
      assertTrue(csv.contains(row));
    }
  }

  private List<String> mockStorageDistributionData(int numNodes) throws Exception {
    List<String> csvRows = new ArrayList<>();
    List<String> headers = Arrays.asList(
        "HostName",
        "Datanode UUID",
        "Filesystem Capacity",
        "Filesystem Used Space",
        "Filesystem Remaining Space",
        "Ozone Capacity",
        "Ozone Used Space",
        "Ozone Remaining Space",
        "PreAllocated Container Space",
        "Reserved Space",
        "Minimum Free Space",
        "Pending Block Size");
    csvRows.add(String.join(",", headers));

    List<DatanodePendingDeletionMetrics> pendingDeletionMetrics = new ArrayList<>();
    List<DatanodeInfo> dataNodes = new ArrayList<>();

    for (int i = 0; i < numNodes; i++) {
      UUID uuid = UUID.randomUUID();
      String hostName = HOSTNAME_PREFIX + i;
      DatanodeDetails datanode = DatanodeDetails.newBuilder()
          .setUuid(uuid)
          .setHostName(hostName)
          .build();
      pendingDeletionMetrics.add(new DatanodePendingDeletionMetrics(hostName,
          uuid.toString(), PENDING_DELETION_SIZE));
      dataNodes.add(new DatanodeInfo(datanode, null, null));
      when(nodeManager.getNodeStat(datanode))
          .thenReturn(new SCMNodeMetric(OZONE_CAPACITY, OZONE_USED, OZONE_REMAINING, COMMITTED,
              MIN_FREE_SPACE, RESERVED));
      when(nodeManager.getTotalFilesystemUsage(datanode))
          .thenReturn(new SpaceUsageSource.Fixed(FS_CAPACITY, FS_AVAILABLE, FS_USED));

      csvRows.add(String.join(CSV_SEPARATOR,
          Arrays.asList(hostName,
              uuid.toString(),
              String.valueOf(FS_CAPACITY),
              String.valueOf(FS_USED),
              String.valueOf(FS_AVAILABLE),
              String.valueOf(OZONE_CAPACITY),
              String.valueOf(OZONE_USED),
              String.valueOf(OZONE_REMAINING),
              String.valueOf(COMMITTED),
              String.valueOf(RESERVED),
              String.valueOf(MIN_FREE_SPACE),
              String.valueOf(PENDING_DELETION_SIZE))));

    }
    when(nodeManager.getAllNodes()).thenReturn(dataNodes);
    when(nodeManager.getStats())
        .thenReturn(new SCMNodeStat(
          OZONE_CAPACITY * numNodes,
            OZONE_USED * numNodes,
            OZONE_REMAINING * numNodes,
            COMMITTED * numNodes,
            MIN_FREE_SPACE * numNodes,
            RESERVED * numNodes));


    Map<String, Long> pendingSizes = new HashMap<>();
    pendingSizes.put(PENDING_DIRECTORY_SIZE_KEY, PENDING_DIRECTORY_SIZE);
    pendingSizes.put(PENDING_KEY_SIZE_KEY, PENDING_KEY_SIZE);

    when(reconGlobalMetricsService.calculatePendingSizes())
        .thenReturn(pendingSizes);

    when(reconGlobalMetricsService.getOpenKeySummary())
        .thenReturn(Collections.singletonMap(TOTAL_REPLICATED_DATA_SIZE_KEY, OPEN_KEY_BYTES));
    when(reconGlobalMetricsService.getMPUKeySummary())
        .thenReturn(Collections.singletonMap(TOTAL_REPLICATED_DATA_SIZE_KEY, OPEN_MPU_KEY_BYTES));

    DUResponse duResponse = new DUResponse();
    duResponse.setSizeWithReplica(EXPECTED_COMMITTED_KEY_BYTES);
    when(nssummaryEndpoint.getDiskUsage(ROOT_PATH, false, true, false))
        .thenReturn(Response.ok(duResponse).build());
    when(reconGlobalStatsManager.getGlobalStatsValue(anyString()))
        .thenReturn(new GlobalStatsValue(GLOBAL_STAT_KEY_COUNT));

    DataNodeMetricsServiceResponse metricsResponse =
        DataNodeMetricsServiceResponse.newBuilder()
            .setStatus(DataNodeMetricsService.MetricCollectionStatus.FINISHED)
            .setPendingDeletion(pendingDeletionMetrics)
            .build();
    when(dataNodeMetricsService.getCollectedMetrics(null))
        .thenReturn(metricsResponse);
    return csvRows;
  }

  private String readCsv(Response response) throws Exception {
    StreamingOutput output = assertInstanceOf(StreamingOutput.class, response.getEntity());
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    output.write(outputStream);
    return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
  }
}
