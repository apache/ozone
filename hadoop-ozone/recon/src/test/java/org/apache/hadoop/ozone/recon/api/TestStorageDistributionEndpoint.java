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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.scm.container.placement.metrics.LongMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.DeletionPendingBytesByStage;
import org.apache.hadoop.ozone.recon.api.types.GlobalNamespaceReport;
import org.apache.hadoop.ozone.recon.api.types.GlobalStorageReport;
import org.apache.hadoop.ozone.recon.api.types.StorageCapacityDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.UsedSpaceBreakDown;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.junit.jupiter.api.Test;

/**
 * Test class for StorageDistributionEndpoint, responsible for testing
 * the behavior and responses of the storage distribution endpoint in
 * different scenarios, including successful responses and exception cases.
 */
class StorageDistributionEndpointTest {

  private ReconNodeManager mockNodeManager;
  private OMDBInsightEndpoint mockOmdbInsightEndpoint;
  private NSSummaryEndpoint mockNsSummaryEndpoint;
  private StorageContainerLocationProtocol mockScmClient;
  private OzoneStorageContainerManager mockReconScm;
  private DatanodeInfo datanodeDetails;
  private SCMNodeStat mockNodeStat;
  private SCMNodeStat globalStats;
  private DUResponse mockDuResponse;

  @Test
  void testGetStorageDistributionSuccessfulResponse() throws IOException {
    setupMockDependencies();
    setupSuccessfulScenario();
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, mockScmClient);
    Response response = endpoint.getStorageDistribution();

    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse responsePayload = (StorageCapacityDistributionResponse) response.getEntity();
    assertNotNull(responsePayload);

    GlobalStorageReport globalStorage = responsePayload.getGlobalStorage();
    assertEquals(2000L, globalStorage.getTotalUsedSpace());
    assertEquals(3000L, globalStorage.getTotalFreeSpace());
    assertEquals(5000L, globalStorage.getTotalCapacity());

    GlobalNamespaceReport namespaceReport = responsePayload.getGlobalNamespace();
    assertEquals(500L, namespaceReport.getTotalUsedSpace());

    UsedSpaceBreakDown usedSpaceBreakDown = responsePayload.getUsedSpaceBreakDown();
    assertEquals(150L, usedSpaceBreakDown.getOpenKeysBytes());
    assertEquals(300L, usedSpaceBreakDown.getCommittedBytes());

    DeletionPendingBytesByStage deletionBreakdown = usedSpaceBreakDown.getDeletionPendingBytesByStage();
    assertEquals(50L, deletionBreakdown.getByStage().get("OM").get("totalBytes"));
    assertEquals(75L, deletionBreakdown.getByStage().get("SCM").get("pendingBytes"));
    assertEquals(0L, deletionBreakdown.getByStage().get("DN").get("pendingBytes"));
  }

  @Test
  void testGetStorageDistributionWithSCMExceptionResponse() throws IOException {
    setupMockDependencies();
    setupScmExceptionScenario();
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse responsePayload = (StorageCapacityDistributionResponse) response.getEntity();
    assertNotNull(responsePayload);
    GlobalStorageReport globalStorage = responsePayload.getGlobalStorage();
    assertEquals(2000L, globalStorage.getTotalUsedSpace());
    assertEquals(3000L, globalStorage.getTotalFreeSpace());
    assertEquals(5000L, globalStorage.getTotalCapacity());

    GlobalNamespaceReport namespaceReport = responsePayload.getGlobalNamespace();
    assertEquals(500L, namespaceReport.getTotalUsedSpace());

    UsedSpaceBreakDown usedSpaceBreakDown = responsePayload.getUsedSpaceBreakDown();
    assertEquals(150L, usedSpaceBreakDown.getOpenKeysBytes());
    assertEquals(300L, usedSpaceBreakDown.getCommittedBytes());

    DeletionPendingBytesByStage deletionBreakdown = usedSpaceBreakDown.getDeletionPendingBytesByStage();
    assertEquals(50L, deletionBreakdown.getByStage().get("OM").get("totalBytes"));
    assertEquals(0, deletionBreakdown.getByStage().get("SCM").get("pendingBytes"));
  }

  @Test
  void testGetStorageDistributionWithEmptyNodeList() throws IOException {
    setupMockDependencies();
    when(mockNodeManager.getAllNodes()).thenReturn(Collections.emptyList());
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse payload = (StorageCapacityDistributionResponse) response.getEntity();
    assertEquals(5000L, payload.getGlobalStorage().getTotalCapacity());
  }

  @Test
  void testGetStorageDistributionWithNullNodeStats() throws IOException {
    setupMockDependencies();
    when(mockNodeManager.getNodeStat(datanodeDetails)).thenReturn(null);
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse payload = (StorageCapacityDistributionResponse) response.getEntity();
    assertEquals(5000L, payload.getGlobalStorage().getTotalCapacity());
  }

  @Test
  void testGetStorageDistributionWithNullGlobalStats() throws IOException {
    setupMockDependencies();
    when(mockNodeManager.getStats()).thenReturn(null);
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse payload = (StorageCapacityDistributionResponse) response.getEntity();
    assertEquals(0L, payload.getGlobalStorage().getTotalCapacity());
  }

  @Test
  void testGetStorageDistributionWithUnreachableNodes() throws IOException {
    setupMockDependencies();
    setupUnreachableNodesScenario();
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse payload = (StorageCapacityDistributionResponse) response.getEntity();
    assertEquals(2500L, payload.getGlobalStorage().getTotalCapacity());
  }

  @Test
  void testGetStorageDistributionWithJmxMetricsFailure() throws IOException {
    setupMockDependencies();
    setupJmxMetricsFailureScenario();
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse payload = (StorageCapacityDistributionResponse) response.getEntity();
    assertEquals(0L, payload.getUsedSpaceBreakDown().getOpenKeysBytes());
  }

  private void setupMockDependencies() throws IOException {
    mockNodeManager = mock(ReconNodeManager.class);
    mockOmdbInsightEndpoint = mock(OMDBInsightEndpoint.class);
    mockNsSummaryEndpoint = mock(NSSummaryEndpoint.class);
    mockScmClient = mock(StorageContainerLocationProtocol.class);
    mockReconScm = mock(OzoneStorageContainerManager.class);
    when(mockReconScm.getScmNodeManager()).thenReturn(mockNodeManager);
    datanodeDetails = mock(DatanodeInfo.class);
    mockNodeStat = mock(SCMNodeStat.class);
    when(mockNodeManager.getAllNodes()).thenReturn(Collections.singletonList(datanodeDetails));
    when(mockNodeManager.getNodeStat(datanodeDetails)).thenReturn(new SCMNodeMetric(mockNodeStat));
    when(mockNodeStat.getCapacity()).thenReturn(new LongMetric(5000L));
    when(mockNodeStat.getScmUsed()).thenReturn(new LongMetric(2000L));
    when(mockNodeStat.getRemaining()).thenReturn(new LongMetric(3000L));
    when(mockNodeStat.getCommitted()).thenReturn(new LongMetric(100L));

    globalStats = mock(SCMNodeStat.class);
    when(mockNodeManager.getStats()).thenReturn(globalStats);
    when(globalStats.getScmUsed()).thenReturn(new LongMetric(2000L));
    when(globalStats.getCapacity()).thenReturn(new LongMetric(5000L));
    when(globalStats.getRemaining()).thenReturn(new LongMetric(3000L));

    Map<String, Long> pendingDeletedDirSizes = new HashMap<>();
    pendingDeletedDirSizes.put("totalReplicatedDataSize", 50L);
    doAnswer(invocation -> {
      ((Map<String, Long>) invocation.getArgument(0)).putAll(pendingDeletedDirSizes);
      return null;
    }).when(mockOmdbInsightEndpoint).calculateTotalPendingDeletedDirSizes(anyMap());

    Map<String, Long> openKeySummary = new HashMap<>();
    openKeySummary.put("totalReplicatedDataSize", 150L);
    doAnswer(invocation -> {
      ((Map<String, Long>) invocation.getArgument(0)).putAll(openKeySummary);
      return null;
    }).when(mockOmdbInsightEndpoint).createKeysSummaryForOpenKey(anyMap());

    mockDuResponse = mock(DUResponse.class);
    when(mockDuResponse.getSizeWithReplica()).thenReturn(300L);
    when(mockNsSummaryEndpoint.getDiskUsage(eq("/"), eq(false), eq(true), eq(false)))
        .thenReturn(Response.ok(mockDuResponse).build());
  }

  private void setupSuccessfulScenario() throws IOException {
    DeletedBlocksTransactionSummary scmSummary = mock(DeletedBlocksTransactionSummary.class);
    when(scmSummary.getTotalBlockReplicatedSize()).thenReturn(75L);
    when(scmSummary.getTotalBlockSize()).thenReturn(25L);
    when(mockScmClient.getDeletedBlockSummary()).thenReturn(scmSummary);
  }

  private void setupScmExceptionScenario() throws IOException {
    when(mockScmClient.getDeletedBlockSummary()).thenThrow(new IOException("Test Exception"));
  }

  private void setupUnreachableNodesScenario() {
    DatanodeInfo unreachableNode = mock(DatanodeInfo.class);
    List<DatanodeInfo> allNodes = new ArrayList<>();
    allNodes.add(datanodeDetails);
    allNodes.add(unreachableNode);
    when(mockNodeManager.getAllNodes()).thenReturn(allNodes);
    when(mockNodeManager.getNodeStat(unreachableNode)).thenReturn(null);
    when(globalStats.getCapacity()).thenReturn(new LongMetric(2500L));
  }

  private void setupJmxMetricsFailureScenario() {
    doThrow(new RuntimeException("JMX Metrics Failure"))
        .when(mockOmdbInsightEndpoint).createKeysSummaryForOpenKey(anyMap());
  }
}
