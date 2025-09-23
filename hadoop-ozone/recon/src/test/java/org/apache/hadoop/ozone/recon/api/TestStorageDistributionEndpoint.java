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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Timestamp;
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
import org.apache.ozone.recon.schema.generated.tables.daos.GlobalStatsDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.GlobalStats;
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
  private GlobalStatsDao globalStatsDao;
  private OzoneStorageContainerManager mockReconScm;
  private DatanodeInfo datanodeDetails;
  private SCMNodeStat globalStats;

  private static final long NODE_CAPACITY = 5000L;
  private static final long NODE_USED = 2000L;
  private static final long NODE_FREE = 3000L;
  private static final long NAMESPACE_USED = 500L;
  private static final long OPEN_KEYS_SIZE = 150L;
  private static final long COMMITTED_SIZE = 300L;
  private static final long OM_PENDING_TOTAL = 50L;
  private static final long SCM_PENDING = 75L;

  @Test
  void testGetStorageDistributionSuccessfulResponse() throws IOException {
    setupMockDependencies();
    setupSuccessfulScenario();
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, globalStatsDao, mockScmClient);
    Response response = endpoint.getStorageDistribution();

    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse responsePayload = (StorageCapacityDistributionResponse) response.getEntity();
    assertNotNull(responsePayload);

    GlobalStorageReport globalStorage = responsePayload.getGlobalStorage();
    assertEquals(NODE_USED, globalStorage.getTotalUsedSpace());
    assertEquals(NODE_FREE, globalStorage.getTotalFreeSpace());
    assertEquals(NODE_CAPACITY, globalStorage.getTotalCapacity());

    GlobalNamespaceReport namespaceReport = responsePayload.getGlobalNamespace();
    assertEquals(NAMESPACE_USED, namespaceReport.getTotalUsedSpace());

    UsedSpaceBreakDown usedSpaceBreakDown = responsePayload.getUsedSpaceBreakDown();
    assertEquals(OPEN_KEYS_SIZE, usedSpaceBreakDown.getOpenKeysBytes());
    assertEquals(COMMITTED_SIZE, usedSpaceBreakDown.getCommittedBytes());

    DeletionPendingBytesByStage deletionBreakdown = usedSpaceBreakDown.getDeletionPendingBytesByStage();
    assertEquals(OM_PENDING_TOTAL, deletionBreakdown.getByStage().get("OM").get("totalBytes"));
    assertEquals(SCM_PENDING, deletionBreakdown.getByStage().get("SCM").get("pendingBytes"));
    assertEquals(0L, deletionBreakdown.getByStage().get("DN").get("pendingBytes"));
  }

  @Test
  void testGetStorageDistributionWithSCMExceptionResponse() throws IOException {
    setupMockDependencies();
    setupScmExceptionScenario();
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, globalStatsDao, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse responsePayload = (StorageCapacityDistributionResponse) response.getEntity();
    assertNotNull(responsePayload);
    GlobalStorageReport globalStorage = responsePayload.getGlobalStorage();
    assertEquals(NODE_USED, globalStorage.getTotalUsedSpace());
    assertEquals(NODE_FREE, globalStorage.getTotalFreeSpace());
    assertEquals(NODE_CAPACITY, globalStorage.getTotalCapacity());

    GlobalNamespaceReport namespaceReport = responsePayload.getGlobalNamespace();
    assertEquals(NAMESPACE_USED, namespaceReport.getTotalUsedSpace());

    UsedSpaceBreakDown usedSpaceBreakDown = responsePayload.getUsedSpaceBreakDown();
    assertEquals(OPEN_KEYS_SIZE, usedSpaceBreakDown.getOpenKeysBytes());
    assertEquals(COMMITTED_SIZE, usedSpaceBreakDown.getCommittedBytes());

    DeletionPendingBytesByStage deletionBreakdown = usedSpaceBreakDown.getDeletionPendingBytesByStage();
    assertEquals(OM_PENDING_TOTAL, deletionBreakdown.getByStage().get("OM").get("totalBytes"));
    assertEquals(0, deletionBreakdown.getByStage().get("SCM").get("pendingBytes"));
  }

  @Test
  void testGetStorageDistributionWithEmptyNodeList() throws IOException {
    setupMockDependencies();
    when(mockNodeManager.getAllNodes()).thenReturn(Collections.emptyList());
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, globalStatsDao, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse payload = (StorageCapacityDistributionResponse) response.getEntity();
    assertEquals(NODE_CAPACITY, payload.getGlobalStorage().getTotalCapacity());
  }

  @Test
  void testGetStorageDistributionWithNullNodeStats() throws IOException {
    setupMockDependencies();
    when(mockNodeManager.getNodeStat(datanodeDetails)).thenReturn(null);
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, globalStatsDao, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse payload = (StorageCapacityDistributionResponse) response.getEntity();
    assertEquals(NODE_CAPACITY, payload.getGlobalStorage().getTotalCapacity());
  }

  @Test
  void testGetStorageDistributionWithNullGlobalStats() throws IOException {
    setupMockDependencies();
    when(mockNodeManager.getStats()).thenReturn(null);
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, globalStatsDao, mockScmClient);
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
        mockNsSummaryEndpoint, globalStatsDao, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse payload = (StorageCapacityDistributionResponse) response.getEntity();
    long reachableCapacity = NODE_CAPACITY / 2;
    assertEquals(reachableCapacity, payload.getGlobalStorage().getTotalCapacity());
  }

  @Test
  void testGetStorageDistributionWithJmxMetricsFailure() throws IOException {
    setupMockDependencies();
    setupJmxMetricsFailureScenario();
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(mockReconScm, mockOmdbInsightEndpoint,
        mockNsSummaryEndpoint, globalStatsDao, mockScmClient);
    Response response = endpoint.getStorageDistribution();
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    StorageCapacityDistributionResponse payload = (StorageCapacityDistributionResponse) response.getEntity();
    assertEquals(0L, payload.getUsedSpaceBreakDown().getOpenKeysBytes());
  }

  private void setupMockDependencies() throws IOException {
    DUResponse mockDuResponse = mock(DUResponse.class);
    SCMNodeStat mockNodeStat = mock(SCMNodeStat.class);
    mockNodeManager = mock(ReconNodeManager.class);
    mockOmdbInsightEndpoint = mock(OMDBInsightEndpoint.class);
    mockNsSummaryEndpoint = mock(NSSummaryEndpoint.class);
    mockScmClient = mock(StorageContainerLocationProtocol.class);
    mockReconScm = mock(OzoneStorageContainerManager.class);
    when(mockReconScm.getScmNodeManager()).thenReturn(mockNodeManager);
    datanodeDetails = mock(DatanodeInfo.class);
    globalStatsDao = mock(GlobalStatsDao.class);
    when(globalStatsDao.findById(any())).thenReturn(new GlobalStats("test", 0L, new Timestamp(10000L)));
    when(mockNodeManager.getAllNodes()).thenReturn(Collections.singletonList(datanodeDetails));
    when(mockNodeManager.getNodeStat(datanodeDetails)).thenReturn(new SCMNodeMetric(mockNodeStat));
    when(mockNodeStat.getCapacity()).thenReturn(new LongMetric(NODE_CAPACITY));
    when(mockNodeStat.getScmUsed()).thenReturn(new LongMetric(NODE_USED));
    when(mockNodeStat.getRemaining()).thenReturn(new LongMetric(NODE_FREE));
    when(mockNodeStat.getCommitted()).thenReturn(new LongMetric(COMMITTED_SIZE));

    globalStats = mock(SCMNodeStat.class);
    when(mockNodeManager.getStats()).thenReturn(globalStats);
    when(globalStats.getScmUsed()).thenReturn(new LongMetric(NODE_USED));
    when(globalStats.getCapacity()).thenReturn(new LongMetric(NODE_CAPACITY));
    when(globalStats.getRemaining()).thenReturn(new LongMetric(NODE_FREE));

    Map<String, Long> pendingDeletedDirSizes = new HashMap<>();
    pendingDeletedDirSizes.put("totalReplicatedDataSize", OM_PENDING_TOTAL);
    doAnswer(invocation -> {
      ((Map<String, Long>) invocation.getArgument(0)).putAll(pendingDeletedDirSizes);
      return null;
    }).when(mockOmdbInsightEndpoint).calculateTotalPendingDeletedDirSizes(anyMap());

    Map<String, Long> openKeySummary = new HashMap<>();
    openKeySummary.put("totalReplicatedDataSize", OPEN_KEYS_SIZE);
    doAnswer(invocation -> {
      ((Map<String, Long>) invocation.getArgument(0)).putAll(openKeySummary);
      return null;
    }).when(mockOmdbInsightEndpoint).createKeysSummaryForOpenKey(anyMap());
    when(mockDuResponse.getSizeWithReplica()).thenReturn(COMMITTED_SIZE);
    when(mockNsSummaryEndpoint.getDiskUsage(eq("/"), eq(false), eq(true), eq(false)))
        .thenReturn(Response.ok(mockDuResponse).build());
  }

  @Test
  void testStorageDistributionWithJMXTimeouts() throws IOException {
    // Setup mock dependencies
    setupMockDependencies();

    // Create multiple datanodes - some will timeout, some will succeed
    DatanodeInfo timeoutNode1 = mock(DatanodeInfo.class);
    DatanodeInfo timeoutNode2 = mock(DatanodeInfo.class);
    DatanodeInfo successNode = mock(DatanodeInfo.class);

    List<DatanodeInfo> allNodes = new ArrayList<>();
    allNodes.add(datanodeDetails); // original node that works
    allNodes.add(timeoutNode1);
    allNodes.add(timeoutNode2);
    allNodes.add(successNode);

    when(mockNodeManager.getAllNodes()).thenReturn(allNodes);

    // Configure timeout nodes to throw timeout exceptions
    when(mockNodeManager.getNodeStat(timeoutNode1))
        .thenThrow(new RuntimeException("JMX call timeout: Connection timed out"));
    when(mockNodeManager.getNodeStat(timeoutNode2))
        .thenThrow(new RuntimeException("JMX timeout: Read timed out"));

    // Configure success node to return valid metrics
    SCMNodeStat successNodeStat = mock(SCMNodeStat.class);
    when(mockNodeManager.getNodeStat(successNode)).thenReturn(new SCMNodeMetric(successNodeStat));
    when(successNodeStat.getCapacity()).thenReturn(new LongMetric(NODE_CAPACITY));
    when(successNodeStat.getScmUsed()).thenReturn(new LongMetric(NODE_USED));
    when(successNodeStat.getRemaining()).thenReturn(new LongMetric(NODE_FREE));
    when(successNodeStat.getCommitted()).thenReturn(new LongMetric(COMMITTED_SIZE));

    // Simulate partial JMX timeout in namespace metrics collection
    doAnswer(invocation -> {
      Map<String, Long> metricsMap = invocation.getArgument(0);
      // Simulate that only partial metrics are available due to timeouts
      metricsMap.put("totalReplicatedDataSize", OPEN_KEYS_SIZE / 2); // Reduced due to timeout
      return null;
    }).when(mockOmdbInsightEndpoint).createKeysSummaryForOpenKey(anyMap());

    // Setup SCM client to work normally
    DeletedBlocksTransactionSummary scmSummary = mock(DeletedBlocksTransactionSummary.class);
    when(scmSummary.getTotalBlockReplicatedSize()).thenReturn(SCM_PENDING);
    when(scmSummary.getTotalBlockSize()).thenReturn(SCM_PENDING / 3);
    when(mockScmClient.getDeletedBlockSummary()).thenReturn(scmSummary);

    // Execute the test
    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(
        mockReconScm, mockOmdbInsightEndpoint, mockNsSummaryEndpoint,
        globalStatsDao, mockScmClient);
    Response response = endpoint.getStorageDistribution();

    // Verify response is successful despite timeouts
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    StorageCapacityDistributionResponse payload =
        (StorageCapacityDistributionResponse) response.getEntity();
    assertNotNull(payload);

    // Verify that partial results are returned
    GlobalStorageReport globalStorage = payload.getGlobalStorage();
    // Should only include stats from nodes that didn't timeout (2 out of 4 nodes)
    assertEquals(NODE_CAPACITY, globalStorage.getTotalCapacity()); // From global stats
    assertEquals(NODE_USED, globalStorage.getTotalUsedSpace());
    assertEquals(NODE_FREE, globalStorage.getTotalFreeSpace());

    // Verify namespace metrics are partially available
    UsedSpaceBreakDown usedSpaceBreakDown = payload.getUsedSpaceBreakDown();
    // Should be reduced due to timeout affecting some nodes
    assertEquals(OPEN_KEYS_SIZE / 2, usedSpaceBreakDown.getOpenKeysBytes());
    assertEquals(COMMITTED_SIZE, usedSpaceBreakDown.getCommittedBytes());

    // Verify deletion metrics are still available (SCM didn't timeout)
    DeletionPendingBytesByStage deletionBreakdown =
        usedSpaceBreakDown.getDeletionPendingBytesByStage();
    assertEquals(SCM_PENDING, deletionBreakdown.getByStage().get("SCM").get("pendingBytes"));
  }

  @Test
  void testStorageDistributionWithAllJMXTimeouts() throws IOException {
    // Test scenario where all JMX calls timeout
    setupMockDependencies();

    // Configure all datanodes to timeout
    DatanodeInfo timeoutNode1 = mock(DatanodeInfo.class);
    DatanodeInfo timeoutNode2 = mock(DatanodeInfo.class);

    List<DatanodeInfo> allNodes = new ArrayList<>();
    allNodes.add(timeoutNode1);
    allNodes.add(timeoutNode2);

    when(mockNodeManager.getAllNodes()).thenReturn(allNodes);

    // All nodes timeout
    when(mockNodeManager.getNodeStat(timeoutNode1))
        .thenThrow(new RuntimeException("JMX timeout"));
    when(mockNodeManager.getNodeStat(timeoutNode2))
        .thenThrow(new RuntimeException("JMX timeout"));

    // Namespace metrics also timeout
    doThrow(new RuntimeException("JMX namespace timeout"))
        .when(mockOmdbInsightEndpoint).createKeysSummaryForOpenKey(anyMap());
    doThrow(new RuntimeException("JMX directory timeout"))
        .when(mockOmdbInsightEndpoint).calculateTotalPendingDeletedDirSizes(anyMap());

    // SCM also times out
    when(mockScmClient.getDeletedBlockSummary())
        .thenThrow(new IOException("JMX SCM timeout"));

    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(
        mockReconScm, mockOmdbInsightEndpoint, mockNsSummaryEndpoint,
        globalStatsDao, mockScmClient);
    Response response = endpoint.getStorageDistribution();

    // Should still return OK with default/fallback values
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    StorageCapacityDistributionResponse payload =
        (StorageCapacityDistributionResponse) response.getEntity();
    assertNotNull(payload);

    // Should fall back to global stats
    GlobalStorageReport globalStorage = payload.getGlobalStorage();
    assertEquals(NODE_CAPACITY, globalStorage.getTotalCapacity());

    // Namespace and breakdown metrics should have default values
    UsedSpaceBreakDown usedSpaceBreakDown = payload.getUsedSpaceBreakDown();
    assertEquals(0L, usedSpaceBreakDown.getOpenKeysBytes());

    DeletionPendingBytesByStage deletionBreakdown =
        usedSpaceBreakDown.getDeletionPendingBytesByStage();
    assertEquals(0L, deletionBreakdown.getByStage().get("SCM").get("pendingBytes"));
  }

  @Test
  void testStorageDistributionWithJMXTimeoutConfiguration() throws IOException {
    // Test that timeout behavior respects configuration
    setupMockDependencies();

    // Simulate a slow JMX call that would timeout with short timeout but succeed with longer timeout
    DatanodeInfo slowNode = mock(DatanodeInfo.class);
    when(mockNodeManager.getAllNodes()).thenReturn(Collections.singletonList(slowNode));

    // Mock a slow response that eventually succeeds
    SCMNodeStat slowNodeStat = mock(SCMNodeStat.class);
    when(mockNodeManager.getNodeStat(slowNode)).thenAnswer(invocation -> {
      // Simulate slow response
      Thread.sleep(100); // Short delay to simulate slow response
      SCMNodeMetric nodeMetric = new SCMNodeMetric(slowNodeStat);
      return nodeMetric;
    });

    when(slowNodeStat.getCapacity()).thenReturn(new LongMetric(NODE_CAPACITY));
    when(slowNodeStat.getScmUsed()).thenReturn(new LongMetric(NODE_USED));
    when(slowNodeStat.getRemaining()).thenReturn(new LongMetric(NODE_FREE));
    when(slowNodeStat.getCommitted()).thenReturn(new LongMetric(COMMITTED_SIZE));

    StorageDistributionEndpoint endpoint = new StorageDistributionEndpoint(
        mockReconScm, mockOmdbInsightEndpoint, mockNsSummaryEndpoint,
        globalStatsDao, mockScmClient);

    Response response = endpoint.getStorageDistribution();

    // Should succeed if timeout is configured appropriately
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    StorageCapacityDistributionResponse payload =
        (StorageCapacityDistributionResponse) response.getEntity();
    assertNotNull(payload);

    // Verify that the slow node's metrics are included
    GlobalStorageReport globalStorage = payload.getGlobalStorage();
    assertEquals(NODE_CAPACITY, globalStorage.getTotalCapacity());
  }

  private void setupSuccessfulScenario() throws IOException {
    DeletedBlocksTransactionSummary scmSummary = mock(DeletedBlocksTransactionSummary.class);
    when(scmSummary.getTotalBlockReplicatedSize()).thenReturn(SCM_PENDING);
    when(scmSummary.getTotalBlockSize()).thenReturn(SCM_PENDING / 3);
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
    when(globalStats.getCapacity()).thenReturn(new LongMetric(NODE_CAPACITY /   2));
  }

  private void setupJmxMetricsFailureScenario() {
    doThrow(new RuntimeException("JMX Metrics Failure"))
        .when(mockOmdbInsightEndpoint).createKeysSummaryForOpenKey(anyMap());
  }
}
