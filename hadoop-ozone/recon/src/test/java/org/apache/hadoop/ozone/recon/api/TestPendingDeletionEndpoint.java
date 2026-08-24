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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsServiceResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodePendingDeletionMetrics;
import org.apache.hadoop.ozone.recon.api.types.ScmPendingDeletion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class for PendingDeletionEndpoint.
 *
 * This class tests pending deletion endpoint behaviors, including:
 *
 * 1. Component validation and error handling for missing/invalid components.
 * 2. DataNode metrics responses for finished, in-progress, and null-limit requests.
 * 3. SCM pending deletion summaries for success, no-content, and exception fallback.
 * 4. OM pending deletion response pass-through values.
 * 5. CSV download responses for pending, missing metrics, and successful exports.
 */
public class TestPendingDeletionEndpoint {
  private PendingDeletionEndpoint pendingDeletionEndpoint;
  private ReconGlobalMetricsService reconGlobalMetricsService;
  private DataNodeMetricsService dataNodeMetricsService;
  private StorageContainerLocationProtocol scmClient;

  @BeforeEach
  public void setup() {
    reconGlobalMetricsService = mock(ReconGlobalMetricsService.class);
    dataNodeMetricsService = mock(DataNodeMetricsService.class);
    scmClient = mock(StorageContainerLocationProtocol.class);
    pendingDeletionEndpoint = new PendingDeletionEndpoint(
        reconGlobalMetricsService, dataNodeMetricsService, scmClient);
  }

  @Test
  public void testMissingComponentReturnsBadRequest() {
    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent(null, 10);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals("component query parameter is required", response.getEntity());
  }

  @ParameterizedTest
  @ValueSource(strings = {"unknown", "invalid"})
  public void testInvalidComponentReturnsBadRequest(String component) {
    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent(component, 10);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals("component query parameter must be one of dn, scm, om", response.getEntity());
  }

  @Test
  public void testEmptyComponentReturnsBadRequest() {
    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("", 10);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals("component query parameter is required", response.getEntity());
  }

  @Test
  public void testWhitespaceComponentReturnsBadRequest() {
    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("   ", 10);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals("component query parameter must be one of dn, scm, om", response.getEntity());
  }

  @Test
  public void testDnComponentWithInvalidLimit() {
    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("dn", 0);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals("Limit query parameter must be at-least 1", response.getEntity());
  }

  @Test
  public void testDnComponentReturnsOkWhenFinished() {
    DataNodeMetricsServiceResponse metricsResponse = DataNodeMetricsServiceResponse.newBuilder()
        .setStatus(DataNodeMetricsService.MetricCollectionStatus.FINISHED)
        .setTotalPendingDeletionSize(100L)
        .setTotalNodesQueried(1)
        .setTotalNodeQueryFailures(0)
        .setPendingDeletion(Arrays.asList(
            new DatanodePendingDeletionMetrics("dn1", "uuid-1", 100L)))
        .build();
    when(dataNodeMetricsService.getCollectedMetrics(5)).thenReturn(metricsResponse);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("DN", 5);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(metricsResponse, response.getEntity());
  }

  @Test
  public void testDnComponentAllowsNullLimit() {
    DataNodeMetricsServiceResponse metricsResponse = DataNodeMetricsServiceResponse.newBuilder()
        .setStatus(DataNodeMetricsService.MetricCollectionStatus.FINISHED)
        .setTotalPendingDeletionSize(100L)
        .setTotalNodesQueried(1)
        .setTotalNodeQueryFailures(0)
        .build();
    when(dataNodeMetricsService.getCollectedMetrics(null)).thenReturn(metricsResponse);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("dn", null);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(metricsResponse, response.getEntity());
  }

  @Test
  public void testDnComponentReturnsAcceptedWhenInProgress() {
    DataNodeMetricsServiceResponse metricsResponse = DataNodeMetricsServiceResponse.newBuilder()
        .setStatus(DataNodeMetricsService.MetricCollectionStatus.IN_PROGRESS)
        .build();
    when(dataNodeMetricsService.getCollectedMetrics(2)).thenReturn(metricsResponse);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("dn", 2);

    assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    assertEquals(metricsResponse, response.getEntity());
  }

  @Test
  public void testScmComponentReturnsSummary() throws Exception {
    HddsProtos.DeletedBlocksTransactionSummary summary =
        HddsProtos.DeletedBlocksTransactionSummary.newBuilder()
            .setTotalBlockSize(100L)
            .setTotalBlockReplicatedSize(300L)
            .setTotalBlockCount(5L)
            .build();
    when(scmClient.getDeletedBlockSummary()).thenReturn(summary);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("scm", 1);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ScmPendingDeletion pendingDeletion = (ScmPendingDeletion) response.getEntity();
    assertNotNull(pendingDeletion);
    assertEquals(100L, pendingDeletion.getTotalBlocksize());
    assertEquals(300L, pendingDeletion.getTotalReplicatedBlockSize());
    assertEquals(5L, pendingDeletion.getTotalBlocksCount());
  }

  @Test
  public void testScmComponentReturnsNoContentWhenSummaryMissing() throws Exception {
    when(scmClient.getDeletedBlockSummary()).thenReturn(null);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("scm", 1);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @Test
  public void testScmComponentReturnsFallbackOnException() throws Exception {
    when(scmClient.getDeletedBlockSummary()).thenThrow(new RuntimeException("failure"));

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("scm", 1);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ScmPendingDeletion pendingDeletion = (ScmPendingDeletion) response.getEntity();
    assertNotNull(pendingDeletion);
    assertEquals(-1L, pendingDeletion.getTotalBlocksize());
    assertEquals(-1L, pendingDeletion.getTotalReplicatedBlockSize());
    assertEquals(-1L, pendingDeletion.getTotalBlocksCount());
  }

  @Test
  public void testOmComponentReturnsPendingDeletionSizes() {
    Map<String, Long> pendingSizes = new HashMap<>();
    pendingSizes.put("pendingDirectorySize", 200L);
    pendingSizes.put("pendingKeySize", 400L);
    pendingSizes.put("totalSize", 600L);
    when(reconGlobalMetricsService.calculatePendingSizes()).thenReturn(pendingSizes);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("om", 1);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(pendingSizes, response.getEntity());
  }
}
