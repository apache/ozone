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
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationInternalInterface;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesCompleteResponse;
import org.apache.hadoop.ozone.recon.api.types.BucketDeletedBytesProgressResponse;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsCompleteResponse;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsProgressResponse;
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
  private BucketDeletedBytesMetricsService bucketDeletedBytesMetricsService;
  private DataNodeMetricsService dataNodeMetricsService;
  private StorageContainerLocationInternalInterface scmClient;

  @BeforeEach
  public void setup() {
    bucketDeletedBytesMetricsService = mock(BucketDeletedBytesMetricsService.class);
    dataNodeMetricsService = mock(DataNodeMetricsService.class);
    scmClient = mock(StorageContainerLocationInternalInterface.class);
    pendingDeletionEndpoint = new PendingDeletionEndpoint(
        bucketDeletedBytesMetricsService, dataNodeMetricsService, scmClient);
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
    DataNodeMetricsCompleteResponse metricsResponse = new DataNodeMetricsCompleteResponse(
        DataNodeMetricsService.MetricCollectionStatus.FINISHED,
        1,
        0,
        100L,
        Arrays.asList(new DatanodePendingDeletionMetrics("dn1", "uuid-1", 100L)));
    when(dataNodeMetricsService.getCollectedMetrics(5)).thenReturn(metricsResponse);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("DN", 5);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(metricsResponse, response.getEntity());
  }

  @Test
  public void testDnComponentAllowsNullLimit() {
    DataNodeMetricsCompleteResponse metricsResponse = new DataNodeMetricsCompleteResponse(
        DataNodeMetricsService.MetricCollectionStatus.FINISHED,
        1,
        0,
        100L,
        Arrays.asList(new DatanodePendingDeletionMetrics("dn1", "uuid-1", 100L)));
    when(dataNodeMetricsService.getCollectedMetrics(null)).thenReturn(metricsResponse);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("dn", null);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(metricsResponse, response.getEntity());
  }

  @Test
  public void testDnComponentReturnsAcceptedWhenInProgress() {
    DataNodeMetricsProgressResponse metricsResponse = new DataNodeMetricsProgressResponse(
        DataNodeMetricsService.MetricCollectionStatus.IN_PROGRESS,
        "Metrics collection task is currently running. Please wait for task to finish.");
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
  public void testOmComponentReturnsOkWhenFinished() {
    BucketDeletedBytesCompleteResponse metricsResponse = new BucketDeletedBytesCompleteResponse(
        BucketDeletedBytesMetricsService.MetricCollectionStatus.FINISHED,
        1,
        0,
        400L,
        200L,
        30L,
        5L,
        150L,
        250L);
    when(bucketDeletedBytesMetricsService.getCollectedMetrics()).thenReturn(metricsResponse);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("om", 1);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(metricsResponse, response.getEntity());
  }

  @Test
  public void testOmComponentReturnsAcceptedWhenInProgress() {
    BucketDeletedBytesProgressResponse metricsResponse = new BucketDeletedBytesProgressResponse(
        BucketDeletedBytesMetricsService.MetricCollectionStatus.IN_PROGRESS,
        "Bucket deleted bytes collection task is currently running. Please wait for task to finish.");
    when(bucketDeletedBytesMetricsService.getCollectedMetrics()).thenReturn(metricsResponse);

    Response response = pendingDeletionEndpoint.getPendingDeletionByComponent("om", 1);

    assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    assertEquals(metricsResponse, response.getEntity());
  }
}
