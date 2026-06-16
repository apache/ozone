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

package org.apache.hadoop.ozone.recon.chatbot.recon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.ozone.recon.api.BucketEndpoint;
import org.apache.hadoop.ozone.recon.api.ClusterStateEndpoint;
import org.apache.hadoop.ozone.recon.api.ContainerEndpoint;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.NodeEndpoint;
import org.apache.hadoop.ozone.recon.api.OMDBInsightEndpoint;
import org.apache.hadoop.ozone.recon.api.PipelineEndpoint;
import org.apache.hadoop.ozone.recon.api.TaskStatusService;
import org.apache.hadoop.ozone.recon.api.UtilizationEndpoint;
import org.apache.hadoop.ozone.recon.api.VolumeEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestReconEndpointRouter {

  private ReconEndpointRouter router;
  private ClusterStateEndpoint clusterStateEndpoint;
  private NodeEndpoint nodeEndpoint;
  private PipelineEndpoint pipelineEndpoint;
  private ContainerEndpoint containerEndpoint;
  private OMDBInsightEndpoint omdbInsightEndpoint;
  private VolumeEndpoint volumeEndpoint;
  private BucketEndpoint bucketEndpoint;
  private TaskStatusService taskStatusService;
  private UtilizationEndpoint utilizationEndpoint;
  private NSSummaryEndpoint nsSummaryEndpoint;
  private ReconApiAllowlist reconApiAllowlist;

  @BeforeEach
  public void setUp() {
    clusterStateEndpoint = mock(ClusterStateEndpoint.class);
    nodeEndpoint = mock(NodeEndpoint.class);
    pipelineEndpoint = mock(PipelineEndpoint.class);
    containerEndpoint = mock(ContainerEndpoint.class);
    omdbInsightEndpoint = mock(OMDBInsightEndpoint.class);
    volumeEndpoint = mock(VolumeEndpoint.class);
    bucketEndpoint = mock(BucketEndpoint.class);
    taskStatusService = mock(TaskStatusService.class);
    utilizationEndpoint = mock(UtilizationEndpoint.class);
    nsSummaryEndpoint = mock(NSSummaryEndpoint.class);
    reconApiAllowlist = mock(ReconApiAllowlist.class);

    org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory factory = mock(org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory.class);
    when(factory.getMetricsServiceProvider()).thenReturn(mock(org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider.class));
    router = new ReconEndpointRouter(
        clusterStateEndpoint, nodeEndpoint, pipelineEndpoint, containerEndpoint,
        omdbInsightEndpoint, volumeEndpoint, bucketEndpoint, taskStatusService,
        utilizationEndpoint, nsSummaryEndpoint, reconApiAllowlist, factory);
  }

  @Test
  public void testHasRoute() {
    when(reconApiAllowlist.isRegistered("api_v1_clusterState")).thenReturn(true);
    when(reconApiAllowlist.isRegistered("api_v1_containers_unhealthy_state")).thenReturn(true);
    when(reconApiAllowlist.isRegistered("api_v1_keys_open")).thenReturn(true);
    when(reconApiAllowlist.isRegistered("api_v1_unknown")).thenReturn(false);

    assertTrue(router.hasRoute("api_v1_clusterState"));
    assertTrue(router.hasRoute("api_v1_containers_unhealthy_state"));
    assertTrue(router.hasRoute("api_v1_keys_open"));
    assertFalse(router.hasRoute("api_v1_unknown"));
  }

  @Test
  public void testRouteClusterState() throws Exception {
    Response mockResponse = Response.ok().build();
    when(clusterStateEndpoint.getClusterState()).thenReturn(mockResponse);

    Response response = router.route("api_v1_clusterState", Collections.emptyMap());
    assertEquals(mockResponse, response);
    verify(clusterStateEndpoint).getClusterState();
  }

  @Test
  public void testRouteContainersUnhealthyState() throws Exception {
    Response mockResponse = Response.ok().build();
    when(containerEndpoint.getUnhealthyContainers(eq("MISSING"), eq(1000), eq(0L), eq(0L))).thenReturn(mockResponse);

    Map<String, String> params = new HashMap<>();
    params.put("state", "MISSING");
    Response response = router.route("api_v1_containers_unhealthy_state", params);
    assertEquals(mockResponse, response);
    verify(containerEndpoint).getUnhealthyContainers("MISSING", 1000, 0L, 0L);
  }

  @Test
  public void testRouteMetricsApi() throws Exception {
    org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider metricsServiceProvider = mock(org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider.class);
    org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory factory = mock(org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory.class);
    when(factory.getMetricsServiceProvider()).thenReturn(metricsServiceProvider);

    ReconEndpointRouter metricsRouter = new ReconEndpointRouter(
        clusterStateEndpoint, nodeEndpoint, pipelineEndpoint, containerEndpoint,
        omdbInsightEndpoint, volumeEndpoint, bucketEndpoint, taskStatusService,
        utilizationEndpoint, nsSummaryEndpoint, reconApiAllowlist, factory);

    java.net.HttpURLConnection conn = mock(java.net.HttpURLConnection.class);
    when(metricsServiceProvider.getMetricsResponse(eq("jvm"), eq("query=test"))).thenReturn(conn);
    when(conn.getResponseCode()).thenReturn(200);
    when(conn.getInputStream()).thenReturn(new java.io.ByteArrayInputStream("{\"status\":\"success\"}".getBytes("UTF-8")));

    Map<String, String> params = new HashMap<>();
    params.put("api", "jvm");
    params.put("query", "query=test");
    
    Response response = metricsRouter.route("api_v1_metrics_api", params);
    assertEquals(200, response.getStatus());
    JsonNode node = (JsonNode) response.getEntity();
    assertEquals("success", node.get("status").asText());
  }


  @Test
  public void testRouteInvalidPath() {
    assertThrows(IllegalArgumentException.class, () -> router.route("api_v1_invalid", Collections.emptyMap()));
  }
}
