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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
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

/** Tests for {@link ReconEndpointRouter}. */
public class TestReconEndpointRouter {

  private ReconEndpointRouter router;
  private ClusterStateEndpoint clusterStateEndpoint;
  private ContainerEndpoint containerEndpoint;
  private OMDBInsightEndpoint omdbInsightEndpoint;
  private BucketEndpoint bucketEndpoint;
  private UtilizationEndpoint utilizationEndpoint;
  private NSSummaryEndpoint nsSummaryEndpoint;
  private ReconApiAllowlist reconApiAllowlist;

  @BeforeEach
  public void setUp() {
    clusterStateEndpoint = mock(ClusterStateEndpoint.class);
    NodeEndpoint nodeEndpoint = mock(NodeEndpoint.class);
    PipelineEndpoint pipelineEndpoint = mock(PipelineEndpoint.class);
    containerEndpoint = mock(ContainerEndpoint.class);
    omdbInsightEndpoint = mock(OMDBInsightEndpoint.class);
    VolumeEndpoint volumeEndpoint = mock(VolumeEndpoint.class);
    bucketEndpoint = mock(BucketEndpoint.class);
    TaskStatusService taskStatusService = mock(TaskStatusService.class);
    utilizationEndpoint = mock(UtilizationEndpoint.class);
    nsSummaryEndpoint = mock(NSSummaryEndpoint.class);
    reconApiAllowlist = mock(ReconApiAllowlist.class);

    router = new ReconEndpointRouter(
        clusterStateEndpoint, nodeEndpoint, pipelineEndpoint, containerEndpoint,
        omdbInsightEndpoint, volumeEndpoint, bucketEndpoint, taskStatusService,
        utilizationEndpoint, nsSummaryEndpoint, reconApiAllowlist);
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
  public void testRouteInvalidPath() {
    assertThrows(IllegalArgumentException.class, () -> router.route("api_v1_invalid", Collections.emptyMap()));
  }

  // ── Per-tool parameter extraction (catches a miswired case or transposed argument) ──────────

  @Test
  public void testRouteMissingContainersPassesLimit() throws Exception {
    router.route("api_v1_containers_missing", params("limit", "250"));
    verify(containerEndpoint).getMissingContainers(250);
  }

  @Test
  public void testRouteContainersMismatchPassesMissingIn() throws Exception {
    router.route("api_v1_containers_mismatch", params("missingIn", "OM"));
    verify(containerEndpoint).getContainerMisMatchInsights(1000, 0L, "OM");
  }

  @Test
  public void testRouteQuasiClosedPassesMinContainerId() throws Exception {
    router.route("api_v1_containers_quasiClosed", params("minContainerId", "42"));
    verify(containerEndpoint).getQuasiClosedContainers(1000, 42L);
  }

  @Test
  public void testRouteOpenKeysPassesIncludeFlagsAndPrefix() throws Exception {
    router.route("api_v1_keys_open",
        params("includeFso", "true", "includeNonFso", "false", "startPrefix", "/vol1/bucket1"));
    verify(omdbInsightEndpoint).getOpenKeyInfo(1000, "", "/vol1/bucket1", true, false);
  }

  @Test
  public void testRouteListKeysPassesFilters() throws Exception {
    router.route("api_v1_keys_listKeys",
        params("replicationType", "EC", "keySize", "1024", "startPrefix", "/vol1/bucket1"));
    verify(omdbInsightEndpoint).listKeys("EC", null, 1024L, "/vol1/bucket1", "", 1000);
  }

  @Test
  public void testRouteBucketsPassesVolume() throws Exception {
    router.route("api_v1_buckets", params("volume", "vol1"));
    verify(bucketEndpoint).getBuckets("vol1", 1000, "");
  }

  @Test
  public void testRouteFileCountPassesVolumeBucketSize() throws Exception {
    router.route("api_v1_utilization_fileCount",
        params("volume", "vol1", "bucket", "bucket1", "fileSize", "2048"));
    verify(utilizationEndpoint).getFileCounts("vol1", "bucket1", 2048L);
  }

  @Test
  public void testRouteContainerCountPassesSize() throws Exception {
    router.route("api_v1_utilization_containerCount", params("containerSize", "4096"));
    verify(utilizationEndpoint).getContainerCounts(4096L);
  }

  @Test
  public void testRouteNamespaceUsagePassesPathAndFlags() throws Exception {
    router.route("api_v1_namespace_usage",
        params("path", "/vol1/bucket1", "files", "true", "replica", "true"));
    verify(nsSummaryEndpoint).getDiskUsage("/vol1/bucket1", true, true, false);
  }

  private static Map<String, String> params(String... kv) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }
}
