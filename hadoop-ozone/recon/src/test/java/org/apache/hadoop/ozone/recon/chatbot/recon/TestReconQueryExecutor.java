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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ReconQueryExecutor}: limit clamping, prevKey stripping, and truncation
 * detection driven by {@link org.apache.hadoop.ozone.recon.chatbot.agent.ChatbotUtils#estimateRecordCount}.
 */
public class TestReconQueryExecutor {

  @Test
  public void testExecute() throws Exception {
    ReconEndpointRouter router = mock(ReconEndpointRouter.class);
    when(router.hasRoute("api_v1_clusterState")).thenReturn(true);
    Response mockResponse = Response.ok(Collections.singletonMap("state", "OK")).build();
    when(router.route(anyString(), any())).thenReturn(mockResponse);

    ReconQueryExecutor handler = new ReconQueryExecutor(router);

    ReconQueryResult outcome = handler.execute("api_v1_clusterState", Collections.emptyMap());
    JsonNode body = outcome.getResponseBody();
    assertEquals("OK", body.get("state").asText());
    assertFalse(outcome.isTruncated());
  }

  @Test
  public void testLimitClampAndPrevKeyStripNonListKeys() throws Exception {
    ReconEndpointRouter router = mock(ReconEndpointRouter.class);
    when(router.hasRoute("api_v1_datanodes")).thenReturn(true);

    Response mockResponse = Response.ok(dataObjectArrayBody(1000)).build();

    when(router.route(anyString(), argThat(p ->
        !p.containsKey("prevKey") && "1000".equals(p.get("limit"))))).thenReturn(mockResponse);

    ReconQueryExecutor handler = new ReconQueryExecutor(router);

    Map<String, String> params = new HashMap<>();
    params.put("limit", "5000");
    params.put("prevKey", "someKey");
    ReconQueryResult outcome = handler.execute("api_v1_datanodes", params);

    verify(router, times(1)).route(anyString(), any());
    assertEquals(1000, outcome.getRecordsProcessed());
    assertTrue(outcome.isTruncated());
  }

  @Test
  public void testTruncationFalseBelowCap() throws Exception {
    ReconEndpointRouter router = mock(ReconEndpointRouter.class);
    when(router.hasRoute("api_v1_datanodes")).thenReturn(true);
    when(router.route(anyString(), any())).thenReturn(Response.ok(dataObjectArrayBody(999)).build());

    ReconQueryExecutor handler = new ReconQueryExecutor(router);
    ReconQueryResult outcome = handler.execute("api_v1_datanodes", Collections.emptyMap());

    assertEquals(999, outcome.getRecordsProcessed());
    assertFalse(outcome.isTruncated());
  }

  @Test
  public void testTruncationTrueForNestedContainersAtCap() throws Exception {
    ReconEndpointRouter router = mock(ReconEndpointRouter.class);
    when(router.hasRoute("api_v1_containers")).thenReturn(true);
    when(router.route(anyString(), any())).thenReturn(
        Response.ok(nestedContainersBody(1000)).build());

    ReconQueryExecutor handler = new ReconQueryExecutor(router);
    ReconQueryResult outcome = handler.execute("api_v1_containers", Collections.emptyMap());

    assertEquals(1000, outcome.getRecordsProcessed());
    assertTrue(outcome.isTruncated());
  }

  @Test
  public void testTruncationTrueAtRequestedLimit() throws Exception {
    ReconEndpointRouter router = mock(ReconEndpointRouter.class);
    when(router.hasRoute("api_v1_datanodes")).thenReturn(true);
    when(router.route(anyString(), any())).thenReturn(Response.ok(dataObjectArrayBody(10)).build());

    ReconQueryExecutor handler = new ReconQueryExecutor(router);

    Map<String, String> params = new HashMap<>();
    params.put("limit", "10");
    ReconQueryResult outcome = handler.execute("api_v1_datanodes", params);

    assertEquals(10, outcome.getRecordsProcessed());
    assertEquals(10, outcome.getMaxRecords());
    assertTrue(outcome.isTruncated());
  }

  @Test
  public void testTruncationFalseBelowRequestedLimit() throws Exception {
    ReconEndpointRouter router = mock(ReconEndpointRouter.class);
    when(router.hasRoute("api_v1_datanodes")).thenReturn(true);
    when(router.route(anyString(), any())).thenReturn(Response.ok(dataObjectArrayBody(9)).build());

    ReconQueryExecutor handler = new ReconQueryExecutor(router);

    Map<String, String> params = new HashMap<>();
    params.put("limit", "10");
    ReconQueryResult outcome = handler.execute("api_v1_datanodes", params);

    assertEquals(9, outcome.getRecordsProcessed());
    assertEquals(10, outcome.getMaxRecords());
    assertFalse(outcome.isTruncated());
  }

  private static Map<String, Object> dataObjectArrayBody(int count) {
    Map<String, Object> entity = new HashMap<>();
    entity.put("data", Collections.nCopies(count, Collections.singletonMap("hostname", "host1")));
    return entity;
  }

  private static Map<String, Object> nestedContainersBody(int count) {
    Map<String, Object> data = new HashMap<>();
    data.put("containers", Collections.nCopies(count, Collections.singletonMap("containerID", 1L)));
    Map<String, Object> entity = new HashMap<>();
    entity.put("data", data);
    return entity;
  }
}
