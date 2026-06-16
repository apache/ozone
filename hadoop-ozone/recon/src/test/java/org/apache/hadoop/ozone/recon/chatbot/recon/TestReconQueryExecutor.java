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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.junit.jupiter.api.Test;

public class TestReconQueryExecutor {

  @Test
  public void testExecute() throws Exception {
    ReconEndpointRouter router = mock(ReconEndpointRouter.class);
    when(router.hasRoute("api_v1_clusterState")).thenReturn(true);
    
    Response mockResponse = Response.ok(Collections.singletonMap("state", "OK")).build();
    when(router.route(anyString(), any())).thenReturn(mockResponse);

    ReconQueryExecutor handler = new ReconQueryExecutor(router);

    ReconQueryRequest req = new ReconQueryRequest("/api/v1/clusterState", "GET", Collections.emptyMap());

    ReconQueryResult outcome = handler.execute(req);
    JsonNode body = (JsonNode) outcome.getResponseBody();
    assertEquals("OK", body.get("state").asText());
    assertFalse(outcome.isTruncated());
  }

  @Test
  public void testLimitClampAndPrevKeyStripNonListKeys() throws Exception {
    ReconEndpointRouter router = mock(ReconEndpointRouter.class);
    when(router.hasRoute("api_v1_datanodes")).thenReturn(true);
    
    Map<String, Object> entity = new HashMap<>();
    entity.put("data", Collections.nCopies(1000, Collections.singletonMap("hostname", "host1")));
    Response mockResponse = Response.ok(entity).build();
    
    when(router.route(anyString(), argThat(p -> {
      return !p.containsKey("prevKey") && "1000".equals(p.get("limit"));
    }))).thenReturn(mockResponse);

    ReconQueryExecutor handler = new ReconQueryExecutor(router);

    Map<String, String> params = new HashMap<>();
    params.put("limit", "5000"); // Should clamp to 1000
    params.put("prevKey", "someKey"); // Should be stripped
    ReconQueryRequest req = new ReconQueryRequest("api_v1_datanodes", "GET", params);

    ReconQueryResult outcome = handler.execute(req);

    verify(router, times(1)).route(anyString(), any());
    assertEquals(1000, outcome.getRecordsProcessed());
    assertTrue(outcome.isTruncated()); // 1000 >= effective (1000)
  }
}
