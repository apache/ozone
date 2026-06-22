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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests listKeys execution policy in {@link ReconQueryExecutor}. */
public class TestReconQueryExecutorListKeys {

  private ReconEndpointRouter router;
  private ReconQueryExecutor handler;

  @BeforeEach
  public void setUp() {
    router = mock(ReconEndpointRouter.class);
    when(router.hasRoute("api_v1_keys_listKeys")).thenReturn(true);
    handler = new ReconQueryExecutor(router);
  }

  // Note: bucket-scoped startPrefix validation for listKeys is enforced upstream by
  // ChatbotAgent.validateToolCall (see TestChatbotAgentListKeysPolicy), not by this executor.
  // ReconQueryExecutor is only responsible for the limit clamp and prevKey strip.

  @Test
  public void testListKeysLimitClampAndPrevKeyStrip() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("startPrefix", "/vol1/bucket1");
    params.put("limit", "5000"); // Should clamp to 1000
    params.put("prevKey", "someKey"); // Should be stripped

    Map<String, Object> entity = new HashMap<>();
    entity.put("keys", Collections.nCopies(1000, Collections.singletonMap("key", "k1")));
    Response mockResponse = Response.ok(entity).build();

    // Verify params passed to router have prevKey removed and limit clamped
    when(router.route(anyString(), argThat(p -> {
      return !p.containsKey("prevKey") && "1000".equals(p.get("limit"));
    }))).thenReturn(mockResponse);

    ReconQueryResult outcome = handler.execute("api_v1_keys_listKeys", params);

    verify(router, times(1)).route(anyString(), any());
    assertEquals(1000, outcome.getRecordsProcessed());
    assertTrue(outcome.isTruncated()); // 1000 >= effective (1000)

    JsonNode resultNode = outcome.getResponseBody();
    assertEquals(1000, resultNode.get("keys").size());
  }

  @Test
  public void testListKeysNegativeLimitRejected() {
    Map<String, String> params = new HashMap<>();
    params.put("startPrefix", "/vol1/bucket1");
    params.put("limit", "-1");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> handler.execute("api_v1_keys_listKeys", params));
    assertTrue(ex.getMessage().contains("limit must be a positive integer"));
  }
}
