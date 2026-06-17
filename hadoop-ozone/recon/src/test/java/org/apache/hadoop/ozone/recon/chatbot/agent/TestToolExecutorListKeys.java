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

package org.apache.hadoop.ozone.recon.chatbot.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests how {@link ToolExecutor} handles and calls the listKeys API, with a focus on pagination.
 *
 * <p><b>Lifecycle Phase:</b> Execution (Between 1st and 2nd LLM Calls). Tests the data-fetching engine.</p>
 *
 * <p><b>Key scenarios tested:</b></p>
 * <ul>
 *   <li><b>Pagination:</b> Fetching and merging multiple pages of keys.</li>
 *   <li><b>Limits:</b> Stopping at configured max pages or max records.</li>
 *   <li><b>Errors:</b> Handling HTTP failures and invalid inputs.</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
public class TestToolExecutorListKeys {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private ToolExecutor toolExecutor;

  @BeforeEach
  public void setUp() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_PAGES, 5);
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_PAGE_SIZE, 200);

    // We spy on the real executor so we can mock executeSingleCall
    toolExecutor = spy(new ToolExecutor(conf));
  }

  @Test
  public void testSinglePage() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("startPrefix", "/vol1/bucket1");

    JsonNode page1 = MAPPER.readTree("{\"keys\": [{\"key\":\"k1\"}, {\"key\":\"k2\"}]}");
    doReturn(page1).when(toolExecutor).executeSingleCall(anyString(), anyString(), any());

    ToolExecutor.ToolExecutionOutcome outcome =
        toolExecutor.executeToolCallWithPolicy("/api/v1/keys/listKeys", "GET", params, 5, 200);

    verify(toolExecutor, times(1)).executeSingleCall(anyString(), anyString(), any());
    assertEquals(2, outcome.getRecordsProcessed());
    assertEquals(1, outcome.getPagesFetched());
    assertFalse(outcome.isTruncated());

    JsonNode resultNode = (JsonNode) outcome.getResponseBody();
    assertEquals(2, resultNode.get("keys").size());
  }

  @Test
  public void testMultiplePages() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("startPrefix", "/vol1/bucket1");

    JsonNode page1 = MAPPER.readTree("{\"keys\": [{\"key\":\"k1\"}, {\"key\":\"k2\"}], \"lastKey\": \"k2\"}");
    JsonNode page2 = MAPPER.readTree("{\"keys\": [{\"key\":\"k3\"}]}");

    // First call returns page1, second call returns page2
    doReturn(page1).doReturn(page2).when(toolExecutor).executeSingleCall(anyString(), anyString(), any());

    ToolExecutor.ToolExecutionOutcome outcome =
        toolExecutor.executeToolCallWithPolicy("/api/v1/keys/listKeys", "GET", params, 5, 200);

    verify(toolExecutor, times(2)).executeSingleCall(anyString(), anyString(), any());
    assertEquals(3, outcome.getRecordsProcessed());
    assertEquals(2, outcome.getPagesFetched());
    assertFalse(outcome.isTruncated());

    JsonNode resultNode = (JsonNode) outcome.getResponseBody();
    assertEquals(3, resultNode.get("keys").size());
  }

  @Test
  public void testMaxPagesLimit() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("startPrefix", "/vol1/bucket1");

    // Always returns a page with 1 key and a lastKey, simulating infinite data
    JsonNode infinitePage = MAPPER.readTree("{\"keys\": [{\"key\":\"k\"}], \"lastKey\": \"next\"}");
    doReturn(infinitePage).when(toolExecutor).executeSingleCall(anyString(), anyString(), any());

    // Set maxPages to 3 for this test
    ToolExecutor.ToolExecutionOutcome outcome =
        toolExecutor.executeToolCallWithPolicy("/api/v1/keys/listKeys", "GET", params, 3, 200);

    // Should stop exactly at 3 pages
    verify(toolExecutor, times(3)).executeSingleCall(anyString(), anyString(), any());
    assertEquals(3, outcome.getRecordsProcessed());
    assertEquals(3, outcome.getPagesFetched());
    assertTrue(outcome.isTruncated());

    JsonNode resultNode = (JsonNode) outcome.getResponseBody();
    assertEquals(3, resultNode.get("keys").size());
    assertTrue(resultNode.get("truncated").asBoolean());
  }

  @Test
  public void testEmptyKeys() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("startPrefix", "/vol1/bucket1");

    JsonNode emptyPage = MAPPER.readTree("{\"keys\": []}");
    doReturn(emptyPage).when(toolExecutor).executeSingleCall(anyString(), anyString(), any());

    ToolExecutor.ToolExecutionOutcome outcome =
        toolExecutor.executeToolCallWithPolicy("/api/v1/keys/listKeys", "GET", params, 5, 200);

    verify(toolExecutor, times(1)).executeSingleCall(anyString(), anyString(), any());
    assertEquals(0, outcome.getRecordsProcessed());
    assertEquals(1, outcome.getPagesFetched());
    assertFalse(outcome.isTruncated());

    JsonNode resultNode = (JsonNode) outcome.getResponseBody();
    assertEquals(0, resultNode.get("keys").size());
  }

  @Test
  public void testMalformedInputMissingPrefix() throws Exception {
    Map<String, String> params = new HashMap<>();
    // No startPrefix

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      toolExecutor.executeToolCallWithPolicy("/api/v1/keys/listKeys", "GET", params, 5, 200);
    });

    assertTrue(exception.getMessage().contains("requires 'startPrefix'"));
    // executeSingleCall should never be reached
    verify(toolExecutor, times(0)).executeSingleCall(anyString(), anyString(), any());
  }

  @Test
  public void testMalformedInputRootPrefix() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("startPrefix", "/");

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      toolExecutor.executeToolCallWithPolicy("/api/v1/keys/listKeys", "GET", params, 5, 200);
    });

    assertTrue(exception.getMessage().contains("requires 'startPrefix'"));
    verify(toolExecutor, times(0)).executeSingleCall(anyString(), anyString(), any());
  }

  @Test
  public void testHttpError() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("startPrefix", "/vol1/bucket1");

    doThrow(new IOException("API request failed with status 500")).when(toolExecutor)
        .executeSingleCall(anyString(), anyString(), any());

    IOException exception = assertThrows(IOException.class, () -> {
      toolExecutor.executeToolCallWithPolicy("/api/v1/keys/listKeys", "GET", params, 5, 200);
    });

    assertEquals("API request failed with status 500", exception.getMessage());
    verify(toolExecutor, times(1)).executeSingleCall(anyString(), anyString(), any());
  }
}
