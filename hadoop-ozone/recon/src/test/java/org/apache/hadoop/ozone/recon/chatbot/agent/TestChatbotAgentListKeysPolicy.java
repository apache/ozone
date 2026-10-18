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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotException;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconApiAllowlist;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconQueryExecutor;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconQueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link ChatbotAgent} specifically handling the {@code api_v1_keys_listKeys} tool.
 *
 * <p>The model selects tools via native tool calls (tool name + JSON arguments). These tests use a
 * mocked {@link LLMClient} that returns a listKeys tool call, and verify the agent's safe-scope
 * validation (a bucket-scoped {@code startPrefix} is required) plus parameter pass-through and
 * exception wrapping around {@link ReconQueryExecutor}.</p>
 */
@ExtendWith(MockitoExtension.class)
public class TestChatbotAgentListKeysPolicy {

  private static final String LIST_KEYS_TOOL = "api_v1_keys_listKeys";

  @Mock
  private LLMClient mockLlmClient;

  @Mock
  private ReconQueryExecutor mockReconQueryExecutor;

  @Mock
  private ReconApiAllowlist mockReconApiAllowlist;

  @Mock
  private LlmToolSpecFactory mockLlmToolSpecFactory;

  private ChatbotAgent agent;

  private static final String SUMMARY_RESPONSE = "Here is the list of keys.";

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ENABLED, true);
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE, true);
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_TOOL_CALLS, 5);

    lenient().when(mockReconQueryExecutor.execute(anyString(), anyMap()))
        .thenReturn(defaultOutcome());

    lenient().when(mockReconApiAllowlist.isRegistered(anyString())).thenReturn(true);

    agent = new ChatbotAgent(mockLlmClient, mockReconQueryExecutor, mockReconApiAllowlist,
        mockLlmToolSpecFactory, conf);
  }

  // ── Safe-scope violations (listKeys without a bucket prefix) ───────

  @Test
  public void testListKeysWithRootPrefixIsRejectedBySafeScopeCheck() throws Exception {
    // startPrefix=/ would scan the entire cluster — must be blocked
    selectListKeys("{\"startPrefix\":\"/\"}");

    String result = agent.processQuery(
        "List all keys in the entire cluster", null, null);

    assertNotNull(result);
    assertTrue(result.toLowerCase().contains("bucket") ||
            result.toLowerCase().contains("prefix"),
        "Response should ask for a bucket-scoped prefix");
    verify(mockReconQueryExecutor, never()).execute(anyString(), anyMap());
  }

  @Test
  public void testListKeysWithNullPrefixIsRejected() throws Exception {
    // No startPrefix field at all — must be rejected
    selectListKeys("{}");

    String result = agent.processQuery("List all keys", null, null);

    assertNotNull(result);
    verify(mockReconQueryExecutor, never()).execute(anyString(), anyMap());
  }

  @Test
  public void testListKeysWithEmptyPrefixIsRejected() throws Exception {
    selectListKeys("{\"startPrefix\":\"\"}");

    String result = agent.processQuery("List all keys", null, null);

    assertNotNull(result);
    verify(mockReconQueryExecutor, never()).execute(anyString(), anyMap());
  }

  @Test
  public void testListKeysWithVolumeOnlyPrefixIsRejected() throws Exception {
    // /myvol alone is not bucket-scoped — must require /<volume>/<bucket>
    selectListKeys("{\"startPrefix\":\"/myvol\"}");

    String result = agent.processQuery("List keys in volume myvol", null, null);

    assertNotNull(result);
    assertTrue(result.toLowerCase().contains("bucket"),
        "Response should ask for a bucket-scoped prefix");
    verify(mockReconQueryExecutor, never()).execute(anyString(), anyMap());
  }

  @Test
  public void testListKeysWithValidBucketScopedPrefixIsAllowed() throws Exception {
    // startPrefix=/vol1/bucket1 is bucket-scoped — must be allowed through
    selectListKeys("{\"startPrefix\":\"/vol1/bucket1\"}");
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(textResp(SUMMARY_RESPONSE));

    agent.processQuery("List keys in bucket1", null, null);

    // Executor must be called with the listKeys tool
    verify(mockReconQueryExecutor, times(1)).execute(anyString(), anyMap());
  }

  @Test
  public void testSafeScopeCheckDisabledAllowsListKeysWithRootPrefix() throws Exception {
    // When requireSafeScope=false, even startPrefix=/ is permitted
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ENABLED, true);
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE, false);
    ChatbotAgent agentNoScope = new ChatbotAgent(mockLlmClient, mockReconQueryExecutor,
        mockReconApiAllowlist, mockLlmToolSpecFactory, conf);

    selectListKeys("{\"startPrefix\":\"/\"}");
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(textResp(SUMMARY_RESPONSE));

    agentNoScope.processQuery("List all keys", null, null);

    // Safe-scope check is off — executor IS called
    verify(mockReconQueryExecutor, times(1)).execute(anyString(), anyMap());
  }

  // ── Exception Handling and Parameter Pass-through ───────────────────────

  @Test
  public void testToolExecutorIoExceptionIsWrappedAsChatbotException() throws Exception {
    selectListKeys("{\"startPrefix\":\"/vol1/bucket1\"}");

    when(mockReconQueryExecutor.execute(anyString(), anyMap()))
        .thenThrow(new IOException("Recon API is down"));

    ChatbotException exception = assertThrows(ChatbotException.class, () -> {
      agent.processQuery("List keys in bucket1", null, null);
    });

    assertTrue(exception.getMessage().contains("Error executing tool call"));
    assertNotNull(exception.getCause());
    assertTrue(exception.getCause() instanceof IOException);
    assertEquals("Recon API is down", exception.getCause().getMessage());
  }

  @Test
  public void testSummarizationLlmFailureIsWrappedAsChatbotException() throws Exception {
    // First (selection) call returns a valid tool call, second (summarization) call throws.
    selectListKeys("{\"startPrefix\":\"/vol1/bucket1\"}");
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenThrow(new RuntimeException("LLM summarization failed"));

    ChatbotException exception = assertThrows(ChatbotException.class, () -> {
      agent.processQuery("List keys in bucket1", null, null);
    });

    assertTrue(exception.getMessage().contains("Error executing tool call") ||
        exception.getMessage().contains("Error generating response"));
    assertNotNull(exception.getCause());
    assertTrue(exception.getCause() instanceof RuntimeException);
    assertEquals("LLM summarization failed", exception.getCause().getMessage());

    // Executor should have been called successfully
    verify(mockReconQueryExecutor, times(1)).execute(anyString(), anyMap());
  }

  @Test
  public void testOptionalParametersArePassedToToolExecutor() throws Exception {
    selectListKeys("{\"startPrefix\":\"/vol1/bucket1\",\"limit\":\"50\"," +
        "\"replicationType\":\"RATIS\",\"keySize\":\"1024\"}");
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(textResp(SUMMARY_RESPONSE));

    agent.processQuery("List 50 RATIS keys in bucket1 larger than 1024 bytes", null, null);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> paramsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockReconQueryExecutor, times(1)).execute(anyString(), paramsCaptor.capture());

    Map<String, String> capturedParams = paramsCaptor.getValue();
    assertEquals("/vol1/bucket1", capturedParams.get("startPrefix"));
    assertEquals("50", capturedParams.get("limit"));
    assertEquals("RATIS", capturedParams.get("replicationType"));
    assertEquals("1024", capturedParams.get("keySize"));
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  /** Stub the tool-selection call to return a native listKeys tool call with the given args. */
  private void selectListKeys(String argumentsJson) throws LLMClient.LLMException {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(new LLMClient.LLMResponse("", "test-model", 10, 20,
            Collections.singletonList(new LLMClient.ToolCallRequest(LIST_KEYS_TOOL, argumentsJson))));
  }

  private LLMClient.LLMResponse textResp(String content) {
    return new LLMClient.LLMResponse(content, "test-model", 10, 20, null);
  }

  private ReconQueryResult defaultOutcome() {
    return new ReconQueryResult(
        JsonNodeFactory.instance.objectNode(), 0, false, 1000);
  }
}
