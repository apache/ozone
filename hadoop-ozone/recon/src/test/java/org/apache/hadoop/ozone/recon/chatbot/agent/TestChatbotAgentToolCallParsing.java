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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotException;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link ChatbotAgent} tool-call routing and JSON parsing through {@code processQuery()}.
 *
 * <p>This class verifies how the agent parses the LLM's JSON responses, routes them to
 * the correct execution path (single endpoint, multi-endpoint, documentation query, or fallback),
 * and handles malformed or unexpected LLM outputs.</p>
 *
 * <p><b>Lifecycle Phase:</b> Post-1st LLM Call to 2nd LLM Call. This tests the orchestration after the first LLM call
 * returns, including routing to the executor, and triggering the second LLM call (summarization or fallback).</p>
 *
 * <p><b>Key scenarios tested:</b></p>
 * <ul>
 *   <li><b>Routing:</b> Ensures SINGLE_ENDPOINT, MULTI_ENDPOINT, and DOCUMENTATION_QUERY are routed correctly.</li>
 *   <li><b>Robustness:</b> Verifies fallbacks are triggered for truncated JSON, missing fields,
 *       or plain prose responses.</li>
 *   <li><b>Exception handling:</b> Ensures LLM exceptions and ToolExecutor IOExceptions are
 *       properly wrapped in ChatbotException.</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
public class TestChatbotAgentToolCallParsing {

  @Mock
  private LLMClient mockLlmClient;

  @Mock
  private ToolExecutor mockToolExecutor;

  private ChatbotAgent agent;

  // ── Canned LLM response strings ───────────────────────────────────────────

  private static final String SINGLE_CLUSTER_STATE =
      "{\"type\":\"SINGLE_ENDPOINT\",\"endpoint\":\"/api/v1/clusterState\"," +
          "\"method\":\"GET\",\"parameters\":{},\"reasoning\":\"need cluster data\"}";

  private static final String SINGLE_DATANODES =
      "{\"type\":\"SINGLE_ENDPOINT\",\"endpoint\":\"/api/v1/datanodes\"," +
          "\"method\":\"GET\",\"parameters\":{},\"reasoning\":\"need datanodes\"}";

  private static final String MULTI_TWO_ENDPOINTS =
      "{\"type\":\"MULTI_ENDPOINT\",\"reasoning\":\"need both\"," +
          "\"tool_calls\":[" +
          "{\"endpoint\":\"/api/v1/clusterState\",\"method\":\"GET\",\"parameters\":{}," +
          "\"reasoning\":\"cluster\"}," +
          "{\"endpoint\":\"/api/v1/datanodes\",\"method\":\"GET\",\"parameters\":{}," +
          "\"reasoning\":\"nodes\"}]}";

  private static final String DOC_QUERY =
      "{\"type\":\"DOCUMENTATION_QUERY\"," +
          "\"answer\":\"Apache Ozone is a scalable distributed storage system.\"," +
          "\"reasoning\":\"general knowledge\"}";

  private static final String SUMMARY_RESPONSE = "The cluster has 5 healthy datanodes.";
  private static final String FALLBACK_RESPONSE =
      "I can only answer questions about Apache Ozone Recon.";

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ENABLED, true);
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE, true);
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_TOOL_CALLS, 5);

    // Lenient default: only applies when a test actually calls the executor.
    // Tests that never reach the executor (fallback/doc paths) won't fail
    // because of this unused stub.
    lenient().when(mockToolExecutor.executeToolCallWithPolicy(
            anyString(), anyString(), any(), anyInt(), anyInt()))
        .thenReturn(defaultOutcome());

    agent = new ChatbotAgent(mockLlmClient, mockToolExecutor, conf);
  }

  // ── Happy path: SINGLE_ENDPOINT ───────────────────────────────────────────

  @Test
  public void testSingleEndpointCallsExecutorOnce() throws Exception {
    // First LLM call (tool selection) returns a SINGLE_ENDPOINT JSON.
    // Second LLM call (summarization) returns a natural-language answer.
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(SINGLE_CLUSTER_STATE))
        .thenReturn(resp(SUMMARY_RESPONSE));

    String result = agent.processQuery("What is the cluster state?", null, null);

    assertNotNull(result);
    // Executor must be called once with the exact endpoint from the LLM response
    verify(mockToolExecutor, times(1)).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    // Summarization requires a second LLM call
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  // ── Happy path: MULTI_ENDPOINT ────────────────────────────────────────────

  @Test
  public void testMultiEndpointCallsExecutorForEachToolCall() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(MULTI_TWO_ENDPOINTS))
        .thenReturn(resp(SUMMARY_RESPONSE));

    String result = agent.processQuery(
        "Show me datanodes and cluster state", null, null);

    assertNotNull(result);
    // Executor must be called once per tool_call in the MULTI_ENDPOINT array
    verify(mockToolExecutor, times(2)).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  // ── Happy path: DOCUMENTATION_QUERY ──────────────────────────────────────

  @Test
  public void testDocumentationQueryReturnsAnswerDirectlyNoApiCall() throws Exception {
    // DOCUMENTATION_QUERY: LLM answers directly from its knowledge.
    // No Recon API call and no summarization LLM call should happen.
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(DOC_QUERY));

    String result = agent.processQuery("What is Apache Ozone?", null, null);

    assertNotNull(result);
    assertTrue(result.contains("Apache Ozone"),
        "Response should contain the answer from the DOCUMENTATION_QUERY");
    // No Recon API call should ever happen for documentation queries
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    // Only one LLM call — no summarization step
    verify(mockLlmClient, times(1)).chatCompletion(anyList(), any(), any());
  }

  // ── ROB-04: Unknown type triggers fallback ────────────────────────────────

  @Test
  public void testUnknownTypeTriggersFallback() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp("{\"type\":\"HACK_SYSTEM\",\"payload\":\"x\"}"))
        .thenReturn(resp(FALLBACK_RESPONSE));

    String result = agent.processQuery("Do something", null, null);

    assertNotNull(result);
    // Executor must never be called when the type is unrecognized
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    // Fallback requires a second LLM call
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  @Test
  public void testMissingTypeFieldTriggersFallback() throws Exception {
    // JSON without a "type" field defaults to "" in the switch — hits default case
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp("{\"endpoint\":\"/api/v1/clusterState\",\"method\":\"GET\"}"))
        .thenReturn(resp(FALLBACK_RESPONSE));

    String result = agent.processQuery("What is the state?", null, null);

    assertNotNull(result);
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  // ── ROB-03: Truncated JSON triggers fallback ──────────────────────────────

  @Test
  public void testTruncatedJsonTriggersFallback() throws Exception {
    // Missing closing brace — extractFirstJsonObject returns null
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp("{\"type\":\"SINGLE_ENDPOINT\",\"endpoint\":\"/api/v1/clusterState\""))
        .thenReturn(resp(FALLBACK_RESPONSE));

    String result = agent.processQuery("What is the state?", null, null);

    assertNotNull(result);
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  @Test
  public void testPlainProseResponseTriggersFallback() throws Exception {
    // LLM returns prose with no JSON object at all
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp("I don't know how to answer this question."))
        .thenReturn(resp(FALLBACK_RESPONSE));

    String result = agent.processQuery("Some query", null, null);

    assertNotNull(result);
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  @Test
  public void testNoSuitableEndpointSentinelTriggersFallback() throws Exception {
    // The LLM uses the sentinel string when it cannot answer — no JSON, no API call
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp("NO_SUITABLE_ENDPOINT"))
        .thenReturn(resp(FALLBACK_RESPONSE));

    String result = agent.processQuery("What is the meaning of life?", null, null);

    assertNotNull(result);
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    // First call returns NO_SUITABLE_ENDPOINT; second call is the fallback
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  // ── ROB-05: Null / missing parameters handled safely ─────────────────────

  @Test
  public void testNullParametersFieldMappedToEmptyMap() throws Exception {
    // When LLM returns "parameters": null, parseSingleToolCall should use an empty map
    String json = "{\"type\":\"SINGLE_ENDPOINT\",\"endpoint\":\"/api/v1/datanodes\"," +
        "\"method\":\"GET\",\"parameters\":null,\"reasoning\":\"test\"}";
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(json))
        .thenReturn(resp(SUMMARY_RESPONSE));

    // Must not throw NullPointerException
    String result = agent.processQuery("How many datanodes?", null, null);

    assertNotNull(result);
    verify(mockToolExecutor, times(1)).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
  }

  @Test
  public void testWrongParametersTypeMappedToEmptyMap() throws Exception {
    // When LLM returns "parameters" as a plain string instead of an object,
    // parseSingleToolCall should fall back to an empty parameters map
    String json = "{\"type\":\"SINGLE_ENDPOINT\",\"endpoint\":\"/api/v1/datanodes\"," +
        "\"method\":\"GET\",\"parameters\":\"should be an object\",\"reasoning\":\"test\"}";
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(json))
        .thenReturn(resp(SUMMARY_RESPONSE));

    String result = agent.processQuery("How many datanodes?", null, null);

    assertNotNull(result);
    verify(mockToolExecutor, times(1)).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
  }

  // ── Missing required endpoint field ──────────────────────────────────────

  @Test
  public void testMissingEndpointFieldTriggersFallback() throws Exception {
    // SINGLE_ENDPOINT response with no "endpoint" field → empty string → fallback
    String json = "{\"type\":\"SINGLE_ENDPOINT\",\"method\":\"GET\"," +
        "\"parameters\":{},\"reasoning\":\"no endpoint\"}";
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(json))
        .thenReturn(resp(FALLBACK_RESPONSE));

    String result = agent.processQuery("What is the state?", null, null);

    assertNotNull(result);
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  // ── EXE-01: Tool-call count is capped at maxToolCalls ────────────────────

  @Test
  public void testMultiEndpointExceedingMaxToolCallsIsCappedAtFive() throws Exception {
    // Build a MULTI_ENDPOINT response with 20 tool_calls; maxToolCalls config is 5
    StringBuilder sb = new StringBuilder();
    sb.append("{\"type\":\"MULTI_ENDPOINT\",\"reasoning\":\"need many\",\"tool_calls\":[");
    for (int i = 0; i < 20; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"endpoint\":\"/api/v1/clusterState\",\"method\":\"GET\"," +
          "\"parameters\":{},\"reasoning\":\"call ").append(i).append("\"}");
    }
    sb.append("]}");

    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(sb.toString()))
        .thenReturn(resp(SUMMARY_RESPONSE));

    agent.processQuery("Tell me everything about the cluster", null, null);

    // Must cap at maxToolCalls=5, not execute all 20
    verify(mockToolExecutor, atMost(5)).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
  }

  @Test
  public void testMultiEndpointWithEmptyToolCallsArrayTriggersFallback() throws Exception {
    String json = "{\"type\":\"MULTI_ENDPOINT\",\"reasoning\":\"test\"," +
        "\"tool_calls\":[]}";
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(json))
        .thenReturn(resp(FALLBACK_RESPONSE));

    String result = agent.processQuery("Show all data", null, null);

    assertNotNull(result);
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  // ── VAL-01: Empty / null query rejected before LLM call ──────────────────

  @Test
  public void testEmptyQueryThrowsChatbotExceptionBeforeLlmCall() throws LLMClient.LLMException {
    assertThrows(ChatbotException.class,
        () -> agent.processQuery("", null, null));

    // No LLM call should be made at all
    verify(mockLlmClient, never()).chatCompletion(anyList(), any(), any());
  }

  @Test
  public void testNullQueryThrowsChatbotExceptionBeforeLlmCall() throws LLMClient.LLMException {
    assertThrows(ChatbotException.class,
        () -> agent.processQuery(null, null, null));

    verify(mockLlmClient, never()).chatCompletion(anyList(), any(), any());
  }

  // ── ROB-01 (via processQuery): Prose-wrapped JSON parsed successfully ─────

  @Test
  public void testProseWrappedJsonIsExtractedAndParsedCorrectly() throws Exception {
    // LLM returns prose before and after the JSON blob
    String wrappedJson = "Sure! Here is the call: " + SINGLE_DATANODES +
        " Hope that helps!";
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(wrappedJson))
        .thenReturn(resp(SUMMARY_RESPONSE));

    String result = agent.processQuery("How many datanodes?", null, null);

    assertNotNull(result);
    verify(mockToolExecutor, times(1)).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
  }

  // ── LLM exception propagation ─────────────────────────────────────────────

  @Test
  public void testLlmExceptionIsPropagatedAsChatbotException() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenThrow(new LLMClient.LLMException("LLM API unavailable"));

    ChatbotException ex = assertThrows(ChatbotException.class,
        () -> agent.processQuery("What is the state?", null, null));

    // The original LLMException should be the cause — not swallowed
    assertNotNull(ex.getCause(),
        "ChatbotException should wrap the original LLMException as its cause");
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
  }

  // ── EXC-01: ToolExecutor IOException is wrapped as ChatbotException ──────

  @Test
  public void testToolExecutorIoExceptionIsWrappedAsChatbotException() throws Exception {
    // First LLM call succeeds (tool selection), then ToolExecutor throws IOException
    // (e.g. Recon API is down). The agent must wrap it as ChatbotException,
    // not let the raw IOException leak to ChatbotEndpoint.
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(SINGLE_CLUSTER_STATE));
    when(mockToolExecutor.executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt()))
        .thenThrow(new IOException("Recon API unreachable"));

    ChatbotException ex = assertThrows(ChatbotException.class,
        () -> agent.processQuery("What is the cluster state?", null, null));

    assertNotNull(ex.getCause(), "IOException must be preserved as the cause");
    assertTrue(ex.getCause() instanceof IOException,
        "Cause should be the original IOException, not swallowed");
    // ToolExecutor was called — the failure happened inside it
    verify(mockToolExecutor, times(1)).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
  }

  // ── EXC-02: Summarization LLM call fails after tool execution succeeds ───

  @Test
  public void testSummarizationLlmFailureIsWrappedAsChatbotException() throws Exception {
    // First LLM call (tool selection) succeeds.
    // ToolExecutor succeeds.
    // Second LLM call (summarization) throws LLMException.
    // The whole pipeline must fail with ChatbotException, not silently return null.
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(SINGLE_CLUSTER_STATE))           // tool selection: OK
        .thenThrow(new LLMClient.LLMException("Rate limit hit on summarization call"));

    ChatbotException ex = assertThrows(ChatbotException.class,
        () -> agent.processQuery("What is the cluster state?", null, null));

    assertNotNull(ex.getCause(), "LLMException from summarization must be the cause");
    assertTrue(ex.getCause() instanceof LLMClient.LLMException,
        "Cause should be the original LLMException from the summarization call");
    // Both LLM call and executor call were made before the failure
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
    verify(mockToolExecutor, times(1)).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
  }

  // ── EXC-03: SINGLE_ENDPOINT with empty endpoint triggers fallback ─────────

  @Test
  public void testSingleEndpointWithEmptyEndpointTriggersFallback() throws Exception {
    // LLM returns a valid SINGLE_ENDPOINT JSON but with an empty "endpoint" value.
    // The agent must treat this as unanswerable and fall back — never call ToolExecutor
    // with an empty string.
    String emptyEndpoint = "{\"type\":\"SINGLE_ENDPOINT\",\"endpoint\":\"\"," +
        "\"method\":\"GET\",\"parameters\":{},\"reasoning\":\"none\"}";
    when(mockLlmClient.chatCompletion(anyList(), any(), any()))
        .thenReturn(resp(emptyEndpoint))
        .thenReturn(resp(FALLBACK_RESPONSE));

    String result = agent.processQuery("What is happening?", null, null);

    assertNotNull(result);
    // ToolExecutor must never be invoked with an empty endpoint
    verify(mockToolExecutor, never()).executeToolCallWithPolicy(
        anyString(), anyString(), any(), anyInt(), anyInt());
    // Fallback requires a second LLM call
    verify(mockLlmClient, times(2)).chatCompletion(anyList(), any(), any());
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private LLMClient.LLMResponse resp(String content) {
    return new LLMClient.LLMResponse(content, "test-model", 10, 20, null);
  }

  private ToolExecutor.ToolExecutionOutcome defaultOutcome() {
    return new ToolExecutor.ToolExecutionOutcome(
        new HashMap<>(), 0, 1, false, null, new HashMap<>());
  }
}
