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
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
 * Tests for {@link ChatbotAgent} native tool-call parsing and {@code processQuery} routing.
 *
 * <p>Covers how the agent dispatches a tool-selection LLM response: single tool, multi tool
 * (including the {@code maxToolCalls} cap), direct text answers, and fallback paths. Allowlist
 * blocking and listKeys safe-scope are covered by sibling test classes.</p>
 */
@ExtendWith(MockitoExtension.class)
public class TestChatbotAgentToolCallParsing {

  private static final String SUMMARY = "The cluster has 5 healthy datanodes.";
  private static final String FALLBACK = "I can only answer questions about Apache Ozone Recon.";
  private static final String DIRECT_ANSWER =
      "Apache Ozone is a scalable distributed storage system.";

  @Mock
  private LLMClient mockLlmClient;

  @Mock
  private ReconQueryExecutor mockReconQueryExecutor;

  @Mock
  private ReconApiAllowlist mockReconApiAllowlist;

  @Mock
  private LlmToolSpecFactory mockLlmToolSpecFactory;

  private ChatbotAgent agent;

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

  @Test
  public void testSingleToolCallExecutesAndSummarizes() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(toolCall("api_v1_datanodes", "{}"));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(SUMMARY));

    String result = agent.processQuery("How many datanodes?", null, null);

    assertEquals(SUMMARY, result);
    verify(mockReconQueryExecutor, times(1)).execute(anyString(), anyMap());
  }

  @Test
  public void testSingleToolCallArgumentsParsedIntoParams() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(toolCall("api_v1_datanodes", "{\"limit\":\"50\"}"));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(SUMMARY));

    agent.processQuery("How many datanodes?", null, null);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> paramsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockReconQueryExecutor, times(1)).execute(anyString(), paramsCaptor.capture());
    assertEquals("50", paramsCaptor.getValue().get("limit"));
  }

  @Test
  public void testMultiToolCallExecutesEach() throws Exception {
    List<LLMClient.ToolCallRequest> reqs = new ArrayList<>();
    reqs.add(new LLMClient.ToolCallRequest("api_v1_clusterState", "{}"));
    reqs.add(new LLMClient.ToolCallRequest("api_v1_datanodes", "{}"));

    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(toolCalls(reqs));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(SUMMARY));

    String result = agent.processQuery("Show datanodes and cluster state", null, null);

    assertEquals(SUMMARY, result);
    verify(mockReconQueryExecutor, times(2)).execute(anyString(), anyMap());
  }

  @Test
  public void testMultiToolCallCappedAtMaxToolCalls() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(repeatedToolCalls("api_v1_clusterState", 20));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(SUMMARY));

    agent.processQuery("Tell me everything about the cluster", null, null);

    verify(mockReconQueryExecutor, atMost(5)).execute(anyString(), anyMap());
  }

  @Test
  public void testDirectAnswerReturnedVerbatimNoExecutor() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(text(DIRECT_ANSWER));

    String result = agent.processQuery("What is Apache Ozone?", null, null);

    assertTrue(result.contains("Apache Ozone"));
    verify(mockReconQueryExecutor, never()).execute(anyString(), anyMap());
    verify(mockLlmClient, never()).chatCompletion(anyList(), any(), any(), any(), isNull());
  }

  @Test
  public void testNoSuitableEndpointSentinelTriggersFallback() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(text("NO_SUITABLE_ENDPOINT"));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(FALLBACK));

    String result = agent.processQuery("What is the meaning of life?", null, null);

    assertEquals(FALLBACK, result);
    verify(mockReconQueryExecutor, never()).execute(anyString(), anyMap());
    verify(mockLlmClient, times(1)).chatCompletion(anyList(), any(), any(), any(), isNull());
  }

  @Test
  public void testEmptyTextResponseTriggersFallback() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(text(""));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(FALLBACK));

    String result = agent.processQuery("What is the state?", null, null);

    assertEquals(FALLBACK, result);
    verify(mockReconQueryExecutor, never()).execute(anyString(), anyMap());
    verify(mockLlmClient, times(1)).chatCompletion(anyList(), any(), any(), any(), isNull());
  }

  @Test
  public void testMalformedToolArgumentsDegradeToEmptyParams() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(toolCall("api_v1_datanodes", "{not json"));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(SUMMARY));

    agent.processQuery("How many datanodes?", null, null);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> paramsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockReconQueryExecutor, times(1)).execute(anyString(), paramsCaptor.capture());
    assertTrue(paramsCaptor.getValue().isEmpty());
  }

  @Test
  public void testEmptyToolNameTriggersFallback() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(toolCall("", "{}"));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(FALLBACK));

    String result = agent.processQuery("What is happening?", null, null);

    assertEquals(FALLBACK, result);
    verify(mockReconQueryExecutor, never()).execute(anyString(), anyMap());
    verify(mockLlmClient, times(1)).chatCompletion(anyList(), any(), any(), any(), isNull());
  }

  @Test
  public void testEmptyQueryThrowsBeforeLlmCall() {
    assertThrows(ChatbotException.class, () -> agent.processQuery("", null, null));
    verifyNoInteractions(mockLlmClient);
  }

  @Test
  public void testNullQueryThrowsBeforeLlmCall() {
    assertThrows(ChatbotException.class, () -> agent.processQuery(null, null, null));
    verifyNoInteractions(mockLlmClient);
  }

  @Test
  public void testLlmExceptionDuringSelectionWrappedAsChatbotException() throws Exception {
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenThrow(new LLMClient.LLMException("LLM API unavailable"));

    ChatbotException ex = assertThrows(ChatbotException.class,
        () -> agent.processQuery("What is the state?", null, null));

    assertNotNull(ex.getCause());
    verify(mockReconQueryExecutor, never()).execute(anyString(), anyMap());
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private LLMClient.LLMResponse text(String content) {
    return new LLMClient.LLMResponse(content, "test-model", 10, 20, null);
  }

  private LLMClient.LLMResponse toolCall(String name, String argsJson) {
    return new LLMClient.LLMResponse("", "test-model", 10, 20,
        Collections.singletonList(new LLMClient.ToolCallRequest(name, argsJson)));
  }

  private LLMClient.LLMResponse toolCalls(List<LLMClient.ToolCallRequest> reqs) {
    return new LLMClient.LLMResponse("", "test-model", 10, 20, reqs);
  }

  private LLMClient.LLMResponse repeatedToolCalls(String toolName, int count) {
    List<LLMClient.ToolCallRequest> reqs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      reqs.add(new LLMClient.ToolCallRequest(toolName, "{}"));
    }
    return toolCalls(reqs);
  }

  private ReconQueryResult defaultOutcome() {
    return new ReconQueryResult(JsonNodeFactory.instance.objectNode(), 0, false, 1000);
  }
}
