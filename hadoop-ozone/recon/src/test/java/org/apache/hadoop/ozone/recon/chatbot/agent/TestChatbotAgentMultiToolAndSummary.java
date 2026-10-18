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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
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
 * Tests for {@link ChatbotAgent} orchestration branches that are not exercised by the
 * security ({@code TestChatbotAgentExecutionPolicy}) or listKeys ({@code TestChatbotAgentListKeysPolicy})
 * suites: multi-tool execution with a partial failure, response-key de-duplication, and the
 * blank-summary fallback.
 */
@ExtendWith(MockitoExtension.class)
public class TestChatbotAgentMultiToolAndSummary {

  private static final String SUMMARY_RESPONSE = "Here is the combined result.";
  private static final String BLANK_SUMMARY_FALLBACK =
      "I retrieved the cluster data but could not generate a summary.";

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
  public void testMultiToolPartialFailureDoesNotAbortAndIsSummarized() throws Exception {
    // Two tools selected; one succeeds and one throws. The request must NOT fail — the failing
    // call is captured as an error result and summarization still runs over both.
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(toolCalls(
            new LLMClient.ToolCallRequest("api_v1_clusterState", "{}"),
            new LLMClient.ToolCallRequest("api_v1_datanodes", "{}")));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(SUMMARY_RESPONSE));

    when(mockReconQueryExecutor.execute(eq("api_v1_clusterState"), anyMap()))
        .thenReturn(defaultOutcome());
    when(mockReconQueryExecutor.execute(eq("api_v1_datanodes"), anyMap()))
        .thenThrow(new IOException("Recon API is down"));

    String result = agent.processQuery("cluster state and datanodes", null, null);

    assertEquals(SUMMARY_RESPONSE, result);
    verify(mockReconQueryExecutor, times(2)).execute(anyString(), anyMap());

    // The error from the failed call is included in the summarization input (not silently dropped),
    // and both endpoints appear — proving the batch was not aborted.
    String summarizationPrompt = captureSummarizationUserPrompt();
    assertTrue(summarizationPrompt.contains("Recon API is down"),
        "Failed call's error should be passed to summarization");
    assertTrue(summarizationPrompt.contains("api_v1_clusterState")
            && summarizationPrompt.contains("api_v1_datanodes"),
        "Both endpoints should appear in the summarization input");
  }

  @Test
  public void testDuplicateToolNamesGetDistinctResponseKeys() throws Exception {
    // The same tool selected twice must not collapse into one map entry — buildResponseKey
    // appends " [call N]" so both results survive.
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(toolCalls(
            new LLMClient.ToolCallRequest("api_v1_datanodes", "{}"),
            new LLMClient.ToolCallRequest("api_v1_datanodes", "{}")));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text(SUMMARY_RESPONSE));

    String result = agent.processQuery("datanodes twice", null, null);

    assertEquals(SUMMARY_RESPONSE, result);
    verify(mockReconQueryExecutor, times(2)).execute(anyString(), anyMap());

    String summarizationPrompt = captureSummarizationUserPrompt();
    assertTrue(summarizationPrompt.contains("api_v1_datanodes [call 1]")
            && summarizationPrompt.contains("api_v1_datanodes [call 2]"),
        "Duplicate tool names should be de-duplicated into [call 1] / [call 2] keys");
  }

  @Test
  public void testBlankSummaryReturnsFallbackSentence() throws Exception {
    // Tool executes fine but the summarization model returns blank content — the agent must
    // return a fixed sentence, never an empty answer.
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), anyList()))
        .thenReturn(toolCalls(new LLMClient.ToolCallRequest("api_v1_datanodes", "{}")));
    when(mockLlmClient.chatCompletion(anyList(), any(), any(), any(), isNull()))
        .thenReturn(text("   "));

    String result = agent.processQuery("how many datanodes", null, null);

    assertNotNull(result);
    assertTrue(result.contains(BLANK_SUMMARY_FALLBACK),
        "Blank summary must fall back to a fixed sentence, got: " + result);
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private String captureSummarizationUserPrompt() throws Exception {
    ArgumentCaptor<List<LLMClient.ChatMessage>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockLlmClient).chatCompletion(captor.capture(), any(), any(), any(), isNull());
    StringBuilder sb = new StringBuilder();
    for (LLMClient.ChatMessage msg : captor.getValue()) {
      if ("user".equals(msg.getRole())) {
        sb.append(msg.getContent());
      }
    }
    return sb.toString();
  }

  private LLMClient.LLMResponse text(String content) {
    return new LLMClient.LLMResponse(content, "test-model", 10, 20, null);
  }

  private LLMClient.LLMResponse toolCalls(LLMClient.ToolCallRequest... calls) {
    return new LLMClient.LLMResponse("", "test-model", 10, 20, Arrays.asList(calls));
  }

  private ReconQueryResult defaultOutcome() {
    return new ReconQueryResult(JsonNodeFactory.instance.objectNode(), 0, false, 1000);
  }
}
