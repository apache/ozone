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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotException;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient.ChatMessage;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient.LLMResponse;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient.ToolCallRequest;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient.ToolSpec;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconApiAllowlist;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconQueryExecutor;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconQueryRequest;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main chatbot agent that orchestrates the conversation flow.
 * Handles tool selection (figuring out what API to call), executing those calls,
 * and summarization (feeding the data back to the LLM to write a nice answer).
 */
@Singleton
public class ChatbotAgent {

  private static final Logger LOG = LoggerFactory.getLogger(ChatbotAgent.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // A specific Recon API endpoint we want to handle carefully because it can return millions of rows.

  /**
   * Allowlist of Recon API path prefixes the chatbot is permitted to call.
   * <p>
   * This is the primary defence against prompt injection: even if an attacker tricks
   * the LLM into outputting an arbitrary endpoint, the Java layer will reject it here
   * before ToolExecutor makes any network call. Only paths listed here can ever be
   * executed. Paths are canonicalized (.. resolved) and matched with a boundary-aware
   * prefix check so /api/v1/keys2 does not match /api/v1/keys.
   */

  // The connection to Gemini/OpenAI
  private static final String LIST_KEYS_TOOL = "api_v1_keys_listKeys";
  private final LLMClient llmClient;
  private final ReconQueryExecutor reconQueryExecutor;
  private final ReconApiAllowlist reconApiAllowlist;
  private final LlmToolSpecFactory llmToolSpecFactory;

  // Prompt preamble for tool selection — loaded from classpath resource
  private final String toolSelectionPreamble;

  // Semantic API guide for tool-selection reasoning (transport-agnostic)
  private final String apiGuide;

  // System prompt for the summarization LLM call — loaded from classpath resource
  private final String summarizationPrompt;

  // Template for the fallback response when no endpoint matches — loaded from classpath resource
  private final String fallbackPromptTemplate;

  // Max API calls we allow per question (so the LLM doesn't DOS our server)
  private final int maxToolCalls;

  private final boolean requireSafeScope;

  @Inject
  public ChatbotAgent(LLMClient llmClient,
                      ReconQueryExecutor reconQueryExecutor,
                      ReconApiAllowlist reconApiAllowlist,
                      LlmToolSpecFactory llmToolSpecFactory,
                      OzoneConfiguration configuration) {
    this.llmClient = llmClient;
    this.reconQueryExecutor = reconQueryExecutor;
    this.reconApiAllowlist = reconApiAllowlist;
    this.llmToolSpecFactory = llmToolSpecFactory;

    // Read the Schema (Cheat Sheet) from the resources' folder.
    // Load prompt texts from classpath resources so they can be edited as plain text
    // without touching Java code. If a file is missing the method returns "" and the
    // prompt builder falls back to an inline default.
    this.toolSelectionPreamble = ChatbotUtils.loadResourceFromClasspath(
        "chatbot/recon-tool-selection-prompt-preamble.txt");
    this.apiGuide = ChatbotUtils.loadResourceFromClasspath(
        "chatbot/recon-tool-semantics.md");
    this.summarizationPrompt = ChatbotUtils.loadResourceFromClasspath(
        "chatbot/recon-summarization-prompt.txt");
    this.fallbackPromptTemplate = ChatbotUtils.loadResourceFromClasspath(
        "chatbot/recon-fallback-prompt-template.txt");

    if (!toolSelectionPreamble.isEmpty()) {
      LOG.info("Loaded tool-selection prompt preamble from classpath");
    }
    if (!apiGuide.isEmpty()) {
      LOG.info("Loaded semantic API guide for tool selection from classpath");
    }
    if (!summarizationPrompt.isEmpty()) {
      LOG.info("Loaded summarization prompt from classpath");
    }
    if (!fallbackPromptTemplate.isEmpty()) {
      LOG.info("Loaded fallback prompt template from classpath");
    }

    // Load all the safeguards and settings from ozone-site.xml
    this.maxToolCalls = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_TOOL_CALLS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_TOOL_CALLS_DEFAULT);
    this.requireSafeScope = configuration.getBoolean(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE_DEFAULT);

    LOG.info("ChatbotAgent initialized with requireSafeScope={}", requireSafeScope);
  }

  /**
   * THE MAIN ENTRY POINT. Processes a user query and returns a response.
   *
   * <p>API keys are always resolved server-side via
   * {@link org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper} — there
   * is no per-request key parameter. All internal errors (LLM failures, IO errors, etc.)
   * are wrapped in {@link ChatbotException} so callers have a single typed exception
   * to handle.</p>
   *
   * @param userQuery the user's question
   * @param model     the LLM model to use (null uses the configured default)
   * @param provider  explicit provider name (optional, e.g. "gemini", "openai")
   * @return the chatbot response
   * @throws ChatbotException if query processing fails for any reason
   */
  public String processQuery(String userQuery, String model, String provider)
      throws ChatbotException {

    // Safety check
    if (StringUtils.isBlank(userQuery)) {
      throw new ChatbotException("Query cannot be empty");
    }

    // Provider and model are resolved in LangChain4jDispatcher (defaults applied there).
    LOG.info("Processing query with model: {}, provider: {}",
        model == null || model.isEmpty() ? "default" : model,
        provider == null || provider.isEmpty() ? "default" : provider);

    try {
      // STEP 1: Ask the LLM what API tools it wants to use to answer the question.
      ToolCall toolCall = getToolCall(userQuery, model, provider);

      // If the LLM doesn't know what API to call...
      if (toolCall == null) {
        // No suitable endpoint found
        LOG.info("Tool selection result: NO_SUITABLE_ENDPOINT; using fallback");
        return handleFallback(userQuery, model, provider);
      }

      // If the user asked a general question (e.g. "What is Ozone?"), the LLM answers it directly without an API call.
      if (toolCall.isDocumentationQuery()) {
        LOG.info("Tool selection result: DOCUMENTATION_QUERY (no Recon API call)");
        return toolCall.getAnswer();
      }

      // STEP 2: Execute the internal Recon API calls
      Map<String, Object> apiResponses;
      Map<String, Object> executionMetadata = new HashMap<>();

      // Scenario A: LLM says we need to call MULTIPLE APIs to get the answer
      if (toolCall.isMultipleEndpoints()) {

        if (toolCall.getToolCalls() == null || toolCall.getToolCalls().isEmpty()) {
          LOG.warn("LLM returned MULTI_ENDPOINT but no tool calls");
          return handleFallback(userQuery, model, provider);
        }
        LOG.info("Tool selection result: MULTI_ENDPOINT count={}",
            toolCall.getToolCalls().size());

        // Check if the LLM asked for something dangerous (like scanning the whole cluster without a limit)
        String clarification = buildClarificationForToolCalls(toolCall.getToolCalls());
        if (clarification != null) {
          LOG.info("Execution policy returned clarification for multi-endpoint " +
              "request: {}", clarification);
          return clarification;
        }
        for (ToolCall selected : toolCall.getToolCalls()) {
          LOG.info("Selected Recon API: method={}, toolName={}, paramKeys={}, reasoning={}",
              selected.getMethod(),
              selected.getToolName(),
              selected.getParameters() == null ? "[]" : selected.getParameters().keySet(),
              selected.getReasoning());
        }

        // Execute all the API calls securely
        apiResponses = executeMultipleToolCalls(toolCall.getToolCalls(), executionMetadata);

        // Scenario B: LLM says we only need ONE API call
      } else {
        if (toolCall.getToolName() == null || toolCall.getToolName().isEmpty()) {
          LOG.warn("LLM returned SINGLE_ENDPOINT with empty toolName");
          return handleFallback(userQuery, model, provider);
        }
        LOG.info("Tool selection result: SINGLE_ENDPOINT method={}, toolName={}, paramKeys={}, reasoning={}",
            toolCall.getMethod(),
            toolCall.getToolName(),
            toolCall.getParameters() == null ? "[]" : toolCall.getParameters().keySet(),
            toolCall.getReasoning());
        String clarification = validateToolCallForExecution(toolCall);
        if (clarification != null) {
          LOG.info("Execution policy returned clarification for toolName {}: {}",
              toolCall.getToolName(), clarification);
          return clarification;
        }
        try {
          ReconQueryRequest request = new ReconQueryRequest(
              toolCall.getToolName(),
              toolCall.getMethod(),
              toolCall.getParameters());
          ReconQueryResult outcome = reconQueryExecutor.execute(request);

          // Save the raw JSON data the API returned
          apiResponses = new HashMap<>();
          apiResponses.put(toolCall.getToolName(), outcome.getResponseBody());
          executionMetadata.put(toolCall.getToolName(), createExecutionMetadataMap(outcome));
        } catch (Exception e) {
          throw new ChatbotException("Error executing tool call: " + e.getMessage(), e);
        }
      }

      // STEP 3: Send the raw JSON data BACK to the LLM to format a nice answer
      LOG.info("Summarization input prepared: endpointCount={}, endpoints={}",
          apiResponses.size(), apiResponses.keySet());
      return summarizeResponse(userQuery, apiResponses, executionMetadata, model, provider);

    } catch (ChatbotException e) {
      throw e;
    } catch (Exception e) {
      throw new ChatbotException("Failed to process chatbot query: " + e.getMessage(), e);
    }
  }

  /**
   * "Step 1" Helper: Talks to the LLM and asks for a JSON object telling us which API to call.
   */
  private ToolCall getToolCall(String userQuery, String model,
                               String provider) throws LLMClient.LLMException, IOException {

    // --- 1. BUILD THE PROMPT ---
    // The system prompt teaches the LLM the Recon API schema and the rules for picking a tool.
    // The user prompt is just the raw question the user typed.
    String systemPrompt = buildToolSelectionPrompt();
    String userPrompt = "User Query: " + userQuery;

    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new ChatMessage("system", systemPrompt));
    messages.add(new ChatMessage("user", userPrompt));

    // --- 2. CONFIGURE GENERATION SETTINGS ---
    // Temperature 0.1: very low creativity — we want strict, deterministic tool selection.
    // max_tokens 8192: allow a large enough reply to fit all tool descriptions.
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("temperature", 0.1);
    parameters.put("max_tokens", 8192);

    // --- 3. SEND TO LLM WITH TOOL SPECS ---
    // Attach all allowed Recon API tools so the LLM can pick which one to invoke.
    // The LLM can either reply in text (JSON) or use native tool-call format.
    List<ToolSpec> specs = llmToolSpecFactory.getToolSpecs();
    LLMResponse response = llmClient.chatWithTools(messages, model, provider, parameters, specs);

    LOG.info("Tool selection LLM response: model={}, promptTokens={}, completionTokens={}, totalTokens={}",
        response.getModel(),
        response.getPromptTokens(),
        response.getCompletionTokens(),
        response.getTotalTokens());

    // --- 4. HANDLE NATIVE TOOL CALLS ---
    // Modern models (e.g. GPT-4, Gemini) return structured tool-call objects instead of text.
    // If the LLM picked one tool, parse it directly.
    // If it picked multiple, wrap them in a MULTI_ENDPOINT ToolCall (capped at maxToolCalls).
    if (response.getToolCalls() != null && !response.getToolCalls().isEmpty()) {
      if (response.getToolCalls().size() == 1) {
        return parseNativeToolCall(response.getToolCalls().get(0));
      } else {
        ToolCall multi = new ToolCall();
        multi.setMultipleEndpoints(true);
        List<ToolCall> calls = new ArrayList<>();
        for (int i = 0; i < Math.min(response.getToolCalls().size(), maxToolCalls); i++) {
          calls.add(parseNativeToolCall(response.getToolCalls().get(i)));
        }
        multi.setToolCalls(calls);
        return multi;
      }
    }

    // --- 5. FALLBACK: PARSE TEXT RESPONSE ---
    // Older models (or when native tool calls are not triggered) reply with plain text.
    String content = response.getContent().trim();

    // If the LLM decided no Recon API can answer the question, signal the caller to use fallback.
    if (content.contains("NO_SUITABLE_ENDPOINT")) {
      return null;
    }

    if (!content.isEmpty()) {
      // The LLM returned free text (e.g. a general Ozone question) — treat it as a direct answer.
      ToolCall docQuery = new ToolCall();
      docQuery.setDocumentationQuery(true);
      docQuery.setAnswer(content);
      docQuery.setReasoning("Direct text response from LLM");
      return docQuery;
    }

    LOG.warn("Empty text response from LLM");
    return null;
  }

  private ToolCall parseNativeToolCall(LLMClient.ToolCallRequest req) {
    ToolCall call = new ToolCall();
    call.setMethod("GET");

    Map<String, String> params = new HashMap<>();
    try {
      JsonNode args = MAPPER.readTree(req.getArgumentsJson());
      if (args != null && args.isObject()) {
        args.fields().forEachRemaining(entry -> {
          params.put(entry.getKey(), entry.getValue().asText());
        });
      }
    } catch (Exception e) {
      LOG.warn("Failed to parse native tool call arguments JSON: {}", req.getArgumentsJson(), e);
    }
    call.setToolName(req.getToolName());
    call.setParameters(params);
    call.setReasoning("Native tool call");
    return call;
  }

  /**
   * Executes multiple tool calls.
   */
  private Map<String, Object> executeMultipleToolCalls(
      List<ToolCall> toolCalls, Map<String, Object> executionMetadata) {
    Map<String, Object> responses = new HashMap<>();

    for (int i = 0; i < toolCalls.size(); i++) {
      ToolCall toolCall = toolCalls.get(i);
      String responseKey = buildResponseKey(toolCall, i, toolCalls.size());
      try {
        LOG.info("Executing Recon API call: method={}, toolName={}", toolCall.getMethod(), toolCall.getToolName());
        ReconQueryRequest request = new ReconQueryRequest(
            toolCall.getToolName(),
            toolCall.getMethod(),
            toolCall.getParameters());
        ReconQueryResult outcome = reconQueryExecutor.execute(request);
        responses.put(responseKey, outcome.getResponseBody());
        executionMetadata.put(responseKey, createExecutionMetadataMap(outcome));
        LOG.info("Recon API call completed: toolName={}, records={}, truncated={}",
            toolCall.getToolName(),
            outcome.getRecordsProcessed(),
            outcome.isTruncated());
      } catch (Exception e) {
        LOG.error("Tool call failed for toolName: {}", toolCall.getToolName(), e);
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("error", e.getMessage());
        responses.put(responseKey, errorMap);
        Map<String, Object> errorMeta = new HashMap<>();
        errorMeta.put("error", e.getMessage());
        errorMeta.put("truncated", false);
        executionMetadata.put(responseKey, errorMeta);
      }
    }

    return responses;
  }

  /**
   * "Step 3" Helper: Takes the raw JSON API data and asks the LLM to write a sentence about it.
   */
  private String summarizeResponse(String userQuery,
                                   Map<String, Object> apiResponses,
                                   Map<String, Object> executionMetadata,
                                   String model, String provider)
      throws ChatbotException {

    // Give the LLM a new set of rules
    String systemPrompt = buildSummarizationPrompt();
    // Stitch the raw JSON strings and the user's original question together
    String userPrompt = buildSummarizationUserPrompt(userQuery, apiResponses, executionMetadata);

    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new ChatMessage("system", systemPrompt));
    messages.add(new ChatMessage("user", userPrompt));

    // Temperature 0.3 allows a tiny bit more natural/human-like language creativity.
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("temperature", 0.3);
    parameters.put("max_tokens", 2000);

    try {
      LLMResponse response = llmClient.chatCompletion(messages, model, provider, parameters);

      LOG.info("Summarization LLM response: model={}, promptTokens={}, " +
              "completionTokens={}, totalTokens={}",
          response.getModel(),
          response.getPromptTokens(),
          response.getCompletionTokens(),
          response.getTotalTokens());

      return response.getContent();
    } catch (Exception e) {
      throw new ChatbotException("Error generating response: " + e.getMessage(), e);
    }
  }

  /**
   * Helper: If the user asks "What is the meaning of life?", we use this to say
   * "Sorry, I only know about Hadoop."
   * The prompt template is loaded from {@code chatbot/recon-fallback-prompt-template.txt}.
   * The single {@code %s} placeholder is substituted with the user's original query.
   * Plain string replacement is used instead of {@code String.format} to avoid
   * {@link java.util.MissingFormatArgumentException} when the user query contains
   * a {@code %} character (e.g. "What is 50% of cluster capacity?").
   */
  private String handleFallback(String userQuery, String model,
                                String provider) throws ChatbotException {
    String prompt = fallbackPromptTemplate.replace("%s", userQuery);

    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new ChatMessage("user", prompt));

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("temperature", 0.5);
    parameters.put("max_tokens", 500);

    try {
      LLMResponse response = llmClient.chatCompletion(messages, model, provider, parameters);

      return response.getContent();
    } catch (Exception e) {
      throw new ChatbotException("Error generating fallback response: " + e.getMessage(), e);
    }
  }

  /**
   * Creates the system prompt for tool selection (Step 1 LLM call).
   * <p>
   * Combines the preamble (security rules, examples, safety rules) with the semantic
   * API guide. Tool names in the native tool list map to guide paths: {@code api_v1_X}
   * corresponds to {@code /api/v1/X} with slashes replaced by underscores.
   */
  private String buildToolSelectionPrompt() {
    if (apiGuide.isEmpty()) {
      return toolSelectionPreamble;
    }
    return toolSelectionPreamble
        + "\n\n---\n\n## Semantic API Guide\n\n"
        + "Use this guide to disambiguate difficult requests. Guide paths like `/keys/listKeys` "
        + "map to tool names like `api_v1_keys_listKeys`.\n\n"
        + apiGuide;
  }

  /**
   * Returns the system prompt for the summarization LLM call (Step 3).
   * Loaded from {@code chatbot/recon-summarization-prompt.txt} at startup.
   */
  private String buildSummarizationPrompt() {
    return summarizationPrompt;
  }

  /**
   * Builds the user prompt for summarization.
   */
  private String buildSummarizationUserPrompt(String userQuery,
                                              Map<String, Object> apiResponses,
                                              Map<String, Object> executionMetadata) {
    StringBuilder sb = new StringBuilder();
    sb.append("User asked: \"").append(userQuery).append("\"\n\n");

    for (Map.Entry<String, Object> entry : apiResponses.entrySet()) {
      sb.append("Endpoint: ").append(entry.getKey()).append('\n');
      try {
        String responseJson = MAPPER.writeValueAsString(entry.getValue());
        sb.append("Response: ").append(responseJson).append("\n\n");
      } catch (Exception e) {
        sb.append("Response: ").append(entry.getValue()).append("\n\n");
      }
      Object metadata = executionMetadata.get(entry.getKey());
      if (metadata != null) {
        try {
          sb.append("ExecutionMetadata: ")
              .append(MAPPER.writeValueAsString(metadata)).append("\n\n");
        } catch (Exception e) {
          sb.append("ExecutionMetadata: ").append(metadata).append("\n\n");
        }
      }
    }

    sb.append("Provide a clear summary that answers the user's question.");
    return sb.toString();
  }

  private String buildClarificationForToolCalls(List<ToolCall> toolCalls) {
    List<String> clarificationMessages = new ArrayList<>();
    for (ToolCall toolCall : toolCalls) {
      String clarification = validateToolCallForExecution(toolCall);
      if (clarification != null) {
        clarificationMessages.add(clarification);
      }
    }
    if (clarificationMessages.isEmpty()) {
      return null;
    }
    return clarificationMessages.get(0);
  }

  /**
   * Safety check: validates the endpoint the LLM wants to call before ToolExecutor
   * makes any network request.
   * <p>
   * Two layers of defence:
   * <p>
   * 1. Allowlist check (always active): the normalised endpoint path must start with
   * one of the known Recon API prefixes in ALLOWED_ENDPOINT_PREFIXES. This is the
   * hard Java-side guard against prompt injection — regardless of what the LLM
   * was tricked into outputting, only pre-approved paths can ever be called.
   * <p>
   * 2. Safe-scope check (when requireSafeScope is true): additional validation for
   * endpoints that can return unbounded data, e.g. /keys/listKeys requires a
   * bucket-scoped startPrefix to avoid memory exhaustion.
   */
  private String validateToolCallForExecution(ToolCall toolCall) {
    if (toolCall == null || toolCall.getToolName() == null) {
      return null;
    }
    String toolName = toolCall.getToolName();

    // Layer 1: Allowlist — reject anything not in our known-safe set.
    if (!reconApiAllowlist.isRegistered(toolName)) {
      LOG.warn("Blocked disallowed toolName from LLM output: {}", toolName);
      return "I can only query known Recon APIs. The requested tool '" +
          toolName + "' is not in the list of permitted tools.";
    }

    // Layer 2: Safe-scope check for endpoints that can return unbounded data.
    if (!requireSafeScope) {
      return null;
    }

    if (!LIST_KEYS_TOOL.equals(toolName)) {
      return null;
    }

    String startPrefix = null;
    if (toolCall.getParameters() != null) {
      startPrefix = toolCall.getParameters().get("startPrefix");
    }
    if (!ChatbotUtils.isBucketScopedListKeysPrefix(startPrefix)) {
      return "I need a bucket-scoped prefix to run listKeys. " +
          "This chatbot returns at most 1000 records per request and is not a " +
          "cluster-wide search engine. Please provide startPrefix as " +
          "/<volume>/<bucket> (optionally with a deeper path), and an optional " +
          "limit up to 1000 to narrow the sample.";
    }
    return null;
  }

  private String buildResponseKey(ToolCall toolCall, int index, int total) {
    String toolName = toolCall == null ? "unknown" : toolCall.getToolName();
    if (total <= 1) {
      return toolName;
    }
    return toolName + " [call " + (index + 1) + "]";
  }

  private Map<String, Object> createExecutionMetadataMap(
      ReconQueryResult outcome) {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("recordsProcessed", outcome.getRecordsProcessed());
    metadata.put("truncated", outcome.isTruncated());
    metadata.put("maxRecords", outcome.getMaxRecords());
    return metadata;
  }

  /**
   * Data Transfer Object representing the JSON tool call the LLM returned.
   */
  private static class ToolCall {
    private String toolName;
    private String method;
    private Map<String, String> parameters;
    private String reasoning;
    private boolean documentationQuery;
    private String answer;
    private boolean multipleEndpoints;
    private List<ToolCall> toolCalls;

    public String getToolName() {
      return toolName;
    }

    public void setToolName(String toolName) {
      this.toolName = toolName;
    }

    public String getMethod() {
      return method;
    }

    public void setMethod(String method) {
      this.method = method;
    }

    public Map<String, String> getParameters() {
      return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
      this.parameters = parameters;
    }

    public String getReasoning() {
      return reasoning;
    }

    public void setReasoning(String reasoning) {
      this.reasoning = reasoning;
    }

    public boolean isDocumentationQuery() {
      return documentationQuery;
    }

    public void setDocumentationQuery(boolean documentationQuery) {
      this.documentationQuery = documentationQuery;
    }

    public String getAnswer() {
      return answer;
    }

    public void setAnswer(String answer) {
      this.answer = answer;
    }

    public boolean isMultipleEndpoints() {
      return multipleEndpoints;
    }

    public void setMultipleEndpoints(boolean multipleEndpoints) {
      this.multipleEndpoints = multipleEndpoints;
    }

    public List<ToolCall> getToolCalls() {
      return toolCalls;
    }

    public void setToolCalls(List<ToolCall> toolCalls) {
      this.toolCalls = toolCalls;
    }
  }
}
