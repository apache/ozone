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
  private static final String LIST_KEYS_ENDPOINT_SUFFIX = "/keys/listKeys";

  /**
   * Allowlist of Recon API path prefixes the chatbot is permitted to call.
   * <p>
   * This is the primary defence against prompt injection: even if an attacker tricks
   * the LLM into outputting an arbitrary endpoint, the Java layer will reject it here
   * before ToolExecutor makes any network call. Only paths listed here can ever be
   * executed. Paths are canonicalized (.. resolved) and matched with a boundary-aware
   * prefix check so /api/v1/keys2 does not match /api/v1/keys.
   */
  private static final String API_V1_ROOT = "/api/v1";

  private static final Set<String> ALLOWED_ENDPOINT_PREFIXES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "/api/v1/clusterState",
          "/api/v1/datanodes",
          "/api/v1/pipelines",
          "/api/v1/containers",
          "/api/v1/keys",
          "/api/v1/volumes",
          "/api/v1/buckets",
          "/api/v1/task/status",
          "/api/v1/metrics",
          "/api/v1/utilization",
          "/api/v1/namespace",
          "/api/v1/om"
      )));

  // The connection to Gemini/OpenAI
  private final LLMClient llmClient;

  // The hands that execute the internal API calls
  private final ToolExecutor toolExecutor;

  // The Cheat Sheet of all available APIs loaded from the .md file
  private final String apiSchema;

  // Prompt preamble for tool selection — loaded from classpath resource
  private final String toolSelectionPreamble;

  // System prompt for the summarization LLM call — loaded from classpath resource
  private final String summarizationPrompt;

  // Template for the fallback response when no endpoint matches — loaded from classpath resource
  private final String fallbackPromptTemplate;

  // Max API calls we allow per question (so the LLM doesn't DOS our server)
  private final int maxToolCalls;

  private final String defaultModel;
  private final int maxPagesPerAnswer;
  private final int pageSizePerCall;
  private final boolean requireSafeScope;

  @Inject
  public ChatbotAgent(LLMClient llmClient,
                      ToolExecutor toolExecutor,
                      OzoneConfiguration configuration) {
    this.llmClient = llmClient;
    this.toolExecutor = toolExecutor;

    // Read the Schema (Cheat Sheet) from the resources' folder.
    this.apiSchema = loadApiSchema();

    // Load prompt texts from classpath resources so they can be edited as plain text
    // without touching Java code. If a file is missing the method returns "" and the
    // prompt builder falls back to an inline default.
    this.toolSelectionPreamble = ChatbotUtils.loadResourceFromClasspath(
        "chatbot/recon-tool-selection-prompt-preamble.txt");
    this.summarizationPrompt = ChatbotUtils.loadResourceFromClasspath(
        "chatbot/recon-summarization-prompt.txt");
    this.fallbackPromptTemplate = ChatbotUtils.loadResourceFromClasspath(
        "chatbot/recon-fallback-prompt-template.txt");

    if (!toolSelectionPreamble.isEmpty()) {
      LOG.info("Loaded tool-selection prompt preamble from classpath");
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
    this.defaultModel = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_DEFAULT_MODEL,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_DEFAULT_MODEL_DEFAULT);
    this.maxPagesPerAnswer = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_PAGES,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_PAGES_DEFAULT);
    this.pageSizePerCall = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_PAGE_SIZE,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_PAGE_SIZE_DEFAULT);
    this.requireSafeScope = configuration.getBoolean(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE_DEFAULT);

    LOG.info("ChatbotAgent initialized with model={}, maxPages={}, " +
            "pageSize={}, requireSafeScope={}",
        defaultModel, maxPagesPerAnswer, pageSizePerCall, requireSafeScope);
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

    // Use default model if the user didn't specify one.
    String effectiveModel = (model != null && !model.isEmpty()) ? model : defaultModel;

    LOG.info("Processing query with model: {}, provider: {}", effectiveModel, provider == null ? "auto" : provider);

    try {
      // STEP 1: Ask the LLM what API tools it wants to use to answer the question.
      ToolCall toolCall = getToolCall(userQuery, effectiveModel, provider);

      // If the LLM doesn't know what API to call...
      if (toolCall == null) {
        // No suitable endpoint found
        LOG.info("Tool selection result: NO_SUITABLE_ENDPOINT; using fallback");
        return handleFallback(userQuery, effectiveModel, provider);
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
          return handleFallback(userQuery, effectiveModel, provider);
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
          LOG.info("Selected Recon API: method={}, endpoint={}, paramKeys={}, reasoning={}",
              selected.getMethod(),
              selected.getEndpoint(),
              selected.getParameters() == null ? "[]" : selected.getParameters().keySet(),
              selected.getReasoning());
        }

        // Execute all the API calls securely
        apiResponses = executeMultipleToolCalls(toolCall.getToolCalls(), executionMetadata);

        // Scenario B: LLM says we only need ONE API call
      } else {
        if (toolCall.getEndpoint() == null || toolCall.getEndpoint().isEmpty()) {
          LOG.warn("LLM returned SINGLE_ENDPOINT with empty endpoint");
          return handleFallback(userQuery, effectiveModel, provider);
        }
        LOG.info("Tool selection result: SINGLE_ENDPOINT method={}, endpoint={}, paramKeys={}, reasoning={}",
            toolCall.getMethod(),
            toolCall.getEndpoint(),
            toolCall.getParameters() == null ? "[]" : toolCall.getParameters().keySet(),
            toolCall.getReasoning());
        String clarification = validateToolCallForExecution(toolCall);
        if (clarification != null) {
          LOG.info("Execution policy returned clarification for endpoint {}: {}",
              toolCall.getEndpoint(), clarification);
          return clarification;
        }
        try {
          ToolExecutor.ToolExecutionOutcome outcome = toolExecutor.executeToolCallWithPolicy(
              toolCall.getEndpoint(),
              toolCall.getMethod(),
              toolCall.getParameters(),
              maxPagesPerAnswer,
              pageSizePerCall);

          // Save the raw JSON data the API returned
          apiResponses = new HashMap<>();
          apiResponses.put(toolCall.getEndpoint(), outcome.getResponseBody());
          executionMetadata.put(toolCall.getEndpoint(), createExecutionMetadataMap(outcome));
        } catch (Exception e) {
          throw new ChatbotException("Error executing tool call: " + e.getMessage(), e);
        }
      }

      // STEP 3: Send the raw JSON data BACK to the LLM to format a nice answer
      LOG.info("Summarization input prepared: endpointCount={}, endpoints={}",
          apiResponses.size(), apiResponses.keySet());
      return summarizeResponse(userQuery, apiResponses, executionMetadata, effectiveModel, provider);

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

    // Build the "cheat sheet" prompt (includes the recon-api-guide.md)
    String systemPrompt = buildToolSelectionPrompt();
    String userPrompt = "User Query: " + userQuery;

    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new ChatMessage("system", systemPrompt));
    messages.add(new ChatMessage("user", userPrompt));

    // Tuning the LLM: Temperature 0.1 means we want it to be very strict and robotic, not creative.
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("temperature", 0.1);
    parameters.put("max_tokens", 8192);
    if (provider != null && !provider.isEmpty()) {
      parameters.put("_provider", provider);
    }

    LLMResponse response = llmClient.chatCompletion(messages, model, parameters);

    LOG.info("Tool selection LLM response: model={}, promptTokens={}, completionTokens={}, totalTokens={}",
        response.getModel(),
        response.getPromptTokens(),
        response.getCompletionTokens(),
        response.getTotalTokens());

    String content = response.getContent().trim();

    if (content.contains("NO_SUITABLE_ENDPOINT")) {
      return null;
    }

    // Extract the first complete JSON object from the response.
    // LLMs sometimes wrap their JSON in prose text despite being instructed not to.
    String jsonStr = ChatbotUtils.extractFirstJsonObject(content);
    if (jsonStr == null) {
      LOG.warn("No JSON found in LLM response");
      return null;
    }

    JsonNode jsonNode = MAPPER.readTree(jsonStr);
    return parseToolCall(jsonNode);
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
        LOG.info("Executing Recon API call: method={}, endpoint={}", toolCall.getMethod(), toolCall.getEndpoint());
        ToolExecutor.ToolExecutionOutcome outcome = toolExecutor.executeToolCallWithPolicy(
            toolCall.getEndpoint(),
            toolCall.getMethod(),
            toolCall.getParameters(),
            maxPagesPerAnswer,
            pageSizePerCall);
        responses.put(responseKey, outcome.getResponseBody());
        executionMetadata.put(responseKey, createExecutionMetadataMap(outcome));
        LOG.info("Recon API call completed: endpoint={}, records={}, pages={}, truncated={}",
            toolCall.getEndpoint(),
            outcome.getRecordsProcessed(),
            outcome.getPagesFetched(),
            outcome.isTruncated());
      } catch (Exception e) {
        LOG.error("Tool call failed for endpoint: {}", toolCall.getEndpoint(), e);
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
    if (provider != null && !provider.isEmpty()) {
      parameters.put("_provider", provider);
    }

    try {
      LLMResponse response = llmClient.chatCompletion(messages, model, parameters);

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
    if (provider != null && !provider.isEmpty()) {
      parameters.put("_provider", provider);
    }

    try {
      LLMResponse response = llmClient.chatCompletion(messages, model, parameters);

      return response.getContent();
    } catch (Exception e) {
      throw new ChatbotException("Error generating fallback response: " + e.getMessage(), e);
    }
  }

  /**
   * Creates the system prompt for tool selection (Step 1 LLM call).
   * <p>
   * The preamble (security rules, task description, JSON format examples, safety rules)
   * is loaded from {@code chatbot/recon-tool-selection-prompt-preamble.txt} at startup.
   * The API specification is appended at runtime so the schema stays the single source
   * of truth for available endpoints.
   */
  private String buildToolSelectionPrompt() {
    return toolSelectionPreamble + "API Specification:\n" + apiSchema;
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
    if (toolCall == null || toolCall.getEndpoint() == null) {
      return null;
    }
    String rawEndpoint = ChatbotUtils.normalizeEndpoint(toolCall.getEndpoint());
    String endpoint = ChatbotUtils.canonicalizeEndpointPath(rawEndpoint);
    if (endpoint.isEmpty()) {
      LOG.warn("Blocked invalid endpoint path from LLM output: {}", rawEndpoint);
      return "I can only query known Recon APIs. The requested endpoint '" +
          rawEndpoint + "' is not in the list of permitted paths.";
    }

    // Layer 1: Allowlist — reject anything not in our known-safe prefix set.
    boolean allowed = false;
    for (String prefix : ALLOWED_ENDPOINT_PREFIXES) {
      if (ChatbotUtils.matchesAllowedPrefix(endpoint, prefix)) {
        allowed = true;
        break;
      }
    }
    if (!allowed) {
      LOG.warn("Blocked disallowed endpoint from LLM output: {}", endpoint);
      return "I can only query known Recon APIs. The requested endpoint '" +
          endpoint + "' is not in the list of permitted paths.";
    }

    // Layer 2: Safe-scope check for endpoints that can return unbounded data.
    if (!requireSafeScope) {
      return null;
    }

    if (!endpoint.endsWith(LIST_KEYS_ENDPOINT_SUFFIX)) {
      return null;
    }

    String startPrefix = null;
    if (toolCall.getParameters() != null) {
      startPrefix = toolCall.getParameters().get("startPrefix");
    }
    if (!ChatbotUtils.isBucketScopedListKeysPrefix(startPrefix)) {
      return "I need a bucket-scoped prefix to run listKeys safely. " +
          "Please provide startPrefix in the form /<volume>/<bucket> " +
          "(optionally with a deeper path), plus optional limit and page " +
          "range if you want targeted analysis.";
    }
    return null;
  }

  private String buildResponseKey(ToolCall toolCall, int index, int total) {
    String endpoint = toolCall == null ? "unknown" : toolCall.getEndpoint();
    if (total <= 1) {
      return endpoint;
    }
    return endpoint + " [call " + (index + 1) + "]";
  }

  private Map<String, Object> createExecutionMetadataMap(
      ToolExecutor.ToolExecutionOutcome outcome) {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("recordsProcessed", outcome.getRecordsProcessed());
    metadata.put("pagesFetched", outcome.getPagesFetched());
    metadata.put("truncated", outcome.isTruncated());
    metadata.put("nextCursor", outcome.getNextCursor());
    metadata.put("limitsApplied", outcome.getLimitsApplied());
    return metadata;
  }

  /**
   * Parses the LLM's JSON response into a {@link ToolCall}.
   *
   * <p>The LLM always returns a unified JSON envelope with a {@code "type"} field that
   * identifies one of three response shapes. All three share the same outer structure,
   * differing only in which additional fields are present:
   *
   * <pre>
   * SINGLE_ENDPOINT  — one Recon API call needed:
   * {
   *   "type": "SINGLE_ENDPOINT",
   *   "endpoint": "/api/v1/datanodes",
   *   "method": "GET",
   *   "parameters": { "limit": "10" },
   *   "reasoning": "..."
   * }
   *
   * MULTI_ENDPOINT   — several Recon API calls needed:
   * {
   *   "type": "MULTI_ENDPOINT",
   *   "reasoning": "why multiple endpoints are needed",
   *   "tool_calls": [
   *     { "endpoint": "/api/v1/clusterState", "method": "GET", "parameters": {}, "reasoning": "..." },
   *     { "endpoint": "/api/v1/datanodes",    "method": "GET", "parameters": {}, "reasoning": "..." }
   *   ]
   * }
   *
   * DOCUMENTATION_QUERY — general question answered directly from the LLM's knowledge:
   * {
   *   "type": "DOCUMENTATION_QUERY",
   *   "answer": "Apache Ozone is ...",
   *   "reasoning": "..."
   * }
   * </pre>
   *
   * <p>The {@code type} field is the single discriminator. Having it present on every response
   * lets the parser be a simple {@code switch} rather than a chain of field-existence checks.</p>
   *
   * <p><b>Unrecognized or missing type:</b> Since all three response shapes always include a
   * {@code "type"} field, a missing or unrecognized value means the LLM returned something
   * completely unexpected. In that case this method returns {@code null}. The caller
   * ({@link #getToolCall}) propagates {@code null} to {@link #processQuery}, which then calls
   * {@link #handleFallback} to produce a graceful "I cannot answer this" response.</p>
   */
  private ToolCall parseToolCall(JsonNode jsonNode) {
    String type = jsonNode.path("type").asText("");

    switch (type) {
    case "SINGLE_ENDPOINT":
      return parseSingleToolCall(jsonNode);
    case "MULTI_ENDPOINT":
      return parseMultiEndpointToolCall(jsonNode);
    case "DOCUMENTATION_QUERY":
      return parseDocumentationQueryToolCall(jsonNode);
    default:
      // "type" is missing or unrecognized — the LLM returned something unexpected.
      // Return null so the caller triggers handleFallback() with a graceful error response.
      LOG.warn("Unrecognized LLM response type '{}' — cannot parse tool call, using fallback", type);
      return null;
    }
  }

  private ToolCall parseMultiEndpointToolCall(JsonNode jsonNode) {
    ToolCall toolCall = new ToolCall();
    toolCall.setMultipleEndpoints(true);
    toolCall.setToolCalls(parseToolCallList(jsonNode.get("tool_calls")));
    return toolCall;
  }

  private ToolCall parseDocumentationQueryToolCall(JsonNode jsonNode) {
    ToolCall toolCall = new ToolCall();
    toolCall.setDocumentationQuery(true);
    toolCall.setAnswer(jsonNode.path("answer").asText(""));
    toolCall.setReasoning(jsonNode.path("reasoning").asText(""));
    return toolCall;
  }

  /**
   * Parses the {@code tool_calls} array from a {@code MULTI_ENDPOINT} response into a list
   * of individual {@link ToolCall} objects, capped at {@link #maxToolCalls}.
   *
   * <p>Each element in the array has the same shape as a {@code SINGLE_ENDPOINT} response
   * (endpoint, method, parameters, reasoning), so {@link #parseSingleToolCall} is reused.
   * Entries with a missing or empty endpoint are silently skipped.</p>
   */
  private List<ToolCall> parseToolCallList(JsonNode toolCallsArray) {
    List<ToolCall> result = new ArrayList<>();
    if (toolCallsArray == null || !toolCallsArray.isArray()) {
      return result;
    }
    for (JsonNode tc : toolCallsArray) {
      if (result.size() >= maxToolCalls) {
        LOG.info("Truncating tool_calls from LLM to maxToolCalls={}", maxToolCalls);
        break;
      }
      ToolCall parsed = parseSingleToolCall(tc);
      if (parsed.getEndpoint() != null && !parsed.getEndpoint().isEmpty()) {
        result.add(parsed);
      }
    }
    return result;
  }

  /**
   * Parses a single endpoint entry from JSON.
   *
   * <p>Used both for standalone {@code SINGLE_ENDPOINT} responses and for each element
   * inside the {@code tool_calls} array of a {@code MULTI_ENDPOINT} response. The shape
   * is identical in both cases:
   * <pre>
   * {
   *   "endpoint":   "/api/v1/datanodes",
   *   "method":     "GET",
   *   "parameters": { "key": "value" },
   *   "reasoning":  "..."
   * }
   * </pre>
   * All fields have safe defaults: {@code endpoint} defaults to {@code ""}, {@code method}
   * defaults to {@code "GET"}, and missing parameters produce an empty map.</p>
   */
  private ToolCall parseSingleToolCall(JsonNode jsonNode) {
    ToolCall toolCall = new ToolCall();
    toolCall.setEndpoint(jsonNode.path("endpoint").asText(""));
    toolCall.setMethod(jsonNode.path("method").asText("GET"));

    Map<String, String> parameters = new HashMap<>();
    JsonNode paramsNode = jsonNode.get("parameters");
    if (paramsNode != null && paramsNode.isObject()) {
      paramsNode.fields().forEachRemaining(entry ->
          parameters.put(entry.getKey(), entry.getValue().asText()));
    }
    toolCall.setParameters(parameters);
    toolCall.setReasoning(jsonNode.path("reasoning").asText(""));
    return toolCall;
  }

  /**
   * Loads the API context for the LLM tool-selection prompt.
   *
   * <p>Both documents are always loaded and concatenated:
   * <ul>
   *   <li>{@code recon-api-guide.md} — human-readable guide written for LLM consumption,
   *       describing what each endpoint returns and how to use it.</li>
   *   <li>{@code recon-api.yaml} — full OpenAPI specification with exact paths, parameters,
   *       and response shapes, giving the LLM precise endpoint details.</li>
   * </ul>
   * </p>
   */
  private String loadApiSchema() {
    String guide = ChatbotUtils.loadResourceFromClasspath("chatbot/recon-api-guide.md");
    String yaml = ChatbotUtils.loadResourceFromClasspath("chatbot/recon-api.yaml");

    if (guide.isEmpty() && yaml.isEmpty()) {
      LOG.warn("Neither recon-api-guide.md nor recon-api.yaml found on classpath — using empty schema");
      return "";
    }

    StringBuilder schema = new StringBuilder();
    if (!guide.isEmpty()) {
      LOG.info("Loaded API guide from classpath: chatbot/recon-api-guide.md");
      schema.append(guide);
    }
    if (!yaml.isEmpty()) {
      LOG.info("Loaded API spec from classpath: chatbot/recon-api.yaml");
      if (schema.length() > 0) {
        schema.append("\n\n---\n\n");
      }
      schema.append(yaml);
    }
    return schema.toString();
  }

  /**
   * Data Transfer Object representing the JSON tool call the LLM returned.
   */
  private static class ToolCall {
    private String endpoint;
    private String method;
    private Map<String, String> parameters;
    private String reasoning;
    private boolean documentationQuery;
    private String answer;
    private boolean multipleEndpoints;
    private List<ToolCall> toolCalls;

    public String getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(String endpoint) {
      this.endpoint = endpoint;
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
