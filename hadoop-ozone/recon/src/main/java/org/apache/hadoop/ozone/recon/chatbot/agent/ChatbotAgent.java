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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotException;
import org.apache.hadoop.ozone.recon.chatbot.llm.GenParams;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient.ChatMessage;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient.LLMResponse;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient.ToolSpec;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconApiAllowlist;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconQueryExecutor;
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
      ToolSelection selection = chooseToolsForQuery(userQuery, model, provider);

      // If the LLM doesn't know what API to call...
      if (selection == null) {
        // No suitable endpoint found
        LOG.info("Tool selection result: NO_SUITABLE_ENDPOINT; using fallback");
        return handleFallback(userQuery, model, provider);
      }

      // If the user asked a general question (e.g. "What is Ozone?"), the LLM answers it directly without an API call.
      if (selection.kind() == ToolSelectionKind.DIRECT_ANSWER) {
        LOG.info("Tool selection result: DOCUMENTATION_QUERY (no Recon API call)");
        return selection.answer();
      }

      // STEP 2: Execute the internal Recon API calls
      Map<String, EndpointResult> apiResults;

      // Scenario A: LLM says we need to call MULTIPLE APIs to get the answer
      if (selection.kind() == ToolSelectionKind.MULTI) {

        if (selection.calls() == null || selection.calls().isEmpty()) {
          LOG.warn("LLM returned MULTI_ENDPOINT but no tool calls");
          return handleFallback(userQuery, model, provider);
        }
        LOG.info("Tool selection result: MULTI_ENDPOINT count={}",
            selection.calls().size());

        String error = validateToolCalls(selection.calls());
        if (error != null) {
          LOG.info("Blocked multi-tool request: {}", error);
          return error;
        }
        for (ToolSelection selected : selection.calls()) {
          LOG.info("Selected Recon API: toolName={}, paramKeys={}",
              selected.toolName(),
              selected.parameters() == null ? "[]" : selected.parameters().keySet());
        }

        // Execute all the API calls securely
        apiResults = executeMultipleToolCalls(selection.calls());

        // Scenario B: LLM says we only need ONE API call
      } else {
        if (selection.toolName() == null || selection.toolName().isEmpty()) {
          LOG.warn("LLM returned SINGLE_ENDPOINT with empty toolName");
          return handleFallback(userQuery, model, provider);
        }
        LOG.info("Tool selection result: SINGLE_ENDPOINT toolName={}, paramKeys={}",
            selection.toolName(),
            selection.parameters() == null ? "[]" : selection.parameters().keySet());

        // Check if any safety condition is violated by the tool
        String error = validateToolCall(selection.toolName(), selection.parameters());
        if (error != null) {
          LOG.info("Blocked tool call {}: {}", selection.toolName(), error);
          return error;
        }
        try {
          ReconQueryResult outcome = reconQueryExecutor.execute(selection.toolName(), selection.parameters());

          // Save the raw JSON data the API returned
          apiResults = new HashMap<>();
          apiResults.put(selection.toolName(),
              new EndpointResult(outcome.getResponseBody(), createExecutionMetadataMap(outcome)));
        } catch (Exception e) {
          throw new ChatbotException("Error executing tool call: " + e.getMessage(), e);
        }
      }

      // STEP 3: Send the raw JSON data BACK to the LLM to format a nice answer
      LOG.info("Summarization input prepared: endpointCount={}, endpoints={}", apiResults.size(), apiResults.keySet());
      return summarizeResponse(userQuery, apiResults, model, provider);

    } catch (ChatbotException e) {
      throw e;
    } catch (Exception e) {
      throw new ChatbotException("Failed to process chatbot query: " + e.getMessage(), e);
    }
  }

  /**
   * "Step 1" Helper: Talks to the LLM and asks for a JSON object telling us which API to call.
   */
  private ToolSelection chooseToolsForQuery(String userQuery, String model,
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
    GenParams params = new GenParams(0.1, 8192);

    // --- 3. SEND TO LLM WITH TOOL SPECS ---
    // Attach all allowed Recon API tools so the LLM can pick which one to invoke.
    // The LLM can either reply in text (JSON) or use native tool-call format.
    List<ToolSpec> specs = llmToolSpecFactory.getToolSpecs();
    LLMResponse response = llmClient.chatCompletion(messages, model, provider, params, specs);

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
        List<ToolSelection> calls = new ArrayList<>();
        for (int i = 0; i < Math.min(response.getToolCalls().size(), maxToolCalls); i++) {
          calls.add(parseNativeToolCall(response.getToolCalls().get(i)));
        }
        return ToolSelection.multi(calls);
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
      return ToolSelection.directAnswer(content);
    }

    LOG.warn("Empty text response from LLM");
    return null;
  }

  /**
   * Turns the LLM's native tool call into a {@code ToolSelection} the agent can run.
   *
   * <p>The model returns a tool name plus arguments as a JSON string. This method parses that JSON
   * into a {@code Map<String, String>} (all values as strings). If the JSON is bad, we log a
   * warning and continue with empty or partial params instead of failing the request.
   *
   * @param req tool name and arguments JSON from the tool-selection LLM call
   * @return single-tool selection for validation and {@link ReconQueryExecutor}
   */
  private ToolSelection parseNativeToolCall(LLMClient.ToolCallRequest req) {
    Map<String, String> params = new HashMap<>();
    try {
      JsonNode args = MAPPER.readTree(req.getArgumentsJson());
      if (args != null && args.isObject()) {
        // Walk each key/value and copy into the params map as plain strings.
        // e.g. {"startPrefix": "/vol1/bucket1", "limit": 50} → {"startPrefix":"\/vol1\/bucket1","limit":"50"}
        args.fields().forEachRemaining(entry -> {
          params.put(entry.getKey(), entry.getValue().asText());
        });
      }
    } catch (Exception e) {
      // Malformed JSON from the model — degrade gracefully rather than abort.
      LOG.warn("Failed to parse native tool call arguments JSON: {}", req.getArgumentsJson(), e);
    }
    // Wrap the tool name and extracted params into the agent's internal format.
    return ToolSelection.single(req.getToolName(), params);
  }

  /**
   * Executes multiple tool calls, collecting each endpoint's body and metadata under one map.
   */
  private Map<String, EndpointResult> executeMultipleToolCalls(List<ToolSelection> toolCalls) {
    Map<String, EndpointResult> responses = new HashMap<>();

    for (int i = 0; i < toolCalls.size(); i++) {
      ToolSelection toolCall = toolCalls.get(i);
      String responseKey = buildResponseKey(toolCall, i, toolCalls.size());
      try {
        LOG.info("Executing Recon API call: toolName={}", toolCall.toolName());
        ReconQueryResult outcome = reconQueryExecutor.execute(toolCall.toolName(), toolCall.parameters());
        responses.put(responseKey,
            new EndpointResult(outcome.getResponseBody(), createExecutionMetadataMap(outcome)));
        LOG.info("Recon API call completed: toolName={}, records={}, truncated={}",
            toolCall.toolName(),
            outcome.getRecordsProcessed(),
            outcome.isTruncated());
      } catch (Exception e) {
        LOG.error("Tool call failed for toolName: {}", toolCall.toolName(), e);
        Map<String, Object> errorBody = new HashMap<>();
        errorBody.put("error", e.getMessage());
        Map<String, Object> errorMeta = new HashMap<>();
        errorMeta.put("error", e.getMessage());
        errorMeta.put("truncated", false);
        responses.put(responseKey, new EndpointResult(errorBody, errorMeta));
      }
    }

    return responses;
  }

  /**
   * "Step 3" Helper: Takes the raw JSON API data and asks the LLM to write a sentence about it.
   */
  private String summarizeResponse(String userQuery,
                                   Map<String, EndpointResult> apiResults,
                                   String model, String provider)
      throws ChatbotException {

    // Give the LLM a new set of rules
    String systemPrompt = buildSummarizationPrompt();
    // Stitch the raw JSON strings and the user's original question together
    String userPrompt = buildSummarizationUserPrompt(userQuery, apiResults);

    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new ChatMessage("system", systemPrompt));
    messages.add(new ChatMessage("user", userPrompt));

    // Temperature 0.3 allows a tiny bit more natural/human-like language creativity.
    // max_tokens 8192: reasoning models (e.g. gemini-2.5-pro) may use tokens on internal
    // thinking before visible text; a low cap yields null content from the provider.
    GenParams params = new GenParams(0.3, 8192);

    try {
      LLMResponse response = llmClient.chatCompletion(messages, model, provider, params, null);

      LOG.info("Summarization LLM response: model={}, promptTokens={}, " +
              "completionTokens={}, totalTokens={}",
          response.getModel(),
          response.getPromptTokens(),
          response.getCompletionTokens(),
          response.getTotalTokens());

      String summary = response.getContent();
      if (StringUtils.isBlank(summary)) {
        return "I retrieved the cluster data but could not generate a summary. "
            + "Please try again or rephrase your question.";
      }
      return summary;
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

    GenParams params = new GenParams(0.5, 2048);

    try {
      LLMResponse response = llmClient.chatCompletion(messages, model, provider, params, null);

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
                                              Map<String, EndpointResult> apiResults) {
    StringBuilder sb = new StringBuilder();
    sb.append("User asked: \"").append(userQuery).append("\"\n\n");

    for (Map.Entry<String, EndpointResult> entry : apiResults.entrySet()) {
      EndpointResult result = entry.getValue();
      sb.append("Endpoint: ").append(entry.getKey()).append('\n');
      try {
        String responseJson = MAPPER.writeValueAsString(result.getResponseBody());
        sb.append("Response: ").append(responseJson).append("\n\n");
      } catch (Exception e) {
        sb.append("Response: ").append(result.getResponseBody()).append("\n\n");
      }
      Object metadata = result.getMetadata();
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

  /**
   * Validates each tool call and returns the first error message, if any.
   *
   * @return error message when a tool call is not allowed; {@code null} when all pass
   */
  private String validateToolCalls(List<ToolSelection> toolCalls) {
    for (ToolSelection toolCall : toolCalls) {
      String error = validateToolCall(toolCall.toolName(), toolCall.parameters());
      if (error != null) {
        return error;
      }
    }
    return null;
  }

  /**
   * Safety check before executing a tool call.
   *
   * <p>Returns {@code null} when the call is allowed. Otherwise returns a message shown
   * directly to the user explaining why execution was blocked.
   *
   * <p>Two layers of defence:
   * <ol>
   *   <li>Allowlist — only registered tool names from {@link ReconApiAllowlist} may run.</li>
   *   <li>Safe-scope (when {@code requireSafeScope} is true) — {@code api_v1_keys_listKeys}
   *       requires a bucket-scoped {@code startPrefix}.</li>
   * </ol>
   */
  private String validateToolCall(String toolName, Map<String, String> parameters) {
    if (toolName == null) {
      return null;
    }

    // Layer 1: Allowlist — only registered Recon tools are ever permitted.
    if (!reconApiAllowlist.isRegistered(toolName)) {
      LOG.warn("Blocked disallowed toolName from LLM output: {}", toolName);
      return "I can only query known Recon APIs. The requested tool '" +
          toolName + "' is not in the list of permitted tools.";
    }

    // Layer 2: Safe-scope — listKeys can return unbounded data, so when the safe-scope guard
    // is enabled we require startPrefix to be scoped to at least /<volume>/<bucket>. Only this
    // one tool is affected; everything else has already passed Layer 1 and is good to run.
    if (requireSafeScope && LIST_KEYS_TOOL.equals(toolName) && !hasBucketScopedPrefix(parameters)) {
      return "I need a bucket-scoped prefix to run listKeys. " +
          "This chatbot returns at most 1000 records per request and is not a " +
          "cluster-wide search engine. Please provide startPrefix as " +
          "/<volume>/<bucket> (optionally with a deeper path), and an optional " +
          "limit up to 1000 to narrow the sample.";
    }

    // All checks passed — null signals "allowed" to the caller in processQuery.
    return null;
  }

  /**
   * Returns true when {@code parameters} contains a {@code startPrefix} scoped to at least
   * {@code /<volume>/<bucket>}. Used by the listKeys safe-scope check.
   */
  private static boolean hasBucketScopedPrefix(Map<String, String> parameters) {
    String startPrefix = parameters == null ? null : parameters.get("startPrefix");
    return ChatbotUtils.isBucketScopedListKeysPrefix(startPrefix);
  }

  private String buildResponseKey(ToolSelection toolCall, int index, int total) {
    String toolName = toolCall == null ? "unknown" : toolCall.toolName();
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
    if (outcome.isTruncated()) {
      metadata.put("truncationNote",
          "Response is a partial sample capped at maxRecords; do not treat the list as complete.");
    }
    return metadata;
  }

  /**
   * Immutable result of tool selection (the first LLM call). Exactly one of three shapes:
   * <ul>
   *   <li>{@link Kind#SINGLE} — one Recon tool to call ({@code toolName} + {@code parameters}).</li>
   *   <li>{@link Kind#MULTI} — several tools to call ({@code calls}).</li>
   *   <li>{@link Kind#DIRECT_ANSWER} — the LLM answered in text; return {@code answer} verbatim.</li>
   * </ul>
   * A {@code null} {@code ToolSelection} signals "no suitable endpoint" and routes to the fallback.
   */
  private enum ToolSelectionKind {
    SINGLE, MULTI, DIRECT_ANSWER
  }

  private static final class ToolSelection {

    private final ToolSelectionKind kind;
    private final String toolName;
    private final Map<String, String> parameters;
    private final List<ToolSelection> calls;
    private final String answer;

    private ToolSelection(ToolSelectionKind kind, String toolName, Map<String, String> parameters,
                          List<ToolSelection> calls, String answer) {
      this.kind = kind;
      this.toolName = toolName;
      this.parameters = parameters;
      this.calls = calls;
      this.answer = answer;
    }

    static ToolSelection single(String toolName, Map<String, String> parameters) {
      return new ToolSelection(ToolSelectionKind.SINGLE, toolName, parameters, null, null);
    }

    static ToolSelection multi(List<ToolSelection> calls) {
      return new ToolSelection(ToolSelectionKind.MULTI, null, null, calls, null);
    }

    static ToolSelection directAnswer(String answer) {
      return new ToolSelection(ToolSelectionKind.DIRECT_ANSWER, null, null, null, answer);
    }

    ToolSelectionKind kind() {
      return kind;
    }

    String toolName() {
      return toolName;
    }

    Map<String, String> parameters() {
      return parameters;
    }

    List<ToolSelection> calls() {
      return calls;
    }

    String answer() {
      return answer;
    }
  }

  /**
   * One endpoint's outcome carried into summarization: the raw JSON body and the execution
   * metadata map ({@code recordsProcessed}/{@code truncated}/{@code maxRecords}, or an error).
   */
  private static final class EndpointResult {
    private final Object responseBody;
    private final Map<String, Object> metadata;

    EndpointResult(Object responseBody, Map<String, Object> metadata) {
      this.responseBody = responseBody;
      this.metadata = metadata;
    }

    Object getResponseBody() {
      return responseBody;
    }

    Map<String, Object> getMetadata() {
      return metadata;
    }
  }
}
