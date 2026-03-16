/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMProvider;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMProvider.ChatMessage;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMProvider.LLMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Main chatbot agent that orchestrates the conversation flow.
 * Handles tool selection (figuring out what API to call), executing those calls,
 * and summarization (feeding the data back to the LLM to write a nice answer).
 */
@Singleton
public class ChatbotAgent {

  private static final Logger LOG = LoggerFactory.getLogger(ChatbotAgent.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern JSON_PATTERN = Pattern.compile("\\{.*\\}", Pattern.DOTALL);

  // A specific Recon API endpoint we want to handle carefully because it can return millions of rows.
  private static final String LIST_KEYS_ENDPOINT_SUFFIX = "/keys/listKeys";

  // The connection to Gemini/OpenAI
  private final LLMProvider llmProvider;

  // The hands that execute the internal API calls
  private final ToolExecutor toolExecutor;

  // The Cheat Sheet of all available APIs loaded from the .md file
  private final String apiSchema;

  // Max API calls we allow per question (so the LLM doesn't DOS our server)
  private final int maxToolCalls;


  private final String defaultModel;
  private final int maxRecordsPerAnswer;
  private final int maxPagesPerAnswer;
  private final int pageSizePerCall;
  private final boolean requireSafeScope;

  /** Set per-request by processQuery; used to inject provider hint. */
  private volatile String currentProvider;

  @Inject
  public ChatbotAgent(LLMProvider llmProvider,
      ToolExecutor toolExecutor,
      OzoneConfiguration configuration) {
    this.llmProvider = llmProvider;
    this.toolExecutor = toolExecutor;

    // Read the Schema (Cheat Sheet) from the resources' folder.
    this.apiSchema = loadApiSchema();

    // Load all the safeguards and settings from ozone-site.xml
    this.maxToolCalls = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_TOOL_CALLS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_TOOL_CALLS_DEFAULT);
    this.defaultModel = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_DEFAULT_MODEL,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_DEFAULT_MODEL_DEFAULT);
    this.maxRecordsPerAnswer = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_RECORDS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_RECORDS_DEFAULT);
    this.maxPagesPerAnswer = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_PAGES,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_MAX_PAGES_DEFAULT);
    this.pageSizePerCall = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_PAGE_SIZE,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_PAGE_SIZE_DEFAULT);
    this.requireSafeScope = configuration.getBoolean(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_EXEC_REQUIRE_SAFE_SCOPE_DEFAULT);

    LOG.info("ChatbotAgent initialized with model={}, maxRecords={}, " +
        "maxPages={}, pageSize={}, requireSafeScope={}",
        defaultModel, maxRecordsPerAnswer, maxPagesPerAnswer,
        pageSizePerCall, requireSafeScope);
  }

  /**
   * THE MAIN ENTRY POINT. Processes a user query and returns a response.
   *
   * @param userQuery the user's question
   * @param model     the LLM model to use
   * @param provider  explicit provider name (optional, e.g. "gemini", "openai")
   * @param apiKey    the user's API key (optional)
   * @return the chatbot response
   */
  public String processQuery(String userQuery, String model,
      String provider, String apiKey)
      throws Exception {

    // Safety check
    if (userQuery == null || userQuery.trim().isEmpty()) {
      throw new IllegalArgumentException("Query cannot be empty");
    }

    // Use default model if the user didn't specify one.
    String effectiveModel = (model != null && !model.isEmpty()) ? model : defaultModel;

    // Store provider so private helper methods can inject it
    // into LLM call parameters.
    this.currentProvider = provider;

    LOG.info("Processing query with model: {}, provider: {}", effectiveModel, provider == null ? "auto" : provider);

    // STEP 1: Ask the LLM what API tools it wants to use to answer the question.
    ToolCall toolCall = getToolCall(userQuery, effectiveModel, apiKey);

    // If the LLM doesn't know what API to call...
    if (toolCall == null) {
      // No suitable endpoint found
      LOG.info("Tool selection result: NO_SUITABLE_ENDPOINT; using fallback");
      return handleFallback(userQuery, effectiveModel, apiKey);
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
        return handleFallback(userQuery, effectiveModel, apiKey);
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
        return handleFallback(userQuery, effectiveModel, apiKey);
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
      // Go fetch the data using our ToolExecutor!
      ToolExecutor.ToolExecutionOutcome outcome = toolExecutor.executeToolCallWithPolicy(
          toolCall.getEndpoint(),
          toolCall.getMethod(),
          toolCall.getParameters(),
          maxRecordsPerAnswer,
          maxPagesPerAnswer,
          pageSizePerCall);

      // Save the raw JSON data the API returned
      apiResponses = new HashMap<>();
      apiResponses.put(toolCall.getEndpoint(), outcome.getResponseBody());
      executionMetadata.put(toolCall.getEndpoint(),
          createExecutionMetadataMap(outcome));
    }

    // STEP 3: Send the raw JSON data BACK to the LLM to format a nice answer
    LOG.info("Summarization input prepared: endpointCount={}, endpoints={}",
        apiResponses.size(), apiResponses.keySet());
    return summarizeResponse(userQuery, apiResponses, executionMetadata, effectiveModel, apiKey);
  }

  /**
   * "Step 1" Helper: Talks to the LLM and asks for a JSON object telling us which API to call.
   */
  private ToolCall getToolCall(String userQuery, String model, String apiKey)
      throws Exception {

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
    if (currentProvider != null && !currentProvider.isEmpty()) {
      parameters.put("_provider", currentProvider);
    }

    // Send the request to the LLM
    LLMResponse response = llmProvider.chatCompletion(messages, model, apiKey, parameters);

    LOG.info("Tool selection LLM response: model={}, promptTokens={}, completionTokens={}, totalTokens={}",
        response.getModel(),
        response.getPromptTokens(),
        response.getCompletionTokens(),
        response.getTotalTokens());

    String content = response.getContent().trim();

    if (content.contains("NO_SUITABLE_ENDPOINT")) {
      return null;
    }

    // Extract JSON from response
    Matcher matcher = JSON_PATTERN.matcher(content);
    if (!matcher.find()) {
      LOG.warn("No JSON found in LLM response");
      return null;
    }

    // Convert the JSON string into our Java "ToolCall" object
    String jsonStr = matcher.group();
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
            maxRecordsPerAnswer,
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
      String model, String apiKey)
      throws Exception {

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
    if (currentProvider != null && !currentProvider.isEmpty()) {
      parameters.put("_provider", currentProvider);
    }

    // Send the request to the LLM
    LLMResponse response = llmProvider.chatCompletion(messages, model, apiKey, parameters);

    LOG.info("Summarization LLM response: model={}, promptTokens={}, " +
        "completionTokens={}, totalTokens={}",
        response.getModel(),
        response.getPromptTokens(),
        response.getCompletionTokens(),
        response.getTotalTokens());

    return response.getContent();
  }

  /**
   * Helper: If the user asks "What is the meaning of life?", we use this to say
   * "Sorry, I only know about Hadoop."
   */
  private String handleFallback(String userQuery, String model, String apiKey)
      throws Exception {
    String prompt = String.format(
        "The user asked: \"%s\"\n\n" +
            "This question cannot be answered using the available " +
            "Ozone Recon API endpoints.\n\n" +
            "Provide a helpful response that:\n" +
            "1. Politely explains that you can only answer questions about " +
            "Ozone Recon cluster data\n" +
            "2. Briefly mentions the types of information you can provide " +
            "(containers, keys, datanodes, pipelines, cluster state, etc.)\n" +
            "3. Suggests how they might rephrase their question if it's related to Ozone\n\n" +
            "Keep the response friendly and concise.",
        userQuery);

    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new ChatMessage("user", prompt));

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("temperature", 0.5);
    parameters.put("max_tokens", 500);
    if (currentProvider != null && !currentProvider.isEmpty()) {
      parameters.put("_provider", currentProvider);
    }

    LLMResponse response = llmProvider.chatCompletion(
        messages, model, apiKey, parameters);

    return response.getContent();
  }

  /**
   * Creates the master rules (System Prompt) we send to the LLM during Step 1.
   * Notice how we teach the LLM exactly what JSON to output!
   */
  private String buildToolSelectionPrompt() {
    return "You are an expert on Apache Ozone Recon, a service that provides insights into Ozone cluster data.\n\n" +
        "Your task is to analyze user queries and determine the appropriate response:\n\n" +
        "1. **For DATA queries** (asking for current cluster information): Identify the most appropriate API endpoint(s) to call\n" +
        "2. **For DOCUMENTATION queries** (asking about API use cases, purposes, or capabilities): Respond with a DOCUMENTATION_QUERY and provide the information directly\n\n" +
        "IMPORTANT: If the user's question requires data from MULTIPLE API endpoints to give a complete answer, return ALL needed endpoints in an array.\n\n" +
        "For SINGLE endpoint DATA queries, return this JSON format:\n" +
        "{\n" +
        "  \"endpoint\": \"/api/v1/path\",\n" +
        "  \"method\": \"GET\",\n" +
        "  \"parameters\": {},\n" +
        "  \"reasoning\": \"Brief explanation of why this endpoint was chosen\"\n" +
        "}\n\n" +
        "For MULTIPLE endpoint DATA queries, return this JSON format:\n" +
        "{\n" +
        "  \"tool_calls\": [\n" +
        "    { \"endpoint\": \"/api/v1/path1\", \"method\": \"GET\", \"parameters\": {}, \"reasoning\": \"Explain what data this provides\" },\n" +
        "    { \"endpoint\": \"/api/v1/path2\", \"method\": \"GET\", \"parameters\": {}, \"reasoning\": \"Explain what data this provides\" }\n" +
        "  ],\n" +
        "  \"requires_multiple_calls\": true\n" +
        "}\n\n" +
        "Examples requiring MULTIPLE endpoints:\n" +
        "- \"How many total keys and how many are open?\" -> /clusterState + /keys/open/summary\n" +
        "- \"Show datanodes and pipeline status\" -> /datanodes + /pipelines\n" +
        "- \"List unhealthy and missing containers\" -> /containers/unhealthy + /containers/missing\n" +
        "- \"Cluster state and open keys summary\" -> /clusterState + /keys/open/summary\n\n" +
        "For DOCUMENTATION queries, return this JSON format:\n" +
        "{\n" +
        "  \"type\": \"DOCUMENTATION_QUERY\",\n" +
        "  \"answer\": \"Direct answer based on the API guide\",\n" +
        "  \"reasoning\": \"Explanation of what documentation was referenced\"\n" +
        "}\n\n" +
        "If the query cannot be answered by any available API endpoint OR documentation, respond with: NO_SUITABLE_ENDPOINT\n\n" +
        "Safety rules:\n" +
        "- Do not invent parameter values.\n" +
        "- For /keys/listKeys, always provide startPrefix with at least " +
        "/<volume>/<bucket> scope when selecting this tool.\n\n" +
        "API Specification:\n" + apiSchema;
  }

  /**
   * Builds the system prompt for response summarization.
   */
  private String buildSummarizationPrompt() {
    return "You are an expert on Apache Ozone Recon data analysis.\n\n" +
        "Your task is to analyze API response data and provide clear, concise summaries that directly answer the user's question.\n\n" +
        "Guidelines:\n" +
        "- Focus on the key information that answers the user's specific question\n" +
        "- Combine information from all endpoints to give a comprehensive response if multiple endpoints were called\n" +
        "- Clearly present numbers, counts, and statistics from each data source\n" +
        "- Use clear, non-technical language when possible\n" +
        "- If the data shows problems (unhealthy containers, missing data, etc.), highlight them\n" +
        "- If the API response is empty, doesn't contain relevant data, or an endpoint failed, say so clearly\n" +
        "- If execution metadata says response was truncated, clearly mention that the answer is based on limited records/pages\n" +
        "- If truncated and a next cursor is present, suggest user provide a specific page/range and limit for deeper analysis\n" +
        "- Keep responses cohesive, well-structured, and informative\n\n" +
        "IMPORTANT: Format your response using proper Markdown syntax:\n" +
        "- Use **bold** for emphasis (e.g., **5 datanodes**)\n" +
        "- For bullet lists, ALWAYS add a blank line before the list starts\n" +
        "- Use hyphens (-) for bullet points, not asterisks (*)\n" +
        "- Example:\n" +
        "  Here are the datanodes:\n" +
        "  \n" +
        "  - datanode1: HEALTHY\n" +
        "  - datanode2: HEALTHY\n\n" +
        "Format your response as a direct, complete answer to the user's question.";
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
      sb.append("Endpoint: ").append(entry.getKey()).append("\n");
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
   * Safety check: Ensure the LLM didn't try to crash our server.
   */
  private String validateToolCallForExecution(ToolCall toolCall) {
    if (!requireSafeScope || toolCall == null || toolCall.getEndpoint() == null) {
      return null;
    }
    String endpoint = normalizeEndpoint(toolCall.getEndpoint());

    // If the LLM tries to query the "/keys/listKeys" endpoint...
    if (!endpoint.endsWith(LIST_KEYS_ENDPOINT_SUFFIX)) {
      return null;
    }

    // We MUST make sure it provides a specific bucket to search in.
    // If it asks for the ENTIRE cluster ("/"), we block it and ask for clarification,
    // otherwise our server would run out of memory!
    String startPrefix = null;
    if (toolCall.getParameters() != null) {
      startPrefix = toolCall.getParameters().get("startPrefix");
    }
    if (startPrefix == null || startPrefix.trim().isEmpty() ||
        "/".equals(startPrefix.trim())) {
      return "I need a bucket-scoped prefix to run listKeys safely. " +
          "Please provide startPrefix in the form /<volume>/<bucket> " +
          "(optionally with a deeper path), plus optional limit and page " +
          "range if you want targeted analysis.";
    }
    if (!startPrefix.trim().startsWith("/")) {
      return "The provided startPrefix must start with '/'. Please use " +
          "a value like /<volume>/<bucket> or deeper path.";
    }
    return null; // All good
  }

  private String normalizeEndpoint(String endpoint) {
    if (endpoint == null) {
      return "";
    }
    if (endpoint.startsWith("/api/v1/")) {
      return endpoint;
    }
    return "/api/v1" + (endpoint.startsWith("/") ? endpoint : "/" + endpoint);
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
   * Parses the tool call JSON from the LLM response.
   */
  private ToolCall parseToolCall(JsonNode jsonNode) {
    ToolCall toolCall = new ToolCall();

    // Check if documentation query
    if (jsonNode.has("type") &&
        "DOCUMENTATION_QUERY".equals(jsonNode.get("type").asText())) {
      toolCall.setDocumentationQuery(true);
      toolCall.setAnswer(jsonNode.path("answer").asText(""));
      toolCall.setReasoning(jsonNode.path("reasoning").asText(""));
      return toolCall;
    }

    // Check if multiple endpoints
    if (jsonNode.has("requires_multiple_calls") &&
        jsonNode.get("requires_multiple_calls").asBoolean()) {
      toolCall.setMultipleEndpoints(true);
      List<ToolCall> toolCalls = new ArrayList<>();
      JsonNode toolCallsArray = jsonNode.get("tool_calls");
      if (toolCallsArray != null && toolCallsArray.isArray()) {
        int added = 0;
        for (JsonNode tc : toolCallsArray) {
          if (added >= maxToolCalls) {
            LOG.info("Truncating tool_calls from LLM to maxToolCalls={}",
                maxToolCalls);
            break;
          }
          ToolCall parsed = parseSingleToolCall(tc);
          if (parsed.getEndpoint() != null && !parsed.getEndpoint().isEmpty()) {
            toolCalls.add(parsed);
            added++;
          }
        }
      }
      toolCall.setToolCalls(toolCalls);
      return toolCall;
    }

    // Single endpoint
    return parseSingleToolCall(jsonNode);
  }

  /**
   * Parses a single tool call from JSON.
   */
  private ToolCall parseSingleToolCall(JsonNode jsonNode) {
    ToolCall toolCall = new ToolCall();
    toolCall.setEndpoint(jsonNode.path("endpoint").asText(""));
    toolCall.setMethod(jsonNode.path("method").asText("GET"));

    Map<String, String> parameters = new HashMap<>();
    JsonNode paramsNode = jsonNode.get("parameters");
    if (paramsNode != null && paramsNode.isObject()) {
      paramsNode.fields().forEachRemaining(entry -> {
        parameters.put(entry.getKey(), entry.getValue().asText());
      });
    }
    toolCall.setParameters(parameters);
    toolCall.setReasoning(jsonNode.path("reasoning").asText(""));

    return toolCall;
  }

  // =========================================================================
  // File Loading
  // =========================================================================
  /**
   * Loads the Markdown or Yaml schema file (the "Cheat Sheet").
   */
  private String loadApiSchema() {
    String fromMarkdown = loadApiGuideFromClasspath("chatbot/recon-api-guide.md");
    if (!fromMarkdown.isEmpty()) {
      LOG.info("Loaded API guide from classpath: chatbot/recon-api-guide.md");
      return fromMarkdown;
    }

    String fromYaml = loadApiGuideFromClasspath("chatbot/recon-api.yaml");
    if (!fromYaml.isEmpty()) {
      LOG.info("Loaded API schema from classpath: chatbot/recon-api.yaml");
      return fromYaml;
    }

    fromYaml = loadApiGuideFromClasspath("chatbot/recon-api-schema.yaml");
    if (!fromYaml.isEmpty()) {
      LOG.info("Loaded API schema from classpath: chatbot/recon-api-schema.yaml");
      return fromYaml;
    }

    LOG.warn("No API guide/schema found, using empty schema");
    return "";
  }

  private String loadApiGuideFromClasspath(String resourcePath) {
    try (InputStream is = getClass().getClassLoader()
        .getResourceAsStream(resourcePath)) {
      if (is == null) {
        return "";
      }
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int length;
      while ((length = is.read(buffer)) != -1) {
        result.write(buffer, 0, length);
      }
      return result.toString(StandardCharsets.UTF_8.name());
    } catch (IOException e) {
      LOG.error("Failed to load API guide/schema resource: {}",
          resourcePath, e);
      return "";
    }
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
