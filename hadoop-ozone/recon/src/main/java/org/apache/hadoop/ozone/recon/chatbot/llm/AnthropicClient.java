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
package org.apache.hadoop.ozone.recon.chatbot.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Anthropic (Claude) provider implementation of {@link LLMClient}.
 *
 * <p>This class is responsible for talking to Anthropic's API
 * (claude-sonnet-4-6, claude-opus-4-6, etc.).
 * Like the other provider clients, it receives a common list of {@link LLMClient.ChatMessage}
 * objects from the chatbot agent and translates them into the specific JSON shape that
 * Anthropic's API requires, fires the HTTP request, and normalises the response back into
 * a standard {@link LLMClient.LLMResponse}.</p>
 *
 * <h3>How Anthropic's format differs from OpenAI's</h3>
 * <p>Anthropic separates the system prompt from the conversation, similar to Gemini:</p>
 * <ul>
 *   <li><b>System message</b> becomes a top-level {@code "system"} string field — it is
 *       not placed inside the {@code messages} array.</li>
 *   <li><b>Conversation turns</b> (user and assistant) go into the {@code messages} array,
 *       same role names as OpenAI — no renaming needed.</li>
 *   <li><b>{@code max_tokens} is required</b> by Anthropic (OpenAI treats it as optional).
 *       We always include it, defaulting to 4096 if the caller did not specify one.</li>
 * </ul>
 *
 * <h3>Authentication</h3>
 * <p>Anthropic uses three custom HTTP headers:
 * <ul>
 *   <li>{@code x-api-key} — the API key.</li>
 *   <li>{@code anthropic-version} — pins the API contract version.</li>
 *   <li>{@code anthropic-beta} — optional; enables preview features such as extended
 *       context windows. Configurable via
 *       {@link org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys#OZONE_RECON_CHATBOT_ANTHROPIC_BETA_HEADER}.
 *       Set to empty string to disable.</li>
 * </ul>
 * </p>
 *
 * <h3>Adding a new Claude model</h3>
 * <p>Add the model name string to {@link #getSupportedModels()}. No other changes needed.</p>
 */
public class AnthropicClient implements LLMClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * The Anthropic Messages API version this client targets.
   * Pinning this ensures the request/response contract stays stable even if Anthropic
   * releases a new API version in the future.
   */
  private static final String ANTHROPIC_VERSION = "2023-06-01";

  private final OzoneConfiguration configuration;
  private final CredentialHelper credentialHelper;

  /** Shared HTTP utility — handles the actual POST request over the network. */
  private final LLMNetworkClient networkClient;

  /**
   * The value of the {@code anthropic-beta} header to send with each request.
   * Loaded once at startup from config. If empty, the header is omitted entirely.
   */
  private final String anthropicBetaHeader;

  /**
   * Creates a new Anthropic client.
   *
   * @param configuration  Ozone config, used to read the base URL and beta header.
   * @param credentialHelper  Used to securely resolve the API key from JCEKS or config.
   * @param timeoutMs  How long (in milliseconds) to wait for Anthropic to respond before giving up.
   */
  public AnthropicClient(OzoneConfiguration configuration,
                         CredentialHelper credentialHelper,
                         int timeoutMs) {
    this.configuration = configuration;
    this.credentialHelper = credentialHelper;
    this.networkClient = new LLMNetworkClient(timeoutMs);
    this.anthropicBetaHeader = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_BETA_HEADER,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_BETA_HEADER_DEFAULT);
  }

  /**
   * Sends the conversation to Anthropic Claude and returns the model's reply.
   *
   * <p>Steps performed:
   * <ol>
   *   <li>Resolve the API key.</li>
   *   <li>Build Anthropic's JSON body, lifting the system message out of the messages array.</li>
   *   <li>Send the HTTP POST to Anthropic's messages endpoint with the required headers.</li>
   *   <li>Parse the response and return a standardised {@link LLMResponse}.</li>
   * </ol>
   *
   * @param messages    The conversation so far (system instructions + user question).
   * @param model       Which Claude model to use, e.g. {@code "claude-sonnet-4-6"}.
   * @param apiKey      Optional per-request API key. If blank, falls back to server config.
   * @param parameters  Optional tuning values: {@code temperature} and {@code max_tokens}.
   * @throws LLMException if the API key is missing, the network fails, or Anthropic returns an error.
   */
  @Override
  public LLMResponse chatCompletion(List<ChatMessage> messages, String model,
                                    String apiKey, Map<String, Object> parameters)
      throws LLMException {
    String resolvedKey = resolveApiKey(apiKey);
    if (resolvedKey == null || resolvedKey.isEmpty()) {
      throw new LLMException("No API key configured for provider 'anthropic'.");
    }

    String url = getBaseUrl() + "/v1/messages";

    ObjectNode body = MAPPER.createObjectNode();
    body.put("model", model);

    // Split the message list: system message goes to a top-level "system" field,
    // all other roles (user / assistant) go into the "messages" array.
    // Note: if multiple system messages are present, only the last one is kept because
    // body.put("system", …) overwrites the previous value.
    ArrayNode messagesArray = body.putArray("messages");
    for (ChatMessage msg : messages) {
      if ("system".equals(msg.getRole())) {
        body.put("system", msg.getContent());
      } else {
        ObjectNode m = messagesArray.addObject();
        m.put("role", msg.getRole());
        m.put("content", msg.getContent());
      }
    }

    // Anthropic requires max_tokens on every request — it will reject the call without it.
    // We default to 4096 if the caller didn't specify a value.
    if (parameters != null) {
      if (parameters.containsKey("max_tokens")) {
        body.put("max_tokens", ((Number) parameters.get("max_tokens")).intValue());
      } else {
        body.put("max_tokens", 4096);
      }
      if (parameters.containsKey("temperature")) {
        body.put("temperature", ((Number) parameters.get("temperature")).doubleValue());
      }
    } else {
      body.put("max_tokens", 4096);
    }

    // Anthropic authenticates via custom headers (not a standard Bearer token).
    Map<String, String> headers = new HashMap<>();
    headers.put("x-api-key", resolvedKey);
    headers.put("anthropic-version", ANTHROPIC_VERSION);
    // The beta header is optional. If it is empty (set to "" in config), skip it entirely.
    if (anthropicBetaHeader != null && !anthropicBetaHeader.isEmpty()) {
      headers.put("anthropic-beta", anthropicBetaHeader);
    }

    try {
      String responseBody = networkClient.executePost(url, headers,
          MAPPER.writeValueAsString(body), "anthropic");
      return parseAnthropicResponse(responseBody, model);
    } catch (Exception e) {
      if (e instanceof LLMException) {
        throw (LLMException) e;
      }
      throw new LLMException("Anthropic Request Failed: " + e.getMessage(), e);
    }
  }

  /**
   * Returns {@code true} if an Anthropic API key is present in the server configuration.
   * Used by the health-check endpoint to report provider availability.
   */
  @Override
  public boolean isAvailable() {
    String key = credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_API_KEY);
    return key != null && !key.isEmpty();
  }

  /**
   * Returns the list of Claude model names this client advertises to the UI drop-down.
   * To add a new model, simply append its name here.
   */
  @Override
  public List<String> getSupportedModels() {
    return Arrays.asList("claude-opus-4-6", "claude-sonnet-4-6");
  }

  /**
   * Picks the right API key to use.
   * Per-request key takes priority; falls back to the admin-configured key.
   */
  private String resolveApiKey(String perRequestKey) {
    if (perRequestKey != null && !perRequestKey.isEmpty()) {
      return perRequestKey;
    }
    return credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_API_KEY);
  }

  /**
   * Returns the Anthropic base URL. Defaults to {@code https://api.anthropic.com}
   * but can be overridden in ozone-site.xml for testing or proxying.
   */
  private String getBaseUrl() {
    return configuration.get(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_BASE_URL,
                             ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_BASE_URL_DEFAULT);
  }

  /**
   * Parses Anthropic's JSON response and extracts the text the model wrote.
   *
   * <p>Anthropic returns its answer inside a {@code content} array of typed blocks.
   * We only care about blocks with {@code "type": "text"} — other types (e.g. tool use)
   * are ignored. All text blocks are concatenated in order.</p>
   *
   * <p>Token usage is stored under {@code usage.input_tokens} and
   * {@code usage.output_tokens} (different field names from both OpenAI and Gemini).
   * The stop reason is at {@code stop_reason} (OpenAI calls it {@code finish_reason}).</p>
   */
  private LLMResponse parseAnthropicResponse(String responseBody, String model)
      throws LLMException {
    try {
      JsonNode root = MAPPER.readTree(responseBody);

      // Anthropic wraps the reply in a "content" array of typed blocks.
      // A standard text reply will have exactly one block with type="text".
      JsonNode content = root.get("content");
      if (content == null || !content.isArray() || content.isEmpty()) {
        throw new LLMException("Invalid Anthropic response: no content blocks found");
      }

      // Collect all text blocks and join them into a single string.
      StringBuilder text = new StringBuilder();
      for (JsonNode block : content) {
        if ("text".equals(block.path("type").asText())) {
          text.append(block.path("text").asText());
        }
      }

      // Anthropic uses "input_tokens" / "output_tokens" (vs OpenAI's prompt/completion naming).
      int inputTokens = root.path("usage").path("input_tokens").asInt(0);
      int outputTokens = root.path("usage").path("output_tokens").asInt(0);

      Map<String, Object> metadata = new HashMap<>();
      // Anthropic calls it "stop_reason" — OpenAI calls the same concept "finish_reason".
      metadata.put("finish_reason", root.path("stop_reason").asText("unknown"));
      metadata.put("response_id", root.path("id").asText(""));
      metadata.put("provider", "anthropic");

      return new LLMResponse(text.toString(), model, inputTokens, outputTokens, metadata);
    } catch (Exception e) {
      throw new LLMException("Failed to parse Anthropic response", e);
    }
  }
}
