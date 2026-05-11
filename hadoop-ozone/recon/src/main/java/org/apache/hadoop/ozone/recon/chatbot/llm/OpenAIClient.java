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
 * OpenAI provider implementation of {@link LLMClient}.
 *
 * <p>This class is responsible for talking to OpenAI's API (GPT-4.1, GPT-4.1-mini, etc.).
 * Think of it as a translator: the rest of the chatbot speaks a common internal language
 * (a list of {@link LLMClient.ChatMessage} objects), and this class translates that into
 * the exact JSON format that OpenAI expects, fires the HTTP request, and translates
 * OpenAI's response back into the common {@link LLMClient.LLMResponse} format.</p>
 *
 * <h3>How OpenAI's format works</h3>
 * <p>OpenAI uses a simple "chat transcript" style. Every message — whether it's the system
 * instructions, the user's question, or a previous AI reply — goes into one flat
 * {@code messages} array. Each item has a {@code role} (system / user / assistant)
 * and a {@code content} string. This is the most straightforward of the three providers.</p>
 *
 * <h3>Authentication</h3>
 * <p>The API key is sent as an HTTP header: {@code Authorization: Bearer <key>}.
 * The key is resolved first from the per-request value (if provided), and then falls back
 * to the admin-configured JCEKS / ozone-site value via {@link CredentialHelper}.</p>
 *
 * <h3>Adding a new OpenAI model</h3>
 * <p>Add the model name string to {@link #getSupportedModels()}. No other changes needed.</p>
 */
public class OpenAIClient implements LLMClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final OzoneConfiguration configuration;
  private final CredentialHelper credentialHelper;

  /** Shared HTTP utility — handles the actual POST request over the network. */
  private final LLMNetworkClient networkClient;

  /**
   * Creates a new OpenAI client.
   *
   * @param configuration  Ozone config, used to read the base URL override if set.
   * @param credentialHelper  Used to securely resolve the API key from JCEKS or config.
   * @param timeoutMs  How long (in milliseconds) to wait for OpenAI to respond before giving up.
   */
  public OpenAIClient(OzoneConfiguration configuration,
                      CredentialHelper credentialHelper,
                      int timeoutMs) {
    this.configuration = configuration;
    this.credentialHelper = credentialHelper;
    this.networkClient = new LLMNetworkClient(timeoutMs);
  }

  /**
   * Sends the conversation to OpenAI and returns the model's reply.
   *
   * <p>Steps performed:
   * <ol>
   *   <li>Resolve the API key (per-request key takes priority over the configured key).</li>
   *   <li>Build the OpenAI JSON request body from the message list and parameters.</li>
   *   <li>Send the HTTP POST request to OpenAI's chat completions endpoint.</li>
   *   <li>Parse the raw JSON response and return a standardised {@link LLMResponse}.</li>
   * </ol>
   *
   * @param messages    The conversation so far (system instructions + user question).
   * @param model       Which OpenAI model to use, e.g. {@code "gpt-4.1"}.
   * @param apiKey      Optional per-request API key. If blank, falls back to server config.
   * @param parameters  Optional tuning values like {@code temperature} and {@code max_tokens}.
   * @throws LLMException if the API key is missing, the network fails, or OpenAI returns an error.
   */
  @Override
  public LLMResponse chatCompletion(List<ChatMessage> messages, String model,
                                    String apiKey, Map<String, Object> parameters)
      throws LLMException {
    String resolvedKey = resolveApiKey(apiKey);
    if (resolvedKey == null || resolvedKey.isEmpty()) {
      throw new LLMException("No API key configured for provider 'openai'.");
    }

    String url = getBaseUrl() + "/v1/chat/completions";
    ObjectNode body = buildOpenAIRequestBody(messages, model, parameters);

    // OpenAI authenticates via a standard HTTP Bearer token header.
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer " + resolvedKey);

    try {
      String responseBody = networkClient.executePost(url, headers,
          MAPPER.writeValueAsString(body), "openai");
      return parseOpenAIResponse(responseBody, model);
    } catch (Exception e) {
      if (e instanceof LLMException) {
        throw (LLMException) e;
      }
      throw new LLMException("OpenAI Request Failed: " + e.getMessage(), e);
    }
  }

  /**
   * Returns {@code true} if an OpenAI API key is present in the server configuration.
   * Used by the health-check endpoint to report provider availability.
   */
  @Override
  public boolean isAvailable() {
    String key = credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY);
    return key != null && !key.isEmpty();
  }

  /**
   * Returns the list of OpenAI model names this client advertises to the UI drop-down.
   * To add a new model, simply append its name here.
   */
  @Override
  public List<String> getSupportedModels() {
    return Arrays.asList("gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano");
  }

  /**
   * Picks the right API key to use.
   * If the caller passed in a key (e.g. from a user's personal token), use that.
   * Otherwise, use the admin-configured key stored securely in JCEKS / ozone-site.xml.
   */
  private String resolveApiKey(String perRequestKey) {
    if (perRequestKey != null && !perRequestKey.isEmpty()) {
      return perRequestKey;
    }
    return credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY);
  }

  /**
   * Returns the OpenAI base URL. Defaults to {@code https://api.openai.com} but can be
   * overridden in ozone-site.xml — useful for pointing at a local proxy or a compatible API.
   */
  private String getBaseUrl() {
    return configuration.get(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_BASE_URL,
                             ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_BASE_URL_DEFAULT);
  }

  /**
   * Builds the JSON body that OpenAI expects.
   *
   * <p>OpenAI's format is a flat chat transcript: every message (system instructions,
   * user question, previous assistant replies) goes into a single {@code messages} array.
   * Each item carries the original {@code role} string unchanged — no renaming needed.</p>
   *
   * <p>Extra tuning parameters (temperature, max_tokens, etc.) are copied directly onto
   * the root of the JSON body — OpenAI accepts them all at the top level.</p>
   */
  private ObjectNode buildOpenAIRequestBody(List<ChatMessage> messages, String model,
                                            Map<String, Object> params) {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("model", model);

    // Every message goes into one array — system, user, and assistant all live here.
    ArrayNode messagesArray = body.putArray("messages");
    for (ChatMessage msg : messages) {
      ObjectNode m = messagesArray.addObject();
      m.put("role", msg.getRole());
      m.put("content", msg.getContent());
    }

    // Copy supported parameter types (temperature, max_tokens, etc.) onto the root body.
    // Unrecognised types (e.g. internal "_provider" hints) are silently skipped.
    if (params != null) {
      for (Map.Entry<String, Object> e : params.entrySet()) {
        Object v = e.getValue();
        if (v instanceof Integer) {
          body.put(e.getKey(), (Integer) v);
        } else if (v instanceof Double) {
          body.put(e.getKey(), (Double) v);
        } else if (v instanceof Boolean) {
          body.put(e.getKey(), (Boolean) v);
        } else if (v instanceof String) {
          body.put(e.getKey(), (String) v);
        }
      }
    }
    return body;
  }

  /**
   * Parses OpenAI's JSON response and extracts the text the model wrote.
   *
   * <p>OpenAI wraps its answer inside: {@code choices[0].message.content}.
   * Token usage (for cost tracking) is read from the {@code usage} block.</p>
   */
  private LLMResponse parseOpenAIResponse(String responseBody, String model)
      throws LLMException {
    try {
      JsonNode root = MAPPER.readTree(responseBody);

      // OpenAI may theoretically return multiple "choices" (alternative answers).
      // We always use the first one, which is the primary response.
      JsonNode choices = root.get("choices");
      if (choices == null || !choices.isArray() || choices.isEmpty()) {
        throw new LLMException("Invalid response: no choices found");
      }

      JsonNode firstChoice = choices.get(0);
      String content = firstChoice.get("message").get("content").asText();

      // Token counts let us track how much of the context window each request used.
      int promptTokens = 0, completionTokens = 0;
      JsonNode usage = root.get("usage");
      if (usage != null) {
        promptTokens = usage.path("prompt_tokens").asInt(0);
        completionTokens = usage.path("completion_tokens").asInt(0);
      }

      Map<String, Object> metadata = new HashMap<>();
      metadata.put("finish_reason", firstChoice.path("finish_reason").asText("unknown"));
      metadata.put("response_id", root.path("id").asText(""));
      metadata.put("provider", "openai");

      return new LLMResponse(content, model, promptTokens, completionTokens, metadata);
    } catch (Exception e) {
      throw new LLMException("Failed to parse openai response", e);
    }
  }
}
