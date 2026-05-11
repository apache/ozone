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
 * Google Gemini provider implementation of {@link LLMClient}.
 *
 * <p>This class is responsible for talking to Google's Gemini API
 * (gemini-2.5-pro, gemini-2.5-flash, etc.).
 * Like the other provider clients, it receives a common list of {@link LLMClient.ChatMessage}
 * objects from the chatbot agent and translates them into the specific JSON shape that
 * Google's API requires, fires the HTTP request, and normalises the response back into
 * a standard {@link LLMClient.LLMResponse}.</p>
 *
 * <h3>How Gemini's format differs from OpenAI's</h3>
 * <p>Gemini does <em>not</em> use a single flat message array. It separates concerns:</p>
 * <ul>
 *   <li><b>System instructions</b> go into a dedicated top-level {@code systemInstruction}
 *       block — they are not mixed into the conversation turns.</li>
 *   <li><b>Conversation turns</b> (user and assistant messages) go into the {@code contents}
 *       array. Note: Gemini calls the assistant side {@code "model"}, not {@code "assistant"},
 *       so we rename it during translation.</li>
 *   <li><b>Tuning parameters</b> like max length and temperature go into a nested
 *       {@code generationConfig} block, and {@code max_tokens} is renamed to
 *       {@code maxOutputTokens} to match Gemini's naming convention.</li>
 * </ul>
 *
 * <h3>Authentication</h3>
 * <p>Unlike OpenAI, Gemini authenticates via a {@code ?key=} query parameter appended to
 * the URL — no {@code Authorization} header is used.</p>
 *
 * <h3>Adding a new Gemini model</h3>
 * <p>Add the model name string to {@link #getSupportedModels()}. No other changes needed.</p>
 */
public class GeminiClient implements LLMClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final OzoneConfiguration configuration;
  private final CredentialHelper credentialHelper;

  /** Shared HTTP utility — handles the actual POST request over the network. */
  private final LLMNetworkClient networkClient;

  /**
   * Creates a new Gemini client.
   *
   * @param configuration  Ozone config, used to read the base URL override if set.
   * @param credentialHelper  Used to securely resolve the API key from JCEKS or config.
   * @param timeoutMs  How long (in milliseconds) to wait for Gemini to respond before giving up.
   */
  public GeminiClient(OzoneConfiguration configuration,
                      CredentialHelper credentialHelper,
                      int timeoutMs) {
    this.configuration = configuration;
    this.credentialHelper = credentialHelper;
    this.networkClient = new LLMNetworkClient(timeoutMs);
  }

  /**
   * Sends the conversation to Google Gemini and returns the model's reply.
   *
   * <p>Steps performed:
   * <ol>
   *   <li>Resolve the API key.</li>
   *   <li>Build Gemini's JSON body, splitting system messages out of the main conversation.</li>
   *   <li>Send the HTTP POST to Gemini's generateContent endpoint (key in URL).</li>
   *   <li>Parse the response and return a standardised {@link LLMResponse}.</li>
   * </ol>
   *
   * @param messages    The conversation so far (system instructions + user question).
   * @param model       Which Gemini model to use, e.g. {@code "gemini-2.5-flash"}.
   * @param apiKey      Optional per-request API key. If blank, falls back to server config.
   * @param parameters  Optional tuning values: {@code temperature} and {@code max_tokens}.
   * @throws LLMException if the API key is missing, the network fails, or Gemini returns an error.
   */
  @Override
  public LLMResponse chatCompletion(List<ChatMessage> messages, String model,
                                    String apiKey, Map<String, Object> parameters)
      throws LLMException {
    String resolvedKey = resolveApiKey(apiKey);
    if (resolvedKey == null || resolvedKey.isEmpty()) {
      throw new LLMException("No API key configured for provider 'gemini'.");
    }

    // Gemini embeds the API key directly in the URL, not in a header.
    // The model name is also part of the path: /v1beta/models/<model>:generateContent
    String url = getBaseUrl() + "/v1beta/models/" + model + ":generateContent?key=" + resolvedKey;

    ObjectNode body = MAPPER.createObjectNode();

    // "contents" holds the back-and-forth conversation turns (user + model).
    // System instructions are kept separate (see loop below).
    ArrayNode contents = body.putArray("contents");

    for (ChatMessage msg : messages) {
      if ("system".equals(msg.getRole())) {
        // Gemini keeps system instructions in a dedicated top-level field, not in contents.
        // Note: if multiple system messages are present, only the last one is kept
        // because putObject() replaces any previous value for that key.
        ObjectNode sysInstruction = body.putObject("systemInstruction");
        ArrayNode sysParts = sysInstruction.putArray("parts");
        sysParts.addObject().put("text", msg.getContent());
      } else {
        // Gemini calls the AI side "model", not "assistant" — rename it here.
        String role = "assistant".equals(msg.getRole()) ? "model" : msg.getRole();
        ObjectNode content = contents.addObject();
        content.put("role", role);
        // Gemini wraps each message's text in a "parts" array to support multi-modal
        // content (text + images). We only use the text part.
        ArrayNode parts = content.putArray("parts");
        parts.addObject().put("text", msg.getContent());
      }
    }

    // Gemini's tuning parameters go into a nested "generationConfig" block —
    // they cannot be placed on the root like in OpenAI.
    // Also note: OpenAI calls it "max_tokens" but Gemini calls it "maxOutputTokens".
    ObjectNode genConfig = body.putObject("generationConfig");
    if (parameters != null) {
      if (parameters.containsKey("max_tokens")) {
        genConfig.put("maxOutputTokens", ((Number) parameters.get("max_tokens")).intValue());
      }
      if (parameters.containsKey("temperature")) {
        genConfig.put("temperature", ((Number) parameters.get("temperature")).doubleValue());
      }
    }

    try {
      // Gemini does not need any custom headers (auth is in the URL), so headers = null.
      String responseBody = networkClient.executePost(url, null,
          MAPPER.writeValueAsString(body), "gemini");
      return parseGeminiResponse(responseBody, model);
    } catch (Exception e) {
      if (e instanceof LLMException) {
        throw (LLMException) e;
      }
      throw new LLMException("Gemini Request Failed: " + e.getMessage(), e);
    }
  }

  /**
   * Returns {@code true} if a Gemini API key is present in the server configuration.
   * Used by the health-check endpoint to report provider availability.
   */
  @Override
  public boolean isAvailable() {
    String key = credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY);
    return key != null && !key.isEmpty();
  }

  /**
   * Returns the list of Gemini model names this client advertises to the UI drop-down.
   * To add a new model, simply append its name here.
   */
  @Override
  public List<String> getSupportedModels() {
    return Arrays.asList("gemini-2.5-pro", "gemini-2.5-flash",
        "gemini-3-flash-preview", "gemini-3.1-pro-preview");
  }

  /**
   * Picks the right API key to use.
   * Per-request key takes priority; falls back to the admin-configured key.
   */
  private String resolveApiKey(String perRequestKey) {
    if (perRequestKey != null && !perRequestKey.isEmpty()) {
      return perRequestKey;
    }
    return credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY);
  }

  /**
   * Returns the Gemini base URL. Defaults to {@code https://generativelanguage.googleapis.com}
   * but can be overridden in ozone-site.xml for testing or proxying.
   */
  private String getBaseUrl() {
    return configuration.get(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL,
                             ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL_DEFAULT);
  }

  /**
   * Parses Gemini's JSON response and extracts the text the model wrote.
   *
   * <p>Gemini wraps its answer inside: {@code candidates[0].content.parts[*].text}.
   * Multiple parts are concatenated in order (Gemini can split long responses into
   * several text blocks). Token usage is read from the {@code usageMetadata} block.</p>
   */
  private LLMResponse parseGeminiResponse(String responseBody, String model)
      throws LLMException {
    try {
      JsonNode root = MAPPER.readTree(responseBody);

      // Gemini calls its response options "candidates" (equivalent to OpenAI's "choices").
      // We always use the first candidate.
      JsonNode candidates = root.get("candidates");
      if (candidates == null || !candidates.isArray() || candidates.isEmpty()) {
        throw new LLMException("Invalid Gemini response: no candidates found");
      }

      JsonNode firstCandidate = candidates.get(0);
      JsonNode content = firstCandidate.get("content");
      JsonNode parts = content != null ? content.get("parts") : null;

      // Gemini may split a long response into multiple "parts" — join them all.
      StringBuilder text = new StringBuilder();
      if (parts != null && parts.isArray()) {
        for (JsonNode part : parts) {
          if (part.has("text")) {
            text.append(part.get("text").asText());
          }
        }
      }

      // Token counts for cost and context-window tracking.
      // Gemini uses different field names than OpenAI: promptTokenCount / candidatesTokenCount.
      int promptTokens = 0, completionTokens = 0;
      JsonNode usageMetadata = root.get("usageMetadata");
      if (usageMetadata != null) {
        promptTokens = usageMetadata.path("promptTokenCount").asInt(0);
        completionTokens = usageMetadata.path("candidatesTokenCount").asInt(0);
      }

      Map<String, Object> metadata = new HashMap<>();
      metadata.put("finish_reason", firstCandidate.path("finishReason").asText("unknown"));
      metadata.put("provider", "gemini");

      return new LLMResponse(text.toString(), model, promptTokens, completionTokens, metadata);
    } catch (Exception e) {
      throw new LLMException("Failed to parse Gemini response", e);
    }
  }
}
