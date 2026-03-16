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
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for direct LLM provider implementations.
 * Handles common HTTP plumbing, JSON serialisation, error handling,
 * and API key resolution via {@link CredentialHelper}.
 *
 * <p>
 * Concrete implementations need only override a handful of
 * template methods to adapt to each provider's API format.
 * </p>
 */
public abstract class DirectLLMProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(DirectLLMProvider.class);

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  protected final OzoneConfiguration configuration;
  protected final CredentialHelper credentialHelper;
  protected final int timeoutMs;

  protected DirectLLMProvider(OzoneConfiguration configuration,
                              CredentialHelper credentialHelper,
                              int timeoutMs) {
    this.configuration = configuration;
    this.credentialHelper = credentialHelper;
    this.timeoutMs = timeoutMs;
  }

  // ---- Template methods (override in subclasses) ----

  /**
   * Short provider name, e.g. {@code "openai"}, {@code "gemini"}.
   */
  public abstract String getProviderName();

  /**
   * Config key used to look up this provider's API key.
   */
  protected abstract String getApiKeyConfigKey();

  /**
   * Config key for the optional base URL override.
   */
  protected abstract String getBaseUrlConfigKey();

  /**
   * Default base URL for this provider.
   */
  protected abstract String getDefaultBaseUrl();

  /**
   * Builds the provider-specific HTTP connection for a chat completion.
   *
   * @param messages the chat messages
   * @param model    the model identifier
   * @param apiKey   the resolved API key
   * @param params   additional parameters (temperature, max_tokens ...)
   * @return a configured {@link HttpURLConnection} ready to execute
   */
  protected abstract HttpURLConnection buildChatRequest(
      List<LLMProvider.ChatMessage> messages,
      String model,
      String apiKey,
      Map<String, Object> params) throws IOException;

  /**
   * Parses the provider-specific response body into an
   * {@link LLMProvider.LLMResponse}.
   */
  protected abstract LLMProvider.LLMResponse parseResponse(
      String responseBody, String model) throws LLMProvider.LLMException;

  /**
   * Returns the list of models this provider supports.
   */
  public abstract List<String> getSupportedModels();

  // ---- Shared implementation ----

  /**
   * Resolves the API key — either a per-request override or the
   * JCEKS-managed system key.
   */
  protected String resolveApiKey(String perRequestKey) {
    if (perRequestKey != null && !perRequestKey.isEmpty()) {
      return perRequestKey;
    }
    return credentialHelper.getSecret(getApiKeyConfigKey());
  }

  /**
   * Gets the effective base URL (configuration override or default).
   */
  protected String getBaseUrl() {
    return configuration.get(getBaseUrlConfigKey(), getDefaultBaseUrl());
  }

  /**
   * Executes a chat completion against this provider.
   */
  public LLMProvider.LLMResponse chatCompletion(
      List<LLMProvider.ChatMessage> messages,
      String model,
      String apiKey,
      Map<String, Object> parameters) throws LLMProvider.LLMException {

    String resolvedKey = resolveApiKey(apiKey);
    if (resolvedKey == null || resolvedKey.isEmpty()) {
      throw new LLMProvider.LLMException(
          "No API key configured for provider '" + getProviderName()
              + "'. Set it via JCEKS or config key '"
              + getApiKeyConfigKey() + "'");
    }

    HttpURLConnection conn = null;
    try {
      conn = buildChatRequest(messages, model, resolvedKey, parameters != null ? parameters : new HashMap<>());

      LOG.debug("Sending chat request to {}: model={}",
          getProviderName(), model);

      int statusCode = conn.getResponseCode();

      String responseBody;
      if (statusCode == 200) {
        responseBody = readResponse(conn);
      } else {
        responseBody = readErrorResponse(conn);
        String errorMsg =
            String.format("%s request failed with status %d: %s", getProviderName(), statusCode, responseBody);
        LOG.error(errorMsg);
        throw new LLMProvider.LLMException(errorMsg, statusCode);
      }

      return parseResponse(responseBody, model);

    } catch (LLMProvider.LLMException e) {
      throw e;
    } catch (IOException e) {
      LOG.error("Failed to communicate with {}", getProviderName(), e);
      throw new LLMProvider.LLMException(
          "Failed to communicate with " + getProviderName() + ": "
              + e.getMessage(),
          e);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /**
   * Checks provider availability by making a lightweight request.
   */
  public boolean isAvailable() {
    String key = credentialHelper.getSecret(getApiKeyConfigKey());
    return key != null && !key.isEmpty();
  }

  // ---- HTTP helpers ----

  /**
   * Creates and configures a POST connection for the given URL.
   */
  protected HttpURLConnection createPostConnection(String url)
      throws IOException {
    HttpURLConnection conn =
        (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setConnectTimeout(timeoutMs);
    conn.setReadTimeout(timeoutMs);
    conn.setRequestProperty("Content-Type", "application/json");
    return conn;
  }

  /**
   * Writes a JSON body to the connection output stream.
   */
  protected void writeBody(HttpURLConnection conn, String body)
      throws IOException {
    try (OutputStream os = conn.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }
  }

  /**
   * Reads the successful response body from a connection.
   */
  protected String readResponse(HttpURLConnection conn) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(conn.getInputStream(),
            StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    }
    return sb.toString();
  }

  /**
   * Reads the error response body from a connection.
   */
  protected String readErrorResponse(HttpURLConnection conn) {
    try {
      if (conn.getErrorStream() != null) {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
            new InputStreamReader(conn.getErrorStream(),
                StandardCharsets.UTF_8))) {
          String line;
          while ((line = br.readLine()) != null) {
            sb.append(line);
          }
        }
        return sb.toString();
      }
    } catch (IOException e) {
      LOG.debug("Failed to read error stream", e);
    }
    return "";
  }

  // ---- Helpers shared by OpenAI-compatible providers ----

  /**
   * Builds the standard OpenAI-format request body used by OpenAI
   * and other OpenAI-compatible providers.
   */
  protected ObjectNode buildOpenAIRequestBody(
      List<LLMProvider.ChatMessage> messages,
      String model,
      Map<String, Object> params) {

    ObjectNode body = MAPPER.createObjectNode();
    body.put("model", model);

    ArrayNode messagesArray = body.putArray("messages");
    for (LLMProvider.ChatMessage msg : messages) {
      ObjectNode m = messagesArray.addObject();
      m.put("role", msg.getRole());
      m.put("content", msg.getContent());
    }

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
   * Parses the standard OpenAI-format response (choices + usage).
   */
  protected LLMProvider.LLMResponse parseOpenAIResponse(
      String responseBody, String model) throws LLMProvider.LLMException {
    try {
      JsonNode root = MAPPER.readTree(responseBody);

      JsonNode choices = root.get("choices");
      if (choices == null || !choices.isArray() || choices.isEmpty()) {
        throw new LLMProvider.LLMException(
            "Invalid response: no choices found");
      }

      JsonNode firstChoice = choices.get(0);
      JsonNode message = firstChoice.get("message");
      String content = message.get("content").asText();

      int promptTokens = 0;
      int completionTokens = 0;
      JsonNode usage = root.get("usage");
      if (usage != null) {
        promptTokens = usage.path("prompt_tokens").asInt(0);
        completionTokens = usage.path("completion_tokens").asInt(0);
      }

      Map<String, Object> metadata = new HashMap<>();
      metadata.put("finish_reason",
          firstChoice.path("finish_reason").asText("unknown"));
      metadata.put("response_id", root.path("id").asText(""));
      metadata.put("provider", getProviderName());

      return new LLMProvider.LLMResponse(
          content, model, promptTokens, completionTokens, metadata);

    } catch (LLMProvider.LLMException e) {
      throw e;
    } catch (Exception e) {
      throw new LLMProvider.LLMException(
          "Failed to parse " + getProviderName() + " response", e);
    }
  }

  /**
   * Masks an API key for safe logging.
   */
  protected static String maskApiKey(String key) {
    if (key == null || key.isEmpty()) {
      return "none";
    }
    if (key.length() <= 8) {
      return "****";
    }
    return key.substring(0, 4) + "..." + key.substring(key.length() - 4);
  }
}
