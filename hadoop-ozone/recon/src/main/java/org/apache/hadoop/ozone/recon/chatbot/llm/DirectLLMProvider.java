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
 * DirectLLMProvider is the "Parent" (Abstract Base Class) for all specific AI providers like OpenAIProvider or GeminiProvider.
 * 
 * Purpose:
 * Instead of rewriting the complicated HTTP network code for every single AI we add,
 * we put all the shared "plumbing" (like connecting to the internet, handling timeouts, and reading JSON) in this one file.
 * Any specific AI provider (the "Child" classes) just inherits this file and fills in the blanks (like their specific URL).
 */
public abstract class DirectLLMProvider implements LLMProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(DirectLLMProvider.class);

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  protected final OzoneConfiguration configuration;
  
  // A helper to safely retrieve passwords and API keys without printing them in plain text
  protected final CredentialHelper credentialHelper;
  protected final int timeoutMs;

  /**
   * The Constructor. When a child class (like OpenAIProvider) is created,
   * it must pass these basic settings up to this parent.
   */
  protected DirectLLMProvider(OzoneConfiguration configuration,
                              CredentialHelper credentialHelper,
                              int timeoutMs) {
    this.configuration = configuration;
    this.credentialHelper = credentialHelper;
    this.timeoutMs = timeoutMs;
  }

  // =========================================================================
  // Template Methods (The "Blanks" the Children Must Fill In)
  // =========================================================================

  /**
   * The provider must provide its name. (e.g., returns "openai" or "gemini")
   */
  public abstract String getProviderName();

  /**
   * The provider must say which setting name stores its API key. (e.g., "ozone.recon.chatbot.openai.api.key")
   */
  protected abstract String getApiKeyConfigKey();

  /**
   * The provider must say which setting name stores a custom URL,
   * just in case the user wants to route traffic through a proxy.
   */
  protected abstract String getBaseUrlConfigKey();

  /**
   * The provider must provide its official internet address. (e.g., "https://api.openai.com/v1/")
   */
  protected abstract String getDefaultBaseUrl();

  /**
   * Every AI wants its incoming JSON data formatted differently. 
   * The provider must construct the specific HTTP connection and JSON body it needs.
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
   * Every AI provider sends data back differently.
   * The child must read the raw JSON string the internet returned and organize it into our standard LLMResponse object.
   */
  protected abstract LLMProvider.LLMResponse parseResponse(
      String responseBody, String model) throws LLMProvider.LLMException;

  /**
   * The provider must list which AI models it supports (e.g., "gpt-4", "gpt-3.5-turbo").
   */
  public abstract List<String> getSupportedModels();

  // =========================================================================
  // Shared Implementation (Code Every Child Uses Automatically)
  // =========================================================================

  /**
   * Safely figures out which API key to use. 
   * It first checks if the user provided one directly for this specific request.
   * If not, it digs into the secure JCEKS system using the CredentialHelper.
   */
  protected String resolveApiKey(String perRequestKey) {
    // If the user typed an API key directly into the UI, use that one
    if (perRequestKey != null && !perRequestKey.isEmpty()) {
      return perRequestKey;
    }
    // Otherwise, fetch the saved key from the secure vault
    return credentialHelper.getSecret(getApiKeyConfigKey());
  }

  /**
   * Figures out the destination URL. Usually it's the default, but admins can override it in configurations.
   */
  protected String getBaseUrl() {
    return configuration.get(getBaseUrlConfigKey(), getDefaultBaseUrl());
  }

  /**
   * THE MAIN ENGINE. 
   * This handles the entire lifecycle of sending a prompt to the AI and getting an answer back.
   */
  public LLMProvider.LLMResponse chatCompletion(
      List<LLMProvider.ChatMessage> messages,
      String model,
      String apiKey,
      Map<String, Object> parameters) throws LLMProvider.LLMException {

    // Step 1: Securely get the API Key
    String resolvedKey = resolveApiKey(apiKey);
    
    // Safety Check: If we can't find a key, we can't talk to the AI. Throw an error.
    if (resolvedKey == null || resolvedKey.isEmpty()) {
      throw new LLMProvider.LLMException(
          "No API key configured for provider '" + getProviderName()
              + "'. Set it via JCEKS or config key '"
              + getApiKeyConfigKey() + "'");
    }

    HttpURLConnection conn = null;
    try {
      // Step 2: Use the child's specific instructions to build the HTTP network request
      conn = buildChatRequest(messages, model, resolvedKey, parameters != null ? parameters : new HashMap<>());

      LOG.debug("Sending chat request to {}: model={}", getProviderName(), model);

      // Step 3: Fire the request over the internet! 
      // This will pause the code until the AI responds.
      int statusCode = conn.getResponseCode();

      String responseBody;
      
      // Step 4: Check if the AI responded happily (Status 200 = OK)
      if (statusCode == 200) {
        // Read the success data
        responseBody = readResponse(conn);
      } else {
        // If the AI crashed or returned an error (like 401 Unauthorized or 500 Server Error)
        // Read the error message, log it, and throw it back up to the ChatbotAgent to handle
        responseBody = readErrorResponse(conn);
        String errorMsg =
            String.format("%s request failed with status %d: %s", getProviderName(), statusCode, responseBody);
        LOG.error(errorMsg);
        throw new LLMProvider.LLMException(errorMsg, statusCode);
      }

      // Step 5: Convert the raw text data from the internet back into a Java Object
      return parseResponse(responseBody, model);

    } catch (LLMProvider.LLMException e) {
      throw e;
    } catch (IOException e) {
      // If the internet connection itself failed wildly (e.g., DNS error or timeout)
      LOG.error("Failed to communicate with {}", getProviderName(), e);
      throw new LLMProvider.LLMException(
          "Failed to communicate with " + getProviderName() + ": " + e.getMessage(), e);
    } finally {
      // Step 6: Cleanup
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /**
   * A quick check to see if this AI is even turned on (i.e. does it have an API key saved?)
   */
  public boolean isAvailable() {
    String key = credentialHelper.getSecret(getApiKeyConfigKey());
    return key != null && !key.isEmpty();
  }

  // =========================================================================
  // HTTP Helpers (Tools for touching the internet)
  // =========================================================================

  /**
   * Sets up a standard POST request, telling it we are sending and receiving JSON.
   */
  protected HttpURLConnection createPostConnection(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true); // Allows us to send data IN the body
    conn.setConnectTimeout(timeoutMs); // How long to wait to plug in the initial cable
    conn.setReadTimeout(timeoutMs);    // How long to wait for the AI to type out its answer
    conn.setRequestProperty("Content-Type", "application/json");
    return conn;
  }

  /**
   * Writes a JSON body to the connection output stream.
   */
  protected void writeBody(HttpURLConnection conn, String body) throws IOException {
    try (OutputStream os = conn.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }
  }

  /**
   * Reads a successful response from the AI, line by line, until it's finished.
   */
  protected String readResponse(HttpURLConnection conn) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()
        , StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    }
    return sb.toString();
  }

  /**
   * Reads an error response from the AI (like "Invalid API Key"). 
   * Networks use a separate "Error Stream" from the "Input Stream" when something goes wrong!
   */
  protected String readErrorResponse(HttpURLConnection conn) {
    try {
      if (conn.getErrorStream() != null) {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
            new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
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
    return ""; // If we can't read the error, just return a blank string
  }

  // =========================================================================
  // Shared OpenAI Format Helpers
  // (Many AIs copy OpenAI's exact JSON format, so we put those tools here to be shared)
  // =========================================================================

  /**
   * Builds a JSON body that perfectly mimics the shape OpenAI expects.
   * Other AIs (like DeepSeek or Local LLaMA) often use this exact same format.
   */
  protected ObjectNode buildOpenAIRequestBody(
      List<LLMProvider.ChatMessage> messages,
      String model,
      Map<String, Object> params) {

    // Create the empty {} JSON root
    ObjectNode body = MAPPER.createObjectNode();
    
    // Add {"model": "gpt-4"}
    body.put("model", model);

    // Create the ["messages"] array
    ArrayNode messagesArray = body.putArray("messages");
    
    // Add all of our messages into the array format OpenAI requires
    for (LLMProvider.ChatMessage msg : messages) {
      ObjectNode m = messagesArray.addObject();
      m.put("role", msg.getRole());
      m.put("content", msg.getContent());
    }

    // Add optional settings (like temperature=0.7)
    if (params != null) {
      for (Map.Entry<String, Object> e : params.entrySet()) {
        Object v = e.getValue();
        // Since we don't know if the setting is a number, boolean, or string, check its type!
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
    return body; // Return the finished JSON object
  }

  /**
   * Unpacks a JSON string that is formatted the way OpenAI sends responses back.
   */
  protected LLMProvider.LLMResponse parseOpenAIResponse(
      String responseBody, String model) throws LLMProvider.LLMException {
    try {
      JsonNode root = MAPPER.readTree(responseBody);

      // Grab the "choices" array. If it's missing, the response is broken!
      JsonNode choices = root.get("choices");
      if (choices == null || !choices.isArray() || choices.isEmpty()) {
        throw new LLMProvider.LLMException("Invalid response: no choices found");
      }

      // Grab the very first answer (choice 0) out of the options
      JsonNode firstChoice = choices.get(0);
      JsonNode message = firstChoice.get("message");
      
      // Extract the actual human-readable text!
      String content = message.get("content").asText();

      // See how many tokens (words) we used so we can track costs
      int promptTokens = 0;
      int completionTokens = 0;
      JsonNode usage = root.get("usage");
      if (usage != null) {
        promptTokens = usage.path("prompt_tokens").asInt(0);
        completionTokens = usage.path("completion_tokens").asInt(0);
      }

      // Collect some extra metadata about how the prompt finished
      Map<String, Object> metadata = new HashMap<>();
      metadata.put("finish_reason", firstChoice.path("finish_reason").asText("unknown"));
      metadata.put("response_id", root.path("id").asText(""));
      metadata.put("provider", getProviderName());

      // Bundle it all up into our clean, standard Java DTO Exception to return
      return new LLMProvider.LLMResponse(content, model, promptTokens, completionTokens, metadata);

    } catch (LLMProvider.LLMException e) {
      throw e;
    } catch (Exception e) {
      throw new LLMProvider.LLMException("Failed to parse " + getProviderName() + " response", e);
    }
  }

  /**
   * Helper to ensure we don't accidentally log real API passwords into the server console.
   * e.g. "sk-abc12345" becomes "sk-a...2345"
   */
  protected static String maskApiKey(String key) {
    if (key == null || key.isEmpty()) {
      return "none";
    }
    // If it's extremely short, just star it all out
    if (key.length() <= 8) {
      return "****";
    }
    // Keep first 4 and last 4 characters visible for debugging, hide the rest
    return key.substring(0, 4) + "..." + key.substring(key.length() - 4);
  }
}
