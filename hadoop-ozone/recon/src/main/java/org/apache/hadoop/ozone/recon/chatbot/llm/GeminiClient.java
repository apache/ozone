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
 * Direct client for Google Gemini models using Composition.
 */
public class GeminiClient implements LLMClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final OzoneConfiguration configuration;
  private final CredentialHelper credentialHelper;
  private final LLMNetworkClient networkClient;

  public GeminiClient(OzoneConfiguration configuration,
                      CredentialHelper credentialHelper,
                      int timeoutMs) {
    this.configuration = configuration;
    this.credentialHelper = credentialHelper;
    this.networkClient = new LLMNetworkClient(timeoutMs);
  }

  @Override
  public LLMResponse chatCompletion(List<ChatMessage> messages, String model, String apiKey, Map<String, Object> parameters) throws LLMException {
    String resolvedKey = resolveApiKey(apiKey);
    if (resolvedKey == null || resolvedKey.isEmpty()) {
      throw new LLMException("No API key configured for provider 'gemini'.");
    }

    String url = getBaseUrl() + "/v1beta/models/" + model + ":generateContent?key=" + resolvedKey;

    ObjectNode body = MAPPER.createObjectNode();
    ArrayNode contents = body.putArray("contents");

    for (ChatMessage msg : messages) {
      if ("system".equals(msg.getRole())) {
        ObjectNode sysInstruction = body.putObject("systemInstruction");
        ArrayNode sysParts = sysInstruction.putArray("parts");
        sysParts.addObject().put("text", msg.getContent());
      } else {
        ObjectNode content = contents.addObject();
        String role = "assistant".equals(msg.getRole()) ? "model" : msg.getRole();
        content.put("role", role);
        ArrayNode parts = content.putArray("parts");
        parts.addObject().put("text", msg.getContent());
      }
    }

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
      String responseBody = networkClient.executePost(url, null, MAPPER.writeValueAsString(body), "gemini");
      return parseGeminiResponse(responseBody, model);
    } catch (Exception e) {
      if (e instanceof LLMException) {
        throw (LLMException) e;
      }
      throw new LLMException("Gemini Request Failed: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean isAvailable() {
    String key = credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY);
    return key != null && !key.isEmpty();
  }

  @Override
  public List<String> getSupportedModels() {
    return Arrays.asList("gemini-2.5-pro", "gemini-2.5-flash", "gemini-3-flash-preview", "gemini-3.1-pro-preview");
  }

  private String resolveApiKey(String perRequestKey) {
    if (perRequestKey != null && !perRequestKey.isEmpty()) {
      return perRequestKey;
    }
    return credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY);
  }

  private String getBaseUrl() {
    return configuration.get(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL,
                             ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL_DEFAULT);
  }

  private LLMResponse parseGeminiResponse(String responseBody, String model) throws LLMException {
    try {
      JsonNode root = MAPPER.readTree(responseBody);
      JsonNode candidates = root.get("candidates");
      if (candidates == null || !candidates.isArray() || candidates.isEmpty()) {
        throw new LLMException("Invalid Gemini response: no candidates found");
      }

      JsonNode firstCandidate = candidates.get(0);
      JsonNode content = firstCandidate.get("content");
      JsonNode parts = content != null ? content.get("parts") : null;

      StringBuilder text = new StringBuilder();
      if (parts != null && parts.isArray()) {
        for (JsonNode part : parts) {
          if (part.has("text")) {
            text.append(part.get("text").asText());
          }
        }
      }

      JsonNode usageMetadata = root.get("usageMetadata");
      int promptTokens = 0, completionTokens = 0;
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
