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
 * Direct client for OpenAI models (GPT-4, GPT-4o, o1, o3, etc.).
 * Talks to {@code api.openai.com/v1/chat/completions}.
 */
public class OpenAIClient implements LLMClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final OzoneConfiguration configuration;
  private final CredentialHelper credentialHelper;
  private final LLMNetworkClient networkClient;

  public OpenAIClient(OzoneConfiguration configuration,
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
      throw new LLMException("No API key configured for provider 'openai'.");
    }

    String url = getBaseUrl() + "/v1/chat/completions";
    ObjectNode body = buildOpenAIRequestBody(messages, model, parameters);

    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer " + resolvedKey);

    try {
      String responseBody = networkClient.executePost(url, headers, MAPPER.writeValueAsString(body), "openai");
      return parseOpenAIResponse(responseBody, model);
    } catch (Exception e) {
      if (e instanceof LLMException) {
        throw (LLMException) e;
      }
      throw new LLMException("OpenAI Request Failed: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean isAvailable() {
    String key = credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY);
    return key != null && !key.isEmpty();
  }

  @Override
  public List<String> getSupportedModels() {
    return Arrays.asList("gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano");
  }

  private String resolveApiKey(String perRequestKey) {
    if (perRequestKey != null && !perRequestKey.isEmpty()) {
      return perRequestKey;
    }
    return credentialHelper.getSecret(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY);
  }

  private String getBaseUrl() {
    return configuration.get(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_BASE_URL,
                             ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_BASE_URL_DEFAULT);
  }

  private ObjectNode buildOpenAIRequestBody(List<ChatMessage> messages, String model, Map<String, Object> params) {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("model", model);

    ArrayNode messagesArray = body.putArray("messages");
    for (ChatMessage msg : messages) {
      ObjectNode m = messagesArray.addObject();
      m.put("role", msg.getRole());
      m.put("content", msg.getContent());
    }

    if (params != null) {
      for (Map.Entry<String, Object> e : params.entrySet()) {
        Object v = e.getValue();
        if (v instanceof Integer) body.put(e.getKey(), (Integer) v);
        else if (v instanceof Double) body.put(e.getKey(), (Double) v);
        else if (v instanceof Boolean) body.put(e.getKey(), (Boolean) v);
        else if (v instanceof String) body.put(e.getKey(), (String) v);
      }
    }
    return body;
  }

  private LLMResponse parseOpenAIResponse(String responseBody, String model) throws LLMException {
    try {
      JsonNode root = MAPPER.readTree(responseBody);
      JsonNode choices = root.get("choices");
      if (choices == null || !choices.isArray() || choices.isEmpty()) {
        throw new LLMException("Invalid response: no choices found");
      }

      JsonNode firstChoice = choices.get(0);
      JsonNode message = firstChoice.get("message");
      String content = message.get("content").asText();

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
