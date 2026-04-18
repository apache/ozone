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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LLMDispatcher acts as the "Traffic Cop" Proxy.
 * It implements LLMClient so the application thinks it's talking to an AI,
 * but it secretly routes requests to the correct underlying AI client instead.
 */
@Singleton
public class LLMDispatcher implements LLMClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(LLMDispatcher.class);

  private final Map<String, LLMClient> clients = new HashMap<>();
  private final OzoneConfiguration configuration;

  @Inject
  public LLMDispatcher(OzoneConfiguration configuration,
                       CredentialHelper credentialHelper) {
    this.configuration = configuration;

    int timeoutMs = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_TIMEOUT_MS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_TIMEOUT_MS_DEFAULT);

    // Register all supported specific AI Clients!
    clients.put("openai", new OpenAIClient(configuration, credentialHelper, timeoutMs));
    clients.put("gemini", new GeminiClient(configuration, credentialHelper, timeoutMs));
    clients.put("anthropic", new AnthropicClient(configuration, credentialHelper, timeoutMs));

    LOG.info("LLMDispatcher initialized with clients: {}", clients.keySet());
  }

  /**
   * Figure out exactly which AI client should handle a prompt.
   * Format allowed in UI: "provider:model" (e.g. "openai:gpt-4" or "anthropic:claude-sonnet-4-6")
   */
  public LLMClient resolveClient(String requestProvider, String requestModel) {
    if (requestProvider != null && !requestProvider.isEmpty() && clients.containsKey(requestProvider.toLowerCase())) {
      return clients.get(requestProvider.toLowerCase());
    }

    if (requestModel != null) {
      if (requestModel.contains(":")) {
        String[] parts = requestModel.split(":", 2);
        String prefix = parts[0].toLowerCase();
        if (clients.containsKey(prefix)) {
          return clients.get(prefix);
        }
      }
      
      String m = requestModel.toLowerCase();
      if (m.startsWith("gpt-") || m.startsWith("o1") || m.startsWith("o3")) {
        return clients.get("openai");
      }
      if (m.startsWith("gemini")) {
        return clients.get("gemini");
      }
      if (m.startsWith("claude")) {
        return clients.get("anthropic");
      }
    }

    String defaultProvider = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER_DEFAULT);
    
    LLMClient defaultClient = clients.get(defaultProvider.toLowerCase());
    if (defaultClient == null) {
      throw new IllegalArgumentException("Default LLM provider '" + defaultProvider + "' is not registered.");
    }
    return defaultClient;
  }

  @Override
  public LLMResponse chatCompletion(
      List<ChatMessage> messages,
      String modelStr,
      String apiKey,
      Map<String, Object> parameters) throws LLMException {

    String providerHint = null;
    String actualModel = modelStr;
    if (parameters != null && parameters.containsKey("provider")) {
        providerHint = (String) parameters.get("provider");
    }
    if (modelStr != null && modelStr.contains(":")) {
        String[] parts = modelStr.split(":", 2);
        providerHint = parts[0];
        actualModel = parts[1];
    }

    LLMClient selectedClient = resolveClient(providerHint, actualModel);
    LOG.debug("Routing chat completion for model={} to client class={}", actualModel, selectedClient.getClass().getSimpleName());

    return selectedClient.chatCompletion(messages, actualModel, apiKey, parameters);
  }

  @Override
  public boolean isAvailable() {
    for (LLMClient client : clients.values()) {
      if (client.isAvailable()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<String> getSupportedModels() {
    List<String> allModels = new ArrayList<>();
    for (LLMClient client : clients.values()) {
      allModels.addAll(client.getSupportedModels());
    }
    return allModels;
  }
}
