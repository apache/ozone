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
 * LLMProviderRouter acts as the "Traffic Cop" or "Dispatcher" for all AI requests.
 * <p>
 * Purpose:
 * ChatbotAgent only knows how to talk to a generic "LLMProvider". It doesn't know if it's talking to Google or OpenAI.
 * This Router pretends to be that single generic LLMProvider. When it receives a request, it looks at the name
 * of the AI model being requested (like "gpt-4" or "gemini-pro"), and silently routes the request in the background
 * to the correct specific provider class.
 *
 * <p>
 * Model-to-provider routing:
 * <ul>
 * <li>{@code gemini-*} → GeminiProvider</li>
 * <li>{@code gpt-*, o1-*, o3-*} → OpenAIProvider</li>
 * <li>{@code claude-*} → AnthropicProvider</li>
 * </ul>
 * </p>
 */
@Singleton
public class LLMProviderRouter implements LLMProvider {

  private static final Logger LOG = LoggerFactory.getLogger(LLMProviderRouter.class);

  // (HashMap) holding the active connections to every configured AI Provider
  private final Map<String, DirectLLMProvider> providers;

  // If the user doesn't specify an AI, which one should we use by default? (e.g., "openai")
  private final String defaultProviderName;

  @Inject
  public LLMProviderRouter(OzoneConfiguration configuration,
                           CredentialHelper credentialHelper) {
    int timeoutMs = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_TIMEOUT_MS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_TIMEOUT_MS_DEFAULT);

    this.providers = new HashMap<>();

    // Boot up all three AI Providers instantly and put them in our HashMap cabinet.
    // Even if the user hasn't provided API keys for all of them, they wait on standby.
    providers.put("openai", new OpenAIProvider(configuration, credentialHelper, timeoutMs));
    providers.put("gemini", new GeminiProvider(configuration, credentialHelper, timeoutMs));
    providers.put("anthropic", new AnthropicProvider(configuration, credentialHelper, timeoutMs));

    this.defaultProviderName = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER_DEFAULT);

    LOG.info("LLMProviderRouter initialized: defaultProvider={}, registeredProviders={}", defaultProviderName,
        providers.keySet());
  }

  /**
   * The Main Intercept!
   * The ChatbotAgent calls this method thinking it is talking to the AI directly.
   */
  @Override
  public LLMResponse chatCompletion(
      List<ChatMessage> messages, String model,
      String apiKey, Map<String, Object> parameters) throws LLMException {

    // Safety check: Cannot send an empty question to an AI
    if (messages == null || messages.isEmpty()) {
      throw new LLMException("Messages cannot be null or empty");
    }

    // Check for explicit provider hint in parameters.
    String explicitProvider = null;
    if (parameters != null && parameters.containsKey("_provider")) {
      explicitProvider = (String) parameters.remove("_provider");
    }

    // Step 1: Detect which AI Provider to use (e.g., look for "gpt" and select OpenAIProvider)
    DirectLLMProvider provider = resolveProvider(model, explicitProvider);

    LOG.info("Routing chat request: model={}, provider={}", model, provider.getProviderName());

    // Step 2: Forward the exact same arguments over to that specific provider!
    return provider.chatCompletion(messages, model, apiKey, parameters);
  }

  /**
   * Checks if the default provider has an API key configured.
   */
  @Override
  public boolean isAvailable() {
    DirectLLMProvider provider = providers.get(defaultProviderName);
    return provider != null && provider.isAvailable();
  }

  /**
   * Loops through all the active AI providers in our HashMap and asks them to list their supported models.
   * Combines them into one master list for the Chatbot UI drop-down menu.
   */
  @Override
  public List<String> getSupportedModels() {
    List<String> allModels = new ArrayList<>();

    for (DirectLLMProvider provider : providers.values()) {
      if (provider.isAvailable()) {
        allModels.addAll(provider.getSupportedModels());
      }
    }

    // Fallback: If no provider is available (maybe API keys aren't set yet),
    // return the supported list of the default provider anyway so the UI isn't completely empty.
    if (allModels.isEmpty()) {
      DirectLLMProvider defaultProvider = providers.get(defaultProviderName);
      if (defaultProvider != null) {
        allModels.addAll(defaultProvider.getSupportedModels());
      }
    }
    return allModels;
  }

  /**
   * Resolves the correct specific AI Provider.
   * Priority: Explicit UI request -> Model string prefix matching -> Configured Default.
   */
  private DirectLLMProvider resolveProvider(String model, String explicitProvider) throws LLMException {

    // 1. Explicit provider (from UI dropdown).
    if (explicitProvider != null && !explicitProvider.isEmpty()) {
      LOG.debug("Using explicit provider '{}'", explicitProvider);
      return getProvider(explicitProvider.toLowerCase());
    }

    // 2. Infer from model name prefix.
    if (model != null && !model.isEmpty()) {
      String lowerModel = model.toLowerCase();

      // If they asked for "gemini-1.5", route to Google Gemini
      if (lowerModel.startsWith("gemini-")) {
        return getProvider("gemini");
      }
      // If they asked for "gpt-4o" or newer "o1"/"o3" models, route to OpenAI
      else if (lowerModel.startsWith("gpt-") || lowerModel.startsWith("o1") || lowerModel.startsWith("o3")) {
        return getProvider("openai");
      }
      // If they asked for "claude-3-sonnet", route to Anthropic
      else if (lowerModel.startsWith("claude-")) {
        return getProvider("anthropic");
      }
    }

    // 3. Fall back to configured default.
    LOG.warn("Cannot determine provider from model '{}', using default '{}'", model, defaultProviderName);
    return getDefaultProvider();
  }

  private DirectLLMProvider getProvider(String name) throws LLMException {
    DirectLLMProvider provider = providers.get(name);
    if (provider == null) {
      throw new LLMException("Unknown provider: " + name);
    }
    return provider;
  }

  private DirectLLMProvider getDefaultProvider() throws LLMException {
    DirectLLMProvider provider = providers.get(defaultProviderName);
    if (provider == null) {
      throw new LLMException(
          "Default provider '" + defaultProviderName + "' not found. " + "Available: " + providers.keySet());
    }
    return provider;
  }
}
