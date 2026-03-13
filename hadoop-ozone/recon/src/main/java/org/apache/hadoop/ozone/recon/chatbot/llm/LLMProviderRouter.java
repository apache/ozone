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
 * Central router that implements {@link LLMProvider} and delegates
 * to the correct {@link DirectLLMProvider} based on the model name
 * or configured default provider.
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

    private final Map<String, DirectLLMProvider> providers;
    private final String defaultProviderName;

    @Inject
    public LLMProviderRouter(OzoneConfiguration configuration,
            CredentialHelper credentialHelper) {
        int timeoutMs = configuration.getInt(
                ChatbotConfigKeys.OZONE_RECON_CHATBOT_TIMEOUT_MS,
                ChatbotConfigKeys.OZONE_RECON_CHATBOT_TIMEOUT_MS_DEFAULT);

        this.providers = new HashMap<>();
        providers.put("openai",
                new OpenAIProvider(configuration, credentialHelper, timeoutMs));
        providers.put("gemini",
                new GeminiProvider(configuration, credentialHelper, timeoutMs));
        providers.put("anthropic",
                new AnthropicProvider(configuration, credentialHelper, timeoutMs));

        this.defaultProviderName = configuration.get(
                ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER,
                ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER_DEFAULT);

        LOG.info("LLMProviderRouter initialized: defaultProvider={}, "
                + "registeredProviders={}",
                defaultProviderName, providers.keySet());
    }

    @Override
    public LLMResponse chatCompletion(
            List<ChatMessage> messages, String model,
            String apiKey, Map<String, Object> parameters) throws LLMException {

        if (messages == null || messages.isEmpty()) {
            throw new LLMException("Messages cannot be null or empty");
        }

        // Check for explicit provider hint in parameters.
        String explicitProvider = null;
        if (parameters != null && parameters.containsKey("_provider")) {
            explicitProvider = (String) parameters.remove("_provider");
        }

        DirectLLMProvider provider = resolveProvider(model, explicitProvider);
        LOG.info("Routing chat request: model={}, provider={}",
                model, provider.getProviderName());

        return provider.chatCompletion(messages, model, apiKey, parameters);
    }

    @Override
    public boolean isAvailable() {
        DirectLLMProvider provider = providers.get(defaultProviderName);
        return provider != null && provider.isAvailable();
    }

    @Override
    public List<String> getSupportedModels() {
        List<String> allModels = new ArrayList<>();
        for (DirectLLMProvider provider : providers.values()) {
            if (provider.isAvailable()) {
                allModels.addAll(provider.getSupportedModels());
            }
        }
        if (allModels.isEmpty()) {
            // Return defaults even if no keys are configured yet.
            DirectLLMProvider defaultProvider = providers.get(defaultProviderName);
            if (defaultProvider != null) {
                allModels.addAll(defaultProvider.getSupportedModels());
            }
        }
        return allModels;
    }

    /**
     * Resolves the correct provider.
     * Priority: explicit provider > model name prefix > default.
     */
    private DirectLLMProvider resolveProvider(String model,
            String explicitProvider)
            throws LLMException {
        // 1. Explicit provider (from UI dropdown).
        if (explicitProvider != null && !explicitProvider.isEmpty()) {
            LOG.debug("Using explicit provider '{}'", explicitProvider);
            return getProvider(explicitProvider.toLowerCase());
        }

        // 2. Infer from model name prefix.
        if (model != null && !model.isEmpty()) {
            String lowerModel = model.toLowerCase();

            if (lowerModel.startsWith("gemini-")) {
                return getProvider("gemini");
            } else if (lowerModel.startsWith("gpt-")
                    || lowerModel.startsWith("o1")
                    || lowerModel.startsWith("o3")) {
                return getProvider("openai");
            } else if (lowerModel.startsWith("claude-")) {
                return getProvider("anthropic");
            }
        }

        // 3. Fall back to configured default.
        LOG.debug("Cannot determine provider from model '{}', "
                + "using default '{}'", model, defaultProviderName);
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
                    "Default provider '" + defaultProviderName + "' not found. "
                            + "Available: " + providers.keySet());
        }
        return provider;
    }
}
