/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.chatbot.llm;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.output.TokenUsage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link LLMClient} implementation backed by
 * <a href="https://github.com/langchain4j/langchain4j">LangChain4j</a>.
 *
 * <p>This is the only class in the chatbot that knows about LangChain4j. It resolves the
 * correct provider for a given model, builds a {@link ChatLanguageModel}, translates the
 * message list into LangChain4j types, fires the completion, and returns a normalised
 * {@link LLMResponse}. Everything above this class ({@code ChatbotAgent},
 * {@code ChatbotEndpoint}) depends only on the {@link LLMClient} interface.</p>
 *
 * <p><b>Startup:</b> reads configuration and checks which providers have API keys. No
 * network calls are made until {@link #chatCompletion} is first invoked.</p>
 *
 * <p><b>Provider routing</b> — resolved in this order on every call:</p>
 * <ol>
 *   <li>Explicit {@code _provider} key in the parameters map, or a {@code provider:model}
 *       prefix in the model string.</li>
 *   <li>Reverse lookup in the configured model lists ({@link #supportedModels}): the same
 *       map that drives {@code GET /chatbot/models}. Adding a model to
 *       {@code ozone.recon.chatbot.openai.models} in {@code ozone-site.xml} makes it
 *       routable with no code change.</li>
 *   <li>If the model is not found and no hint was given, {@link LLMException} is thrown
 *       directing the caller to {@code GET /api/v1/chatbot/models}.</li>
 * </ol>
 *
 * <p><b>Model caching:</b> building a {@link ChatLanguageModel} creates an HTTP client and
 * SSL context, so each {@code (provider, model)} pair is built once and cached in
 * {@link #modelCache}. If the first call with that model fails, the entry is evicted so a
 * bad model name cannot get stuck in the cache permanently.</p>
 */
@Singleton
public class LangChain4jDispatcher implements LLMClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(LangChain4jDispatcher.class);

  private final OzoneConfiguration configuration;
  private final CredentialHelper credentialHelper;
  private final Duration timeout;
  private final String defaultProvider;

  /**
   * Per-provider static model lists — used by getSupportedModels() and isAvailable().
   * A provider only appears here if its API key is configured.
   */
  private final Map<String, List<String>> supportedModels = new HashMap<>();

  /**
   * Cache of built {@link ChatLanguageModel} instances, keyed by {@code "provider:model"}.
   *
   * <p>Building a model involves constructing an HTTP client, SSL context, and connection pool —
   * expensive operations that should happen once, not on every request. This cache ensures each
   * (provider, model) pair is built exactly once and then reused for all subsequent calls.</p>
   *
   * <p>{@link ConcurrentHashMap} is used because multiple chatbot executor threads may call
   * {@link #chatCompletion} concurrently. In the unlikely event two threads request the same
   * model simultaneously on the first call, both may build an instance, but the map will
   * simply retain one — both instances are functionally identical.</p>
   */
  private final Map<String, ChatLanguageModel> modelCache = new ConcurrentHashMap<>();

  @Inject
  public LangChain4jDispatcher(OzoneConfiguration configuration,
                               CredentialHelper credentialHelper) {
    this.configuration = configuration;
    this.credentialHelper = credentialHelper;

    int timeoutMs = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_TIMEOUT_MS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_TIMEOUT_MS_DEFAULT);
    this.timeout = Duration.ofMillis(timeoutMs);

    this.defaultProvider = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER_DEFAULT);

    // Register available providers. A provider is considered "available" only if
    // a non-empty API key has been configured for it. Model lists are read from
    // ozone-site.xml so admins can update them without a code change when vendors
    // rename, add, or retire models.
    if (!credentialHelper.getSecret(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY).isEmpty()) {
      supportedModels.put("openai", parseModelList(configuration,
          ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_MODELS,
          ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_MODELS_DEFAULT));
    }
    if (!credentialHelper.getSecret(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY).isEmpty()) {
      supportedModels.put("gemini", parseModelList(configuration,
          ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_MODELS,
          ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_MODELS_DEFAULT));
    }
    if (!credentialHelper.getSecret(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_API_KEY).isEmpty()) {
      supportedModels.put("anthropic", parseModelList(configuration,
          ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_MODELS,
          ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_MODELS_DEFAULT));
    }

    LOG.info("LangChain4jDispatcher initialized. Available providers: {}, default: {}",
        supportedModels.keySet(), defaultProvider);
  }

  /**
   * Sends the conversation to the appropriate LLM provider and returns a standardised response.
   *
   * <p>Steps:
   * <ol>
   *   <li>Determine which provider to use from model name prefix or explicit provider hint.</li>
   *   <li>Build a LangChain4j {@link ChatLanguageModel} for that provider + model.</li>
   *   <li>Translate internal {@link ChatMessage} list to LangChain4j message types.</li>
   *   <li>Call the model, extract text + token counts, return {@link LLMResponse}.</li>
   * </ol>
   */
  @Override
  public LLMResponse chatCompletion(List<ChatMessage> messages, String modelStr, Map<String, Object> parameters)
      throws LLMException {

    if (messages == null || messages.isEmpty()) {
      throw new LLMException("Messages cannot be null or empty");
    }

    // Extract provider hint and actual model name from "provider:model" format if present.
    String providerHint = null;
    String actualModel = modelStr;
    if (parameters != null && parameters.containsKey("_provider")) {
      providerHint = (String) parameters.get("_provider");
    }
    if (modelStr != null && modelStr.contains(":")) {
      String[] parts = modelStr.split(":", 2);
      providerHint = parts[0].toLowerCase();
      actualModel = parts[1];
    }

    String provider = resolveProvider(providerHint, actualModel);
    LOG.debug("Routing chatCompletion: model={}, resolvedProvider={}", actualModel, provider);

    // Build the LangChain4j model for this specific request.
    ChatLanguageModel chatModel = buildModel(provider, actualModel);

    // Translate our internal ChatMessage list into LangChain4j's message types.
    List<dev.langchain4j.data.message.ChatMessage> lc4jMessages =
        translateMessages(messages);

    try {
      ChatRequest chatRequest = ChatRequest.builder()
          .messages(lc4jMessages)
          .build();
      ChatResponse response = chatModel.chat(chatRequest);

      String content = response.aiMessage().text();
      if (content == null) {
        content = "";
      }

      // Extract token usage for cost tracking. LangChain4j normalises this across providers.
      TokenUsage usage = response.tokenUsage();
      int promptTokens = usage != null ? safeInt(usage.inputTokenCount()) : 0;
      int completionTokens = usage != null ? safeInt(usage.outputTokenCount()) : 0;

      Map<String, Object> metadata = new HashMap<>();
      metadata.put("provider", provider);
      if (response.finishReason() != null) {
        metadata.put("finish_reason", response.finishReason().toString());
      }

      return new LLMResponse(content, actualModel, promptTokens, completionTokens, metadata);

    } catch (Exception e) {
      modelCache.remove(provider + ":" + actualModel);
      LOG.error("LangChain4j call failed for provider={}, model={}", provider, actualModel, e);
      throw new LLMException(
          "LLM request failed for provider '" + provider + "': " + e.getMessage(), e);
    }
  }

  /**
   * Returns true if at least one provider has a valid API key configured.
   */
  @Override
  public boolean isAvailable() {
    return !supportedModels.isEmpty();
  }

  /**
   * Returns the combined list of model names across all configured providers.
   * Used to populate the model drop-down in the UI.
   */
  @Override
  public List<String> getSupportedModels() {
    List<String> all = new ArrayList<>();
    for (List<String> models : supportedModels.values()) {
      all.addAll(models);
    }
    return all;
  }

  // =========================================================================
  // Private helpers
  // =========================================================================

  /**
   * Returns the provider for the given model.
   * If a hint is supplied (via explicit field or "provider:model" prefix), it is used directly.
   * Otherwise, the model name is looked up in the configured model lists (same data the UI uses).
   * Throws if the model is not found in any list — callers should use GET /chatbot/models.
   */
  private String resolveProvider(String providerHint, String model) throws LLMException {
    if (providerHint != null && !providerHint.isEmpty()) {
      return providerHint.toLowerCase();
    }
    if (model != null) {
      for (Map.Entry<String, List<String>> entry : supportedModels.entrySet()) {
        if (entry.getValue().contains(model)) {
          return entry.getKey();
        }
      }
    }
    throw new LLMException(
        "Model '" + model + "' is not recognised. "
            + "Use GET /api/v1/chatbot/models for the list of supported models.");
  }

  /**
   * Returns a {@link ChatLanguageModel} for the given provider and model, building and caching
   * it on first use. Subsequent calls for the same (provider, model) pair return the cached
   * instance immediately — no HTTP client or SSL context is re-created.
   */
  private ChatLanguageModel buildModel(String provider, String model) throws LLMException {
    String cacheKey = provider + ":" + model;
    ChatLanguageModel cached = modelCache.get(cacheKey);
    if (cached != null) {
      return cached;
    }
    ChatLanguageModel built = buildModelInternal(provider, model);
    modelCache.put(cacheKey, built);
    LOG.info("Built and cached ChatLanguageModel for provider={}, model={}", provider, model);
    return built;
  }

  /**
   * Constructs a new LangChain4j {@link ChatLanguageModel} for the given provider and model name.
   * The API key is always resolved from the server configuration via {@link CredentialHelper}.
   * Callers should prefer {@link #buildModel} which caches the result.
   */
  private ChatLanguageModel buildModelInternal(String provider, String model) throws LLMException {
    switch (provider) {
    case "openai":
      return buildOpenAiModel(model);
    case "gemini":
      return buildGeminiModel(model);
    case "anthropic":
      return buildAnthropicModel(model);
    default:
      throw new LLMException("Unknown or unconfigured provider: '" + provider + "'");
    }
  }

  private ChatLanguageModel buildOpenAiModel(String model) throws LLMException {
    String key = resolveKey(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY, "openai");
    String baseUrl = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_BASE_URL,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_BASE_URL_DEFAULT);
    return OpenAiChatModel.builder()
        .apiKey(key)
        .modelName(model)
        .baseUrl(baseUrl)
        .timeout(timeout)
        .build();
  }

  private ChatLanguageModel buildGeminiModel(String model) throws LLMException {
    String key = resolveKey(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "gemini");
    String baseUrl = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL_DEFAULT);

    // LangChain4j 0.35.0's native Gemini client has a known bug where it ignores read timeouts.
    // Since Google's Gemini API is fully compatible with the OpenAI API spec via the /openai/
    // endpoint, we route Gemini requests through the OpenAiChatModel to ensure timeouts are honored.
    return OpenAiChatModel.builder()
        .apiKey(key)
        .modelName(model)
        .baseUrl(baseUrl)
        .timeout(timeout)
        .build();
  }

  private ChatLanguageModel buildAnthropicModel(String model) throws LLMException {
    String key = resolveKey(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_API_KEY, "anthropic");
    String betaHeader = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_BETA_HEADER,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_BETA_HEADER_DEFAULT);
    AnthropicChatModel.AnthropicChatModelBuilder builder =
        AnthropicChatModel.builder()
            .apiKey(key)
            .modelName(model)
            .timeout(timeout);
    if (betaHeader != null && !betaHeader.isEmpty()) {
      builder.beta(betaHeader);
    }
    return builder.build();
  }

  /**
   * Resolves the API key for the given provider from the Hadoop credential store or
   * ozone-site.xml via {@link CredentialHelper}.
   * Throws {@link LLMException} immediately if no key is configured.
   */
  private String resolveKey(String configKey, String providerName) throws LLMException {
    String configured = credentialHelper.getSecret(configKey);
    if (configured == null || configured.isEmpty()) {
      throw new LLMException(
          "No API key configured for provider '" + providerName + "'. "
              + "Set " + configKey + " in ozone-site.xml or the Hadoop credential store.");
    }
    return configured;
  }

  /**
   * Translates internal {@link ChatMessage} objects into LangChain4j message types.
   *
   * <ul>
   *   <li>{@code system} → {@link SystemMessage}</li>
   *   <li>{@code user} → {@link UserMessage}</li>
   *   <li>{@code assistant} → {@link AiMessage}</li>
   * </ul>
   */
  private List<dev.langchain4j.data.message.ChatMessage> translateMessages(
      List<ChatMessage> messages) {
    List<dev.langchain4j.data.message.ChatMessage> result = new ArrayList<>();
    for (ChatMessage msg : messages) {
      switch (msg.getRole()) {
      case "system":
        result.add(SystemMessage.from(msg.getContent()));
        break;
      case "assistant":
        result.add(AiMessage.from(msg.getContent()));
        break;
      default:
        LOG.warn("Unknown message role '{}', treating as user message", msg.getRole());
        result.add(UserMessage.from(msg.getContent()));
        break;
      }
    }
    return result;
  }

  /**
   * Reads a comma-separated model list from config, trims whitespace from each entry,
   * and filters out any blank tokens. Falls back to the provided default string if
   * the config value is empty or missing.
   *
   * <p>Example config value: {@code "gemini-2.5-pro, gemini-2.5-flash, gemini-3-flash-preview"}
   */
  private List<String> parseModelList(OzoneConfiguration conf,
                                      String configKey,
                                      String defaultValue) {
    String raw = conf.get(configKey, defaultValue);
    if (StringUtils.isBlank(raw)) {
      raw = defaultValue;
    }
    List<String> models = new ArrayList<>();
    for (String token : raw.split(",")) {
      String trimmed = token.trim();
      if (!trimmed.isEmpty()) {
        models.add(trimmed);
      }
    }
    return models;
  }

  /**
   * Safely unboxes a nullable Integer, returning 0 for null.
   */
  private int safeInt(Integer value) {
    return value != null ? value : 0;
  }
}
