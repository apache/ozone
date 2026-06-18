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
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolParameters;
import dev.langchain4j.agent.tool.ToolSpecification;
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
 * <p><b>Provider/model routing</b> — resolved on every call via {@link LlmRouting}:</p>
 * <ol>
 *   <li>Use the requested provider if it is configured with an API key.</li>
 *   <li>Else infer provider from a supported model name, else use the configured default provider.</li>
 *   <li>Use the requested model if it appears in any configured model list, else the default model.</li>
 *   <li>If the model is not valid for the chosen provider, fall back to default provider + default model.</li>
 * </ol>
 *
 * <p><b>Model caching:</b> building a {@link ChatLanguageModel} creates an HTTP client and
 * SSL context, so each {@code (provider, model, temperature, max_tokens)} combination is built
 * once and cached in {@link #modelCache}. If the first call with that combination fails, the
 * entry is evicted so a bad configuration cannot get stuck in the cache permanently.</p>
 */
@Singleton
public class LangChain4jDispatcher implements LLMClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(LangChain4jDispatcher.class);

  private static final String PROVIDER_OPENAI = "openai";
  private static final String PROVIDER_GEMINI = "gemini";
  private static final String PROVIDER_ANTHROPIC = "anthropic";

  private final OzoneConfiguration configuration;
  private final CredentialHelper credentialHelper;
  private final Duration timeout;
  private final String defaultProvider;
  private final String defaultModel;
  private final LlmRouting routing;

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
    this.defaultModel = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_DEFAULT_MODEL,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_DEFAULT_MODEL_DEFAULT);

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

    this.routing = new LlmRouting(defaultProvider, defaultModel, supportedModels);

    LOG.info("LangChain4jDispatcher initialized. Available providers: {}, default: {}/{}",
        supportedModels.keySet(), defaultProvider, defaultModel);
  }

  /**
   * Sends the conversation to the appropriate LLM provider and returns a standardised response.
   * When {@code tools} is non-null the model may reply with native tool calls instead of (or in
   * addition to) text; text-only callers (summarization, fallback) pass {@code null}.
   *
   * <p>Steps:
   * <ol>
   *   <li>Resolve provider and model via {@link LlmRouting}.</li>
   *   <li>Build a LangChain4j {@link ChatLanguageModel} for that provider + model
   *       (including optional {@code temperature} and {@code max_tokens} from parameters).</li>
   *   <li>Translate internal {@link ChatMessage} list to LangChain4j message types and attach
   *       tool specifications when {@code tools} is supplied.</li>
   *   <li>Call the model, extract text + token counts, return {@link LLMResponse}.</li>
   * </ol>
   */
  @Override
  public LLMResponse chatCompletion(List<ChatMessage> messages, String modelStr, String providerStr,
                                    GenParams params, List<ToolSpec> tools)
      throws LLMException {

    if (messages == null || messages.isEmpty()) {
      throw new LLMException("Messages cannot be null or empty");
    }

    // Pick the provider/model we can actually call (user request may be unsupported).
    LlmRouting.Resolved resolved = routing.resolve(providerStr, modelStr);
    String provider = resolved.getProvider();
    String actualModel = resolved.getModel();
    LOG.debug("Routing LLM call: requested provider={}, model={} -> resolved provider={}, model={}",
        providerStr, modelStr, provider, actualModel);

    // Cached HTTP client + model for this (provider, model, temperature, max_tokens).
    ChatLanguageModel chatModel = buildModel(provider, actualModel, params);
    // Messages for the LLM; attach Recon tool specs when tools != null (tool-selection step).
    ChatRequest chatRequest = buildChatRequest(translateMessages(messages), tools);

    try {
      ChatResponse response = invokeModel(chatModel, chatRequest, provider, actualModel);
      // Reasoning models may return no visible text; treat that as empty, not a 500.
      return response == null ? emptyTextResponse(actualModel) : toLLMResponse(response, actualModel);
    } catch (Exception e) {
      // Drop cached model so a bad config is not reused on the next request.
      modelCache.remove(buildCacheKey(provider, actualModel, params));
      LOG.error("LangChain4j call failed for provider={}, model={}", provider, actualModel, e);
      throw new LLMException(
          "LLM request failed for provider '" + provider + "': " + e.getMessage(), e);
    }
  }

  /**
   * Builds the outgoing LangChain4j {@link ChatRequest} from the translated messages, attaching
   * tool specifications when the caller supplied any.
   */
  private ChatRequest buildChatRequest(List<dev.langchain4j.data.message.ChatMessage> messages,
                                       List<ToolSpec> tools) {
    ChatRequest.Builder requestBuilder = ChatRequest.builder().messages(messages);
    if (tools != null && !tools.isEmpty()) {
      requestBuilder.toolSpecifications(toLangChain4jToolSpecs(tools));
    }
    return requestBuilder.build();
  }

  /**
   * Converts our internal {@link ToolSpec} list into LangChain4j {@link ToolSpecification}s,
   * mapping each parameter to a JSON-schema-like {@code {type: ...}} property.
   */
  private List<ToolSpecification> toLangChain4jToolSpecs(List<ToolSpec> tools) {
    List<ToolSpecification> toolSpecs = new ArrayList<>();
    for (ToolSpec spec : tools) {
      ToolSpecification.Builder specBuilder = ToolSpecification.builder()
          .name(spec.getName())
          .description(spec.getDescription());
      if (spec.getParametersSchema() != null && !spec.getParametersSchema().isEmpty()) {
        Map<String, Map<String, Object>> props = new HashMap<>();
        for (Map.Entry<String, Object> entry : spec.getParametersSchema().entrySet()) {
          Map<String, Object> typeMap = new HashMap<>();
          typeMap.put("type", entry.getValue());
          props.put(entry.getKey(), typeMap);
        }
        ToolParameters toolParams = ToolParameters.builder()
            .type("object")
            .properties(props)
            .build();
        specBuilder.parameters(toolParams);
      }
      toolSpecs.add(specBuilder.build());
    }
    return toolSpecs;
  }

  /**
   * Fires the actual provider call. Returns {@code null} when the provider returned a null text
   * body — LangChain4j 0.35.0 surfaces this as an {@link IllegalArgumentException}, common with
   * reasoning models that exhaust {@code max_tokens} on thinking before any visible text.
   */
  private ChatResponse invokeModel(ChatLanguageModel chatModel, ChatRequest chatRequest,
                                   String provider, String model) {
    try {
      return chatModel.chat(chatRequest);
    } catch (IllegalArgumentException e) {
      if (isNullTextContentFromProvider(e)) {
        LOG.warn("Model returned null text for provider={}, model={}; treating as empty response",
            provider, model);
        return null;
      }
      throw e;
    }
  }

  /**
   * Normalises a LangChain4j {@link ChatResponse} into our internal {@link LLMResponse}: text
   * content (empty when the model only wants to call a tool), any native tool calls, and token
   * usage for cost tracking.
   */
  private LLMResponse toLLMResponse(ChatResponse response, String model) {
    String content = response.aiMessage().text();
    if (content == null) {
      content = "";
    }

    List<ToolCallRequest> toolCallRequests = null;
    if (response.aiMessage().hasToolExecutionRequests()) {
      toolCallRequests = new ArrayList<>();
      for (ToolExecutionRequest req : response.aiMessage().toolExecutionRequests()) {
        toolCallRequests.add(new ToolCallRequest(req.name(), req.arguments()));
      }
    }

    TokenUsage usage = response.tokenUsage();
    int promptTokens = usage != null ? safeInt(usage.inputTokenCount()) : 0;
    int completionTokens = usage != null ? safeInt(usage.outputTokenCount()) : 0;

    return new LLMResponse(content, model, promptTokens, completionTokens, toolCallRequests);
  }

  private static boolean isNullTextContentFromProvider(IllegalArgumentException e) {
    return e.getMessage() != null && e.getMessage().contains("text cannot be null");
  }

  private static LLMResponse emptyTextResponse(String model) {
    return new LLMResponse("", model, 0, 0, null);
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
   * Returns a {@link ChatLanguageModel} for the given provider and model, building and caching
   * it on first use. Subsequent calls for the same (provider, model) pair return the cached
   * instance immediately — no HTTP client or SSL context is re-created.
   */
  private ChatLanguageModel buildModel(String provider, String model,
                                       GenParams params) throws LLMException {
    String cacheKey = buildCacheKey(provider, model, params);
    ChatLanguageModel cached = modelCache.get(cacheKey);
    if (cached != null) {
      return cached;
    }
    ChatLanguageModel built =
        buildModelInternal(provider, model, params.temperature(), params.maxTokens());
    modelCache.put(cacheKey, built);
    LOG.info("Built and cached ChatLanguageModel for provider={}, model={}, temperature={}, maxTokens={}",
        provider, model, params.temperature(), params.maxTokens());
    return built;
  }

  private static String buildCacheKey(String provider, String model, GenParams params) {
    return provider + ":" + model
        + ":t=" + params.temperature()
        + ":m=" + params.maxTokens();
  }

  /**
   * Constructs a new LangChain4j {@link ChatLanguageModel} for the given provider and model name.
   * The API key is always resolved from the server configuration via {@link CredentialHelper}.
   * Callers should prefer {@link #buildModel} which caches the result.
   */
  private ChatLanguageModel buildModelInternal(String provider, String model,
                                               double temperature, int maxTokens)
      throws LLMException {
    switch (provider) {
    case PROVIDER_OPENAI:
      return buildOpenAiModel(model, temperature, maxTokens);
    case PROVIDER_GEMINI:
      return buildGeminiModel(model, temperature, maxTokens);
    case PROVIDER_ANTHROPIC:
      return buildAnthropicModel(model, temperature, maxTokens);
    default:
      throw new LLMException("Unknown or unconfigured provider: '" + provider + "'");
    }
  }

  private ChatLanguageModel buildOpenAiModel(String model, double temperature, int maxTokens)
      throws LLMException {
    String key = resolveKey(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY, "openai");
    String baseUrl = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_BASE_URL,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_BASE_URL_DEFAULT);
    OpenAiChatModel.OpenAiChatModelBuilder builder = OpenAiChatModel.builder()
        .apiKey(key)
        .modelName(model)
        .baseUrl(baseUrl)
        .timeout(timeout);
    applyGenerationParams(builder, temperature, maxTokens);
    return builder.build();
  }

  private ChatLanguageModel buildGeminiModel(String model, double temperature, int maxTokens)
      throws LLMException {
    String key = resolveKey(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "gemini");
    String baseUrl = configuration.get(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL_DEFAULT);

    // LangChain4j 0.35.0's native Gemini client has a known bug where it ignores read timeouts.
    // Since Google's Gemini API is fully compatible with the OpenAI API spec via the /openai/
    // endpoint, we route Gemini requests through the OpenAiChatModel to ensure timeouts are honored.
    OpenAiChatModel.OpenAiChatModelBuilder builder = OpenAiChatModel.builder()
        .apiKey(key)
        .modelName(model)
        .baseUrl(baseUrl)
        .timeout(timeout);
    applyGenerationParams(builder, temperature, maxTokens);
    return builder.build();
  }

  private ChatLanguageModel buildAnthropicModel(String model, double temperature, int maxTokens)
      throws LLMException {
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
    applyGenerationParams(builder, temperature, maxTokens);
    return builder.build();
  }

  private static void applyGenerationParams(OpenAiChatModel.OpenAiChatModelBuilder builder,
                                            double temperature, int maxTokens) {
    builder.temperature(temperature);
    builder.maxTokens(maxTokens);
  }

  private static void applyGenerationParams(AnthropicChatModel.AnthropicChatModelBuilder builder,
                                            double temperature, int maxTokens) {
    builder.temperature(temperature);
    builder.maxTokens(maxTokens);
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
      case "user":
        result.add(UserMessage.from(msg.getContent()));
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
