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
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.googleai.GoogleAiGeminiChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.output.TokenUsage;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link LLMClient} implementation that sends chat requests to cloud LLM providers using
 * <a href="https://github.com/langchain4j/langchain4j">LangChain4j</a>.
 *
 * <h2>Purpose</h2>
 * <p>The Recon chatbot needs to call external APIs (OpenAI, Google Gemini, Anthropic Claude)
 * with a stable Java contract. This class is the only place that talks to LangChain4j: it
 * picks the right provider, builds a {@link ChatLanguageModel} for the requested model,
 * converts messages into LangChain4j types, runs one completion, and maps the result back to
 * {@link LLMResponse}. Higher layers ({@link org.apache.hadoop.ozone.recon.chatbot.agent.ChatbotAgent},
 * {@link org.apache.hadoop.ozone.recon.chatbot.api.ChatbotEndpoint}) depend only on {@link LLMClient}.</p>
 *
 * <h2>Lifecycle (no background work)</h2>
 * <p>The class is registered in Guice as a singleton: one instance exists for the whole Recon process.
 * There is no timer, no scheduled task, and no long-lived outbound connection. At startup
 * the constructor only reads configuration and records which providers have API keys (for
 * {@link #isAvailable()} and {@link #getSupportedModels()}). Actual network calls happen
 * only when {@link #chatCompletion} runs on an HTTP request thread.</p>
 *
 * <h2>Request flow (one chat completion)</h2>
 * <p>Each user message is handled synchronously on the thread that serves the REST call:</p>
 * <pre>
 * User HTTP request
 *         |
 *         v
 * Jersey dispatches to ChatbotEndpoint (request thread)
 *         |
 *         v
 * ChatbotAgent orchestrates tool selection / summarization
 *         |
 *         v
 * LangChain4jDispatcher.chatCompletion(...)
 *         |
 *         +-- Resolve provider (see below)
 *         |
 *         +-- Build a new ChatLanguageModel for that provider + model name (configuration only)
 *         |
 *         +-- Translate ChatMessage list to LangChain4j messages (system / user / assistant)
 *         |
 *         +-- chatModel.chat(ChatRequest)  --&gt; outbound HTTPS to the vendor (may take seconds)
 *         |
 *         v
 * LLMResponse returned to the agent, then JSON to the client
 * </pre>
 *
 * <h2>How provider routing works</h2>
 * <p>When {@link #chatCompletion} runs, the provider is chosen in this order:</p>
 * <ol>
 *   <li>Optional {@code _provider} entry in the parameters map (e.g. {@code "gemini"}).</li>
 *   <li>If the model string looks like {@code provider:model}, the prefix before {@code :}
 *       is the provider.</li>
 *   <li>Otherwise the model name: {@code gpt-} / {@code o1} / {@code o3} → {@code openai};
 *       {@code gemini} → {@code gemini}; {@code claude} → {@code anthropic}.</li>
 *   <li>If still unclear, {@link ChatbotConfigKeys#OZONE_RECON_CHATBOT_PROVIDER} is used.</li>
 * </ol>
 * <p>For each call, a fresh {@link ChatLanguageModel} is built with the exact model id the
 * caller passed (for example {@code gemini-2.5-flash}). That object holds provider settings
 * and timeout; the heavy work is the single {@code chat(...)} call. Different users on
 * different threads each follow this flow independently; only read-only configuration is
 * shared on the dispatcher instance.</p>
 *
 * <h2>Supported models listing</h2>
 * <p>{@link #getSupportedModels()} returns a fixed list per provider for which a non-empty
 * API key exists in configuration. It is not a live query to each vendor's model catalogue.</p>
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
  public LLMResponse chatCompletion(List<ChatMessage> messages, String modelStr,
                                    String apiKey, Map<String, Object> parameters)
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
    ChatLanguageModel chatModel = buildModel(provider, actualModel, apiKey);

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
   * Determines which provider string to use for a given request.
   * Priority: explicit provider hint → model name prefix heuristics → configured default.
   */
  private String resolveProvider(String providerHint, String model) {
    if (providerHint != null && !providerHint.isEmpty()) {
      return providerHint.toLowerCase();
    }
    if (model != null) {
      String m = model.toLowerCase();
      if (m.startsWith("gpt-") || m.startsWith("o1") || m.startsWith("o3")) {
        return "openai";
      }
      if (m.startsWith("gemini")) {
        return "gemini";
      }
      if (m.startsWith("claude")) {
        return "anthropic";
      }
    }
    return defaultProvider.toLowerCase();
  }

  /**
   * Builds a LangChain4j {@link ChatLanguageModel} for the given provider and model name.
   *
   * <p>The per-request API key (if provided) takes priority over the server-configured key.
   * If neither is available, an exception is thrown immediately rather than letting the
   * library discover it at network call time.</p>
   */
  private ChatLanguageModel buildModel(String provider, String model,
                                       String perRequestApiKey) throws LLMException {
    switch (provider) {
      case "openai": {
        String key = resolveKey(perRequestApiKey,
            ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY, "openai");
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
      case "gemini": {
        String key = resolveKey(perRequestApiKey,
            ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "gemini");
        return GoogleAiGeminiChatModel.builder()
            .apiKey(key)
            .modelName(model)
            .timeout(timeout)
            .build();
      }
      case "anthropic": {
        String key = resolveKey(perRequestApiKey,
            ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_API_KEY, "anthropic");
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
      default:
        throw new LLMException("Unknown or unconfigured provider: '" + provider + "'");
    }
  }

  /**
   * Resolves the API key to use: per-request key takes priority, then the configured key.
   * Throws {@link LLMException} if neither is available, giving a clear error message.
   */
  private String resolveKey(String perRequestKey, String configKey,
                             String providerName) throws LLMException {
    if (perRequestKey != null && !perRequestKey.isEmpty()) {
      return perRequestKey;
    }
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
    if (raw == null || raw.trim().isEmpty()) {
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

  /** Safely unboxes a nullable Integer, returning 0 for null. */
  private int safeInt(Integer value) {
    return value != null ? value : 0;
  }
}
