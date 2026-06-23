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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LangChain4jDispatcher}.
 *
 * <p>All tests avoid real network calls. Provider routing is verified either via
 * {@link LangChain4jDispatcher#getSupportedModels()} (which reflects the configured model lists)
 * or by confirming that an explicit provider hint reaches {@code resolveKey}, which throws a
 * clear "No API key configured for provider X" error when no key is set — proving the correct
 * provider code path was entered.</p>
 */
public class TestLangChain4jDispatcher {

  private OzoneConfiguration conf;
  private LangChain4jDispatcher dispatcher;

  @BeforeEach
  public void setUp() {
    conf = new OzoneConfiguration();
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER, "gemini");
    CredentialHelper credentialHelper = new CredentialHelper(conf);
    dispatcher = new LangChain4jDispatcher(conf, credentialHelper);
  }

  // ── Input validation ──────────────────────────────────────────────────────

  @Test
  public void testEmptyMessagesThrows() {
    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "gpt-4.1", new HashMap<>()));
  }

  @Test
  public void testNullMessagesThrows() {
    assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(null, "gpt-4.1", new HashMap<>()));
  }

  // ── isAvailable / getSupportedModels ─────────────────────────────────────

  @Test
  public void testIsAvailableWithoutKeys() {
    assertFalse(dispatcher.isAvailable());
  }

  @Test
  public void testIsAvailableWithGeminiKey() {
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "fake-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));
    assertTrue(dispatcher.isAvailable());
  }

  @Test
  public void testIsAvailableWithOpenAIKey() {
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY, "fake-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));
    assertTrue(dispatcher.isAvailable());
  }

  @Test
  public void testGetSupportedModelsEmptyWithoutKeys() {
    List<String> models = dispatcher.getSupportedModels();
    assertNotNull(models);
    assertTrue(models.isEmpty());
  }

  @Test
  public void testGetSupportedModelsWithGeminiKey() {
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "fake-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));
    List<String> models = dispatcher.getSupportedModels();
    assertFalse(models.isEmpty());
    assertTrue(models.stream().anyMatch(m -> m.startsWith("gemini")));
  }

  // ── Provider routing via configured model list ───────────────────────────

  @Test
  public void testGeminiModelAppearsInListWhenGeminiKeyConfigured() {
    // Configuring the gemini key populates the gemini model list.
    // A model in that list will be routed to gemini by the reverse-lookup in resolveProvider.
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "fake-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));
    assertTrue(dispatcher.getSupportedModels().contains("gemini-2.5-flash"),
        "gemini-2.5-flash should be in the supported list when the gemini key is configured");
  }

  @Test
  public void testOpenAIModelAppearsInListWhenOpenAIKeyConfigured() {
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY, "fake-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));
    assertTrue(dispatcher.getSupportedModels().contains("gpt-4.1"),
        "gpt-4.1 should be in the supported list when the OpenAI key is configured");
  }

  @Test
  public void testAnthropicModelAppearsInListWhenAnthropicKeyConfigured() {
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_API_KEY, "fake-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));
    assertTrue(dispatcher.getSupportedModels().contains("claude-sonnet-4-6"),
        "claude-sonnet-4-6 should be in the supported list when the Anthropic key is configured");
  }

  // ── Unknown / unconfigured model rejection ───────────────────────────────

  @Test
  public void testUnknownModelThrowsNotRecognisedError() {
    // No keys configured → supportedModels is empty → any model is rejected immediately.
    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    messages.add(new LLMClient.ChatMessage("user", "hello"));

    LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "some-unknown-model", new HashMap<>()));

    assertTrue(ex.getMessage().contains("not recognised"),
        "Error should say the model is not recognised");
    assertTrue(ex.getMessage().contains("GET /api/v1/chatbot/models"),
        "Error should point the user to the models endpoint");
  }

  @Test
  public void testModelNotInListThrowsEvenWhenOtherKeysAreConfigured() {
    // Gemini key is set (gemini models are in the list), but the request asks for
    // "gpt-4.1" which is an OpenAI model — and no OpenAI key is configured.
    // resolveProvider must not fall back to gemini; it should throw.
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "fake-gemini-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));

    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    messages.add(new LLMClient.ChatMessage("user", "hello"));

    LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "gpt-4.1", new HashMap<>()));

    assertTrue(ex.getMessage().contains("not recognised"),
        "gpt-4.1 should not route to gemini just because the gemini key is configured");
  }

  // ── Explicit provider hint bypasses model list ───────────────────────────

  @Test
  public void testExplicitProviderHintRoutesCorrectly() {
    // Passing "_provider" = "openai" bypasses the supportedModels lookup entirely.
    // With no OpenAI key configured, the call fails at resolveKey — but the error
    // confirms the request was routed to the openai code path, not rejected as "not recognised".
    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    messages.add(new LLMClient.ChatMessage("user", "hello"));

    Map<String, Object> params = new HashMap<>();
    params.put("_provider", "openai");

    LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "any-model-name", params));

    assertTrue(ex.getMessage().toLowerCase().contains("openai"),
        "Error should mention openai because the explicit hint routed the call there");
    assertFalse(ex.getMessage().contains("not recognised"),
        "Explicit hint should bypass the model-list check — error must not say 'not recognised'");
  }

  @Test
  public void testExplicitProviderPrefixInModelString() {
    // "anthropic:claude-sonnet-4-6" should route to anthropic regardless of the model list.
    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    messages.add(new LLMClient.ChatMessage("user", "hello"));

    LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "anthropic:claude-sonnet-4-6", new HashMap<>()));

    assertTrue(ex.getMessage().toLowerCase().contains("anthropic"),
        "provider:model prefix should route to anthropic");
    assertFalse(ex.getMessage().contains("not recognised"),
        "Explicit prefix should bypass the model-list check");
  }
}
