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
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LangChain4jDispatcher}.
 *
 * <p>All tests avoid real network calls. Provider routing is verified either via
 * {@link LangChain4jDispatcher#getSupportedModels()} or by confirming that routing
 * reaches {@code resolveKey}, which throws a clear "No API key configured" error.</p>
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

  @Test
  public void testEmptyMessagesThrows() {
    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "gpt-4.1", null, new GenParams(0.1, 1000), null));
  }

  @Test
  public void testNullMessagesThrows() {
    assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(null, "gpt-4.1", null, new GenParams(0.1, 1000), null));
  }

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

  @Test
  public void testUnknownModelFallsBackToDefaultProvider() {
    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    messages.add(new LLMClient.ChatMessage("user", "hello"));

    LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "some-unknown-model", null, new GenParams(0.1, 1000), null));

    assertTrue(ex.getMessage().toLowerCase().contains("gemini"),
        "Unknown model should fall back to default provider gemini");
  }

  @Test
  public void testUnknownModelWithGeminiKeyUsesDefaultModel() {
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "fake-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));

    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    messages.add(new LLMClient.ChatMessage("user", "hello"));

    LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "some-unknown-model", null, new GenParams(0.1, 1000), null));

    assertTrue(ex.getMessage().contains("LLM request failed"),
        "Should attempt call with default gemini model after fallback");
  }

  @Test
  public void testSupportedModelInfersOpenAiWhenOnlyGeminiKeyConfigured() {
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "fake-gemini-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));

    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    messages.add(new LLMClient.ChatMessage("user", "hello"));

    LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "gpt-4.1", null, new GenParams(0.1, 1000), null));

    assertTrue(ex.getMessage().toLowerCase().contains("openai"),
        "gpt-4.1 should route to openai even when only gemini key is configured");
  }

  @Test
  public void testExplicitProviderRoutesCorrectly() {
    // An explicit provider is honored only when that provider is configured with an API key;
    // routing never falls back to an unconfigured provider. Configure openai so the explicit
    // request is actually routed there.
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY, "fake-openai-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));

    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    messages.add(new LLMClient.ChatMessage("user", "hello"));

    LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "gpt-4.1", "openai", new GenParams(0.1, 1000), null));

    assertTrue(ex.getMessage().toLowerCase().contains("openai"),
        "Explicit configured provider should route to openai");
  }

  @Test
  public void testMismatchedProviderAndModelFallsBackToDefaults() {
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "fake-key");
    conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY, "fake-openai-key");
    dispatcher = new LangChain4jDispatcher(conf, new CredentialHelper(conf));

    List<LLMClient.ChatMessage> messages = new ArrayList<>();
    messages.add(new LLMClient.ChatMessage("user", "hello"));

    LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
        dispatcher.chatCompletion(messages, "gemini-2.5-flash", "openai", new GenParams(0.1, 1000), null));

    assertTrue(ex.getMessage().toLowerCase().contains("gemini"),
        "Mismatched pair should fall back to default gemini provider");
  }
}
