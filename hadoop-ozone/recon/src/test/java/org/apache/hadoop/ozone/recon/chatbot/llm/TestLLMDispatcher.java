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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link LangChain4jDispatcher}.
 *
 * <p>These tests exercise routing (which provider is selected for a given model name)
 * and availability checking without making any real network calls.
 * All tests that verify provider routing do so by confirming the correct provider name
 * appears in the exception message thrown when no API key is configured — this is the
 * cheapest way to prove the routing decision without mocking the LangChain4j internals.</p>
 */
public class TestLLMDispatcher {

    private OzoneConfiguration conf;
    private CredentialHelper credentialHelper;
    private LangChain4jDispatcher dispatcher;

    @BeforeEach
    public void setUp() {
        conf = new OzoneConfiguration();
        conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER, "gemini");
        credentialHelper = new CredentialHelper(conf);
        dispatcher = new LangChain4jDispatcher(conf, credentialHelper);
    }

    @Test
    public void testEmptyMessagesThrows() {
        List<LLMClient.ChatMessage> messages = new ArrayList<>();
        assertThrows(LLMClient.LLMException.class, () ->
            dispatcher.chatCompletion(messages, "gpt-4.1", null, new HashMap<>()));
    }

    @Test
    public void testNullMessagesThrows() {
        assertThrows(LLMClient.LLMException.class, () ->
            dispatcher.chatCompletion(null, "gpt-4.1", null, new HashMap<>()));
    }

    @Test
    public void testIsAvailableWithoutKeys() {
        // No API keys configured — no provider should be registered.
        assertFalse(dispatcher.isAvailable());
    }

    @Test
    public void testIsAvailableWithGeminiKey() {
        conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "test-gemini-key");
        credentialHelper = new CredentialHelper(conf);
        dispatcher = new LangChain4jDispatcher(conf, credentialHelper);
        assertTrue(dispatcher.isAvailable());
    }

    @Test
    public void testIsAvailableWithOpenAIKey() {
        conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY, "test-openai-key");
        credentialHelper = new CredentialHelper(conf);
        dispatcher = new LangChain4jDispatcher(conf, credentialHelper);
        assertTrue(dispatcher.isAvailable());
    }

    @Test
    public void testGetSupportedModelsEmptyWithoutKeys() {
        // No keys configured → no supported models returned.
        List<String> models = dispatcher.getSupportedModels();
        assertNotNull(models);
        assertTrue(models.isEmpty(),
            "Without any API keys, supported models list should be empty");
    }

    @Test
    public void testGetSupportedModelsWithGeminiKey() {
        conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY, "test-key");
        credentialHelper = new CredentialHelper(conf);
        dispatcher = new LangChain4jDispatcher(conf, credentialHelper);
        List<String> models = dispatcher.getSupportedModels();
        assertNotNull(models);
        assertFalse(models.isEmpty(), "Should return Gemini models when key is configured");
        assertTrue(models.stream().anyMatch(m -> m.startsWith("gemini")),
            "Gemini models should start with 'gemini'");
    }

    @Test
    public void testRoutingGeminiModel() {
        // A "gemini-*" model name should route to the gemini provider.
        // With no key configured the dispatcher throws mentioning "gemini".
        List<LLMClient.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMClient.ChatMessage("user", "hello"));

        LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
            dispatcher.chatCompletion(messages, "gemini-2.5-flash", null, new HashMap<>()));
        assertTrue(ex.getMessage().toLowerCase().contains("gemini"),
            "Error should mention gemini provider");
    }

    @Test
    public void testRoutingOpenAIModel() {
        // A "gpt-*" model name should route to the openai provider.
        List<LLMClient.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMClient.ChatMessage("user", "hello"));

        LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
            dispatcher.chatCompletion(messages, "gpt-4.1", null, new HashMap<>()));
        assertTrue(ex.getMessage().toLowerCase().contains("openai"),
            "Error should mention openai provider");
    }

    @Test
    public void testRoutingClaudeModel() {
        // A "claude-*" model name should route to the anthropic provider.
        List<LLMClient.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMClient.ChatMessage("user", "hello"));

        LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
            dispatcher.chatCompletion(messages, "claude-sonnet-4-6", null, new HashMap<>()));
        assertTrue(ex.getMessage().toLowerCase().contains("anthropic"),
            "Error should mention anthropic provider");
    }

    @Test
    public void testUnknownModelUsesDefaultProvider() {
        // An unrecognised model name should fall back to the configured default (gemini).
        List<LLMClient.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMClient.ChatMessage("user", "hello"));

        LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
            dispatcher.chatCompletion(messages, "some-unknown-model", null, new HashMap<>()));
        assertTrue(ex.getMessage().toLowerCase().contains("gemini"),
            "Unknown model should route to the default gemini provider");
    }

    @Test
    public void testCustomDefaultProvider() {
        // When default is changed to openai, unknown models should route there.
        conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER, "openai");
        credentialHelper = new CredentialHelper(conf);
        dispatcher = new LangChain4jDispatcher(conf, credentialHelper);

        List<LLMClient.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMClient.ChatMessage("user", "hello"));

        LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
            dispatcher.chatCompletion(messages, "some-unknown-model", null, new HashMap<>()));
        assertTrue(ex.getMessage().toLowerCase().contains("openai"),
            "Should route to openai when it is the configured default");
    }

    @Test
    public void testExplicitProviderPrefixInModelString() {
        // "anthropic:claude-sonnet-4-6" should route to anthropic regardless of model prefix.
        List<LLMClient.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMClient.ChatMessage("user", "hello"));

        LLMClient.LLMException ex = assertThrows(LLMClient.LLMException.class, () ->
            dispatcher.chatCompletion(
                messages, "anthropic:claude-sonnet-4-6", null, new HashMap<>()));
        assertTrue(ex.getMessage().toLowerCase().contains("anthropic"),
            "Explicit provider prefix should route to anthropic");
    }
}
