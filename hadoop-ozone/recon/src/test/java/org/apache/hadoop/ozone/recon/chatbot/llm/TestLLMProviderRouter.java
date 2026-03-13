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
 * Tests for {@link LLMProviderRouter}.
 */
public class TestLLMProviderRouter {

    private OzoneConfiguration conf;
    private CredentialHelper credentialHelper;
    private LLMProviderRouter router;

    @BeforeEach
    public void setUp() {
        conf = new OzoneConfiguration();
        // Set Gemini as default provider.
        conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER, "gemini");
        credentialHelper = new CredentialHelper(conf);
        router = new LLMProviderRouter(conf, credentialHelper);
    }

    @Test
    public void testEmptyMessagesThrows() {
        List<LLMProvider.ChatMessage> messages = new ArrayList<>();
        assertThrows(LLMProvider.LLMException.class, () -> {
            router.chatCompletion(messages, "gpt-4", null, new HashMap<>());
        });
    }

    @Test
    public void testNullMessagesThrows() {
        assertThrows(LLMProvider.LLMException.class, () -> {
            router.chatCompletion(null, "gpt-4", null, new HashMap<>());
        });
    }

    @Test
    public void testGetSupportedModelsNotEmpty() {
        List<String> models = router.getSupportedModels();
        assertNotNull(models);
        // Even without keys, should return default provider's models.
        assertFalse(models.isEmpty());
    }

    @Test
    public void testIsAvailableWithoutKeys() {
        // No API keys configured, so should be unavailable.
        assertFalse(router.isAvailable());
    }

    @Test
    public void testIsAvailableWithKey() {
        conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY,
                "test-key");
        // Recreate helper and router with new config.
        credentialHelper = new CredentialHelper(conf);
        router = new LLMProviderRouter(conf, credentialHelper);
        assertTrue(router.isAvailable());
    }

    @Test
    public void testRoutingGeminiModel() {
        // Verify gemini model routes to gemini provider by checking
        // that chatCompletion throws LLMException about missing key
        // (not about unknown provider).
        List<LLMProvider.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMProvider.ChatMessage("user", "hello"));

        LLMProvider.LLMException ex = assertThrows(
                LLMProvider.LLMException.class,
                () -> router.chatCompletion(
                        messages, "gemini-2.0-flash", null, new HashMap<>()));
        assertTrue(ex.getMessage().contains("gemini"),
                "Error should mention gemini provider");
    }

    @Test
    public void testRoutingOpenAIModel() {
        List<LLMProvider.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMProvider.ChatMessage("user", "hello"));

        LLMProvider.LLMException ex = assertThrows(
                LLMProvider.LLMException.class,
                () -> router.chatCompletion(
                        messages, "gpt-4", null, new HashMap<>()));
        assertTrue(ex.getMessage().contains("openai"),
                "Error should mention openai provider");
    }

    @Test
    public void testRoutingClaudeModel() {
        List<LLMProvider.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMProvider.ChatMessage("user", "hello"));

        LLMProvider.LLMException ex = assertThrows(
                LLMProvider.LLMException.class,
                () -> router.chatCompletion(
                        messages, "claude-3-sonnet-20240229", null, new HashMap<>()));
        assertTrue(ex.getMessage().contains("anthropic"),
                "Error should mention anthropic provider");
    }

    @Test
    public void testUnknownModelUsesDefault() {
        List<LLMProvider.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMProvider.ChatMessage("user", "hello"));

        // Unknown model should route to the default (gemini).
        LLMProvider.LLMException ex = assertThrows(
                LLMProvider.LLMException.class,
                () -> router.chatCompletion(
                        messages, "some-unknown-model", null, new HashMap<>()));
        assertTrue(ex.getMessage().contains("gemini"),
                "Unknown model should route to default gemini provider");
    }

    @Test
    public void testCustomDefaultProvider() {
        conf.set(ChatbotConfigKeys.OZONE_RECON_CHATBOT_PROVIDER, "openai");
        credentialHelper = new CredentialHelper(conf);
        router = new LLMProviderRouter(conf, credentialHelper);

        List<LLMProvider.ChatMessage> messages = new ArrayList<>();
        messages.add(new LLMProvider.ChatMessage("user", "hello"));

        LLMProvider.LLMException ex = assertThrows(
                LLMProvider.LLMException.class,
                () -> router.chatCompletion(
                        messages, "some-unknown-model", null, new HashMap<>()));
        assertTrue(ex.getMessage().contains("openai"),
                "Should route to openai (custom default)");
    }
}
