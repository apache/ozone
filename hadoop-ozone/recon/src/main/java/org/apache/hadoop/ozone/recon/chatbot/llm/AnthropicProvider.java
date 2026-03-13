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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Direct provider for Anthropic Claude models.
 *
 * <p>
 * Anthropic uses a different API format:
 * <ul>
 * <li>API key in {@code x-api-key} header (not Authorization:
 * Bearer)</li>
 * <li>Requires {@code anthropic-version} header</li>
 * <li>System message is a top-level parameter, not in the messages
 * array</li>
 * <li>Response uses content blocks instead of choices</li>
 * </ul>
 * </p>
 */
public class AnthropicProvider extends DirectLLMProvider {

    private static final String ANTHROPIC_VERSION = "2023-06-01";
    private static final String ANTHROPIC_BETA_CONTEXT = "context-1m-2025-08-07";

    public AnthropicProvider(OzoneConfiguration configuration,
            CredentialHelper credentialHelper,
            int timeoutMs) {
        super(configuration, credentialHelper, timeoutMs);
    }

    @Override
    public String getProviderName() {
        return "anthropic";
    }

    @Override
    protected String getApiKeyConfigKey() {
        return ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_API_KEY;
    }

    @Override
    protected String getBaseUrlConfigKey() {
        return ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_BASE_URL;
    }

    @Override
    protected String getDefaultBaseUrl() {
        return ChatbotConfigKeys.OZONE_RECON_CHATBOT_ANTHROPIC_BASE_URL_DEFAULT;
    }

    @Override
    protected HttpURLConnection buildChatRequest(
            List<LLMProvider.ChatMessage> messages,
            String model, String apiKey,
            Map<String, Object> params) throws IOException {

        ObjectNode body = MAPPER.createObjectNode();
        body.put("model", model);

        // Anthropic puts system as a top-level field, not in messages.
        ArrayNode messagesArray = body.putArray("messages");
        for (LLMProvider.ChatMessage msg : messages) {
            if ("system".equals(msg.getRole())) {
                body.put("system", msg.getContent());
            } else {
                ObjectNode m = messagesArray.addObject();
                m.put("role", msg.getRole());
                m.put("content", msg.getContent());
            }
        }

        // Map standard params to Anthropic equivalents.
        if (params != null) {
            if (params.containsKey("max_tokens")) {
                body.put("max_tokens", ((Number) params.get("max_tokens")).intValue());
            } else {
                body.put("max_tokens", 4096); // Anthropic requires this field.
            }
            if (params.containsKey("temperature")) {
                body.put("temperature",
                        ((Number) params.get("temperature")).doubleValue());
            }
        } else {
            body.put("max_tokens", 4096);
        }

        String url = getBaseUrl() + "/v1/messages";

        HttpURLConnection conn = createPostConnection(url);
        conn.setRequestProperty("x-api-key", apiKey);
        conn.setRequestProperty("anthropic-version", ANTHROPIC_VERSION);
        conn.setRequestProperty("anthropic-beta", ANTHROPIC_BETA_CONTEXT);
        writeBody(conn, MAPPER.writeValueAsString(body));
        return conn;
    }

    @Override
    protected LLMProvider.LLMResponse parseResponse(
            String responseBody, String model) throws LLMProvider.LLMException {
        try {
            JsonNode root = MAPPER.readTree(responseBody);

            // Anthropic returns content blocks.
            JsonNode content = root.get("content");
            if (content == null || !content.isArray() || content.isEmpty()) {
                throw new LLMProvider.LLMException(
                        "Invalid Anthropic response: no content blocks found");
            }

            // Concatenate all text blocks.
            StringBuilder text = new StringBuilder();
            for (JsonNode block : content) {
                if ("text".equals(block.path("type").asText())) {
                    text.append(block.path("text").asText());
                }
            }

            int inputTokens = root.path("usage").path("input_tokens").asInt(0);
            int outputTokens = root.path("usage").path("output_tokens").asInt(0);

            Map<String, Object> metadata = new HashMap<>();
            metadata.put("finish_reason",
                    root.path("stop_reason").asText("unknown"));
            metadata.put("response_id", root.path("id").asText(""));
            metadata.put("provider", getProviderName());

            return new LLMProvider.LLMResponse(
                    text.toString(), model, inputTokens, outputTokens, metadata);
        } catch (LLMProvider.LLMException e) {
            throw e;
        } catch (Exception e) {
            throw new LLMProvider.LLMException(
                    "Failed to parse Anthropic response", e);
        }
    }

    @Override
    public List<String> getSupportedModels() {
        return Arrays.asList(
                "claude-opus-4-6", "claude-sonnet-4-6");
    }
}
