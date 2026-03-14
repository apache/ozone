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
 * Direct provider for Google Gemini models.
 *
 * <p>
 * Gemini uses a different API format than OpenAI:
 * <ul>
 * <li>System message is in a separate {@code systemInstruction}
 * field</li>
 * <li>Messages use {@code parts[].text} instead of
 * {@code content}</li>
 * <li>API key is passed as a query parameter, not a header</li>
 * <li>Response uses {@code candidates[].content.parts[].text}</li>
 * </ul>
 * </p>
 */
public class GeminiProvider extends DirectLLMProvider {

    public GeminiProvider(OzoneConfiguration configuration,
            CredentialHelper credentialHelper,
            int timeoutMs) {
        super(configuration, credentialHelper, timeoutMs);
    }

    @Override
    public String getProviderName() {
        return "gemini";
    }

    @Override
    protected String getApiKeyConfigKey() {
        return ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_API_KEY;
    }

    @Override
    protected String getBaseUrlConfigKey() {
        return ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL;
    }

    @Override
    protected String getDefaultBaseUrl() {
        return ChatbotConfigKeys
            .OZONE_RECON_CHATBOT_GEMINI_BASE_URL_DEFAULT;
    }

    @Override
    protected HttpURLConnection buildChatRequest(
            List<LLMProvider.ChatMessage> messages,
            String model, String apiKey,
            Map<String, Object> params) throws IOException {

        ObjectNode body = MAPPER.createObjectNode();

        // Handle system message separately for Gemini.
        ArrayNode contents = body.putArray("contents");
        for (LLMProvider.ChatMessage msg : messages) {
            if ("system".equals(msg.getRole())) {
                ObjectNode sysInstruction = body.putObject("systemInstruction");
                ArrayNode sysParts = sysInstruction.putArray("parts");
                sysParts.addObject().put("text", msg.getContent());
            } else {
                ObjectNode content = contents.addObject();
                String role = "assistant".equals(msg.getRole()) ? "model" : msg.getRole();
                content.put("role", role);
                ArrayNode parts = content.putArray("parts");
                parts.addObject().put("text", msg.getContent());
            }
        }

        // Map standard params to Gemini equivalents.
        ObjectNode genConfig = body.putObject("generationConfig");
        if (params != null) {
            if (params.containsKey("max_tokens")) {
                genConfig.put("maxOutputTokens",
                        ((Number) params.get("max_tokens")).intValue());
            }
            if (params.containsKey("temperature")) {
                genConfig.put("temperature", ((Number) params.get("temperature")).doubleValue());
            }
        }

        String url = getBaseUrl()
            + "/v1beta/models/" + model + ":generateContent" + "?key=" + apiKey;

        HttpURLConnection conn = createPostConnection(url);
        writeBody(conn, MAPPER.writeValueAsString(body));
        return conn;
    }

    @Override
    protected LLMProvider.LLMResponse parseResponse(
            String responseBody, String model) throws LLMProvider.LLMException {
        try {
            JsonNode root = MAPPER.readTree(responseBody);

            JsonNode candidates = root.get("candidates");
            if (candidates == null || !candidates.isArray()
                    || candidates.isEmpty()) {
                throw new LLMProvider.LLMException(
                        "Invalid Gemini response: no candidates found");
            }

            JsonNode firstCandidate = candidates.get(0);
            JsonNode content = firstCandidate.get("content");
            JsonNode parts = content != null ? content.get("parts") : null;

            StringBuilder text = new StringBuilder();
            if (parts != null && parts.isArray()) {
                for (JsonNode part : parts) {
                    if (part.has("text")) {
                        text.append(part.get("text").asText());
                    }
                }
            }

            JsonNode usageMetadata = root.get("usageMetadata");
            int promptTokens = 0;
            int completionTokens = 0;
            if (usageMetadata != null) {
                promptTokens = usageMetadata.path("promptTokenCount").asInt(0);
                completionTokens = usageMetadata.path("candidatesTokenCount").asInt(0);
            }

            Map<String, Object> metadata = new HashMap<>();
            metadata.put("finish_reason",
                    firstCandidate.path("finishReason").asText("unknown"));
            metadata.put("provider", getProviderName());

            return new LLMProvider.LLMResponse(
                    text.toString(), model, promptTokens,
                    completionTokens, metadata);

        } catch (LLMProvider.LLMException e) {
            throw e;
        } catch (Exception e) {
            throw new LLMProvider.LLMException(
                    "Failed to parse Gemini response", e);
        }
    }

    @Override
    public List<String> getSupportedModels() {
        return Arrays.asList(
                "gemini-2.5-pro",
                "gemini-2.5-flash",
                "gemini-3-flash-preview",
                "gemini-3.1-pro-preview");
    }
}
