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
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Direct provider for Google Gemini models.
 *
 * <p>
 * Uses the native Gemini REST API at
 * {@code generativelanguage.googleapis.com/v1beta/models/{model}:generateContent}
 * which supports all Gemini models including preview releases.
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
        return ChatbotConfigKeys.OZONE_RECON_CHATBOT_GEMINI_BASE_URL_DEFAULT;
    }

    @Override
    protected HttpRequest buildChatRequest(
            List<LLMProvider.ChatMessage> messages,
            String model, String apiKey,
            Map<String, Object> params) throws IOException {

        ObjectNode body = MAPPER.createObjectNode();

        // Native Gemini format: system goes in "systemInstruction",
        // user/assistant messages go in "contents".
        ArrayNode contentsArray = body.putArray("contents");
        for (LLMProvider.ChatMessage msg : messages) {
            if ("system".equals(msg.getRole())) {
                // System message is a top-level field in native API.
                ObjectNode sysInstruction = body.putObject("systemInstruction");
                ArrayNode sysParts = sysInstruction.putArray("parts");
                sysParts.addObject().put("text", msg.getContent());
            } else {
                ObjectNode turn = contentsArray.addObject();
                // Gemini uses "model" instead of "assistant".
                turn.put("role",
                        "assistant".equals(msg.getRole()) ? "model" : msg.getRole());
                ArrayNode parts = turn.putArray("parts");
                parts.addObject().put("text", msg.getContent());
            }
        }

        // Map standard params to Gemini's generationConfig.
        ObjectNode genConfig = body.putObject("generationConfig");
        if (params != null) {
            if (params.containsKey("max_tokens")) {
                genConfig.put("maxOutputTokens",
                        ((Number) params.get("max_tokens")).intValue());
            }
            if (params.containsKey("temperature")) {
                genConfig.put("temperature",
                        ((Number) params.get("temperature")).doubleValue());
            }
        }

        // Native API uses API key as query parameter.
        String url = getBaseUrl() + "/v1beta/models/" + model + ":generateContent"
                + "?key=" + apiKey;

        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(java.time.Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        MAPPER.writeValueAsString(body)))
                .build();
    }

    @Override
    protected LLMProvider.LLMResponse parseResponse(
            String responseBody, String model) throws LLMProvider.LLMException {
        try {
            JsonNode root = MAPPER.readTree(responseBody);

            // Native Gemini response uses "candidates" array.
            JsonNode candidates = root.get("candidates");
            if (candidates == null || !candidates.isArray()
                    || candidates.isEmpty()) {
                throw new LLMProvider.LLMException(
                        "Invalid Gemini response: no candidates found");
            }

            JsonNode firstCandidate = candidates.get(0);
            JsonNode content = firstCandidate.path("content");
            JsonNode parts = content.path("parts");

            // Concatenate all text parts (skip thoughtSignature etc.).
            StringBuilder text = new StringBuilder();
            for (JsonNode part : parts) {
                if (part.has("text")) {
                    text.append(part.get("text").asText());
                }
            }

            // Parse usage metadata.
            int promptTokens = 0;
            int completionTokens = 0;
            JsonNode usage = root.get("usageMetadata");
            if (usage != null) {
                promptTokens = usage.path("promptTokenCount").asInt(0);
                completionTokens = usage.path("candidatesTokenCount").asInt(0);
            }

            Map<String, Object> metadata = new HashMap<>();
            metadata.put("finish_reason",
                    firstCandidate.path("finishReason").asText("unknown"));
            metadata.put("response_id",
                    root.path("responseId").asText(""));
            metadata.put("model_version",
                    root.path("modelVersion").asText(model));
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
                "gemini-2.5-pro", "gemini-2.5-flash",
                "gemini-3-flash-preview", "gemini-3.1-pro-preview");
    }
}
