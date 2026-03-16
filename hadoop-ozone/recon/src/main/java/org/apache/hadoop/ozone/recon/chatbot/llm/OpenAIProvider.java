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

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Direct provider for OpenAI models (GPT-4, GPT-4o, o1, o3, etc.).
 * Talks to {@code api.openai.com/v1/chat/completions}.
 */
public class OpenAIProvider extends DirectLLMProvider {

  public OpenAIProvider(OzoneConfiguration configuration,
                        CredentialHelper credentialHelper,
                        int timeoutMs) {
    super(configuration, credentialHelper, timeoutMs);
  }

  @Override
  public String getProviderName() {
    return "openai";
  }

  @Override
  protected String getApiKeyConfigKey() {
    return ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_API_KEY;
  }

  @Override
  protected String getBaseUrlConfigKey() {
    return ChatbotConfigKeys.OZONE_RECON_CHATBOT_OPENAI_BASE_URL;
  }

  @Override
  protected String getDefaultBaseUrl() {
    return ChatbotConfigKeys
        .OZONE_RECON_CHATBOT_OPENAI_BASE_URL_DEFAULT;
  }

  @Override
  protected HttpURLConnection buildChatRequest(
      List<LLMProvider.ChatMessage> messages,
      String model, String apiKey,
      Map<String, Object> params) throws IOException {

    ObjectNode body = buildOpenAIRequestBody(messages, model, params);
    String url = getBaseUrl() + "/v1/chat/completions";

    HttpURLConnection conn = createPostConnection(url);
    conn.setRequestProperty("Authorization", "Bearer " + apiKey);
    writeBody(conn, MAPPER.writeValueAsString(body));
    return conn;
  }

  @Override
  protected LLMProvider.LLMResponse parseResponse(
      String responseBody, String model) throws LLMProvider.LLMException {
    return parseOpenAIResponse(responseBody, model);
  }

  @Override
  public List<String> getSupportedModels() {
    return Arrays.asList(
        "gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano");
  }
}
