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

import java.util.List;
import java.util.Map;

/**
 * Interface for LLM providers.
 * Abstracts the communication with different LLM services.
 */
public interface LLMProvider {

  /**
   * Sends a chat completion request to the LLM.
   *
   * @param messages list of chat messages
   * @param model the model to use
   * @param apiKey user's API key (optional, may use system key)
   * @param parameters additional parameters (temperature, max_tokens, etc.)
   * @return the LLM response
   * @throws LLMException if the request fails
   */
  LLMResponse chatCompletion(
      List<ChatMessage> messages,
      String model,
      String apiKey,
      Map<String, Object> parameters) throws LLMException;

  /**
   * Checks if the provider is available and healthy.
   *
   * @return true if the provider is available
   */
  boolean isAvailable();

  /**
   * Gets the list of supported models.
   *
   * @return list of model names
   */
  List<String> getSupportedModels();

  /**
   * Represents a chat message.
   */
  class ChatMessage {
    private final String role; // "system", "user", "assistant"
    private final String content;

    public ChatMessage(String role, String content) {
      this.role = role;
      this.content = content;
    }

    public String getRole() {
      return role;
    }

    public String getContent() {
      return content;
    }
  }

  /**
   * Represents an LLM response.
   */
  class LLMResponse {
    private final String content;
    private final String model;
    private final int promptTokens;
    private final int completionTokens;
    private final Map<String, Object> metadata;

    public LLMResponse(String content, String model,
                       int promptTokens, int completionTokens,
                       Map<String, Object> metadata) {
      this.content = content;
      this.model = model;
      this.promptTokens = promptTokens;
      this.completionTokens = completionTokens;
      this.metadata = metadata;
    }

    public String getContent() {
      return content;
    }

    public String getModel() {
      return model;
    }

    public int getPromptTokens() {
      return promptTokens;
    }

    public int getCompletionTokens() {
      return completionTokens;
    }

    public int getTotalTokens() {
      return promptTokens + completionTokens;
    }

    public Map<String, Object> getMetadata() {
      return metadata;
    }
  }

  /**
   * Exception thrown when LLM operations fail.
   */
  class LLMException extends Exception {
    private final int statusCode;

    public LLMException(String message) {
      this(message, -1);
    }

    public LLMException(String message, int statusCode) {
      super(message);
      this.statusCode = statusCode;
    }

    public LLMException(String message, Throwable cause) {
      this(message, -1, cause);
    }

    public LLMException(String message, int statusCode, Throwable cause) {
      super(message, cause);
      this.statusCode = statusCode;
    }

    public int getStatusCode() {
      return statusCode;
    }
  }
}
