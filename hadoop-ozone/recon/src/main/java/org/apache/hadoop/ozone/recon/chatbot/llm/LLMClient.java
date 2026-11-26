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

import java.util.List;
import java.util.Map;

/**
 * LLMClient is the "Master Contract" for the whole Chatbot system.
 * <p>
 * Purpose:
 * The ChatbotAgent doesn't know (or care) if it's talking to OpenAI, Gemini, or a Local LLM.
 * It strictly relies on this interface. This interface forces every AI client to guarantee
 * that they will accept exactly the same input and return exactly the same output.
 * <p>
 * By using this contract, we can add 10 new AI models to Recon tomorrow,
 * and we will never have to edit the ChatbotAgent's code to support them!
 */
public interface LLMClient {

  /**
   * The core action: Send a conversation to an AI and wait for its answer.
   *
   * <p>When {@code tools} is non-null, the model may reply with native tool calls instead of (or in
   * addition to) text. Text-only callers (summarization, fallback) pass {@code null}.</p>
   *
   * <p>API keys are always resolved server-side via
   * {@link org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper} from
   * the Hadoop credential store or {@code ozone-site.xml}. There is no per-request
   * key parameter — all callers should be cluster admins using the shared server key.</p>
   *
   * @param messages The back-and-forth chat history so far (System Prompts, User Questions, etc.)
   * @param model    Requested model name (optional; falls back to configured default when unsupported)
   * @param provider Requested provider name (optional; falls back via routing rules when unsupported)
   * @param params   Generation settings (temperature, max tokens), applied when building the provider
   *                 model (LangChain4j 0.35.0 does not support per-request overrides on {@code ChatRequest})
   * @param tools    Tools the model may call, or {@code null} for a plain text-only completion
   * @return A standardized LLMResponse object containing the AI's final text.
   * @throws LLMException if the network fails, the API key is missing, or the provider returns an error.
   */
  LLMResponse chatCompletion(
      List<ChatMessage> messages,
      String model,
      String provider,
      GenParams params,
      List<ToolSpec> tools) throws LLMException;

  /**
   * Returns whether this client is ready to work (e.g. has an API key configured).
   */
  boolean isAvailable();

  /**
   * Asks the AI client for a list of all the different models it supports right now.
   * We use this to populate the drop-down menu in the user interface!
   */
  List<String> getSupportedModels();

  // =========================================================================
  // Data Transfer Objects (DTOs)
  // These are the standardized containers we use to pass information around.
  // =========================================================================

  /**
   * A single message in a conversation.
   * Every message needs a "role" (who is speaking: user or assistant)
   * and "content" (what they actually said).
   */
  class ChatMessage {
    private final String role;
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
   * The standardized package that every AI MUST return when it finishes thinking.
   * Instead of OpenAI returning one JSON format and Gemini returning a completely different one,
   * our background code forces them both to output this clean Java object.
   */
  class LLMResponse {

    // The actual text the AI typed out
    private final String content;

    // Which AI model specifically answered this? (e.g. "gpt-4")
    private final String model;

    // How many "words" the user asked
    private final int promptTokens;

    // How many "words" the AI answered with
    private final int completionTokens;

    // Native tool calls requested by the LLM
    private final List<ToolCallRequest> toolCalls;

    public LLMResponse(String content, String model,
                       int promptTokens, int completionTokens,
                       List<ToolCallRequest> toolCalls) {
      this.content = content;
      this.model = model;
      this.promptTokens = promptTokens;
      this.completionTokens = completionTokens;
      this.toolCalls = toolCalls;
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

    // Helps us track total costs! AI companies charge by the Total Token.
    public int getTotalTokens() {
      return promptTokens + completionTokens;
    }

    public List<ToolCallRequest> getToolCalls() {
      return toolCalls;
    }
  }

  /** Native tool definition passed to the LLM (name, description, JSON parameter schema). */
  class ToolSpec {
    private final String name;
    private final String description;
    private final Map<String, Object> parametersSchema;

    public ToolSpec(String name, String description, Map<String, Object> parametersSchema) {
      this.name = name;
      this.description = description;
      this.parametersSchema = parametersSchema;
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }

    public Map<String, Object> getParametersSchema() {
      return parametersSchema;
    }
  }

  /** One tool invocation requested by the LLM (tool name and JSON arguments). */
  class ToolCallRequest {
    private final String toolName;
    private final String argumentsJson;

    public ToolCallRequest(String toolName, String argumentsJson) {
      this.toolName = toolName;
      this.argumentsJson = argumentsJson;
    }

    public String getToolName() {
      return toolName;
    }

    public String getArgumentsJson() {
      return argumentsJson;
    }
  }

  /**
   * A standardized Error object.
   * No matter which AI crashes, we wrap their specific crash report in an LLMException
   * so the ChatbotAgent always knows how to "catch" it and show a friendly error to the user.
   */
  class LLMException extends Exception {

    public LLMException(String message) {
      super(message);
    }

    public LLMException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
