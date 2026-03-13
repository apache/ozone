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
package org.apache.hadoop.ozone.recon.chatbot.api;

import javax.inject.Inject;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.agent.ChatbotAgent;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST API endpoint for the Recon Chatbot.
 *
 * <p>
 * API keys are managed via JCEKS (admin-configured),
 * so there are no per-user key storage endpoints.
 * </p>
 */
@Path("/chatbot")
@Produces(MediaType.APPLICATION_JSON)
public class ChatbotEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(ChatbotEndpoint.class);

  private final ChatbotAgent chatbotAgent;
  private final LLMProvider llmProvider;
  private final OzoneConfiguration configuration;

  @Inject
  public ChatbotEndpoint(ChatbotAgent chatbotAgent,
      LLMProvider llmProvider,
      OzoneConfiguration configuration) {
    this.chatbotAgent = chatbotAgent;
    this.llmProvider = llmProvider;
    this.configuration = configuration;

    LOG.info("ChatbotEndpoint initialized via Guice injection");
  }

  /**
   * Checks if the chatbot is enabled in configuration.
   */
  private boolean isChatbotEnabled() {
    return configuration.getBoolean(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_ENABLED,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_ENABLED_DEFAULT);
  }

  /**
   * Health check endpoint.
   */
  @GET
  @Path("/health")
  public Response health() {
    Map<String, Object> response = new HashMap<>();
    boolean enabled = isChatbotEnabled();
    response.put("enabled", enabled);
    response.put("llmProviderAvailable",
        enabled && llmProvider != null && llmProvider.isAvailable());
    return Response.ok(response).build();
  }

  /**
   * Chat endpoint - processes a user query.
   */
  @POST
  @Path("/chat")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response chat(ChatRequest request) {
    if (!isChatbotEnabled()) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(Map.of("error", "Chatbot service is not enabled"))
          .build();
    }

    if (request.getQuery() == null || request.getQuery().trim().isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(Map.of("error", "Query cannot be empty"))
          .build();
    }

    try {
      LOG.info("Chat request: userId={}, model={}, provider={}",
          sanitizeUserId(request.getUserId()),
          request.getModel() == null ? "default" : request.getModel(),
          request.getProvider() == null ? "auto" : request.getProvider());

      // Process the query — API key resolved from JCEKS by the provider.
      String response = chatbotAgent.processQuery(
          request.getQuery(),
          request.getModel(),
          request.getProvider(),
          null);

      ChatResponse chatResponse = new ChatResponse();
      chatResponse.setResponse(response);
      chatResponse.setSuccess(true);

      return Response.ok(chatResponse).build();

    } catch (Exception e) {
      LOG.error("Error processing chat request", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Map.of("error", e.getMessage()))
          .build();
    }
  }

  /**
   * List supported models.
   */
  @GET
  @Path("/models")
  public Response getSupportedModels() {
    if (!isChatbotEnabled()) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(Map.of("error", "Chatbot service is not enabled"))
          .build();
    }

    try {
      List<String> models = llmProvider.getSupportedModels();
      return Response.ok(Map.of("models", models)).build();
    } catch (Exception e) {
      LOG.error("Error fetching supported models", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Map.of("error", "Failed to fetch models"))
          .build();
    }
  }

  /**
   * Masks user ID for safe logging while preserving traceability.
   */
  private String sanitizeUserId(String userId) {
    if (userId == null || userId.isEmpty()) {
      return "none";
    }
    int atIndex = userId.indexOf('@');
    if (atIndex > 0 && atIndex < userId.length() - 1) {
      String local = userId.substring(0, atIndex);
      String domain = userId.substring(atIndex + 1);
      String maskedLocal = local.length() <= 2 ? "**"
          : local.substring(0, 2) + "***";
      return maskedLocal + "@" + domain;
    }
    if (userId.length() <= 4) {
      return "****";
    }
    return userId.substring(0, 2) + "***" +
        userId.substring(userId.length() - 2);
  }

  /**
   * Chat request DTO.
   */
  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
  public static class ChatRequest {
    private String query;
    private String model;
    private String provider;
    private String userId;

    public String getQuery() {
      return query;
    }

    public void setQuery(String query) {
      this.query = query;
    }

    public String getModel() {
      return model;
    }

    public void setModel(String model) {
      this.model = model;
    }

    public String getProvider() {
      return provider;
    }

    public void setProvider(String provider) {
      this.provider = provider;
    }

    public String getUserId() {
      return userId;
    }

    public void setUserId(String userId) {
      this.userId = userId;
    }
  }

  /**
   * Chat response DTO.
   */
  @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
  public static class ChatResponse {
    private String response;
    private boolean success;

    public String getResponse() {
      return response;
    }

    public void setResponse(String response) {
      this.response = response;
    }

    public boolean isSuccess() {
      return success;
    }

    public void setSuccess(boolean success) {
      this.success = success;
    }
  }
}
