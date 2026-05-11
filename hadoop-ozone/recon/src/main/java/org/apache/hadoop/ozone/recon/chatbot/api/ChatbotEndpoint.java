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
import org.apache.hadoop.ozone.recon.chatbot.ChatbotException;
import org.apache.hadoop.ozone.recon.chatbot.agent.ChatbotAgent;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;

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
  private final LLMClient llmClient;
  private final OzoneConfiguration configuration;

  /**
   * Dedicated thread pool for chatbot requests. Each query blocks a thread for the full
   * round-trip duration (up to 2 LLM calls + up to 5 Recon API calls). Using a separate
   * pool keeps Jetty's main thread pool free so other Recon UI pages remain responsive
   * even when chatbot calls are slow.
   *
   * <p>The pool uses a bounded {@link ArrayBlockingQueue}. When all threads are busy and
   * the queue is full, new requests are rejected immediately with HTTP 503 rather than
   * queuing indefinitely. This prevents unbounded memory growth under sustained load.</p>
   *
   * <p>Pool size: {@link ChatbotConfigKeys#OZONE_RECON_CHATBOT_THREAD_POOL_SIZE}<br>
   * Max queue depth: {@link ChatbotConfigKeys#OZONE_RECON_CHATBOT_MAX_QUEUE_SIZE}</p>
   */
  private final ExecutorService chatbotExecutor;

  @Inject
  public ChatbotEndpoint(ChatbotAgent chatbotAgent,
      LLMClient llmClient,
      OzoneConfiguration configuration) {
    this.chatbotAgent = chatbotAgent;
    this.llmClient = llmClient;
    this.configuration = configuration;

    int poolSize = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_THREAD_POOL_SIZE,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_THREAD_POOL_SIZE_DEFAULT);
    int maxQueueSize = configuration.getInt(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_QUEUE_SIZE,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_QUEUE_SIZE_DEFAULT);

    // AbortPolicy (the default) throws RejectedExecutionException when the queue
    // is full, which we catch in chat() and convert to a 503 response.
    this.chatbotExecutor = new ThreadPoolExecutor(
        poolSize, poolSize,
        0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(maxQueueSize));

    LOG.info("ChatbotEndpoint initialized: threadPoolSize={}, maxQueueSize={}",
        poolSize, maxQueueSize);
  }

  /**
   * Shuts down the chatbot thread pool gracefully on Recon process stop.
   * Waits up to 30 seconds for in-flight queries to complete before forcing shutdown.
   */
  @PreDestroy
  public void shutdown() {
    LOG.info("Shutting down chatbot executor");
    chatbotExecutor.shutdown();
    try {
      if (!chatbotExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("Chatbot executor did not terminate within 30s — forcing shutdown");
        chatbotExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      chatbotExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns whether the chatbot is enabled. Delegates to
   * {@link ChatbotConfigKeys#isChatbotEnabled(OzoneConfiguration)} so the
   * check is consistent with the Guice module installation guard in
   * {@code ReconControllerModule}.
   */
  private boolean isChatbotEnabled() {
    return ChatbotConfigKeys.isChatbotEnabled(configuration);
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
    response.put("llmClientAvailable",
        enabled && llmClient != null && llmClient.isAvailable());
    return Response.ok(response).build();
  }

  /**
   * Chat endpoint - processes a user query asynchronously.
   *
   * <p>The request is handed off immediately to a dedicated thread pool so that Jetty's
   * main thread is released right away. This prevents slow chatbot calls (which can take
   * several minutes in the worst case — 2 LLM calls + up to 5 Recon API calls) from
   * exhausting Jetty's thread pool and blocking other Recon UI pages.</p>
   *
   * <p>JAX-RS delivers the response to the client via {@link AsyncResponse#resume} once
   * the chatbot thread finishes.</p>
   */
  @POST
  @Path("/chat")
  @Consumes(MediaType.APPLICATION_JSON)
  public void chat(ChatRequest request, @Suspended AsyncResponse asyncResponse) {

    // Safety check 1: If chatbot is disabled, return immediately — no need to queue.
    if (!isChatbotEnabled()) {
      asyncResponse.resume(Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(Collections.singletonMap("error", "Chatbot service is not enabled"))
          .build());
      return;
    }

    // Safety check 2: Validate the query before queuing.
    if (request.getQuery() == null || request.getQuery().trim().isEmpty()) {
      asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST)
          .entity(Collections.singletonMap("error", "Query cannot be empty"))
          .build());
      return;
    }

    LOG.info("Chat request queued: userId={}, model={}, provider={}",
        sanitizeUserId(request.getUserId()),
        request.getModel() == null ? "default" : request.getModel(),
        request.getProvider() == null ? "auto" : request.getProvider());

    // Set a wall-clock timeout on the client connection. If the chatbot thread
    // has not called resume() within this window, JAX-RS fires the timeout handler
    // which returns 504 to the client and interrupts the worker thread.
    long requestTimeoutMs = configuration.getLong(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS_DEFAULT);
    asyncResponse.setTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS);

    // Submit to the dedicated chatbot thread pool — Jetty thread is now free.
    // RejectedExecutionException is thrown when all threads are busy AND the
    // bounded queue is full, which we convert to a 503.
    try {
      Future<?> future = chatbotExecutor.submit(() -> {
        try {
          String response = chatbotAgent.processQuery(
              request.getQuery(),
              request.getModel(),
              request.getProvider());

          ChatResponse chatResponse = new ChatResponse();
          chatResponse.setResponse(response);
          chatResponse.setSuccess(true);
          asyncResponse.resume(Response.ok(chatResponse).build());

        } catch (ChatbotException e) {
          LOG.error("Chatbot query processing failed", e);
          asyncResponse.resume(
              Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                  .entity(Collections.singletonMap("error",
                      "An error occurred processing your request."))
                  .build());
        }
      });

      // If the request times out, send 504 to the client and interrupt the
      // worker thread so it stops waiting on any blocked LLM or HTTP call.
      asyncResponse.setTimeoutHandler(ar -> {
        LOG.warn("Chatbot request timed out after {}ms — cancelling worker thread",
            requestTimeoutMs);
        future.cancel(true);
        ar.resume(Response.status(Response.Status.GATEWAY_TIMEOUT)
            .entity(Collections.singletonMap("error",
                "The chatbot request timed out. The LLM or Recon API took too long " +
                "to respond. Please try again or use a faster model."))
            .build());
      });

    } catch (RejectedExecutionException e) {
      LOG.warn("Chatbot request rejected — thread pool and queue are full");
      asyncResponse.resume(
          Response.status(Response.Status.SERVICE_UNAVAILABLE)
              .entity(Collections.singletonMap("error",
                  "The chatbot is currently handling too many requests. " +
                  "Please try again in a moment."))
              .build());
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
          .entity(Collections.singletonMap("error", "Chatbot service is not enabled"))
          .build();
    }

    try {
      List<String> models = llmClient.getSupportedModels();
      return Response.ok(Collections.singletonMap("models", models)).build();
    } catch (Exception e) {
      LOG.error("Error fetching supported models", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Collections.singletonMap("error", "Failed to fetch models"))
          .build();
    }
  }

  /**
   * Helper function: Masks user ID for safe logging.
   * E.g., turns "admin@example.com" into "ad***@example.com"
   * This is important so we don't leak user identities in system logs.
   */
  private String sanitizeUserId(String userId) {
    if (userId == null || userId.isEmpty()) {
      return "none";
    }
    int atIndex = userId.indexOf('@');
    // If it's an email address...
    if (atIndex > 0 && atIndex < userId.length() - 1) {
      String local = userId.substring(0, atIndex);
      String domain = userId.substring(atIndex + 1);
      String maskedLocal = local.length() <= 2 ? "**"
          : local.substring(0, 2) + "***";
      return maskedLocal + "@" + domain;
    }

    // If it's just a short username
    if (userId.length() <= 4) {
      return "****";
    }

    // If it's a longer username
    return userId.substring(0, 2) + "***" +
        userId.substring(userId.length() - 2);
  }

  // =========================================================================
  // Data Transfer Objects (DTOs)
  // These are simple classes that translate JSON into Java objects and vice versa.
  // =========================================================================
  /**
   * Chat request DTO. (This maps to the JSON we send in our Curl command)
   * The JsonIgnoreProperties annotation tells the JSON parser not to crash
   * if the user sends an extra field we aren't expecting.
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
   * Chat response DTO. (This maps to the JSON we send BACK to the user)
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
