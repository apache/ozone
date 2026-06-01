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

package org.apache.hadoop.ozone.recon.chatbot.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.agent.ChatbotAgent;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST API endpoint for the Recon Chatbot.
 *
 * <p>
 * API keys are managed via JCEKS (admin-configured),
 * so there are no per-user key storage endpoints.
 * </p>
 */
@Singleton
@Path("/chatbot")
@Produces(MediaType.APPLICATION_JSON)
public class ChatbotEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(ChatbotEndpoint.class);

  private final ChatbotAgent chatbotAgent;
  private final LLMClient llmClient;
  private final OzoneConfiguration configuration;

  /**
   * Dedicated thread pool for chatbot requests.
   *
   * <p>Each chatbot query is offloaded to this pool and the Jetty thread blocks on
   * {@link Future#get} with the configured request timeout. This limits concurrent
   * chatbot occupancy of Jetty threads to {@code poolSize} (default 5) rather than
   * allowing unlimited blocking. Requests beyond {@code poolSize + maxQueueSize}
   * are rejected immediately with HTTP 503.</p>
   *
   * <p>Note: JAX-RS {@code @Suspended AsyncResponse} requires Servlet 3.x async
   * support which is not enabled in this container; the synchronous Future approach
   * is used instead.</p>
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
   * Chat endpoint - processes a user query.
   *
   * <p>The work is submitted to a dedicated bounded thread pool and the Jetty thread
   * blocks on {@link Future#get} with the configured request timeout. This caps
   * concurrent chatbot occupancy of Jetty threads to the pool size (default 5).
   * Requests beyond pool + queue capacity receive HTTP 503 immediately.</p>
   */
  @POST
  @Path("/chat")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response chat(ChatRequest request) {

    if (!isChatbotEnabled()) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(Collections.singletonMap("error", "Chatbot service is not enabled"))
          .build();
    }

    if (StringUtils.isBlank(request.getQuery())) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(Collections.singletonMap("error", "Query cannot be empty"))
          .build();
    }

    long requestTimeoutMs = configuration.getLong(
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS,
        ChatbotConfigKeys.OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS_DEFAULT);

    LOG.info("Chat request received: userId={}, model={}, provider={}",
        sanitizeUserId(request.getUserId()),
        request.getModel() == null ? "default" : request.getModel(),
        request.getProvider() == null ? "auto" : request.getProvider());

    // Submit chatbot work to the dedicated pool and block the Jetty thread with
    // a hard timeout. At most poolSize Jetty threads are ever blocked on chatbot
    // work; requests beyond pool+queue capacity are rejected immediately.
    Future<String> future;
    try {
      future = chatbotExecutor.submit(() ->
          chatbotAgent.processQuery(
              request.getQuery(),
              request.getModel(),
              request.getProvider()));
    } catch (RejectedExecutionException e) {
      LOG.warn("Chatbot request rejected — thread pool and queue are full");
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(Collections.singletonMap("error",
              "The chatbot is currently handling too many requests. " +
                  "Please try again in a moment."))
          .build();
    }

    try {
      String result = future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
      ChatResponse chatResponse = new ChatResponse();
      chatResponse.setResponse(result);
      chatResponse.setSuccess(true);
      return Response.ok(chatResponse).build();

    } catch (TimeoutException e) {
      future.cancel(true);
      LOG.warn("Chatbot request timed out after {}ms", requestTimeoutMs);
      return Response.status(Response.Status.GATEWAY_TIMEOUT)
          .entity(Collections.singletonMap("error",
              "The chatbot request timed out. The LLM or Recon API took too long " +
                  "to respond. Please try again or use a different model."))
          .build();

    } catch (ExecutionException e) {
      LOG.error("Chatbot query processing failed", e.getCause());
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Collections.singletonMap("error",
              "An error occurred processing your request."))
          .build();

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(Collections.singletonMap("error",
              "Request was interrupted. Please try again."))
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
  @JsonIgnoreProperties(ignoreUnknown = true)
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
  @JsonIgnoreProperties(ignoreUnknown = true)
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
