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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotConfigKeys;
import org.apache.hadoop.ozone.recon.chatbot.ChatbotException;
import org.apache.hadoop.ozone.recon.chatbot.agent.ChatbotAgent;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ChatbotEndpoint} — the JAX-RS REST entry point for the chatbot.
 *
 * <p>All tests instantiate {@code ChatbotEndpoint} directly (no servlet container)
 * and inject mocked {@link ChatbotAgent} and {@link LLMClient} dependencies.
 * The thread pool and queue behaviour is tested using a small pool configuration
 * and concurrent submissions from a test {@link ExecutorService}.</p>
 *
 * <p>Verified properties per test:</p>
 * <ul>
 *   <li>HTTP status code returned by {@code Response.getStatus()}.</li>
 *   <li>Response entity type and content (no stack traces, no secrets).</li>
 *   <li>Whether the mocked agent was called the expected number of times.</li>
 *   <li>Concurrency limits: queue saturation → 503, request timeout → 504.</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
public class TestChatbotEndpoint {

  @Mock
  private ChatbotAgent mockAgent;

  @Mock
  private LLMClient mockLlmClient;

  private ChatbotEndpoint endpoint;
  private OzoneConfiguration conf;

  @BeforeEach
  public void setUp() {
    conf = new OzoneConfiguration();
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ENABLED, true);
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_THREAD_POOL_SIZE, 5);
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_QUEUE_SIZE, 10);
    conf.setLong(ChatbotConfigKeys.OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS, 30_000L);
    endpoint = new ChatbotEndpoint(mockAgent, mockLlmClient, conf);
  }

  @AfterEach
  public void tearDown() {
    endpoint.shutdown();
  }

  // ── VAL-01: Input validation ───────────────────────────────────────────────

  @Test
  public void testEmptyQueryReturnsBadRequest() {
    ChatbotEndpoint.ChatRequest request = new ChatbotEndpoint.ChatRequest();
    request.setQuery("");

    Response response = endpoint.chat(request);

    assertEquals(400, response.getStatus());
    assertErrorMessagePresent(response);
  }

  @Test
  public void testNullQueryReturnsBadRequest() {
    ChatbotEndpoint.ChatRequest request = new ChatbotEndpoint.ChatRequest();
    request.setQuery(null);

    Response response = endpoint.chat(request);

    assertEquals(400, response.getStatus());
    assertErrorMessagePresent(response);
  }

  @Test
  public void testWhitespaceOnlyQueryReturnsBadRequest() {
    ChatbotEndpoint.ChatRequest request = new ChatbotEndpoint.ChatRequest();
    request.setQuery("   ");

    Response response = endpoint.chat(request);

    assertEquals(400, response.getStatus());
  }

  // ── Happy path ────────────────────────────────────────────────────────────

  @Test
  public void testSuccessfulResponseReturnsOkWithSuccessFlag() throws Exception {
    when(mockAgent.processQuery(anyString(), any(), any()))
        .thenReturn("The cluster has 5 healthy datanodes.");

    Response response = endpoint.chat(chatRequest("How many datanodes?"));

    assertEquals(200, response.getStatus());
    ChatbotEndpoint.ChatResponse body =
        (ChatbotEndpoint.ChatResponse) response.getEntity();
    assertNotNull(body);
    assertTrue(body.isSuccess(), "Response success flag should be true");
    assertNotNull(body.getResponse(), "Response text should not be null");
  }

  // ── Chatbot disabled ──────────────────────────────────────────────────────

  @Test
  public void testChatbotDisabledReturnsServiceUnavailable() {
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ENABLED, false);
    // Re-create endpoint with disabled flag
    ChatbotEndpoint disabledEndpoint =
        new ChatbotEndpoint(mockAgent, mockLlmClient, conf);
    try {
      Response response = disabledEndpoint.chat(chatRequest("test query"));
      assertEquals(503, response.getStatus());
    } finally {
      disabledEndpoint.shutdown();
    }
  }

  // ── Agent exception handling ──────────────────────────────────────────────

  @Test
  public void testAgentChatbotExceptionReturnsInternalServerError() throws Exception {
    when(mockAgent.processQuery(anyString(), any(), any()))
        .thenThrow(new ChatbotException("LLM API unavailable"));

    Response response = endpoint.chat(chatRequest("What is the state?"));

    assertEquals(500, response.getStatus());
    assertErrorMessagePresent(response);
    // Error message must not contain stack traces or internal exception details
    String errorBody = response.getEntity().toString();
    assertFalse(errorBody.contains("ChatbotException"),
        "Error response must not expose exception class names");
    assertFalse(errorBody.contains("at org.apache"),
        "Error response must not contain stack trace fragments");
  }

  // ── CON-02: Request timeout → 504 ────────────────────────────────────────

  @Test
  public void testSlowAgentExceedingTimeoutReturns504() throws Exception {
    // Configure a very short timeout (200ms)
    conf.setLong(ChatbotConfigKeys.OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS, 200L);
    ChatbotEndpoint shortTimeoutEndpoint =
        new ChatbotEndpoint(mockAgent, mockLlmClient, conf);

    try {
      // Agent sleeps much longer than the timeout
      when(mockAgent.processQuery(anyString(), any(), any()))
          .thenAnswer(inv -> {
            Thread.sleep(5_000L);
            return "done";
          });

      long start = System.currentTimeMillis();
      Response response = shortTimeoutEndpoint.chat(chatRequest("slow query"));
      long elapsed = System.currentTimeMillis() - start;

      assertEquals(504, response.getStatus());
      assertErrorMessagePresent(response);
      // The Jetty thread should have unblocked well within 2 seconds
      assertTrue(elapsed < 2_000L,
          "Endpoint should have returned 504 within 2s, but took " + elapsed + "ms");
    } finally {
      shortTimeoutEndpoint.shutdown();
    }
  }

  // ── CON-01: Queue saturation → 503 ───────────────────────────────────────

  @Test
  public void testQueueSaturationReturnsServiceUnavailable() throws Exception {
    // Pool=2, Queue=2 → capacity=4. Submitting 10 concurrent requests means
    // at least 6 should be rejected immediately with HTTP 503.
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_THREAD_POOL_SIZE, 2);
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_QUEUE_SIZE, 2);
    conf.setLong(ChatbotConfigKeys.OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS, 10_000L);

    CountDownLatch agentLatch = new CountDownLatch(1);
    ChatbotEndpoint smallEndpoint =
        new ChatbotEndpoint(mockAgent, mockLlmClient, conf);

    try {
      // All agent calls block until we release the latch, ensuring the pool stays full
      when(mockAgent.processQuery(anyString(), any(), any()))
          .thenAnswer(inv -> {
            try {
              agentLatch.await(8, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return "done";
          });

      ExecutorService testPool = Executors.newFixedThreadPool(10);
      List<Future<Response>> futures = new ArrayList<>();
      ChatbotEndpoint.ChatRequest request = chatRequest("test query");

      for (int i = 0; i < 10; i++) {
        futures.add(testPool.submit(() -> smallEndpoint.chat(request)));
      }

      // Give threads time to be submitted and queued/rejected
      Thread.sleep(300);

      // Release the latch so accepted tasks can complete
      agentLatch.countDown();

      // Collect all responses
      AtomicInteger count503 = new AtomicInteger(0);
      for (Future<Response> f : futures) {
        Response r = f.get(12, TimeUnit.SECONDS);
        if (r.getStatus() == 503) {
          count503.incrementAndGet();
        }
      }

      testPool.shutdown();
      testPool.awaitTermination(5, TimeUnit.SECONDS);

      // Pool=2 + Queue=2 = capacity 4. The remaining 6 must receive 503.
      assertTrue(count503.get() >= 6,
          "Expected >= 6 requests to get 503 due to queue saturation, but got: "
              + count503.get());
    } finally {
      agentLatch.countDown(); // Ensure tasks are not stuck if test fails early
      smallEndpoint.shutdown();
    }
  }

  // ── CON-03: Singleton — agent called N times, not re-initialized ──────────

  @Test
  public void testSingleEndpointInstanceHandlesMultipleRequestsWithoutReinit()
      throws Exception {
    when(mockAgent.processQuery(anyString(), any(), any()))
        .thenReturn("response");

    // Submit 5 sequential requests through the same endpoint instance
    for (int i = 0; i < 5; i++) {
      Response r = endpoint.chat(chatRequest("query " + i));
      assertEquals(200, r.getStatus());
    }

    // The mock agent (representing the singleton) must have been called 5 times
    // on the same instance — not a new instance per request.
    verify(mockAgent, times(5)).processQuery(anyString(), any(), any());
  }

  // ── Health endpoint ───────────────────────────────────────────────────────

  @Test
  public void testHealthEndpointReturnsEnabledStatus() {
    when(mockLlmClient.isAvailable()).thenReturn(true);

    Response response = endpoint.health();

    assertEquals(200, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertNotNull(body);
    assertTrue((Boolean) body.get("enabled"), "Health endpoint should report enabled=true");
    assertTrue((Boolean) body.get("llmClientAvailable"),
        "Health endpoint should report llmClientAvailable=true");
  }

  @Test
  public void testHealthEndpointReportsUnavailableWhenNoApiKey() {
    when(mockLlmClient.isAvailable()).thenReturn(false);

    Response response = endpoint.health();

    assertEquals(200, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertFalse((Boolean) body.get("llmClientAvailable"),
        "Health endpoint should report llmClientAvailable=false when no key is configured");
  }

  // ── Models endpoint ───────────────────────────────────────────────────────

  @Test
  public void testModelsEndpointReturnsSupportedModelList() {
    when(mockLlmClient.getSupportedModels())
        .thenReturn(Arrays.asList("gemini-2.5-flash", "gemini-2.5-pro"));

    Response response = endpoint.getSupportedModels();

    assertEquals(200, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertNotNull(body);
    assertTrue(body.containsKey("models"), "Response should contain 'models' key");
    @SuppressWarnings("unchecked")
    List<String> models = (List<String>) body.get("models");
    assertFalse(models.isEmpty(), "Model list must not be empty");
  }

  @Test
  public void testModelsEndpointReturns500OnException() {
    when(mockLlmClient.getSupportedModels())
        .thenThrow(new RuntimeException("provider error"));

    Response response = endpoint.getSupportedModels();

    assertEquals(500, response.getStatus());
    assertErrorMessagePresent(response);
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private ChatbotEndpoint.ChatRequest chatRequest(String query) {
    ChatbotEndpoint.ChatRequest req = new ChatbotEndpoint.ChatRequest();
    req.setQuery(query);
    return req;
  }

  @SuppressWarnings("unchecked")
  private void assertErrorMessagePresent(Response response) {
    Object entity = response.getEntity();
    assertNotNull(entity, "Error response entity must not be null");
    Map<String, Object> body = (Map<String, Object>) entity;
    assertTrue(body.containsKey("error"),
        "Error response must contain an 'error' key");
    assertNotNull(body.get("error"),
        "Error message must not be null");
  }
}
