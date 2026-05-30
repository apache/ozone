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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.Response;
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

/**
 * Tests the HTTP contract and REST layer of the Chatbot API.
 *
 * <p><b>Lifecycle Phase:</b> Pre-LLM (Phase 0). Tests the entry point before any LLM calls.</p>
 *
 * <p><b>Key scenarios tested:</b></p>
 * <ul>
 *   <li><b>HTTP Status Codes:</b> Handling 200 OK, 400 Bad Request, and 500 Internal Server Error.</li>
 *   <li><b>Feature Toggles:</b> Returning 503 when the chatbot is disabled.</li>
 *   <li><b>Concurrency:</b> Rejecting excess requests with 429 Too Many Requests.</li>
 *   <li><b>Timeouts:</b> Aborting requests that exceed the configured timeout.</li>
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
    Response response = endpoint.chat(chatRequest(""));

    assertEquals(400, response.getStatus());
    assertExactErrorMessage(response, "Query cannot be empty");
  }

  @Test
  public void testNullQueryReturnsBadRequest() {
    Response response = endpoint.chat(chatRequest(null));

    assertEquals(400, response.getStatus());
    assertExactErrorMessage(response, "Query cannot be empty");
  }

  @Test
  public void testWhitespaceOnlyQueryReturnsBadRequest() {
    Response response = endpoint.chat(chatRequest("   "));

    assertEquals(400, response.getStatus());
    assertExactErrorMessage(response, "Query cannot be empty");
  }

  // ── Happy path — data answer ───────────────────────────────────────────────

  @Test
  public void testSuccessfulResponseReturnsHttp200WithSuccessTrue() throws Exception {
    when(mockAgent.processQuery(anyString(), any(), any()))
        .thenReturn("The cluster has 5 healthy datanodes.");

    Response response = endpoint.chat(chatRequest("How many datanodes?"));

    assertEquals(200, response.getStatus());
    ChatbotEndpoint.ChatResponse body =
        (ChatbotEndpoint.ChatResponse) response.getEntity();
    assertNotNull(body);
    assertTrue(body.isSuccess(), "success flag must be true on HTTP 200");
    assertEquals("The cluster has 5 healthy datanodes.", body.getResponse());
  }

  // ── Happy path — fallback answer (HTTP 200, not an error) ─────────────────

  @Test
  public void testFallbackResponseReturnsHttp200WithSuccessTrue() throws Exception {
    // Fallback text is what the agent returns when the LLM cannot map the query
    // to any Recon API. It is still a successful chatbot response at the HTTP level.
    String fallbackText =
        "I can only answer questions about Ozone Recon cluster data such as " +
            "containers, datanodes, pipelines, keys, volumes, and cluster state.";
    when(mockAgent.processQuery(anyString(), any(), any()))
        .thenReturn(fallbackText);

    Response response = endpoint.chat(chatRequest("What is the weather in London?"));

    assertEquals(200, response.getStatus());
    ChatbotEndpoint.ChatResponse body =
        (ChatbotEndpoint.ChatResponse) response.getEntity();
    assertNotNull(body);
    assertTrue(body.isSuccess(),
        "Fallback response must still return success=true at the HTTP level");
    assertNotNull(body.getResponse(), "Fallback response text must not be null");
  }

  // ── Chatbot disabled ───────────────────────────────────────────────────────

  @Test
  public void testChatbotDisabledOnChatReturnsServiceUnavailable() {
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ENABLED, false);
    ChatbotEndpoint disabledEndpoint =
        new ChatbotEndpoint(mockAgent, mockLlmClient, conf);
    try {
      Response response = disabledEndpoint.chat(chatRequest("test query"));

      assertEquals(503, response.getStatus());
      assertExactErrorMessage(response, "Chatbot service is not enabled");
    } finally {
      disabledEndpoint.shutdown();
    }
  }

  @Test
  public void testChatbotDisabledOnModelsEndpointReturns503() {
    conf.setBoolean(ChatbotConfigKeys.OZONE_RECON_CHATBOT_ENABLED, false);
    ChatbotEndpoint disabledEndpoint =
        new ChatbotEndpoint(mockAgent, mockLlmClient, conf);
    try {
      Response response = disabledEndpoint.getSupportedModels();

      assertEquals(503, response.getStatus());
      assertExactErrorMessage(response, "Chatbot service is not enabled");
    } finally {
      disabledEndpoint.shutdown();
    }
  }

  // ── Agent exception handling ───────────────────────────────────────────────

  @Test
  public void testAgentChatbotExceptionReturns500WithGenericMessage() throws Exception {
    when(mockAgent.processQuery(anyString(), any(), any()))
        .thenThrow(new ChatbotException("LLM API unavailable — rate limit hit"));

    Response response = endpoint.chat(chatRequest("What is the state?"));

    assertEquals(500, response.getStatus());
    // Client must get the generic message, not the internal exception detail
    assertExactErrorMessage(response, "An error occurred processing your request.");
    // Confirm no internal information leaks into the body
    String errorBody = response.getEntity().toString();
    assertFalse(errorBody.contains("ChatbotException"),
        "Exception class name must not be exposed to the client");
    assertFalse(errorBody.contains("at org.apache"),
        "Stack trace must not be exposed to the client");
    assertFalse(errorBody.contains("rate limit"),
        "Internal error detail must not be exposed to the client");
  }

  // ── CON-02: Request timeout → 504 ─────────────────────────────────────────

  @Test
  public void testSlowAgentExceedingTimeoutReturns504() throws Exception {
    conf.setLong(ChatbotConfigKeys.OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS, 200L);
    ChatbotEndpoint shortTimeoutEndpoint =
        new ChatbotEndpoint(mockAgent, mockLlmClient, conf);

    try {
      when(mockAgent.processQuery(anyString(), any(), any()))
          .thenAnswer(inv -> {
            Thread.sleep(5_000L);
            return "done";
          });

      long start = System.currentTimeMillis();
      Response response = shortTimeoutEndpoint.chat(chatRequest("slow query"));
      long elapsed = System.currentTimeMillis() - start;

      assertEquals(504, response.getStatus());
      // Error message must mention timeout so the user knows what happened
      assertErrorMessageContains(response, "timed out");
      assertTrue(elapsed < 2_000L,
          "Endpoint should have unblocked within 2s but took " + elapsed + "ms");
    } finally {
      shortTimeoutEndpoint.shutdown();
    }
  }

  // ── CON-01: Queue saturation → 503 ────────────────────────────────────────

  @Test
  public void testQueueSaturationReturnsServiceUnavailable() throws Exception {
    // Pool=2, Queue=2 → total capacity 4.
    // Send 10 concurrent requests: at least 6 must be rejected immediately with 503.
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_THREAD_POOL_SIZE, 2);
    conf.setInt(ChatbotConfigKeys.OZONE_RECON_CHATBOT_MAX_QUEUE_SIZE, 2);
    conf.setLong(ChatbotConfigKeys.OZONE_RECON_CHATBOT_REQUEST_TIMEOUT_MS, 10_000L);

    CountDownLatch agentLatch = new CountDownLatch(1);
    ChatbotEndpoint smallEndpoint =
        new ChatbotEndpoint(mockAgent, mockLlmClient, conf);
    AtomicReference<String> capturedQueueErrorMessage = new AtomicReference<>();

    try {
      when(mockAgent.processQuery(anyString(), any(), any()))
          .thenAnswer(inv -> {
            boolean awaited = agentLatch.await(8, TimeUnit.SECONDS);
            if (!awaited) {
              throw new RuntimeException("Latch timed out waiting for agent");
            }
            return "done";
          });

      ExecutorService testPool = Executors.newFixedThreadPool(10);
      List<Future<Response>> futures = new ArrayList<>();

      for (int i = 0; i < 10; i++) {
        futures.add(testPool.submit(() -> smallEndpoint.chat(chatRequest("query"))));
      }

      Thread.sleep(300);
      agentLatch.countDown();

      AtomicInteger count503 = new AtomicInteger(0);
      for (Future<Response> f : futures) {
        Response r = f.get(12, TimeUnit.SECONDS);
        if (r.getStatus() == 503) {
          count503.incrementAndGet();
          // Capture error message from first 503 to assert the exact text
          if (capturedQueueErrorMessage.get() == null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = (Map<String, Object>) r.getEntity();
            if (body != null && body.get("error") != null) {
              capturedQueueErrorMessage.set(body.get("error").toString());
            }
          }
        }
      }

      testPool.shutdown();
      testPool.awaitTermination(5, TimeUnit.SECONDS);

      assertTrue(count503.get() >= 6,
          "Expected >=6 requests rejected with 503, got: " + count503.get());
      // Verify the exact queue-full error message is returned
      assertNotNull(capturedQueueErrorMessage.get(),
          "A 503 queue-full response must contain an error message");
      assertTrue(
          capturedQueueErrorMessage.get().contains("too many requests"),
          "Queue-full error should say 'too many requests', got: "
              + capturedQueueErrorMessage.get());
    } finally {
      agentLatch.countDown();
      smallEndpoint.shutdown();
    }
  }

  // ── CON-03: Singleton — same instance handles all requests ────────────────

  @Test
  public void testSingleEndpointInstanceHandlesMultipleRequestsWithoutReinit()
      throws Exception {
    when(mockAgent.processQuery(anyString(), any(), any()))
        .thenReturn("response");

    for (int i = 0; i < 5; i++) {
      assertEquals(200, endpoint.chat(chatRequest("query " + i)).getStatus());
    }

    verify(mockAgent, times(5)).processQuery(anyString(), any(), any());
  }

  // ── Health endpoint ────────────────────────────────────────────────────────

  @Test
  public void testHealthEndpointReturnsEnabledTrue() {
    when(mockLlmClient.isAvailable()).thenReturn(true);

    Response response = endpoint.health();

    assertEquals(200, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertTrue((Boolean) body.get("enabled"));
    assertTrue((Boolean) body.get("llmClientAvailable"));
  }

  @Test
  public void testHealthEndpointReportsUnavailableWhenNoApiKey() {
    when(mockLlmClient.isAvailable()).thenReturn(false);

    Response response = endpoint.health();

    assertEquals(200, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertFalse((Boolean) body.get("llmClientAvailable"));
  }

  // ── Models endpoint ────────────────────────────────────────────────────────

  @Test
  public void testModelsEndpointReturnsSupportedModelList() {
    when(mockLlmClient.getSupportedModels())
        .thenReturn(Arrays.asList("gemini-2.5-flash", "gemini-2.5-pro"));

    Response response = endpoint.getSupportedModels();

    assertEquals(200, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertTrue(body.containsKey("models"));
    @SuppressWarnings("unchecked")
    List<String> models = (List<String>) body.get("models");
    assertFalse(models.isEmpty());
    assertTrue(models.contains("gemini-2.5-flash"));
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
    assertTrue(body.containsKey("error"), "Error response must have an 'error' key");
    assertNotNull(body.get("error"), "Error message must not be null");
  }

  @SuppressWarnings("unchecked")
  private void assertExactErrorMessage(Response response, String expected) {
    assertErrorMessagePresent(response);
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertEquals(expected, body.get("error").toString(),
        "Error message text does not match expected");
  }

  @SuppressWarnings("unchecked")
  private void assertErrorMessageContains(Response response, String substring) {
    assertErrorMessagePresent(response);
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertTrue(body.get("error").toString().contains(substring),
        "Error message should contain '" + substring + "' but was: " + body.get("error"));
  }
}
