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

package org.apache.hadoop.ozone.recon.chatbot.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests the extraction of JSON objects from LLM responses.
 *
 * <p><b>Lifecycle Phase:</b> Post-1st LLM Call. Tests processing of raw LLM text before parsing.</p>
 *
 * <p><b>Key scenarios tested:</b></p>
 * <ul>
 *   <li><b>Prose-wrapped JSON:</b> Extracting JSON surrounded by conversational text.</li>
 *   <li><b>Nested braces:</b> Handling braces inside JSON string values correctly.</li>
 *   <li><b>Truncated JSON:</b> Returning null gracefully for incomplete JSON.</li>
 *   <li><b>Edge cases:</b> Handling empty strings, null inputs, and multiple JSON objects.</li>
 * </ul>
 */
public class TestChatbotAgentJsonExtraction {

  // ── Happy-path extraction ──────────────────────────────────────────────────

  @Test
  public void testSimpleJsonObjectReturnedUnchanged() {
    String input = "{\"type\":\"SINGLE_ENDPOINT\"}";
    assertEquals(input, ChatbotUtils.extractFirstJsonObject(input));
  }

  @Test
  public void testEmptyJsonObjectReturnedUnchanged() {
    assertEquals("{}", ChatbotUtils.extractFirstJsonObject("{}"));
  }

  @Test
  public void testFullSingleEndpointJson() {
    String input = "{\"type\":\"SINGLE_ENDPOINT\",\"endpoint\":\"/api/v1/clusterState\"," +
        "\"method\":\"GET\",\"parameters\":{},\"reasoning\":\"need cluster data\"}";
    assertEquals(input, ChatbotUtils.extractFirstJsonObject(input));
  }

  @Test
  public void testDeeplyNestedJsonReturnedCorrectly() {
    String input = "{\"a\":{\"b\":{\"c\":{\"d\":\"val\"}}}}";
    assertEquals(input, ChatbotUtils.extractFirstJsonObject(input));
  }

  @Test
  public void testMultiEndpointJsonWithNestedArray() {
    // Multi-endpoint style JSON with a nested array of tool-call objects
    String input = "{\"type\":\"MULTI_ENDPOINT\",\"tool_calls\":[" +
        "{\"endpoint\":\"/api/v1/datanodes\"}," +
        "{\"endpoint\":\"/api/v1/pipelines\"}]}";
    assertEquals(input, ChatbotUtils.extractFirstJsonObject(input));
  }

  // ── ROB-01: Prose-wrapped JSON ─────────────────────────────────────────────

  @Test
  public void testProseBeforeAndAfterJsonIsStripped() {
    // LLM wraps the JSON in prose despite being told not to
    String json = "{\"type\":\"SINGLE_ENDPOINT\",\"endpoint\":\"/api/v1/datanodes\"}";
    String input = "Certainly! Here is the tool call: " + json + " Let me know if you need more.";
    assertEquals(json, ChatbotUtils.extractFirstJsonObject(input));
  }

  @Test
  public void testMarkdownCodeFenceJsonExtractedCorrectly() {
    // LLM returns JSON inside a markdown code block
    String json = "{\"type\":\"SINGLE_ENDPOINT\"}";
    String input = "```json\n" + json + "\n```";
    assertEquals(json, ChatbotUtils.extractFirstJsonObject(input));
  }

  // ── ROB-02: Nested braces inside string fields ─────────────────────────────

  @Test
  public void testBracesInsideStringFieldDoNotConfuseCounter() {
    // The reasoning field contains braces — must not terminate extraction early
    String input = "{\"reasoning\":\"I found a nested {object} here\",\"type\":\"SINGLE_ENDPOINT\"}";
    assertEquals(input, ChatbotUtils.extractFirstJsonObject(input));
  }

  @Test
  public void testClosingBraceInStringFieldDoesNotTerminateEarly() {
    // A closing brace inside a string value must not end the object
    String input = "{\"reasoning\":\"closing brace } inside\",\"type\":\"X\"}";
    assertEquals(input, ChatbotUtils.extractFirstJsonObject(input));
  }

  @Test
  public void testEscapedQuoteInsideStringFieldHandledCorrectly() {
    // An escaped quote must not toggle the inString flag
    String input = "{\"key\":\"value with \\\" escaped quote\",\"type\":\"X\"}";
    assertEquals(input, ChatbotUtils.extractFirstJsonObject(input));
  }

  // ── ROB-03: Truncated JSON ────────────────────────────────────────────────

  @Test
  public void testTruncatedJsonReturnsNull() {
    // Missing closing brace — no complete JSON object
    String input = "{\"type\":\"SINGLE_ENDPOINT\",\"endpoint\":\"/api/v1/clusterState\"";
    assertNull(ChatbotUtils.extractFirstJsonObject(input));
  }

  @Test
  public void testJsonMissingOpeningBraceReturnsNull() {
    // Only a closing brace present — no opening brace
    assertNull(ChatbotUtils.extractFirstJsonObject("\"key\":\"val\"}"));
  }

  // ── Null / empty / whitespace inputs ─────────────────────────────────────

  @Test
  public void testNullInputReturnsNullWithoutException() {
    assertNull(ChatbotUtils.extractFirstJsonObject(null));
  }

  @Test
  public void testEmptyStringReturnsNull() {
    assertNull(ChatbotUtils.extractFirstJsonObject(""));
  }

  @Test
  public void testWhitespaceOnlyReturnsNull() {
    assertNull(ChatbotUtils.extractFirstJsonObject("   \n\t  "));
  }

  @Test
  public void testNoSuitableEndpointLiteralReturnsNull() {
    // The fallback sentinel the LLM is told to return — no JSON present
    assertNull(ChatbotUtils.extractFirstJsonObject("NO_SUITABLE_ENDPOINT"));
  }

  @Test
  public void testPlainProseWithNoJsonReturnsNull() {
    assertNull(ChatbotUtils.extractFirstJsonObject("I don't know how to answer this question."));
  }

  // ── Multiple JSON objects: returns first ──────────────────────────────────

  @Test
  public void testMultipleJsonObjectsReturnsFirstOnly() {
    // Should extract the first complete JSON object and ignore the rest
    String input = "{\"a\":1} {\"b\":2}";
    assertEquals("{\"a\":1}", ChatbotUtils.extractFirstJsonObject(input));
  }

  // ── JSON arrays ───────────────────────────────────────────────────────────

  @Test
  public void testJsonArrayExtractsFirstInnerObject() {
    // The method scans for the first '{...}' regardless of surrounding structure.
    // An array like [{"a":1}] contains a '{' at index 1, so the inner object is extracted.
    // The LLM is instructed to return a bare JSON object, not an array, so this case
    // should not occur in practice — but if it does, the inner object is returned rather
    // than null. The caller (getToolCall) will then fail to find a known "type" field
    // and route to handleFallback.
    assertEquals("{\"a\":1}", ChatbotUtils.extractFirstJsonObject("[{\"a\":1}]"));
  }

  // ── Unicode and special characters ────────────────────────────────────────

  @Test
  public void testUnicodeCharactersInStringFieldHandledCorrectly() {
    String input = "{\"key\":\"你好世界\"}";
    assertEquals(input, ChatbotUtils.extractFirstJsonObject(input));
  }

  @Test
  public void testControlCharacterInStringFieldDoesNotCrash() {
    // Null character inside a string value must not cause an exception
    String input = "{\"k\":\"v\u0000alue\"}";
    String result = ChatbotUtils.extractFirstJsonObject(input);
    assertNotNull(result);
    assertTrue(result.startsWith("{") && result.endsWith("}"));
  }

  // ── Performance ───────────────────────────────────────────────────────────

  @Test
  public void testExtremelyLargeJsonHandledWithoutCrash() {
    // JSON with a very long string value — must not crash, time out, or OOM
    StringBuilder longValue = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      longValue.append('x');
    }
    String input = "{\"key\":\"" + longValue + "\"}";
    String result = ChatbotUtils.extractFirstJsonObject(input);
    assertNotNull(result);
    assertTrue(result.startsWith("{") && result.endsWith("}"));
  }
}
