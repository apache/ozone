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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure helpers in {@link ChatbotUtils}: the listKeys safe-scope prefix check,
 * the limit parser, and the record-count estimator. These functions sit on the security and
 * truncation paths, so their edge cases are worth pinning directly.
 */
public class TestChatbotUtils {

  private static final int DEFAULT_LIMIT = 1000;
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  // ── isBucketScopedListKeysPrefix ───────────────────────────────────────────

  @Test
  public void testBucketScopedPrefixRejectsUnscopedValues() {
    assertFalse(ChatbotUtils.isBucketScopedListKeysPrefix(null), "null");
    assertFalse(ChatbotUtils.isBucketScopedListKeysPrefix(""), "empty");
    assertFalse(ChatbotUtils.isBucketScopedListKeysPrefix("   "), "whitespace only");
    assertFalse(ChatbotUtils.isBucketScopedListKeysPrefix("/"), "root");
    assertFalse(ChatbotUtils.isBucketScopedListKeysPrefix("/vol"), "volume only");
    assertFalse(ChatbotUtils.isBucketScopedListKeysPrefix("vol/bucket"), "no leading slash");
    assertFalse(ChatbotUtils.isBucketScopedListKeysPrefix("/vol/../etc"), "contains ..");
  }

  @Test
  public void testBucketScopedPrefixAcceptsBucketScopedValues() {
    assertTrue(ChatbotUtils.isBucketScopedListKeysPrefix("/vol/bucket"), "volume/bucket");
    assertTrue(ChatbotUtils.isBucketScopedListKeysPrefix("/vol/bucket/dir/key"), "deeper path");
    assertTrue(ChatbotUtils.isBucketScopedListKeysPrefix("/vol/bucket/"), "trailing slash");
    assertTrue(ChatbotUtils.isBucketScopedListKeysPrefix("  /vol/bucket  "), "trimmed");
  }

  // ── parsePositiveInt ───────────────────────────────────────────────────────

  @Test
  public void testParsePositiveIntDefaultsAndParses() {
    assertEquals(DEFAULT_LIMIT, ChatbotUtils.parsePositiveInt(null, DEFAULT_LIMIT));
    assertEquals(DEFAULT_LIMIT, ChatbotUtils.parsePositiveInt("", DEFAULT_LIMIT));
    assertEquals(DEFAULT_LIMIT, ChatbotUtils.parsePositiveInt("   ", DEFAULT_LIMIT));
    assertEquals(DEFAULT_LIMIT, ChatbotUtils.parsePositiveInt("abc", DEFAULT_LIMIT), "non-numeric");
    assertEquals(50, ChatbotUtils.parsePositiveInt("50", DEFAULT_LIMIT));
    assertEquals(50, ChatbotUtils.parsePositiveInt(" 50 ", DEFAULT_LIMIT), "surrounding spaces");
  }

  @Test
  public void testParsePositiveIntRejectsNonPositive() {
    assertThrows(IllegalArgumentException.class,
        () -> ChatbotUtils.parsePositiveInt("0", DEFAULT_LIMIT));
    assertThrows(IllegalArgumentException.class,
        () -> ChatbotUtils.parsePositiveInt("-1", DEFAULT_LIMIT));
  }

  // ── estimateRecordCount ────────────────────────────────────────────────────

  @Test
  public void testEstimateRecordCountForNullAndScalar() {
    assertEquals(0, ChatbotUtils.estimateRecordCount(null), "null node");
    assertEquals(0, ChatbotUtils.estimateRecordCount(NODES.numberNode(5)), "scalar node");
    assertEquals(0, ChatbotUtils.estimateRecordCount(NODES.objectNode().put("foo", 1)),
        "object without keys/data array");
  }

  @Test
  public void testEstimateRecordCountForArrayNode() {
    ArrayNode arr = NODES.arrayNode().add("a").add("b").add("c");
    assertEquals(3, ChatbotUtils.estimateRecordCount(arr));
  }

  @Test
  public void testEstimateRecordCountForKeysAndDataArrays() {
    ObjectNode withKeys = NODES.objectNode();
    withKeys.set("keys", NODES.arrayNode().add("k1").add("k2"));
    assertEquals(2, ChatbotUtils.estimateRecordCount(withKeys), "keys[] array");

    ObjectNode withData = NODES.objectNode();
    withData.set("data", NODES.arrayNode().add("d1").add("d2").add("d3").add("d4"));
    assertEquals(4, ChatbotUtils.estimateRecordCount(withData), "data[] array");
  }
}
