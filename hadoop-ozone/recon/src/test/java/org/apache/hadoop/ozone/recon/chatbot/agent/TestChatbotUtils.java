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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for the pure helpers in {@link ChatbotUtils}: the listKeys safe-scope prefix check,
 * the limit parser, and the record-count estimator. These functions sit on the security and
 * truncation paths, so their edge cases are worth pinning directly.
 */
public class TestChatbotUtils {

  private static final int DEFAULT_LIMIT = 1000;
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
  private static final ObjectMapper MAPPER = new ObjectMapper();

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
  public void testEstimateRecordCountForNullScalarAndEmptyObject() {
    assertEquals(0, ChatbotUtils.estimateRecordCount(null), "null node");
    assertEquals(0, ChatbotUtils.estimateRecordCount(NODES.numberNode(5)), "scalar node");
    assertEquals(0, ChatbotUtils.estimateRecordCount(NODES.objectNode()), "empty object");
  }

  @Test
  public void testEstimateRecordCountForBareObjectArray() {
    assertEquals(3, ChatbotUtils.estimateRecordCount(objectArray(3)));
  }

  @Test
  public void testEstimateRecordCountForEmptyArrays() {
    assertEquals(0, ChatbotUtils.estimateRecordCount(NODES.arrayNode()));
    ObjectNode emptyContainers = NODES.objectNode();
    emptyContainers.set("containers", NODES.arrayNode());
    assertEquals(0, ChatbotUtils.estimateRecordCount(emptyContainers));
  }

  @Test
  public void testEstimateRecordCountFindsNestedRecordArrays() {
    ObjectNode containersResponse = NODES.objectNode();
    ObjectNode data = NODES.objectNode();
    data.set("containers", objectArray(3));
    containersResponse.set("data", data);
    assertEquals(3, ChatbotUtils.estimateRecordCount(containersResponse));
  }

  @Test
  public void testEstimateRecordCountForLegacyDataObjectArray() throws Exception {
    JsonNode response = parseJson("{\"data\":[{\"id\":0},{\"id\":1},{\"id\":2}]}");
    assertEquals(3, ChatbotUtils.estimateRecordCount(response));
  }

  @Test
  public void testEstimateRecordCountForMixedObjectAndScalarArray() {
    ArrayNode mixed = NODES.arrayNode();
    mixed.add(NODES.objectNode());
    mixed.add(NODES.numberNode(5));
    mixed.add(NODES.objectNode());
    // Any object element means the whole array is treated as a record list.
    assertEquals(3, ChatbotUtils.estimateRecordCount(mixed));
  }

  @Test
  public void testEstimateRecordCountIgnoresScalarFields() {
    ObjectNode response = NODES.objectNode();
    response.set("containers", objectArray(5));
    response.put("missingCount", 5);
    assertEquals(5, ChatbotUtils.estimateRecordCount(response));
  }

  @Test
  public void testEstimateRecordCountSumsMultipleRecordArrays() {
    ObjectNode response = NODES.objectNode();
    response.set("fso", objectArray(2));
    response.set("nonFSO", objectArray(3));
    response.set("deletedKeyInfo", objectArray(4));
    assertEquals(9, ChatbotUtils.estimateRecordCount(response));
  }

  @Test
  public void testEstimateRecordCountSumsSplitKeyCollections() {
    ObjectNode keyInsightResponse = NODES.objectNode();
    keyInsightResponse.set("fso", objectArray(2));
    keyInsightResponse.set("nonFSO", objectArray(3));
    assertEquals(5, ChatbotUtils.estimateRecordCount(keyInsightResponse));
  }

  @Test
  public void testEstimateRecordCountForDeletedAndMismatchArrays() {
    ObjectNode deletePendingResponse = NODES.objectNode();
    deletePendingResponse.set("deletedKeyInfo", objectArray(4));
    assertEquals(4, ChatbotUtils.estimateRecordCount(deletePendingResponse));

    ObjectNode mismatchResponse = NODES.objectNode();
    mismatchResponse.set("containerDiscrepancyInfo", objectArray(6));
    assertEquals(6, ChatbotUtils.estimateRecordCount(mismatchResponse));
  }

  @Test
  public void testEstimateRecordCountOverCountsNestedRecordArraysSafely() {
    // Over-counting only ever over-warns about truncation, never under-reports.
    ArrayNode containers = NODES.arrayNode();
    for (int i = 0; i < 2; i++) {
      ObjectNode container = NODES.objectNode();
      container.set("replicas", objectArray(2));
      containers.add(container);
    }
    ObjectNode response = NODES.objectNode();
    response.set("containers", containers);
    assertEquals(6, ChatbotUtils.estimateRecordCount(response));
  }

  @Test
  public void testEstimateRecordCountIgnoresClaimedTotalCount() throws Exception {
    JsonNode response = parseJson(
        "{\"data\":{\"totalCount\":1234,\"prevKey\":0,\"containers\":["
            + "{\"id\":0},{\"id\":1},{\"id\":2},{\"id\":3},"
            + "{\"id\":4},{\"id\":5},{\"id\":6}"
            + "]}}");
    assertEquals(7, ChatbotUtils.estimateRecordCount(response));
  }

  @Test
  public void testEstimateRecordCountForKeyInsightInfoShape() {
    ObjectNode response = NODES.objectNode();
    response.put("lastKey", "key-99");
    response.put("replicatedDataSize", 4096);
    response.set("status", NODES.objectNode().put("code", "OK"));
    response.set("fso", objectArray(2));
    response.set("nonFSO", objectArray(3));
    assertEquals(5, ChatbotUtils.estimateRecordCount(response));
  }

  @Test
  public void testEstimateRecordCountIgnoresScalarArrays() {
    ObjectNode withBins = NODES.objectNode();
    withBins.set("bins", scalarArray(1, 2, 3, 4));
    assertEquals(0, ChatbotUtils.estimateRecordCount(withBins));

    ObjectNode withStringKeys = NODES.objectNode();
    withStringKeys.set("keys", NODES.arrayNode().add("k1").add("k2"));
    assertEquals(0, ChatbotUtils.estimateRecordCount(withStringKeys));
  }

  @Test
  public void testEstimateRecordCountNullElementsInArray() {
    ArrayNode withNullAndObject = NODES.arrayNode();
    withNullAndObject.addNull();
    withNullAndObject.add(NODES.objectNode());
    assertEquals(2, ChatbotUtils.estimateRecordCount(withNullAndObject));

    ArrayNode allNulls = NODES.arrayNode();
    allNulls.addNull();
    allNulls.addNull();
    assertEquals(0, ChatbotUtils.estimateRecordCount(allNulls));
  }

  @Test
  public void testEstimateRecordCountAtCapBoundary() {
    assertEquals(1000, ChatbotUtils.estimateRecordCount(objectArray(1000)));
    assertEquals(1001, ChatbotUtils.estimateRecordCount(objectArray(1001)));
    assertEquals(5000, ChatbotUtils.estimateRecordCount(objectArray(5000)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("recordCountShapeSweep")
  public void testEstimateRecordCountShapeSweep(String label, JsonNode payload, int expected) {
    assertEquals(expected, ChatbotUtils.estimateRecordCount(payload), label);
    if (expected > 0) {
      assertTrue(ChatbotUtils.estimateRecordCount(payload) > 0,
          "record-bearing shape must not return 0: " + label);
    }
  }

  private static Stream<Arguments> recordCountShapeSweep() throws Exception {
    ObjectNode unhealthy = NODES.objectNode();
    unhealthy.set("containers", objectArray(5));

    ObjectNode datanodes = NODES.objectNode();
    datanodes.set("datanodes", objectArray(4));

    ObjectNode volumes = NODES.objectNode();
    volumes.set("volumes", objectArray(3));

    return Stream.of(
        Arguments.of("bare object array", objectArray(2), 2),
        Arguments.of("data.containers", nestedContainers(8), 8),
        Arguments.of("legacy data[]", parseJson("{\"data\":[{\"id\":1},{\"id\":2}]}"), 2),
        Arguments.of("fso+nonFSO+deletedKeyInfo", multiKeyInsight(1, 2, 3), 6),
        Arguments.of("containerDiscrepancyInfo", mismatch(5), 5),
        Arguments.of("unhealthy containers", unhealthy, 5),
        Arguments.of("datanodes", datanodes, 4),
        Arguments.of("volumes", volumes, 3),
        Arguments.of("scalar bins only", scalarBins(), 0));
  }

  private static ArrayNode objectArray(int count) {
    ArrayNode arr = NODES.arrayNode();
    for (int i = 0; i < count; i++) {
      arr.add(NODES.objectNode().put("id", i));
    }
    return arr;
  }

  private static ArrayNode scalarArray(int... values) {
    ArrayNode arr = NODES.arrayNode();
    for (int value : values) {
      arr.add(value);
    }
    return arr;
  }

  private static JsonNode parseJson(String json) throws Exception {
    return MAPPER.readTree(json);
  }

  private static ObjectNode nestedContainers(int count) {
    ObjectNode root = NODES.objectNode();
    ObjectNode data = NODES.objectNode();
    data.set("containers", objectArray(count));
    root.set("data", data);
    return root;
  }

  private static ObjectNode multiKeyInsight(int fso, int nonFso, int deleted) {
    ObjectNode root = NODES.objectNode();
    root.set("fso", objectArray(fso));
    root.set("nonFSO", objectArray(nonFso));
    root.set("deletedKeyInfo", objectArray(deleted));
    return root;
  }

  private static ObjectNode mismatch(int count) {
    ObjectNode root = NODES.objectNode();
    root.set("containerDiscrepancyInfo", objectArray(count));
    return root;
  }

  private static ObjectNode scalarBins() {
    ObjectNode root = NODES.objectNode();
    root.set("bins", scalarArray(1, 2, 3));
    return root;
  }
}
