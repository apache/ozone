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

package org.apache.hadoop.hdds.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Test cases for span sampling functionality.
 */
public class TestSpanSampling {

  /**
   * Validates the logic of the LoopSampler through the following cases:
   * 1. Invalid intervals (0 or negative) throw an {@link IllegalArgumentException}.
   * 2. Sampling occurs exactly every Nth attempt for a given interval.
   * 3. An interval of 1 results in every single attempt being sampled.
   */
  @Test
  public void testLoopSamplerLogic() {
    // Tests invalid values
    assertThrows(IllegalArgumentException.class, () -> new LoopSampler(0));
    assertThrows(IllegalArgumentException.class, () -> new LoopSampler(-1));

    // Tests functionality of a correct value
    LoopSampler sampler3 = new LoopSampler(3);
    assertFalse(sampler3.shouldSample(), "1st span should not be sampled");
    assertFalse(sampler3.shouldSample(), "2nd span should not be sampled");
    assertTrue(sampler3.shouldSample(), "3rd span should be sampled");
    assertFalse(sampler3.shouldSample(), "4th span should not be sampled");
    assertFalse(sampler3.shouldSample(), "5th span should not be sampled");
    assertTrue(sampler3.shouldSample(), "6th span should be sampled");

    // Tests every span is sampled for a value of 1
    LoopSampler sampler1 = new LoopSampler(1);
    for (int i = 1; i <= 10; i++) {
      assertTrue(sampler1.shouldSample(), "Span " + i + " should be sampled when interval is 1");
    }
  }

  /**
   * Tests that valid configuration strings result in a Map
   * containing the correct LoopSampler objects.
   */
  @Test
  public void testParseSpanSamplingConfigValid() throws Exception {
    String config = "createVolume:1,createBucket:2,createKey:10";
    Method method = TracingUtil.class.getDeclaredMethod("parseSpanSamplingConfig", String.class);
    method.setAccessible(true);
    Map<String, LoopSampler> result = (Map<String, LoopSampler>) method.invoke(null, config);

    // Verify all 3 valid entries exist
    assertEquals(3, result.size());
    assertTrue(result.containsKey("createVolume"));
    assertTrue(result.containsKey("createBucket"));
    assertTrue(result.containsKey("createKey"));
  }

  /**
   * Tests that invalid entries (decimals, zeros, text, negative numbers) are caught
   * by the try-catch blocks and excluded from the resulting Map.
   */
  @Test
  public void testParseSpanSamplingConfigInvalid() throws Exception {
    String config = "createVolume:0.5,createBucket:0,createKey:-4.5,writeKey:-1";

    Method method = TracingUtil.class.getDeclaredMethod("parseSpanSamplingConfig", String.class);
    method.setAccessible(true);

    Map<String, LoopSampler> result = (Map<String, LoopSampler>) method.invoke(null, config);

    // Verify the map is empty because every entry was invalid
    assertTrue(result.isEmpty(), "The map should be empty as all inputs were invalid");
  }

  /**
   * Tests a mixed configuration to ensure valid entries are
   * preserved while invalid ones are skipped.
   */
  @Test
  public void testParseSpanSamplingConfigMixed() throws Exception {
    String config = "createVolume:1,createBucket:0.5";

    Method method = TracingUtil.class.getDeclaredMethod("parseSpanSamplingConfig", String.class);
    method.setAccessible(true);

    Map<String, LoopSampler> result = (Map<String, LoopSampler>) method.invoke(null, config);

    // Verify createVolume is kept and createBucket is discarded
    assertEquals(1, result.size());
    assertTrue(result.containsKey("createVolume"));
    assertFalse(result.containsKey("createBucket"));
  }

  /** Tests a SpanSampler dropping appropriate samples according to Config.
   * (e.g., keeping every 2nd "createKey").
   */
  @Test
  public void testSpanSamplingWithConfiguration() {
    Map<String, LoopSampler> spanMap = new HashMap<>();
    spanMap.put("createKey", new LoopSampler(2));

    Sampler rootSampler = Sampler.alwaysOn();
    SpanSampler customSampler = new SpanSampler(rootSampler, spanMap);

    // Create a parent span to move from rootSampler to customSampler logic.
    Span parentSpan = Span.wrap(SpanContext.create(
        "00000000000000000000000000000001",
        "0000000000000002",
        TraceFlags.getSampled(),
        TraceState.getDefault()));
    Context parentContext = Context.root().with(parentSpan);

    // result1 will drop and result2 will be sampled according to config
    SamplingResult result1 = customSampler.shouldSample(parentContext, "trace1", "createKey",
        SpanKind.INTERNAL, Attributes.empty(), Collections.emptyList());
    assertEquals(SamplingDecision.DROP, result1.getDecision());
    SamplingResult result2 = customSampler.shouldSample(parentContext, "trace1", "createKey",
        SpanKind.INTERNAL, Attributes.empty(), Collections.emptyList());
    assertEquals(SamplingDecision.RECORD_AND_SAMPLE, result2.getDecision());
  }

  @Test
  public void testSpanSamplingWithTraceSampled() {
    Map<String, LoopSampler> spanMap = new HashMap<>();
    spanMap.put("createKey", new LoopSampler(2));

    Sampler rootSampler = Sampler.alwaysOn();
    SpanSampler customSampler = new SpanSampler(rootSampler, spanMap);
    Context parentContext = Context.current();
    SamplingResult result = customSampler.shouldSample(parentContext, "trace1", "unknownSpan",
        SpanKind.INTERNAL, Attributes.empty(), Collections.emptyList());

    // Since no parent and not configured, should use root sampler and sample span.
    assertEquals(SamplingDecision.RECORD_AND_SAMPLE, result.getDecision());
  }

  @Test
  public void testSpanSamplingWithTraceNotSampled() {
    Map<String, LoopSampler> spanMap = new HashMap<>();
    Sampler rootSampler = Sampler.alwaysOff();
    SpanSampler customSampler = new SpanSampler(rootSampler, spanMap);
    Context parentContext = Context.current();

    // Root span with alwaysOff should not be sampled.
    SamplingResult rootResult = customSampler.shouldSample(parentContext, "trace1", "rootSpan",
        SpanKind.INTERNAL, Attributes.empty(), Collections.emptyList());
    assertEquals(SamplingDecision.DROP, rootResult.getDecision());
  }
}
