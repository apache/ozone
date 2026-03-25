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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.opentelemetry.api.common.Attributes;
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
   * Tests that valid configuration strings result in a Map
   * containing the correct LoopSampler objects.
   */
  @Test
  public void testParseSpanSamplingConfigValid() throws Exception {
    String config = "createVolume:0.25,createBucket:0.5,createKey:0.75";
    Method method = TracingUtil.class.getDeclaredMethod("parseSpanSamplingConfig", String.class);
    method.setAccessible(true);
    Map<String, LoopSampler> result = (Map<String, LoopSampler>) method.invoke(null, config);

    assertThat(result)
        .hasSize(3)
        .containsKeys("createVolume", "createBucket", "createKey");

  }

  /**
   * Tests that invalid entries (zeros, negative numbers, non-numeric) are caught
   * by the try-catch blocks and excluded from the resulting Map.
   */
  @Test
  public void testParseSpanSamplingConfigInvalid() throws Exception {
    String config = "createVolume:0,createBucket:-0.5,createKey:invalid,writeKey:-1";
    Method method = TracingUtil.class.getDeclaredMethod("parseSpanSamplingConfig", String.class);
    method.setAccessible(true);
    Map<String, LoopSampler> result = (Map<String, LoopSampler>) method.invoke(null, config);

    assertThat(result).as("The map should be empty as all inputs were invalid").isEmpty();
  }

  /**
   * Tests a mixed configuration to ensure valid entries are
   * preserved while invalid ones are skipped.
   */
  @Test
  public void testParseSpanSamplingConfigMixed() throws Exception {
    String config = "createVolume:0.75,createBucket:0,createKey:-5";

    Method method = TracingUtil.class.getDeclaredMethod("parseSpanSamplingConfig", String.class);
    method.setAccessible(true);

    Map<String, LoopSampler> result = (Map<String, LoopSampler>) method.invoke(null, config);

    assertThat(result)
        .hasSize(1)
        .containsKey("createVolume")
        .doesNotContainKeys("createBucket", "createKey");
  }

  /**
   * Test to show sampling of span only if trace is sampled.
   * Trace is always sampled and span name is not mentioned in config, Hence it will be sampled.
   */
  @Test
  public void testSpanSamplingWithTraceSampled() {
    Map<String, LoopSampler> spanMap = new HashMap<>();
    spanMap.put("createKey", new LoopSampler(0.5));

    Sampler rootSampler = Sampler.alwaysOn();
    SpanSampler customSampler = new SpanSampler(rootSampler, spanMap);
    Context parentContext = Context.current();
    SamplingResult result = customSampler.shouldSample(parentContext, "trace1", "unknownSpan",
        SpanKind.INTERNAL, Attributes.empty(), Collections.emptyList());

    // Since no parent and not configured, should use root sampler and sample span.
    assertEquals(SamplingDecision.RECORD_AND_SAMPLE, result.getDecision());
  }

  /**
   * Test to show dropping of span only if trace is not sample sampled.
   * This shows priority given to Trace.
   * */
  @Test
  public void testSpanSamplingWithTraceNotSampled() {
    Map<String, LoopSampler> spanMap = new HashMap<>();
    Sampler rootSampler = Sampler.alwaysOff();
    SpanSampler customSampler = new SpanSampler(rootSampler, spanMap);
    Context parentContext = Context.current();

    SamplingResult result = customSampler.shouldSample(parentContext, "trace1", "rootSpan",
        SpanKind.INTERNAL, Attributes.empty(), Collections.emptyList());

    // Root span with alwaysOff should not be sampled.
    assertEquals(SamplingDecision.DROP, result.getDecision());
  }

  /**
   * Test to show child span is not sampled when parent span is also not sampled.
   */
  @Test
  public void testChildDropsWhenParentIsNotSampled() {
    Map<String, LoopSampler> spanMap = new HashMap<>();
    spanMap.put("createKey", new LoopSampler(1.0));

    SpanSampler customSampler = new SpanSampler(Sampler.alwaysOn(), spanMap);

    io.opentelemetry.api.trace.Span parentSpan = io.opentelemetry.api.trace.Span.wrap(
        io.opentelemetry.api.trace.SpanContext.create(
            "ff000000000000000000000000000041",
            "ff00000000000042",
            TraceFlags.getDefault(),
            TraceState.getDefault()));

    Context parentContext = Context.root().with(parentSpan);

    SamplingResult result = customSampler.shouldSample(parentContext, "trace1", "createKey",
        SpanKind.INTERNAL, Attributes.empty(), Collections.emptyList());

    assertEquals(SamplingDecision.DROP, result.getDecision());
  }
}
