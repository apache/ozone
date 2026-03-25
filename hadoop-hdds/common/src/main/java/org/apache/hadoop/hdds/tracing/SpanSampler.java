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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;
import java.util.Map;

/**
 * Custom Sampler that applies span-level sampling for configured
 * span names, and delegates to parent-based strategy otherwise.
 * When a span name is in the configured spanMap, uses LoopSampler for
 * deterministic 1-in-N sampling, otherwise follows the parent span's
 * sampling decision.
 */
public final class SpanSampler implements Sampler {

  private final Sampler rootSampler;
  private final Map<String, LoopSampler> spanMap;

  public SpanSampler(Sampler rootSampler,
                     Map<String, LoopSampler> spanMap) {
    this.rootSampler = rootSampler;
    this.spanMap = spanMap;
  }

  @Override
  public SamplingResult shouldSample(Context parentContext, String traceId,
                                     String spanName, SpanKind spanKind, Attributes attributes,
                                     List<io.opentelemetry.sdk.trace.data.LinkData> parentLinks) {

    // First, check if we have a valid parent span
    io.opentelemetry.api.trace.Span parentSpan =
        io.opentelemetry.api.trace.Span.fromContext(parentContext);

    if (!parentSpan.getSpanContext().isValid()) {
      // Root span: always delegate to trace-level sampler
      // This ensures OTEL_TRACES_SAMPLER_ARG=0.5 is respected
      return rootSampler.shouldSample(parentContext, traceId, spanName,
          spanKind, attributes, parentLinks);
    }

    // Child span: check parent's sampling status first
    // after the process of sampling trace / parent span then check if it is sampled or not.
    if (!parentSpan.getSpanContext().isSampled()) {
      // Parent was not sampled, so this child should not be sampled either
      // This prevents orphaned spans
      return SamplingResult.drop();
    }

    // Parent was sampled, now check if this span has explicit sampling config
    LoopSampler loopSampler = spanMap.get(spanName);
    if (loopSampler != null) {
      boolean sample = loopSampler.shouldSample();
      return sample ? SamplingResult.recordAndSample() : SamplingResult.drop();
    }

    // No explicit config for this span, follow parent's sampling decision
    return SamplingResult.recordAndSample();
  }

  @Override
  public String getDescription() {
    return "SpanSamplingCustomSampler(spanMap=" + spanMap.keySet() + ")";
  }
}
