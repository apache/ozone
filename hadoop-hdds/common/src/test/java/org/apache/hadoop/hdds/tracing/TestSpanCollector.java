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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Test helper that enables tracing and collects every span when it ends.
 * Call {@link #close} after the test to reset the global tracing state.
 */
public final class TestSpanCollector implements SpanProcessor, AutoCloseable {
  private final List<ReadableSpan> spans = new CopyOnWriteArrayList<>();

  private TestSpanCollector() {
  }

  /** Registers a collecting SDK as the global OpenTelemetry and initializes Ozone tracing with it. */
  public static TestSpanCollector install(String serviceName) {
    TracingUtil.shutdownTracing();
    GlobalOpenTelemetry.resetForTest();
    TestSpanCollector collector = new TestSpanCollector();
    GlobalOpenTelemetry.set(OpenTelemetrySdk.builder()
        .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(collector).build())
        .build());
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.tracing.enabled", true);
    TracingUtil.initTracing(serviceName, conf);
    return collector;
  }

  public List<ReadableSpan> getSpans() {
    return spans;
  }

  public List<ReadableSpan> named(String name) {
    return spans.stream().filter(span -> span.getName().equals(name)).collect(Collectors.toList());
  }

  @Override
  public void close() {
    TracingUtil.shutdownTracing();
    GlobalOpenTelemetry.resetForTest();
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {
  }

  @Override
  public boolean isStartRequired() {
    return false;
  }

  @Override
  public void onEnd(ReadableSpan span) {
    spans.add(span);
  }

  @Override
  public boolean isEndRequired() {
    return true;
  }
}
