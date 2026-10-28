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

import static io.opentelemetry.api.common.AttributeKey.booleanKey;
import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * {@link SpanProcessor} that collects ended spans in memory, so tests can assert on them.
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

  /** Fails unless exactly one ended span is named {@code spanName}. */
  public void assertSingleSpan(String spanName) {
    onlySpan(spanName);
  }

  public void assertError(String spanName) {
    assertEquals(StatusCode.ERROR, onlySpan(spanName).getStatus().getStatusCode(), () -> spanName + " status");
  }

  public void assertError(String spanName, String expectedDescription) {
    SpanData span = onlySpan(spanName);
    assertEquals(StatusCode.ERROR, span.getStatus().getStatusCode(), () -> spanName + " status");
    assertEquals(expectedDescription, span.getStatus().getDescription(), () -> spanName + " status description");
  }

  public void assertKind(String spanName, SpanKind expected) {
    assertEquals(expected, onlySpan(spanName).getKind(), () -> spanName + " kind");
  }

  public void assertNotError(String spanName) {
    assertNotEquals(StatusCode.ERROR, onlySpan(spanName).getStatus().getStatusCode(), () -> spanName + " status");
  }

  public void assertAttribute(String spanName, String key, String expected) {
    assertEquals(expected, onlySpan(spanName).getAttributes().get(stringKey(key)), () -> spanName + " " + key);
  }

  public void assertAttribute(String spanName, String key, long expected) {
    assertEquals(expected, onlySpan(spanName).getAttributes().get(longKey(key)), () -> spanName + " " + key);
  }

  public void assertAttribute(String spanName, String key, boolean expected) {
    assertEquals(expected, onlySpan(spanName).getAttributes().get(booleanKey(key)), () -> spanName + " " + key);
  }

  /** Fails if the span has {@code key} with a value of any type. */
  public void assertNoAttribute(String spanName, String key) {
    Set<String> keys = onlySpan(spanName).getAttributes().asMap().keySet().stream()
        .map(AttributeKey::getKey)
        .collect(Collectors.toSet());
    assertFalse(keys.contains(key), () -> spanName + " has attribute " + key + ": " + keys);
  }

  /** Fails unless the span recorded exactly one exception event, of type {@code type}. */
  public void assertExceptionRecorded(String spanName, Class<? extends Throwable> type) {
    List<String> types = onlySpan(spanName).getEvents().stream()
        .filter(event -> "exception".equals(event.getName()))
        .map(event -> event.getAttributes().get(stringKey("exception.type")))
        .collect(Collectors.toList());
    assertEquals(Collections.singletonList(type.getName()), types, () -> spanName + " exception events");
  }

  private SpanData onlySpan(String spanName) {
    List<ReadableSpan> matches = named(spanName);
    assertEquals(1, matches.size(), () -> "spans named " + spanName + ": " + spans);
    return matches.get(0).toSpanData();
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
