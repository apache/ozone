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

import static org.apache.hadoop.hdds.tracing.OzoneContextStorageProvider.MDC_SPAN_ID;
import static org.apache.hadoop.hdds.tracing.OzoneContextStorageProvider.MDC_TRACE_ID;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorage;
import io.opentelemetry.context.Scope;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Test for {@link OzoneContextStorageProvider}.
 */
public class TestOzoneContextStorageProvider {
  private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
  private static final String PARENT_SPAN_ID = "b7ad6b7169203331";
  private static final String CHILD_SPAN_ID = "00f067aa0ba902b7";

  @AfterEach
  public void clearMdc() {
    MDC.clear();
  }

  private static Span span(String spanId) {
    return Span.wrap(SpanContext.create(TRACE_ID, spanId, TraceFlags.getSampled(), TraceState.getDefault()));
  }

  @Test
  public void testProviderIsRegistered() {
    assertThat(ContextStorage.get()).isInstanceOf(OzoneContextStorageProvider.OzoneContextStorage.class);
  }

  @Test
  public void testMakeCurrentSetsAndRestoresMdc() {
    try (Scope ignored = span(PARENT_SPAN_ID).makeCurrent()) {
      assertThat(MDC.get(MDC_TRACE_ID)).isEqualTo(TRACE_ID);
      assertThat(MDC.get(MDC_SPAN_ID)).isEqualTo(PARENT_SPAN_ID);
      try (Scope ignoredInner = span(CHILD_SPAN_ID).makeCurrent()) {
        assertThat(MDC.get(MDC_SPAN_ID)).isEqualTo(CHILD_SPAN_ID);
      }
      assertThat(MDC.get(MDC_SPAN_ID)).isEqualTo(PARENT_SPAN_ID);
      try (Scope ignoredInner = Span.getInvalid().makeCurrent()) {
        assertThat(MDC.get(MDC_TRACE_ID)).isNull();
        assertThat(MDC.get(MDC_SPAN_ID)).isNull();
      }
      assertThat(MDC.get(MDC_TRACE_ID)).isEqualTo(TRACE_ID);
      assertThat(MDC.get(MDC_SPAN_ID)).isEqualTo(PARENT_SPAN_ID);
    }
    assertThat(MDC.get(MDC_TRACE_ID)).isNull();
    assertThat(MDC.get(MDC_SPAN_ID)).isNull();
  }

  @Test
  public void testStaleMdcIsClearedAfterClose() {
    // e.g. values inherited from the parent thread through log4j's InheritableThreadLocal MDC
    MDC.put(MDC_TRACE_ID, "stale-trace");
    MDC.put(MDC_SPAN_ID, "stale-span");
    try (Scope ignored = span(PARENT_SPAN_ID).makeCurrent()) {
      assertThat(MDC.get(MDC_SPAN_ID)).isEqualTo(PARENT_SPAN_ID);
    }
    assertThat(MDC.get(MDC_TRACE_ID)).isNull();
    assertThat(MDC.get(MDC_SPAN_ID)).isNull();
  }

  @Test
  public void testWrappedTaskOnOtherThread() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ExecutorService clearedExecutor = Executors.newSingleThreadExecutor();
    try {
      // a thread that explicitly cleared its MDC still gets the IDs from the next wrapped task
      clearedExecutor.submit(MDC::clear).get();
      Future<String> inSpan;
      Future<String[]> inSpanCleared;
      try (Scope ignored = span(PARENT_SPAN_ID).makeCurrent()) {
        inSpan = executor.submit(Context.current().wrap(() -> MDC.get(MDC_SPAN_ID)));
        inSpanCleared = clearedExecutor.submit(Context.current().wrap(
            () -> new String[] {MDC.get(MDC_TRACE_ID), MDC.get(MDC_SPAN_ID)}));
      }
      assertThat(inSpan.get()).isEqualTo(PARENT_SPAN_ID);
      assertThat(inSpanCleared.get()).containsExactly(TRACE_ID, PARENT_SPAN_ID);
      // after the wrapped task, the worker threads must not keep the span IDs
      assertThat(executor.submit(() -> MDC.get(MDC_SPAN_ID)).get()).isNull();
      assertThat(clearedExecutor.submit(() -> MDC.get(MDC_TRACE_ID)).get()).isNull();
      assertThat(clearedExecutor.submit(() -> MDC.get(MDC_SPAN_ID)).get()).isNull();
    } finally {
      executor.shutdownNow();
      clearedExecutor.shutdownNow();
    }
  }

  @Test
  public void testCreateActivatedSpanPutsIdsInMdc() {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", true);
    TracingUtil.initTracing("mdc", conf.getObject(TracingConfig.class));
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("mdc-span")) {
      SpanContext ctx = Span.current().getSpanContext();
      assertThat(MDC.get(MDC_TRACE_ID)).isEqualTo(ctx.getTraceId());
      assertThat(MDC.get(MDC_SPAN_ID)).isEqualTo(ctx.getSpanId());
    }
    assertThat(MDC.get(MDC_TRACE_ID)).isNull();
  }
}
