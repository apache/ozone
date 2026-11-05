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

import static org.apache.hadoop.hdds.tracing.TracingUtil.MdcContextStorage.MDC_SPAN_ID;
import static org.apache.hadoop.hdds.tracing.TracingUtil.MdcContextStorage.MDC_TRACE_ID;
import static org.apache.hadoop.hdds.tracing.TracingUtil.createProxy;
import static org.apache.hadoop.hdds.tracing.TracingUtil.exportCurrentSpan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorage;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.ReadableSpan;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.Service;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.ServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Test for {@link TracingUtil}.
 */
public class TestTracingUtil {
  private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
  private static final String PARENT_SPAN_ID = "b7ad6b7169203331";
  private static final String CHILD_SPAN_ID = "00f067aa0ba902b7";

  @AfterEach
  public void clearMdc() {
    MDC.clear();
  }

  private static Context spanContext(String spanId) {
    return Context.root().with(Span.wrap(
        SpanContext.create(TRACE_ID, spanId, TraceFlags.getSampled(), TraceState.getDefault())));
  }

  private static TracingUtil.MdcContextStorage mdcContextStorage() {
    return new TracingUtil.MdcContextStorage(ContextStorage.defaultStorage());
  }

  private static MutableConfigurationSource tracingEnabled() {
    MutableConfigurationSource config = new InMemoryConfigurationForTesting();
    config.setBoolean("ozone.tracing.enabled", true);
    return config;
  }

  private static String traceIdFromExportedCarrier(String parentCarrier) {
    TracingUtil.TextExtractor extractor = new TracingUtil.TextExtractor();
    String traceparent = extractor.get(parentCarrier, "traceparent");
    assertNotNull(traceparent, "carrier missing traceparent: " + parentCarrier);
    // W3C: 00-<traceId 32 hex>-<spanId 16 hex>-<flags 2 hex>
    String[] parts = traceparent.split("-", 4);
    assertEquals(4, parts.length, "bad traceparent: " + traceparent);
    return parts[1];
  }

  private static String parentSpanIdFromExportedCarrier(String parentCarrier) {
    TracingUtil.TextExtractor extractor = new TracingUtil.TextExtractor();
    String traceparent = extractor.get(parentCarrier, "traceparent");
    assertNotNull(traceparent, "carrier missing traceparent: " + parentCarrier);
    String[] parts = traceparent.split("-", 4);
    assertEquals(4, parts.length, "bad traceparent: " + traceparent);
    return parts[2];
  }

  private static SpanKind spanKind(Span span) {
    return ((ReadableSpan) span).getKind();
  }

  @Test
  public void testDefaultMethod() {
    Service subject = createProxy(new ServiceImpl(), Service.class,
        tracingEnabled());

    assertEquals("Hello default", subject.defaultMethod());
  }

  @Test
  public void testInitTracing() {
    TracingUtil.initTracing("testInitTracing", tracingEnabled());
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("initTracing")) {
      exportCurrentSpan();
    } catch (Exception e) {
      fail("Should not get exception");
    }
  }

  @Test
  public void testImportAndCreateSpanNullOrEmptyParent() {
    TracingUtil.initTracing("NoParentService", tracingEnabled().getObject(TracingConfig.class));
    for (String parent : new String[] {null, ""}) {
      Span span = TracingUtil.importAndCreateSpan("root-child", parent);
      try (Scope ignored = span.makeCurrent()) {
        assertTrue(Span.current().getSpanContext().isValid());
      } finally {
        span.end();
      }
    }
  }

  @Test
  public void testImportAndCreateSpanWithExportedParentContext() {
    TracingUtil.initTracing("import-w3c", tracingEnabled().getObject(TracingConfig.class));
    String parentCarrier;
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("parent")) {
      parentCarrier = TracingUtil.exportCurrentSpan();
    }
    assertFalse(parentCarrier.isEmpty(), "exported trace context should not be empty");
    Span child = TracingUtil.importAndCreateSpan("child", parentCarrier);
    try (Scope s = child.makeCurrent()) {
      assertTrue(Span.current().getSpanContext().isValid());
    } finally {
      child.end();
    }
  }

  @Test
  public void testExecuteInNewSpanUsesParentWhenContextHasActiveSpan() {
    TracingUtil.initTracing("nested-span", tracingEnabled().getObject(TracingConfig.class));
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("outer")) {
      TracingUtil.executeInNewSpan("inner", () ->
          assertTrue(Span.current().getSpanContext().isValid()));
    }
  }

  @Test
  public void testExecuteAsChildSpanUsesImportedParentContext() throws Exception {
    TracingUtil.initTracing("child-span", tracingEnabled().getObject(TracingConfig.class));
    String parentCarrier;
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("parent")) {
      parentCarrier = TracingUtil.exportCurrentSpan();
    }

    String expectedTraceId = traceIdFromExportedCarrier(parentCarrier);
    String exportedParentSpanId = parentSpanIdFromExportedCarrier(parentCarrier);

    TracingUtil.executeAsChildSpan("as-child", parentCarrier, () -> {
      SpanContext ctx = Span.current().getSpanContext();
      assertTrue(ctx.isValid());
      assertEquals(expectedTraceId, ctx.getTraceId(),
          "child should stay on the same trace as the exported parent context");
      assertNotEquals(exportedParentSpanId, ctx.getSpanId(),
          "child span id should differ from exported parent span id");
    });
  }

  @Test
  public void testExecuteAsChildSpanPropagatesException() throws Exception {
    TracingUtil.initTracing("child-ex", tracingEnabled().getObject(TracingConfig.class));
    String parentCarrier;
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("parent")) {
      parentCarrier = TracingUtil.exportCurrentSpan();
    }
    IOException thrown = assertThrows(IOException.class,
        () -> TracingUtil.executeAsChildSpan("failing", parentCarrier,
            () -> {
              throw new IOException("expected");
            }));
    assertEquals("expected", thrown.getMessage());
  }

  @Test
  public void testTextExtractorAsTextMapGetter() {
    TracingUtil.TextExtractor getter = new TracingUtil.TextExtractor();
    String carrier =
        "traceparent=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01;";
    assertTrue(getter.keys(carrier).iterator().hasNext());
    assertEquals(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        getter.get(carrier, "traceparent"));
  }

  @Test
  public void testTextExtractorEmptyAndMalformedEntries() {
    TracingUtil.TextExtractor ex = new TracingUtil.TextExtractor();
    ex.keys("");
    assertFalse(ex.keys("").iterator().hasNext());

    TracingUtil.TextExtractor ex2 = new TracingUtil.TextExtractor();
    String carrier = "notkeyvalue;traceparent=00-a-b-01;orphan=";
    assertTrue(ex2.keys(carrier).iterator().hasNext());
    assertEquals("00-a-b-01", ex2.get(carrier, "traceparent"));
    assertEquals("00-a-b-01", ex2.get(carrier, "traceparent"));
  }

  @Test
  public void testImportAndCreateSpanDefaultsToInternal() {
    TracingUtil.initTracing("serviceTestKind",
        tracingEnabled().getObject(TracingConfig.class));

    Span root = TracingUtil.importAndCreateSpan("rootspan", null);
    try {
      assertEquals(SpanKind.INTERNAL, spanKind(root));
    } finally {
      root.end();
    }

    String parent;
    try (TracingUtil.TraceCloseable ignored =
             TracingUtil.createActivatedSpan("parentSpan")) {
      parent = TracingUtil.exportCurrentSpan();
    }
    Span child = TracingUtil.importAndCreateSpan("childSpan", parent);
    try {
      assertEquals(SpanKind.INTERNAL, spanKind(child));
    } finally {
      child.end();
    }
  }

  @Test
  public void testImportAndCreateSpanUsesExplicitKind() {
    TracingUtil.initTracing("span-kind-explicit",
        tracingEnabled().getObject(TracingConfig.class));

    Span server = TracingUtil.importAndCreateSpan(
        "server", null, SpanKind.SERVER);
    try {
      assertEquals(SpanKind.SERVER, spanKind(server));
    } finally {
      server.end();
    }
  }

  @Test
  public void testMdcNotChangedWithoutInstall() {
    assertFalse(ContextStorage.get() instanceof TracingUtil.MdcContextStorage);
    TracingUtil.initTracing("mdc-not-installed", tracingEnabled().getObject(TracingConfig.class));
    MDC.put(MDC_TRACE_ID, "app-trace");
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("span")) {
      assertTrue(Span.current().getSpanContext().isValid());
      assertEquals("app-trace", MDC.get(MDC_TRACE_ID));
      assertNull(MDC.get(MDC_SPAN_ID));
    }
    try (Scope ignored = Span.getInvalid().makeCurrent()) {
      assertEquals("app-trace", MDC.get(MDC_TRACE_ID));
    }
    assertEquals("app-trace", MDC.get(MDC_TRACE_ID));
  }

  @Test
  public void testInstallAfterContextInitializedHasNoEffect() {
    ContextStorage storage = ContextStorage.get();
    Context.current();
    TracingUtil.installMdcContextStorage();
    assertSame(storage, ContextStorage.get());
    try (Scope ignored = spanContext(PARENT_SPAN_ID).makeCurrent()) {
      assertNull(MDC.get(MDC_TRACE_ID));
    }
  }

  @Test
  public void testMdcContextStorageSetsAndRestoresMdc() {
    TracingUtil.MdcContextStorage storage = mdcContextStorage();
    try (Scope ignored = storage.attach(spanContext(PARENT_SPAN_ID))) {
      assertEquals(TRACE_ID, MDC.get(MDC_TRACE_ID));
      assertEquals(PARENT_SPAN_ID, MDC.get(MDC_SPAN_ID));
      try (Scope ignoredInner = storage.attach(spanContext(CHILD_SPAN_ID))) {
        assertEquals(CHILD_SPAN_ID, MDC.get(MDC_SPAN_ID));
      }
      assertEquals(PARENT_SPAN_ID, MDC.get(MDC_SPAN_ID));
      try (Scope ignoredInner = storage.attach(Context.root().with(Span.getInvalid()))) {
        assertNull(MDC.get(MDC_TRACE_ID));
        assertNull(MDC.get(MDC_SPAN_ID));
      }
      assertEquals(TRACE_ID, MDC.get(MDC_TRACE_ID));
      assertEquals(PARENT_SPAN_ID, MDC.get(MDC_SPAN_ID));
    }
    assertNull(MDC.get(MDC_TRACE_ID));
    assertNull(MDC.get(MDC_SPAN_ID));
  }

  @Test
  public void testMdcContextStorageClearsStaleMdc() {
    MDC.put(MDC_TRACE_ID, "stale-trace");
    MDC.put(MDC_SPAN_ID, "stale-span");
    try (Scope ignored = mdcContextStorage().attach(spanContext(PARENT_SPAN_ID))) {
      assertEquals(PARENT_SPAN_ID, MDC.get(MDC_SPAN_ID));
    }
    assertNull(MDC.get(MDC_TRACE_ID));
    assertNull(MDC.get(MDC_SPAN_ID));
  }

  @Test
  public void testMdcContextStorageOnOtherThread() throws Exception {
    TracingUtil.MdcContextStorage storage = mdcContextStorage();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      String[] inSpan = executor.submit(() -> {
        try (Scope ignored = storage.attach(spanContext(PARENT_SPAN_ID))) {
          return new String[] {MDC.get(MDC_TRACE_ID), MDC.get(MDC_SPAN_ID)};
        }
      }).get();
      assertEquals(TRACE_ID, inSpan[0]);
      assertEquals(PARENT_SPAN_ID, inSpan[1]);
      assertNull(executor.submit(() -> MDC.get(MDC_TRACE_ID)).get());
      assertNull(executor.submit(() -> MDC.get(MDC_SPAN_ID)).get());
    } finally {
      executor.shutdownNow();
    }
  }
}
