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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorage;
import io.opentelemetry.context.Scope;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Tests {@link TracingUtil#installMdcContextStorage()}.
 * When running together with other test classes, use fork mode per class.
 */
public class TestMdcContextStorageInstall {

  @BeforeAll
  public static void install() {
    TracingUtil.installMdcContextStorage();
    assertInstanceOf(TracingUtil.MdcContextStorage.class, ContextStorage.get(), "use fork mode per class");
  }

  @BeforeEach
  public void setUp() {
    TracingUtil.shutdownTracing();
    GlobalOpenTelemetry.resetForTest();
    MDC.clear();
  }

  @AfterEach
  public void tearDown() {
    TracingUtil.shutdownTracing();
    GlobalOpenTelemetry.resetForTest();
    MDC.clear();
  }

  private static void initTracing(boolean enabled) {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", enabled);
    TracingUtil.initTracing("mdc-install", conf);
  }

  private static void assertMdcMatchesCurrentSpan() {
    SpanContext current = Span.current().getSpanContext();
    assertTrue(current.isValid());
    assertEquals(current.getTraceId(), MDC.get(MDC_TRACE_ID));
    assertEquals(current.getSpanId(), MDC.get(MDC_SPAN_ID));
  }

  private static void assertMdcEmpty() {
    assertNull(MDC.get(MDC_TRACE_ID));
    assertNull(MDC.get(MDC_SPAN_ID));
  }

  @Test
  public void testActivatedSpanSetsMdc() {
    initTracing(true);
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("outer")) {
      assertMdcMatchesCurrentSpan();
      String traceId = MDC.get(MDC_TRACE_ID);
      String outerSpanId = MDC.get(MDC_SPAN_ID);
      try (TracingUtil.TraceCloseable ignoredInner = TracingUtil.createActivatedSpan("inner")) {
        assertMdcMatchesCurrentSpan();
        assertEquals(traceId, MDC.get(MDC_TRACE_ID));
        assertNotEquals(outerSpanId, MDC.get(MDC_SPAN_ID));
      }
      assertEquals(traceId, MDC.get(MDC_TRACE_ID));
      assertEquals(outerSpanId, MDC.get(MDC_SPAN_ID));
    }
    assertMdcEmpty();
  }

  @Test
  public void testImportedParentSpanSetsMdc() {
    initTracing(true);
    String carrier;
    String parentTraceId;
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("client")) {
      carrier = TracingUtil.exportCurrentSpan();
      parentTraceId = Span.current().getSpanContext().getTraceId();
    }
    assertMdcEmpty();

    Span server = TracingUtil.importAndCreateSpan("server", carrier);
    try (Scope ignored = server.makeCurrent()) {
      assertMdcMatchesCurrentSpan();
      assertEquals(parentTraceId, MDC.get(MDC_TRACE_ID));
    } finally {
      server.end();
    }
    assertMdcEmpty();
  }

  @Test
  public void testContextWrapPropagatesMdcToOtherThread() throws Exception {
    initTracing(true);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("span")) {
        String[] inTask = executor.submit(Context.current().wrap(
            () -> new String[] {MDC.get(MDC_TRACE_ID), MDC.get(MDC_SPAN_ID)})).get();
        assertEquals(MDC.get(MDC_TRACE_ID), inTask[0]);
        assertEquals(MDC.get(MDC_SPAN_ID), inTask[1]);
      }
      assertNull(executor.submit(() -> MDC.get(MDC_TRACE_ID)).get());
      assertNull(executor.submit(() -> MDC.get(MDC_SPAN_ID)).get());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testTracingDisabledLeavesMdcEmpty() {
    initTracing(false);
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("span")) {
      assertMdcEmpty();
    }
    assertMdcEmpty();
  }
}
