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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests client application-aware tracing: when Ozone OTLP tracing is off but an
 * active span exists, spans use {@link GlobalOpenTelemetry}; otherwise the
 * static Ozone {@link TracingUtil} tracer is used, or span creation is skipped.
 */
public class TestTracingUtilClientApplicationAware {

  private static final String OZONE_CLIENT_TRACER_SCOPE = "org.apache.hadoop.ozone.client";

  private final List<SpanData> exportedSpans = new CopyOnWriteArrayList<>();
  private SdkTracerProvider testSdkTracerProvider;

  @BeforeEach
  public void setUp() {
    GlobalOpenTelemetry.resetForTest();
    TracingUtil.reconfigureTracing("client", clientTracingConfig(false, true));
  }

  @AfterEach
  public void tearDown() {
    if (testSdkTracerProvider != null) {
      testSdkTracerProvider.shutdown();
      testSdkTracerProvider = null;
    }
    GlobalOpenTelemetry.resetForTest();
    exportedSpans.clear();
    TracingUtil.reconfigureTracing("client", clientTracingConfig(false, true));
  }

  private static TracingConfig clientTracingConfig(boolean tracingEnabled,
                                                   boolean clientApplicationAware) {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", tracingEnabled);
    conf.setBoolean("ozone.tracing.client.application-aware", clientApplicationAware);
    return conf.getObject(TracingConfig.class);
  }

  /**
   * Registers a global SDK that records ended spans into {@link #exportedSpans}.
   */
  private void registerGlobalTestSdk() {
    SpanExporter exporter = new SpanExporter() {
      @Override
      public CompletableResultCode export(Collection<SpanData> spans) {
        exportedSpans.addAll(spans);
        return CompletableResultCode.ofSuccess();
      }

      @Override
      public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
      }

      @Override
      public CompletableResultCode shutdown() {
        return CompletableResultCode.ofSuccess();
      }
    };
    testSdkTracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
        .build();
    GlobalOpenTelemetry.set(
        OpenTelemetrySdk.builder()
            .setTracerProvider(testSdkTracerProvider)
            .build());
  }

  private boolean hasExportedSpanNamed(String name) {
    return exportedSpans.stream().anyMatch(s -> name.equals(s.getName()));
  }

  @Test
  public void testOzoneTracingOnUsesStaticTracerNotGlobalOpenTelemetry() throws Exception {
    registerGlobalTestSdk();
    TracingUtil.reconfigureTracing("client", clientTracingConfig(true, true));

    TracingUtil.executeInNewSpan("ozone-client-span", () -> {
      assertThat(Span.current().getSpanContext().isValid()).isTrue();
    });

    assertThat(hasExportedSpanNamed("ozone-client-span"))
        .withFailMessage("When Ozone client tracing is on, spans must use the Ozone SDK tracer, "
            + "not GlobalOpenTelemetry, so the test global exporter must not see them.")
        .isFalse();
  }

  @Test
  public void testOzoneTracingOffWithAppAwareAndParentUsesGlobalTracer() throws Exception {
    registerGlobalTestSdk();
    TracingUtil.reconfigureTracing("client", clientTracingConfig(false, true));

    Tracer appTracer = GlobalOpenTelemetry.get().getTracer("test-app");
    Span parent = appTracer.spanBuilder("parent").startSpan();
    final String parentSpanId = parent.getSpanContext().getSpanId();
    try (Scope ignored = parent.makeCurrent()) {
      TracingUtil.executeInNewSpan("child-from-global", () ->
          assertThat(Span.current().getSpanContext().isValid()).isTrue());
    } finally {
      parent.end();
    }
    testSdkTracerProvider.forceFlush();

    assertThat(hasExportedSpanNamed("child-from-global"))
        .withFailMessage("With Ozone tracing off, client application-aware on, and a valid parent span, "
            + "the child must be created with GlobalOpenTelemetry.")
        .isTrue();

    // Child span must use the Ozone client scope on GlobalOpenTelemetry (not the test-app parent scope).
    assertThat(exportedSpans)
        .filteredOn(s -> "child-from-global".equals(s.getName()))
        .singleElement()
        .extracting(s -> s.getInstrumentationScopeInfo().getName())
        .isEqualTo(OZONE_CLIENT_TRACER_SCOPE);

    // Child must link to the application parent span in the same trace.
    assertThat(exportedSpans)
        .filteredOn(s -> "child-from-global".equals(s.getName()))
        .singleElement()
        .extracting(SpanData::getParentSpanId)
        .isEqualTo(parentSpanId);
  }

  @Test
  public void testOzoneTracingOffApplicationAwareFalseSkipsSpan() throws Exception {
    registerGlobalTestSdk();
    TracingUtil.reconfigureTracing("client", clientTracingConfig(false, false));

    Tracer appTracer = GlobalOpenTelemetry.get().getTracer("test-app");
    Span parent = appTracer.spanBuilder("parent").startSpan();
    try (Scope ignored = parent.makeCurrent()) {
      TracingUtil.executeInNewSpan("should-not-exist", () -> {
        assertEquals(
            parent.getSpanContext(),
            Span.current().getSpanContext(),
            "When application-aware is false, TracingUtil should not create a child span; "
                + "the active span should remain the application parent.");
      });
    } finally {
      parent.end();
    }
    testSdkTracerProvider.forceFlush();

    assertThat(hasExportedSpanNamed("should-not-exist")).isFalse();
  }

  @Test
  public void testOzoneTracingOffNoParentContextSkipsSpan() throws Exception {
    registerGlobalTestSdk();
    TracingUtil.reconfigureTracing("client", clientTracingConfig(false, true));

    TracingUtil.executeInNewSpan("no-parent-bypass", () ->
        assertThat(Span.current().getSpanContext().isValid())
            .withFailMessage("Without a valid parent, span creation should be bypassed.")
            .isFalse());
    testSdkTracerProvider.forceFlush();

    assertThat(hasExportedSpanNamed("no-parent-bypass")).isFalse();
  }

  @Test
  public void testCreateActivatedSpanBypassWhenConditionsNotMet() throws Exception {
    registerGlobalTestSdk();
    TracingUtil.reconfigureTracing("client", clientTracingConfig(false, false));

    try (TracingUtil.TraceCloseable closeable =
             TracingUtil.createActivatedSpan("activated-bypass")) {
      assertNotNull(closeable);
    }
    if (testSdkTracerProvider != null) {
      testSdkTracerProvider.forceFlush();
    }
    assertThat(hasExportedSpanNamed("activated-bypass")).isFalse();
  }
}
