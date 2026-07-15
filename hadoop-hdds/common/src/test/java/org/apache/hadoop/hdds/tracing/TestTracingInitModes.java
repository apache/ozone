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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests tracing init for enabled, application-aware configs.
 */
public class TestTracingInitModes {

  /** Reset tracing state before each test. */
  @BeforeEach
  public void resetGlobalState() {
    TracingUtil.shutdownTracing();
    GlobalOpenTelemetry.resetForTest();
  }

  /** Tear down tracing state after each test. */
  @AfterEach
  public void cleanup() {
    TracingUtil.shutdownTracing();
    GlobalOpenTelemetry.resetForTest();
  }

  /** Build config with the two tracing flags set. */
  private static MutableConfigurationSource config(boolean enabled, boolean applicationAware) {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", enabled);
    conf.setBoolean("ozone.tracing.client.application-aware", applicationAware);
    return conf;
  }

  /** enabled=true: Ozone may start a new root span. */
  @Test
  public void testEnabledModeStartsRootSpans() {
    MutableConfigurationSource conf = config(true, true);
    TracingUtil.initTracing("enabled-svc", conf);
    assertTrue(TracingUtil.isTracingActive(conf));

    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("root")) {
      assertTrue(Span.current().getSpanContext().isValid(),
          "Enabled tracing should produce a valid root span");
    }
  }

  /** app-aware, no app tracer: active but no root span without a parent. */
  @Test
  public void testApplicationAwareWithoutGlobalDoesNotStartRoot() {
    MutableConfigurationSource conf = config(false, true);
    TracingUtil.initTracing("app-aware-svc", conf);
    assertTrue(TracingUtil.isTracingActive(conf));

    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("root")) {
      assertFalse(Span.current().getSpanContext().isValid(),
          "Application-aware mode must NOT manufacture a root span");
    }
  }

  /** app-aware + W3C parent on the wire: child span is created. */
  @Test
  public void testApplicationAwareExtendsExtractedContext() {
    SdkTracerProvider provider = SdkTracerProvider.builder().build();
    OpenTelemetrySdk external = OpenTelemetrySdk.builder().setTracerProvider(provider).build();

    Span external1 = external.getTracer("external").spanBuilder("external-root").startSpan();
    String parentCarrier;
    try (Scope ignored = external1.makeCurrent()) {
      parentCarrier = TracingUtil.exportCurrentSpan();
    } finally {
      external1.end();
    }
    provider.shutdown();
    assertFalse(parentCarrier.isEmpty(), "exported carrier should be non-empty");

    MutableConfigurationSource conf = config(false, true);
    TracingUtil.initTracing("app-aware-extract", conf);
    assertTrue(TracingUtil.isTracingActive(conf));

    Span child = TracingUtil.importAndCreateSpan("child", parentCarrier);
    try (Scope ignored = child.makeCurrent()) {
      assertTrue(child.getSpanContext().isValid(),
          "Application-aware mode should honor a wire-propagated parent context");
    } finally {
      child.end();
    }
  }

  /** app-aware + GlobalOpenTelemetry set: still no root span without a parent. */
  @Test
  public void testApplicationAwareAdoptsGlobalTracer() {
    SdkTracerProvider provider = SdkTracerProvider.builder().build();
    OpenTelemetrySdk appGlobal = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
    GlobalOpenTelemetry.set(appGlobal);

    MutableConfigurationSource conf = config(false, true);
    TracingUtil.initTracing("adopt-global", conf);
    assertTrue(TracingUtil.isTracingActive(conf));

    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("root")) {
      assertFalse(Span.current().getSpanContext().isValid(),
          "Application-aware (with adopted global) must NOT manufacture a root span");
    }
    provider.shutdown();
  }

  /** Both flags false: tracing is off. */
  @Test
  public void testOffModeIsInactive() {
    MutableConfigurationSource conf = config(false, false);
    TracingUtil.initTracing("off-svc", conf);
    assertFalse(TracingUtil.isTracingActive(conf),
        "With both flags false, tracing must be inactive");

    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("root")) {
      assertFalse(Span.current().getSpanContext().isValid(),
          "Inactive tracing must not produce a valid span");
    }
  }

  /** Reconfig app-aware → enabled: root spans allowed after reconfig. */
  @Test
  public void testReconfigureFromAppAwareToEnabled() {
    MutableConfigurationSource conf = config(false, true);
    TracingUtil.initTracing("reconfig", conf);
    assertTrue(TracingUtil.isTracingActive(conf));

    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("root")) {
      assertFalse(Span.current().getSpanContext().isValid(),
          "Application-aware mode must not manufacture a root span");
    }

    MutableConfigurationSource newConf = config(true, true);
    TracingUtil.reconfigureTracing("reconfig", newConf.getObject(TracingConfig.class));
    assertTrue(TracingUtil.isTracingActive(newConf));

    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("root")) {
      assertTrue(Span.current().getSpanContext().isValid(),
          "After reconfigure to enabled, a root span should be valid");
    }
  }

  /** OpenTelemetry.noop() is a singleton — used to detect a real app global. */
  @Test
  public void testNoopSingletonIdentity() {
    assertEquals(OpenTelemetry.noop(), OpenTelemetry.noop());
    assertNotEquals(OpenTelemetry.noop(),
        OpenTelemetrySdk.builder().setTracerProvider(SdkTracerProvider.builder().build()).build());
  }
}
