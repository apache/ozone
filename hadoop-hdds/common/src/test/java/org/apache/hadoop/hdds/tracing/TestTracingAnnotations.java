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

import static org.apache.hadoop.hdds.tracing.TracingUtil.createClientProxy;
import static org.apache.hadoop.hdds.tracing.TracingUtil.createProxy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.Service;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.ServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for Tracing related annotations.
 * {@link SkipTracing} , {@link InternalSpanKind}.
 */
public class TestTracingAnnotations {

  @BeforeEach
  public void resetGlobalState() {
    TracingUtil.shutdownTracing();
    GlobalOpenTelemetry.resetForTest();
  }

  /**
   * Tear down tracing state after each test.
   */
  @AfterEach
  public void cleanup() {
    TracingUtil.shutdownTracing();
    GlobalOpenTelemetry.resetForTest();
  }

  /**
   * Puts a real GlobalOpenTelemetry in place with no exporter, so tests stay offline.
   */
  private static void installNoExportGlobalOpenTelemetry() {
    SdkTracerProvider provider =
        SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(SpanExporter.noop())).build();
    OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
    GlobalOpenTelemetry.set(sdk);
  }

  private static MutableConfigurationSource tracingEnabled() {
    MutableConfigurationSource config = new InMemoryConfigurationForTesting();
    config.setBoolean("ozone.tracing.enabled", true);
    return config;
  }

  @Test
  public void testSkipTracingNoSpan() {
    TracingUtil.initTracing("TestService", tracingEnabled());
    ServiceImpl impl = new ServiceImpl();
    Service serviceProxy = createProxy(impl, Service.class, tracingEnabled());

    serviceProxy.skippedMethod();
    assertFalse(impl.wasSpanActive(), "Span should NOT be created for @SkipTracing methods.");
  }

  @Test
  public void testSkipTracingExceptionUnwrapped() {
    TracingUtil.initTracing("TestService", tracingEnabled());
    ServiceImpl impl = new ServiceImpl();
    Service serviceProxy = createProxy(impl, Service.class, tracingEnabled());

    IOException ex = assertThrows(IOException.class,
        () -> serviceProxy.throwingMethod());
    assertEquals("Original Exception", ex.getMessage());
    assertFalse(impl.wasSpanActive(), "Span should NOT have been created for a @SkipTracing throwing method.");
  }

  @Test
  public void testProxyNormalVsSkipped() {
    installNoExportGlobalOpenTelemetry();
    TracingUtil.initTracing("TestService", tracingEnabled());
    ServiceImpl impl = new ServiceImpl();
    Service serviceProxy = createProxy(impl, Service.class, tracingEnabled());

    serviceProxy.normalMethod();
    assertTrue(impl.wasSpanActive(), "Normal method should have an active span.");
  }

  @Test
  public void testInternalSpanKind() {
    installNoExportGlobalOpenTelemetry();
    TracingUtil.initTracing("TestService", tracingEnabled());
    ServiceImpl impl = new ServiceImpl();
    Service serviceProxy = createClientProxy(impl, Service.class, tracingEnabled());
    serviceProxy.localMethod();
    assertTrue(impl.wasSpanActive(), "Local method should have an active span.");
    assertEquals(SpanKind.INTERNAL, impl.lastSpanKind(), "@InternalSpanKind method should use INTERNAL.");
  }

  @Test
  public void testInterfaceMethodAnnotationIsIgnored() {
    try (TestSpanCollector spans = TestSpanCollector.install("TestService")) {
      Service serviceProxy = createClientProxy(new ServiceImpl(), Service.class, tracingEnabled());
      serviceProxy.annotatedOnlyOnInterface();
      spans.assertKind("Service.annotatedOnlyOnInterface", SpanKind.CLIENT);
    }
  }

  @Test
  public void testLambdaDelegateUsesDefaultKind() {
    try (TestSpanCollector spans = TestSpanCollector.install("TestService")) {
      LocalService serviceProxy = createClientProxy(() -> "local", LocalService.class, tracingEnabled());
      assertEquals("local", serviceProxy.call());
      spans.assertKind("LocalService.call", SpanKind.CLIENT);
    }
  }

  @Test
  public void testImplClassAnnotationOverridesKind() {
    try (TestSpanCollector spans = TestSpanCollector.install("TestService")) {
      LocalService serviceProxy = createClientProxy(new LocalServiceImpl(), LocalService.class, tracingEnabled());
      assertEquals("local", serviceProxy.call());
      spans.assertKind("LocalService.call", SpanKind.INTERNAL);
    }
  }

  @Test
  public void testCreateProxyKinds() {
    try (TestSpanCollector spans = TestSpanCollector.install("TestService")) {
      createProxy(new ServiceImpl(), Service.class, tracingEnabled()).normalMethod();
      spans.assertKind("Service.normalMethod", SpanKind.INTERNAL);
    }
    try (TestSpanCollector spans = TestSpanCollector.install("TestService")) {
      createClientProxy(new ServiceImpl(), Service.class, tracingEnabled()).normalMethod();
      spans.assertKind("Service.normalMethod", SpanKind.CLIENT);
    }
  }

  /**
   * Interface with no tracing annotations.
   */
  public interface LocalService {
    String call();
  }

  /**
   * Implementation whose methods are all in-process.
   */
  @InternalSpanKind
  public static class LocalServiceImpl implements LocalService {
    @Override
    public String call() {
      return "local";
    }
  }

}
