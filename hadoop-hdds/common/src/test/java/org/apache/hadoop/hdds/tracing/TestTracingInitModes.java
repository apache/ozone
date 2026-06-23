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

import static org.apache.hadoop.hdds.tracing.TracingUtil.createProxy;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.Service;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.ServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for application-aware and ozone tracing init modes.
 */
public class TestTracingInitModes {

  private OpenTelemetrySdk appSdk;

  @AfterEach
  public void tearDown() {
    TracingUtil.reconfigureTracing("reset", tracingConfig(false, false));
    if (appSdk != null) {
      appSdk.getSdkTracerProvider().shutdown();
      appSdk = null;
    }
    GlobalOpenTelemetry.resetForTest();
  }

  private static MutableConfigurationSource conf(boolean enabled, boolean appAware) {
    MutableConfigurationSource c = new InMemoryConfigurationForTesting();
    c.setBoolean("ozone.tracing.enabled", enabled);
    c.setBoolean("ozone.tracing.client.application-aware", appAware);
    return c;
  }

  private static TracingConfig tracingConfig(boolean enabled, boolean appAware) {
    return conf(enabled, appAware).getObject(TracingConfig.class);
  }

  private void registerAppGlobalTracer() {
    SdkTracerProvider provider = SdkTracerProvider.builder().build();
    appSdk = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
    GlobalOpenTelemetry.set(appSdk);
  }

  @Test
  public void testApplicationAwareClientJoinsExistingTraceOnly() throws Exception {
    registerAppGlobalTracer();
    MutableConfigurationSource config = conf(false, true);

    assertThat(TracingUtil.shouldInstallTraceProxy(config))
        .as("app-aware + global OTel should enable trace proxy")
        .isTrue();
    TracingUtil.initClientTracing("client", config);

    ServiceImpl impl = new ServiceImpl();
    Service proxy = createProxy(impl, Service.class, config);
    assertThat(proxy).isNotSameAs(impl);

    proxy.normalMethod();
    assertThat(impl.wasSpanActive())
        .as("application-aware client must not root-trace alone")
        .isFalse();

    Span appSpan = GlobalOpenTelemetry.get().getTracer("app").spanBuilder("app-op").startSpan();
    try (Scope ignored = appSpan.makeCurrent()) {
      impl = new ServiceImpl();
      proxy = createProxy(impl, Service.class, config);
      proxy.normalMethod();
      assertThat(impl.wasSpanActive())
          .as("application-aware client should join active app trace")
          .isTrue();
    } finally {
      appSpan.end();
    }
  }

  @Test
  public void testClientOffUnlessOzoneTracingEnabled() throws Exception {
    MutableConfigurationSource offConfig = conf(false, true);
    TracingUtil.initClientTracing("client", offConfig);

    ServiceImpl impl = new ServiceImpl();
    assertThat(createProxy(impl, Service.class, offConfig)).isSameAs(impl);
    TracingUtil.executeInNewSpan("alone", () ->
        assertThat(Span.current().getSpanContext().isValid())
            .as("no span should be created when tracing is fully off")
            .isFalse());
    assertThat(TracingUtil.shouldInstallTraceProxy(offConfig))
        .as("no global OTel and tracing disabled → no proxy")
        .isFalse();

    tearDown();

    MutableConfigurationSource onConfig = conf(true, true);
    TracingUtil.initClientTracing("client", onConfig);
    assertThat(TracingUtil.shouldInstallTraceProxy(onConfig))
        .as("ozone tracing enabled → proxy should be installed")
        .isTrue();

    Span root = TracingUtil.importAndCreateSpan("root", null);
    try (Scope ignored = root.makeCurrent()) {
      assertThat(Span.current().getSpanContext().isValid())
          .as("OZONE mode should allow root spans without incoming context")
          .isTrue();
    } finally {
      root.end();
    }
  }

  @Test
  public void testServiceApplicationAwarePassThroughVsOzoneRoots() throws Exception {
    TracingUtil.initServiceTracing("parent-export", tracingConfig(true, false));
    String parentCarrier;
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("parent")) {
      parentCarrier = TracingUtil.exportCurrentSpan();
    }
    assertThat(parentCarrier)
        .as("OZONE mode should export a non-empty W3C trace context")
        .isNotEmpty();
    tearDown();

    TracingUtil.initServiceTracing("scm", tracingConfig(false, true));
    assertThat(TracingUtil.shouldInstallTraceProxy(conf(false, true)))
        .as("server APPLICATION pass-through alone must not wrap client RPC proxies")
        .isFalse();

    assertThat(TracingUtil.importAndCreateSpan("server-op", "").getSpanContext().isValid())
        .as("APPLICATION mode must not start root spans without client trace context")
        .isFalse();

    Span child = TracingUtil.importAndCreateSpan("server-op", parentCarrier);
    assertThat(child.getSpanContext().isValid())
        .as("APPLICATION mode should create a child span from W3C carrier")
        .isTrue();
    try (Scope ignored = child.makeCurrent()) {
      assertThat(Span.current().getSpanContext().isValid())
          .as("imported child span should be active in context")
          .isTrue();
    } finally {
      child.end();
    }

    tearDown();

    TracingUtil.initServiceTracing("scm", tracingConfig(true, false));
    Span root = TracingUtil.importAndCreateSpan("root", null);
    assertThat(root.getSpanContext().isValid())
        .as("OZONE mode should allow root spans without incoming context")
        .isTrue();
    try (Scope ignored = root.makeCurrent()) {
      assertThat(Span.current().getSpanContext().isValid()).isTrue();
    } finally {
      root.end();
    }
  }

  @Test
  public void testReconfigureSwitchesToApplicationPassThrough() throws Exception {
    TracingUtil.initServiceTracing("om", tracingConfig(true, true));
    Span root = TracingUtil.importAndCreateSpan("before", null);
    assertThat(root.getSpanContext().isValid())
        .as("OZONE mode should allow root spans before reconfigure")
        .isTrue();
    root.end();

    TracingUtil.reconfigureTracing("om", tracingConfig(false, true));

    Span after = TracingUtil.importAndCreateSpan("after", "");
    assertThat(after.getSpanContext().isValid())
        .as("reconfigure should use initServiceTracing → APPLICATION pass-through, not root traces")
        .isFalse();
  }

  @Test
  public void testTracingConfigApplicationAwareDefault() {
    TracingConfig cfg = new InMemoryConfigurationForTesting()
        .getObject(TracingConfig.class);
    assertThat(cfg.isClientApplicationAware())
        .as("ozone.tracing.client.application-aware defaults to true")
        .isTrue();
  }
}
