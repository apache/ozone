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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.ratis.util.function.CheckedRunnable;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to collect all the tracing helper methods.
 */
public final class TracingUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TracingUtil.class);
  private static final String NULL_SPAN_AS_STRING = "";

  private static volatile boolean isInit = false;
  private static volatile boolean tracingEnabled;
  private static volatile boolean applicationAware;
  private static Tracer tracer = OpenTelemetry.noop().getTracer("noop");
  private static volatile SdkTracerProvider sdkTracerProvider;
  private static BatchSpanProcessor batchSpanProcessor;
  public static final String GLOBAL_TRACER_NAME = "ozone";

  private TracingUtil() {
  }

  /**
   * Initialize the tracing with the given service name.
   */
  public static synchronized void initTracing(
      String serviceName, TracingConfig tracingConfig) {
    initTracing(serviceName, tracingConfig, false);
  }

  private static synchronized void initTracing(
      String serviceName, TracingConfig tracingConfig, boolean isReconfig) {
    if (isInit) {
      return;
    }

    try {
      initialize(serviceName, tracingConfig, isReconfig);
      isInit = true;
      LOG.info("Initialized tracing service: {} (enabled={}, applicationAware={})",
          serviceName, tracingEnabled, applicationAware);
    } catch (Exception e) {
      LOG.error("Failed to initialize tracing", e);
    }
  }

  /**
   * Receives serviceName and configurationSource.
   * Delegates tracing initiation to {@link #initTracing(String, TracingConfig)}.
   */
  public static synchronized void initTracing(
      String serviceName, ConfigurationSource conf) {
    initTracing(serviceName, conf.getObject(TracingConfig.class));
  }

  /**
   * Shuts down and re-initializes tracing.
   * Called after tracing-related keys are reconfigured on OM/SCM/DN.
   */
  public static synchronized void reconfigureTracing(
      String serviceName, TracingConfig tracingConfig) {
    shutdownTracing();
    initTracing(serviceName, tracingConfig, true);
  }

  /**
   * Drain the BatchSpanProcessor queue without shutting down.
   * Call from short-lived CLIs before the JVM exits.
   */
  public static synchronized void flushTracing() {
    if (batchSpanProcessor == null) {
      return;
    }
    try {
      // Best-effort: wait up to 10s for span export; remaining spans may be dropped on exit.
      batchSpanProcessor.forceFlush().join(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Tracing flush: forceFlush failed", e);
    }
  }

  /**
   * This function initializes tracing, runs the command in a span, and exports spans before returning for CLI spans.
   */
  public static <R, E extends Exception> R execute(
      String serviceName,
      String spanName,
      ConfigurationSource conf,
      CheckedSupplier<R, E> supplier) throws E {
    return execute(serviceName, spanName, SpanKind.INTERNAL, conf, supplier);
  }

  public static <R, E extends Exception> R execute(
      String serviceName,
      String spanName,
      SpanKind spanKind,
      ConfigurationSource conf,
      CheckedSupplier<R, E> supplier) throws E {
    initTracing(serviceName, conf);
    try {
      return executeInNewSpan(spanName, spanKind, supplier);
    } finally {
      flushTracing();
    }
  }

  static void shutdownTracing() {
    try {
      if (sdkTracerProvider != null) {
        sdkTracerProvider.shutdown().join(10L, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      LOG.warn("Tracing shutdown failed", e);
    } finally {
      sdkTracerProvider = null;
      batchSpanProcessor = null;
      tracer = OpenTelemetry.noop().getTracer("noop");
      tracingEnabled = false;
      applicationAware = false;
      isInit = false;
    }
  }

  private static void initialize(String serviceName, TracingConfig cfg, boolean isReconfig) {
    tracingEnabled = cfg.isTracingEnabled();
    applicationAware = cfg.isApplicationAware();

    if (!tracingEnabled && !applicationAware) {
      tracer = OpenTelemetry.noop().getTracer(GLOBAL_TRACER_NAME);
      return;
    }

    // Server reconfiguration reprioritizes Ozone's SDK over any adopted global,
    // and re-registers the global name and tracer.
    if (isReconfig && tracingEnabled) {
      initOzoneSdk(serviceName, cfg, true);
      return;
    }

    // Global first: adopt an application-registered GlobalOpenTelemetry when present.
    if (GlobalOpenTelemetry.isSet() && isRealGlobal(GlobalOpenTelemetry.get())) {
      tracer = GlobalOpenTelemetry.get().getTracer(GLOBAL_TRACER_NAME);
      LOG.info("Tracing: adopted application GlobalOpenTelemetry");
      return;
    }

    // No app-supplied global — build Ozone's SDK and always register it as the JVM global,
    // so any co-resident library observes the same tracer whenever tracing is valid.
    initOzoneSdk(serviceName, cfg, true);
  }

  private static void initOzoneSdk(String serviceName, TracingConfig cfg, boolean registerGlobal) {
    SdkTracerProvider tracerProvider = buildSdkTracerProvider(serviceName, cfg);
    try {
      OpenTelemetrySdk sdk;
      if (registerGlobal) {
        sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
        if (!GlobalOpenTelemetry.isSet() || !isRealGlobal(GlobalOpenTelemetry.get())) {
          GlobalOpenTelemetry.set(sdk);
        }
        tracer = GlobalOpenTelemetry.get().getTracer(GLOBAL_TRACER_NAME);
      } else {
        sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .build();
        tracer = sdk.getTracer(GLOBAL_TRACER_NAME);
      }
      sdkTracerProvider = tracerProvider;
    } catch (RuntimeException e) {
      tracerProvider.shutdown();
      batchSpanProcessor = null;
      throw e;
    }
  }

  /**
   * Distinguish an application-registered GlobalOpenTelemetry from the OTel built-in noop.
   * OpenTelemetry.noop() returns a singleton, so identity comparison is sufficient.
   */
  private static boolean isRealGlobal(OpenTelemetry global) {
    return global != null && global != OpenTelemetry.noop();
  }

  /**
   * Whether to wrap the delegate in a JDK tracing proxy.
   * Fully enabled: always wrap. App-aware: wrap only when parent span is valid
   */
  private static boolean shouldCreateTracingProxy(ConfigurationSource conf) {
    TracingConfig tc = conf.getObject(TracingConfig.class);
    if (tc.isTracingEnabled()) {
      return true;
    }
    if (!tc.isApplicationAware() || !hasUsableTracer()) {
      return false;
    }
    return Span.current().getSpanContext().isValid();
  }

  /**
   * Build the SdkTracerProvider using the configured OTLP endpoint and sampler.
   * Extracted so both enabled and application-aware modes share exporter/sampler setup.
   */
  private static SdkTracerProvider buildSdkTracerProvider(
      String serviceName, TracingConfig tracingConfig) {
    //Fetch and log the right tracing parameters based on config, environment variable and default value priority.
    String otelEndPoint = tracingConfig.getTracingEndpoint();
    double samplerRatio = tracingConfig.getTraceSamplerRatio();
    LOG.info("Sampling Trace Config = '{}'", samplerRatio);
    String spanSamplingConfig = tracingConfig.getSpanSampling();
    LOG.info("Sampling Span Config = '{}'", spanSamplingConfig);

    Map<String, LoopSampler> spanMap = parseSpanSamplingConfig(spanSamplingConfig);

    Resource resource = Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), serviceName));
    SpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
        .setEndpoint(otelEndPoint)
        .build();

    batchSpanProcessor = BatchSpanProcessor.builder(spanExporter).build();

    // Choose sampler based on span sampling config. If it is empty use trace based sampling only.
    // else use custom SpanSampler.
    Sampler sampler;
    if (spanMap.isEmpty()) {
      sampler = Sampler.traceIdRatioBased(samplerRatio);
    } else {
      Sampler rootSampler = Sampler.traceIdRatioBased(samplerRatio);
      sampler = new SpanSampler(rootSampler, spanMap);
    }

    return SdkTracerProvider.builder()
        .addSpanProcessor(batchSpanProcessor)
        .setResource(resource)
        .setSampler(sampler)
        .build();
  }

  private static boolean canStartSpanWithoutParent() {
    return tracingEnabled;
  }

  /**
   * Export the active tracing span as a string.
   * When tracing is disabled, not initialized, or no valid span is in scope,
   * {@link Span#current()} returns an invalid span; there is nothing to encode and this
   * method returns an empty string. Callers must accept that as "no context to propagate".
   *
   * @return encoded W3C trace context, or empty string if there is no valid active span.
   */
  public static String exportCurrentSpan() {
    Span currentSpan = Span.current();
    if (!currentSpan.getSpanContext().isValid()) {
      return NULL_SPAN_AS_STRING;
    }
    StringBuilder builder = new StringBuilder();
    W3CTraceContextPropagator propagator = W3CTraceContextPropagator.getInstance();
    propagator.inject(Context.current(), builder,
        (carrier, key, value) -> carrier.append(key).append('=').append(value).append(';'));
    return builder.toString();
  }

  /**
   * Create a new scope and use the imported span as the parent.
   * Short-circuits to an invalid span when there is no usable tracer:
   *   - tracing was never initialized (tracer is still the noop), or
   *   - application-aware mode is on but no app-supplied SDK was adopted (sdkTracerProvider == null
   *     and the current tracer is the noop).
   *
   * @param name          name of the newly created scope
   * @param encodedParent Encoded parent span (could be null or empty)
   * @return Tracing scope.
   */
  public static Span importAndCreateSpan(String name, String encodedParent) {
    return importAndCreateSpan(name, encodedParent, SpanKind.INTERNAL);
  }

  public static Span importAndCreateSpan(String name, String encodedParent,
                                         SpanKind spanKind) {
    if (!hasUsableTracer()) {
      return Span.getInvalid();
    }
    if (encodedParent == null || encodedParent.isEmpty()) {
      if (!canStartSpanWithoutParent()) {
        return Span.getInvalid();
      }
      return tracer.spanBuilder(name)
          .setNoParent()
          .setSpanKind(spanKind)
          .startSpan();
    }

    W3CTraceContextPropagator propagator = W3CTraceContextPropagator.getInstance();
    Context extract = propagator.extract(Context.current(), encodedParent, new TextExtractor());
    return tracer.spanBuilder(name)
        .setParent(extract)
        .setSpanKind(spanKind)
        .startSpan();
  }

  /**
   * True when the current tracer can actually build spans — an Ozone-owned SDK is configured,
   * or an adopted GlobalOpenTelemetry provides a non-noop tracer.
   */
  private static boolean hasUsableTracer() {
    if (sdkTracerProvider != null) {
      return true;
    }
    return GlobalOpenTelemetry.isSet() && isRealGlobal(GlobalOpenTelemetry.get());
  }

  /**
   * Creates a proxy of the implementation and trace all the method calls.
   *
   * @param delegate the original class instance
   * @param itf the interface which should be implemented by the proxy
   * @param <T> the type of the interface
   * @param conf configuration
   *
   * @return A new interface which implements interface but delegate all the
   * calls to the delegate and also enables tracing.
   */
  public static <T> T createProxy(
      T delegate, Class<T> itf, ConfigurationSource conf) {
    if (!shouldCreateTracingProxy(conf)) {
      return delegate;
    }
    Class<?> aClass = delegate.getClass();
    return itf.cast(Proxy.newProxyInstance(aClass.getClassLoader(),
        new Class<?>[] {itf},
        new TraceAllMethod<>(delegate, itf.getSimpleName())));
  }

  public static boolean isTracingEnabled(ConfigurationSource conf) {
    return conf.getObject(TracingConfig.class).isTracingEnabled();
  }

  /**
   * Returns true when tracing may actually produce spans:
   *   - fully enabled (ozone.tracing.enabled=true), or
   *   - application-aware AND an SDK is configured (either Ozone-owned or an adopted global);
   *     without an SDK, application-aware is a passthrough that would emit noop spans anyway.
   */
  public static boolean isTracingActive(ConfigurationSource conf) {
    TracingConfig tc = conf.getObject(TracingConfig.class);
    if (tc.isTracingEnabled()) {
      return true;
    }
    return tc.isApplicationAware() && hasUsableTracer();
  }

  /**
   * Function to parse span sampling config. The input is in the form <span_name>:<sample_rate>.
   * The sample rate must be a number between 0 and 1. Any value other than that will LOG an error.
   */
  static Map<String, LoopSampler> parseSpanSamplingConfig(String configStr) {
    Map<String, LoopSampler> result = new HashMap<>();
    if (configStr == null || configStr.isEmpty()) {
      return Collections.emptyMap();
    }

    for (String entry : configStr.split(",")) {
      String trimmed = entry.trim();
      int colon = trimmed.indexOf(':');

      if (colon <= 0 || colon >= trimmed.length() - 1) {
        continue;
      }

      String name = trimmed.substring(0, colon).trim();
      String val = trimmed.substring(colon + 1).trim();

      try {
        double rate = Double.parseDouble(val);
        //if the rate  is less than or equal to zero , no sampling config is taken for that key value pair.
        if (rate > 0) {
          // cap it at 1.0 when a number greater than 1 is entered
          double effectiveRate = Math.min(rate, 1.0);
          result.put(name, new LoopSampler(effectiveRate));
        } else {
          LOG.warn("rate for span '{}' is 0 or less, ignoring sample configuration", name);
        }
      } catch (NumberFormatException e) {
        LOG.error("Invalid rate '{}' for span '{}', ignoring sample configuration", val, name);
      }
    }
    return result;
  }

  /**
   * Execute {@code runnable} inside an activated new span.
   * If a parent span exists in the current context, this becomes a child span.
   */
  public static <E extends Exception> void executeInNewSpan(String spanName,
      CheckedRunnable<E> runnable) throws E {
    executeInNewSpan(spanName, SpanKind.INTERNAL, runnable);
  }

  public static <E extends Exception> void executeInNewSpan(String spanName,
      SpanKind spanKind, CheckedRunnable<E> runnable) throws E {
    Span span = buildSpan(spanName, spanKind);
    executeInSpan(span, runnable);
  }

  /**
   * Execute {@code supplier} inside an activated new span.
   */
  public static <R, E extends Exception> R executeInNewSpan(String spanName,
      CheckedSupplier<R, E> supplier) throws E {
    return executeInNewSpan(spanName, SpanKind.INTERNAL, supplier);
  }

  public static <R, E extends Exception> R executeInNewSpan(String spanName,
      SpanKind spanKind, CheckedSupplier<R, E> supplier) throws E {
    Span span = buildSpan(spanName, spanKind);
    return executeInSpan(span, supplier);
  }

  /**
   * Execute {@code supplier} in the given {@code span}.
   *
   * @return the value returned by {@code supplier}
   */
  private static <R, E extends Exception> R executeInSpan(Span span,
      CheckedSupplier<R, E> supplier) throws E {
    try (Scope ignored = span.makeCurrent()) {
      return supplier.get();
    } catch (Exception ex) {
      span.addEvent("Failed with exception: " + ex.getMessage());
      span.setStatus(StatusCode.ERROR);
      throw ex;
    } finally {
      span.end();
    }
  }

  /**
   * Execute {@code runnable} in the given {@code span}.
   */
  private static <E extends Exception> void executeInSpan(Span span,
      CheckedRunnable<E> runnable) throws E {
    try (Scope ignored = span.makeCurrent()) {
      runnable.run();
    } catch (Exception ex) {
      span.addEvent("Failed with exception: " + ex.getMessage());
      span.setStatus(StatusCode.ERROR);
      throw ex;
    } finally {
      span.end();
    }
  }

  /**
   * Execute a new function as a child span of the parent.
   */
  public static <E extends Exception> void executeAsChildSpan(String spanName,
      String parentName, CheckedRunnable<E> runnable) throws E {
    Span span = TracingUtil.importAndCreateSpan(spanName, parentName);
    executeInSpan(span, runnable);
  }

  /**
   * Create an active span with auto-close at finish.
   * <p>
   * This is a simplified way to use span as there is no way to add any tag
   * in case of Exceptions.
   */
  public static TraceCloseable createActivatedSpan(String spanName) {
    return createActivatedSpan(spanName, SpanKind.INTERNAL);
  }

  public static TraceCloseable createActivatedSpan(String spanName, SpanKind spanKind) {
    Span span = buildSpan(spanName, spanKind);
    Scope scope = span.makeCurrent();
    return () -> {
      scope.close();
      span.end();
    };
  }

  public static Span getActiveSpan() {
    return Span.current();
  }

  /**
   * AutoCloseable interface for tracing span but no exception is thrown in close.
   */
  public interface TraceCloseable extends AutoCloseable {
    @Override
    void close();
  }

  /**
   * A TextMapGetter implementation to extract tracing info from String.
   */
  public static class TextExtractor implements io.opentelemetry.context.propagation.TextMapGetter<String> {
    private Map<String, String> map = new HashMap<>();

    @Override
    public Iterable<String> keys(String carrier) {
      if (map.isEmpty()) {
        parse(carrier);
      }
      return map.keySet();
    }

    @Override
    public String get(String carrier, String key) {
      if (map.isEmpty()) {
        parse(carrier);
      }
      return map.get(key);
    }

    private void parse(String carrier) {
      if (carrier == null || carrier.isEmpty()) {
        return;
      }
      String[] parts = carrier.split(";");
      for (String part : parts) {
        String[] kv = part.split("=");
        if (kv.length == 2) {
          map.put(kv[0].trim(), kv[1].trim());
        }
      }
    }
  }

  /**
   * Creates a new span, using the current context as a parent if valid;
   * Otherwise starts a root span only when {@code ozone.tracing.enabled=true};
   * if not, returns an invalid span so application-aware mode never starts a new trace.
   */
  private static Span buildSpan(String spanName, SpanKind spanKind) {
    Context currentContext = Context.current();
    Span parentSpan = Span.fromContext(currentContext);

    if (parentSpan.getSpanContext().isValid()) {
      return tracer.spanBuilder(spanName)
          .setParent(currentContext)
          .setSpanKind(spanKind)
          .startSpan();
    }
    if (!canStartSpanWithoutParent()) {
      return Span.getInvalid();
    }
    return tracer.spanBuilder(spanName)
        .setNoParent()
        .setSpanKind(spanKind)
        .startSpan();
  }

  /**
   * A TextMapGetter implementation to extract tracing info from getHeader.
   */
  public static class HttpHeaderGetter implements TextMapGetter<Function<String, String>> {

    @Override
    public Iterable<String> keys(Function<String, String> carrier) {
      // Not used during the extract call, so returning an empty list.
      return Collections.emptyList();
    }

    @Override
    public String get(Function<String, String> carrier, String key) {
      return carrier == null ? null : carrier.apply(key);
    }
  }

  public static TraceCloseable createActivatedSpanFromW3cHttpHeaders(
      String spanName, Function<String, String> getHeader, ConfigurationSource conf) {
    if (conf == null || !isTracingActive(conf)) {
      return () -> { };
    }

    Context remote = W3CTraceContextPropagator.getInstance()
        .extract(Context.current(), getHeader, new HttpHeaderGetter());

    if (!Span.fromContext(remote).getSpanContext().isValid()) {
      if (!canStartSpanWithoutParent()) {
        return () -> { };
      }
      return createActivatedSpan(spanName);
    }

    Span span = tracer.spanBuilder(spanName)
        .setParent(remote)
        .setSpanKind(SpanKind.SERVER)
        .startSpan();

    Scope scope = span.makeCurrent();

    return () -> {
      scope.close();
      span.end();
    };
  }
}
