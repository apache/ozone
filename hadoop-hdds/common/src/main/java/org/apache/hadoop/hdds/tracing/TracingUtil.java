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
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
  private static final String OZONE_CLIENT_TRACER_SCOPE = "org.apache.hadoop.ozone.client";

  private static volatile boolean isInit = false;
  private static Tracer tracer = OpenTelemetry.noop().getTracer("noop");
  private static SdkTracerProvider sdkTracerProvider;
  private static volatile TracingConfig clientTracingConfig;

  private TracingUtil() {
  }

  /**
   * Initialize the tracing with the given service name.
   */
  public static synchronized void initTracing(
      String serviceName, TracingConfig tracingConfig) {
    clientTracingConfig = tracingConfig;
    if (!tracingConfig.isTracingEnabled() || isInit) {
      return;
    }

    try {
      initialize(serviceName, tracingConfig);
      isInit = true;
      LOG.info("Initialized tracing service: {}", serviceName);
    } catch (Exception e) {
      LOG.error("Failed to initialize tracing", e);
    }
  }

  /**
   * When ozone tracing is off , client tracing is off and no context exist thn
   * span creation should be skipped.
   */
  private static boolean shouldByPassSpanCreation() {
    if (clientTracingConfig == null) {
      return false;
    }
    //if ozone tracing is true then resolve tracer as usual and create span
    if (clientTracingConfig.isTracingEnabled()) {
      return false;
    }
    boolean hasValidContext = Span.current().getSpanContext().isValid();
    boolean clientApplicationTracing = clientTracingConfig.isClientApplicationAware();

    //if ozone tracing is false but context exists and client tracing is true then create span.
    return !(hasValidContext && clientApplicationTracing);
  }

  /**
   * When Ozone OTLP tracing is off but the app has an active span, use the app SDK via GlobalOpenTelemetry.
   */
  private static Tracer resolveTracerForNewSpan(Span parentSpan) {
    if (clientTracingConfig != null
        && !clientTracingConfig.isTracingEnabled()
        && clientTracingConfig.isClientApplicationAware()
        && parentSpan.getSpanContext().isValid()) {
      return GlobalOpenTelemetry.get().getTracer(OZONE_CLIENT_TRACER_SCOPE);
    }
    return tracer;
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
    initTracing(serviceName, tracingConfig);
  }

  private static void shutdownTracing() {
    if (sdkTracerProvider != null) {
      sdkTracerProvider.shutdown();
      sdkTracerProvider = null;
    }
    tracer = OpenTelemetry.noop().getTracer("noop");
    isInit = false;
  }

  private static void initialize(String serviceName, TracingConfig tracingConfig) {
    //Fetch and log the right tracing parameters based on config, environment variable and default value priority.
    String otelEndPoint = tracingConfig.getTracingEndpoint();
    double samplerRatio = tracingConfig.getTraceSamplerRatio();
    LOG.info("Sampling Trace Config = '{}'", samplerRatio);
    String spanSamplingConfig = tracingConfig.getSpanSampling();
    LOG.info("Sampling Span Config = '{}'", spanSamplingConfig);

    Map<String, LoopSampler> spanMap = parseSpanSamplingConfig(spanSamplingConfig);

    Resource resource = Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), serviceName));
    OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
        .setEndpoint(otelEndPoint)
        .build();

    SimpleSpanProcessor spanProcessor = SimpleSpanProcessor.builder(spanExporter).build();

    // Choose sampler based on span sampling config. If it is empty use trace based sampling only.
    // else use custom SpanSampler.
    Sampler sampler;
    if (spanMap.isEmpty()) {
      sampler = Sampler.traceIdRatioBased(samplerRatio);
    } else {
      Sampler rootSampler = Sampler.traceIdRatioBased(samplerRatio);
      sampler = new SpanSampler(rootSampler, spanMap);
    }

    SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(spanProcessor)
        .setResource(resource)
        .setSampler(sampler)
        .build();

    try {
      OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
          .setTracerProvider(tracerProvider)
          .build();
      tracer = openTelemetry.getTracer(serviceName);
      sdkTracerProvider = tracerProvider;
    } catch (RuntimeException e) {
      tracerProvider.shutdown();
      throw e;
    }
  }

  /**
   * Export the active tracing span as a string.
   *
   * @return encoded tracing context.
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
   *
   * @param name          name of the newly created scope
   * @param encodedParent Encoded parent span (could be null or empty)
   * @return Tracing scope.
   */
  public static Span importAndCreateSpan(String name, String encodedParent) {
    if (encodedParent == null || encodedParent.isEmpty()) {
      return tracer.spanBuilder(name).setNoParent().startSpan();
    }

    W3CTraceContextPropagator propagator = W3CTraceContextPropagator.getInstance();
    Context extract = propagator.extract(Context.current(), encodedParent, new TextExtractor());
    return tracer.spanBuilder(name)
        .setParent(extract)
        .startSpan();
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
    if (!isTracingEnabled(conf)) {
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
    if (shouldByPassSpanCreation()) {
      runnable.run();
      return;
    }
    Span span = buildSpan(spanName);
    executeInSpan(span, runnable);
  }

  public static <R, E extends Exception> R executeInNewSpan(String spanName,
      CheckedSupplier<R, E> supplier) throws E {
    if (shouldByPassSpanCreation()) {
      return supplier.get();
    }
    Span span = buildSpan(spanName);
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
    if (shouldByPassSpanCreation()) {
      return () -> { };
    }
    Span span = buildSpan(spanName);
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
   * otherwise, creates a root span.
   */
  private static Span buildSpan(String spanName) {
    Context currentContext = Context.current();
    Span parentSpan = Span.fromContext(currentContext);
    Tracer spanTracer = resolveTracerForNewSpan(parentSpan);

    if (parentSpan.getSpanContext().isValid()) {
      return spanTracer.spanBuilder(spanName).setParent(currentContext).startSpan();
    }
    return spanTracer.spanBuilder(spanName).setNoParent().startSpan();
  }
}
