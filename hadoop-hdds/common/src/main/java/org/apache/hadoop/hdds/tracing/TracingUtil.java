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
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
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
  private static final String OTEL_EXPORTER_OTLP_ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT";
  private static final String OTEL_EXPORTER_OTLP_ENDPOINT_DEFAULT = "http://localhost:4317";
  private static final String OTEL_TRACES_SAMPLER_ARG = "OTEL_TRACES_SAMPLER_ARG";
  private static final double OTEL_TRACES_SAMPLER_RATIO_DEFAULT = 1.0;

  private static volatile boolean isInit = false;
  private static Tracer tracer = OpenTelemetry.noop().getTracer("noop");

  private TracingUtil() {
  }

  /**
   * Initialize the tracing with the given service name.
   */
  public static void initTracing(
      String serviceName, ConfigurationSource conf) {
    if (!isTracingEnabled(conf) || isInit) {
      return;
    }

    try {
      initialize(serviceName);
      isInit = true;
      LOG.info("Initialized tracing service: {}", serviceName);
    } catch (Exception e) {
      LOG.error("Failed to initialize tracing", e);
    }
  }

  private static void initialize(String serviceName) {
    String otelEndPoint = System.getenv(OTEL_EXPORTER_OTLP_ENDPOINT);
    if (otelEndPoint == null || otelEndPoint.isEmpty()) {
      otelEndPoint = OTEL_EXPORTER_OTLP_ENDPOINT_DEFAULT;
    }

    double samplerRatio = OTEL_TRACES_SAMPLER_RATIO_DEFAULT;
    try {
      String sampleStrRatio = System.getenv(OTEL_TRACES_SAMPLER_ARG);
      if (sampleStrRatio != null && !sampleStrRatio.isEmpty()) {
        samplerRatio = Double.parseDouble(System.getenv(OTEL_TRACES_SAMPLER_ARG));
      }
    } catch (NumberFormatException ex) {
      // ignore and use the default value.
    }

    Resource resource = Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), serviceName));
    OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
        .setEndpoint(otelEndPoint)
        .build();

    SimpleSpanProcessor spanProcessor = SimpleSpanProcessor.builder(spanExporter).build();
    SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(spanProcessor)
        .setResource(resource)
        .setSampler(Sampler.traceIdRatioBased(samplerRatio))
        .build();
    OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
        .setTracerProvider(tracerProvider)
        .build();
    tracer = openTelemetry.getTracer(serviceName);
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
   *
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

  public static boolean isTracingEnabled(
      ConfigurationSource conf) {
    return conf.getBoolean(
        ScmConfigKeys.HDDS_TRACING_ENABLED,
        ScmConfigKeys.HDDS_TRACING_ENABLED_DEFAULT);
  }

  /**
   * Execute {@code runnable} inside an activated new span.
   */
  public static <E extends Exception> void executeInNewSpan(String spanName,
      CheckedRunnable<E> runnable) throws E {
    Span span = tracer.spanBuilder(spanName).setNoParent().startSpan();
    executeInSpan(span, runnable);
  }

  /**
   * Execute {@code supplier} inside an activated new span.
   */
  public static <R, E extends Exception> R executeInNewSpan(String spanName,
      CheckedSupplier<R, E> supplier) throws E {
    Span span = tracer.spanBuilder(spanName).setNoParent().startSpan();
    return executeInSpan(span, supplier);
  }

  /**
   * Execute {@code supplier} in the given {@code span}.
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
    Span span = tracer.spanBuilder(spanName).setNoParent().startSpan();
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
}
