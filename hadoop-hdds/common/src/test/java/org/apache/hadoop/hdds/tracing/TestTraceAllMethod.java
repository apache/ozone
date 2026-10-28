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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.trace.ReadableSpan;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link TraceAllMethod}.
 */
public class TestTraceAllMethod {

  @Test
  public void testUnknownMethod() {
    TraceAllMethod<Service> subject = new TraceAllMethod<>(new ServiceImpl(), "service", SpanKind.CLIENT);
    assertThrows(NoSuchMethodException.class, () -> subject.invoke(null,
        ServiceImpl.class.getMethod("no such method"), new Object[]{}));
  }

  @Test
  public void testDefaultMethod() throws Throwable {
    TraceAllMethod<Service> subject = new TraceAllMethod<>(new ServiceImpl(), "service", SpanKind.CLIENT);

    assertEquals("Hello default", subject.invoke(null,
        ServiceImpl.class.getMethod("defaultMethod"), new Object[]{}));
  }

  @Test
  public void testConstructorKindIsUsed() throws Throwable {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      TraceAllMethod<Service> subject = new TraceAllMethod<>(new ServiceImpl(), "service", SpanKind.CLIENT);
      assertEquals("normal", subject.invoke(null, Service.class.getMethod("normalMethod"), null));
      spans.assertKind("service.normalMethod", SpanKind.CLIENT);
    }
  }

  @Test
  public void testMethodAnnotationOverridesConstructorKind() throws Throwable {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      TraceAllMethod<Service> subject = new TraceAllMethod<>(new ServiceImpl(), "service", SpanKind.SERVER);
      assertEquals("normal", subject.invoke(null, Service.class.getMethod("normalMethod"), null));
      assertEquals("local", subject.invoke(null, Service.class.getMethod("localMethod"), null));
      spans.assertKind("service.normalMethod", SpanKind.SERVER);
      spans.assertKind("service.localMethod", SpanKind.INTERNAL);
    }
  }

  @Test
  public void testClassAnnotationSkipsInheritedMethods() throws Throwable {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      TraceAllMethod<Service> subject = new TraceAllMethod<>(new InternalServiceImpl(), "service", SpanKind.CLIENT);
      assertEquals("normal", subject.invoke(null, Service.class.getMethod("normalMethod"), null));
      assertEquals("Internal x", subject.invoke(null, Service.class.getMethod("otherMethod", String.class),
          new Object[]{"x"}));
      spans.assertKind("service.normalMethod", SpanKind.CLIENT);
      spans.assertKind("service.otherMethod", SpanKind.INTERNAL);
    }
  }

  @Test
  public void testOverloadsResolveTheirOwnKind() throws Throwable {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      TraceAllMethod<OverloadedService> subject =
          new TraceAllMethod<>(new OverloadedServiceImpl(), "service", SpanKind.CLIENT);
      assertEquals("a", subject.invoke(null, OverloadedService.class.getMethod("call", String.class),
          new Object[]{"a"}));
      assertEquals("a2", subject.invoke(null, OverloadedService.class.getMethod("call", String.class, int.class),
          new Object[]{"a", 2}));
      assertEquals(Arrays.asList(SpanKind.CLIENT, SpanKind.INTERNAL),
          spans.named("service.call").stream().map(ReadableSpan::getKind).collect(Collectors.toList()));
    }
  }

  @Test
  public void testExceptionIsRecorded() {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      TraceAllMethod<Service> subject = new TraceAllMethod<>(new ServiceImpl(), "service",
          SpanKind.CLIENT);
      IOException ex = assertThrows(IOException.class,
          () -> subject.invoke(null, Service.class.getMethod("tracedThrowingMethod"), null));
      assertEquals("Failed", ex.getMessage());
      spans.assertError("service.tracedThrowingMethod", "Failed");
      spans.assertExceptionRecorded("service.tracedThrowingMethod", IOException.class);
    }
  }

  @Test
  public void testNonPublicDelegateClass() throws Throwable {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      List<String> delegate = Collections.unmodifiableList(Arrays.asList("a", "b"));
      assertFalse(Modifier.isPublic(delegate.getClass().getModifiers()));
      TraceAllMethod<List<String>> subject = new TraceAllMethod<>(delegate, "List", SpanKind.INTERNAL);
      assertEquals(2, subject.invoke(null, List.class.getMethod("size"), null));
      spans.assertKind("List.size", SpanKind.INTERNAL);
    }
  }

  @Test
  public void testObjectMethodsGoToDelegate() throws Throwable {
    try (TestSpanCollector spans = TestSpanCollector.install("test")) {
      ServiceImpl delegate = new ServiceImpl();
      TraceAllMethod<Service> subject = new TraceAllMethod<>(delegate, "service", SpanKind.CLIENT);
      assertEquals(delegate.toString(), subject.invoke(null, Object.class.getMethod("toString"), null));
      assertEquals(delegate.hashCode(), subject.invoke(null, Object.class.getMethod("hashCode"), null));
      assertTrue(spans.getSpans().isEmpty());
    }
  }

  /**
   * Example interface for testing {@link TraceAllMethod}.
   */
  public interface Service {
    default String defaultMethod() {
      return otherMethod("default");
    }

    String localMethod();

    @InternalSpanKind
    String annotatedOnlyOnInterface();

    String skippedMethod();

    void throwingMethod() throws IOException;

    void tracedThrowingMethod() throws IOException;

    String normalMethod();

    String otherMethod(String name);
  }

  /**
   * Example implementation class for testing {@link TraceAllMethod}.
   */
  public static class ServiceImpl implements Service {

    private boolean spanActive = false;
    private SpanKind spanKind;

    public SpanKind lastSpanKind() {
      return spanKind;
    }

    @Override
    public String otherMethod(String name) {
      return "Hello " + name;
    }

    @Override
    @SkipTracing
    public String skippedMethod() {
      this.spanActive = Span.current().getSpanContext().isValid();
      return "skipped";
    }

    @Override
    @SkipTracing
    public void throwingMethod() throws IOException {
      this.spanActive = Span.current().getSpanContext().isValid();
      throw new IOException("Original Exception");
    }

    @Override
    public void tracedThrowingMethod() throws IOException {
      throw new IOException("Failed");
    }

    @Override
    public String normalMethod() {
      this.spanActive = Span.current().getSpanContext().isValid();
      return "normal";
    }

    @Override
    @InternalSpanKind
    public String localMethod() {
      Span span = Span.current();
      this.spanActive = span.getSpanContext().isValid();
      if (spanActive) {
        this.spanKind = ((ReadableSpan) span).getKind();
      }
      return "local";
    }

    @Override
    public String annotatedOnlyOnInterface() {
      return "interfaceLocal";
    }

    public boolean wasSpanActive() {
      return spanActive;
    }
  }

  /**
   * Annotated subclass: only the methods it declares itself are INTERNAL.
   */
  @InternalSpanKind
  public static class InternalServiceImpl extends ServiceImpl {
    @Override
    public String otherMethod(String name) {
      return "Internal " + name;
    }
  }

  /**
   * Interface with an overloaded method.
   */
  public interface OverloadedService {
    String call(String name);

    String call(String name, int count);
  }

  /**
   * Implementation where only one overload is INTERNAL.
   */
  public static class OverloadedServiceImpl implements OverloadedService {
    @Override
    public String call(String name) {
      return name;
    }

    @Override
    @InternalSpanKind
    public String call(String name, int count) {
      return name + count;
    }
  }
}
