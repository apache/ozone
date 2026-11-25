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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Scope;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

/**
 * A Java proxy invocation handler to trace all the methods of the delegate
 * class.
 *
 * @param <T>
 */
public class TraceAllMethod<T> implements InvocationHandler {

  /** Interface method info per delegate class, built by its first proxy and released with the class. */
  private static final ClassValue<Map<Method, DelegatedMethodInfo>> METHODS =
      new ClassValue<Map<Method, DelegatedMethodInfo>>() {
        @Override
        protected Map<Method, DelegatedMethodInfo> computeValue(Class<?> delegateClass) {
          Map<Method, DelegatedMethodInfo> methods = new HashMap<>();
          for (Class<?> c = delegateClass; c != null; c = c.getSuperclass()) {
            for (Class<?> itf : c.getInterfaces()) {
              for (Method method : itf.getMethods()) {
                methods.computeIfAbsent(method, m -> computeDelegatedMethodInfo(delegateClass, m));
              }
            }
          }
          return methods;
        }
      };

  private final Map<Method, DelegatedMethodInfo> methods;
  private final T delegate;
  private final SpanKind spanKind;

  public TraceAllMethod(T delegate, SpanKind spanKind) {
    this.delegate = delegate;
    this.spanKind = spanKind;
    this.methods = METHODS.get(delegate.getClass());
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    DelegatedMethodInfo methodInfo = methods.get(method);
    if (methodInfo == null || methodInfo.shouldSkip || hasUnsampledParent()) {
      return invokeDelegate(method, args);
    }

    SpanKind kind = methodInfo.annotatedSpanKind != null ? methodInfo.annotatedSpanKind : spanKind;
    Span span = TracingUtil.createSpan(methodInfo.spanName, kind);
    try (Scope ignored = span.makeCurrent()) {
      return invokeDelegate(method, args);
    } catch (Throwable ex) {
      TracingUtil.recordException(span, ex);
      throw ex;
    } finally {
      span.end();
    }
  }

  private static boolean hasUnsampledParent() {
    SpanContext parent = Span.current().getSpanContext();
    return parent.isValid() && !parent.isSampled();
  }

  private Object invokeDelegate(Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private static DelegatedMethodInfo computeDelegatedMethodInfo(Class<?> delegateClass, Method method) {
    Method delegateMethod;
    try {
      delegateMethod = delegateClass.getMethod(method.getName(), method.getParameterTypes());
    } catch (NoSuchMethodException e) {
      return null;
    }
    String className = delegateClass.getSimpleName();
    // anonymous, lambda and proxy classes have no stable name
    if (className.isEmpty() || delegateClass.isSynthetic() || Proxy.isProxyClass(delegateClass)) {
      className = method.getDeclaringClass().getSimpleName();
    }
    return new DelegatedMethodInfo(computeSkip(delegateMethod), getAnnotatedSpanKind(delegateMethod),
        className + "." + method.getName());
  }

  private static boolean computeSkip(Method method) {
    return method.isAnnotationPresent(SkipTracing.class);
  }

  private static SpanKind getAnnotatedSpanKind(Method method) {
    if (method.isAnnotationPresent(InternalSpanKind.class)) {
      return SpanKind.INTERNAL;
    }
    return null;
  }

  /**
   * Whether a method should be skipped, and the kind and name of its span.
   */
  private static final class DelegatedMethodInfo {
    private final boolean shouldSkip;
    private final SpanKind annotatedSpanKind;
    private final String spanName;

    private DelegatedMethodInfo(boolean shouldSkip, SpanKind annotatedSpanKind, String spanName) {
      this.shouldSkip = shouldSkip;
      this.annotatedSpanKind = annotatedSpanKind;
      this.spanName = spanName;
    }
  }
}
