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

import static java.util.Collections.emptyMap;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Scope;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A Java proxy invocation handler to trace all the methods of the delegate
 * class.
 *
 * @param <T>
 */
public class TraceAllMethod<T> implements InvocationHandler {

  /**
   * Cache for all the method objects of the delegate class.
   */
  private final Map<String, Map<Class<?>[], DelegatedMethodInfo>> methods = new HashMap<>();
  private final T delegate;
  private final String name;

  public TraceAllMethod(T delegate, String name, SpanKind spanKind) {
    this.delegate = delegate;
    this.name = name;
    for (Method method : delegate.getClass().getMethods()) {
      if (method.getDeclaringClass().equals(Object.class)) {
        continue;
      }
      boolean shouldSkip = method.isAnnotationPresent(SkipTracing.class);
      methods.computeIfAbsent(method.getName(), any -> new HashMap<>())
          .put(method.getParameterTypes(), new DelegatedMethodInfo(shouldSkip, findSpanKind(method, spanKind)));
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    DelegatedMethodInfo methodInfo = findDelegatedMethod(method);
    if (methodInfo == null || methodInfo.shouldSkip) {
      return invokeDelegate(method, args);
    }

    Span span = TracingUtil.createSpan(name + "." + method.getName(), methodInfo.spanKind);
    try (Scope ignored = span.makeCurrent()) {
      return invokeDelegate(method, args);
    } catch (Throwable ex) {
      TracingUtil.recordException(span, ex);
      throw ex;
    } finally {
      span.end();
    }
  }

  private Object invokeDelegate(Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private static SpanKind findSpanKind(Method method, SpanKind defaultSpanKind) {
    if (method.isAnnotationPresent(InternalSpanKind.class)
        || method.getDeclaringClass().isAnnotationPresent(InternalSpanKind.class)) {
      return SpanKind.INTERNAL;
    }
    return defaultSpanKind;
  }

  private DelegatedMethodInfo findDelegatedMethod(Method method) {
    for (Entry<Class<?>[], DelegatedMethodInfo> entry : methods.getOrDefault(
        method.getName(), emptyMap()).entrySet()) {
      if (Arrays.equals(entry.getKey(), method.getParameterTypes())) {
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * Whether a method should be skipped and the delegate method to invoke.
   */
  private static final class DelegatedMethodInfo {
    private final boolean shouldSkip;
    private final SpanKind spanKind;

    private DelegatedMethodInfo(boolean shouldSkip, SpanKind spanKind) {
      this.shouldSkip = shouldSkip;
      this.spanKind = spanKind;
    }
  }
}
