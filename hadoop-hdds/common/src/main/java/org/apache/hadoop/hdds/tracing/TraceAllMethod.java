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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.Pair;

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
  private final Map<String, Map<Class<?>[], Pair<Boolean, Method>>> methods = new HashMap<>();
  private final T delegate;

  private final String name;

  public TraceAllMethod(T delegate, String name) {
    this.delegate = delegate;
    this.name = name;
    for (Method method : delegate.getClass().getMethods()) {
      if (method.getDeclaringClass().equals(Object.class)) {
        continue;
      }
      boolean shouldSkip = method.isAnnotationPresent(SkipTracing.class);
      methods.computeIfAbsent(method.getName(), any -> new HashMap<>())
          .put(method.getParameterTypes(), Pair.of(shouldSkip, method));
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    Pair<Boolean, Method> methodInfo = findDelegatedMethod(method);
    if (methodInfo == null) {
      throw new NoSuchMethodException("Method not found: " + method.getName());
    }
    boolean shouldSkip = methodInfo.getLeft();
    Method delegateMethod = methodInfo.getRight();
    if (shouldSkip) {
      try {
        return delegateMethod.invoke(delegate, args);
      } catch (Exception ex) {
        if (ex.getCause() != null) {
          throw ex.getCause();
        } else {
          throw ex;
        }
      }
    }

    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan(name + "." + method.getName())) {
      try {
        return delegateMethod.invoke(delegate, args);
      } catch (Exception ex) {
        if (ex.getCause() != null) {
          throw ex.getCause();
        } else {
          throw ex;
        }
      }
    }
  }

  private Pair<Boolean, Method> findDelegatedMethod(Method method) {
    for (Entry<Class<?>[], Pair<Boolean, Method>> entry : methods.getOrDefault(
        method.getName(), emptyMap()).entrySet()) {
      if (Arrays.equals(entry.getKey(), method.getParameterTypes())) {
        return entry.getValue();
      }
    }
    return null;
  }
}
