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

package org.apache.hadoop.hdds.scm.ha;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Reflection util for SCM HA.
 */
public final class ReflectionUtil {

  private static Map<String, Class<?>> classCache = new HashMap<>();

  private ReflectionUtil() {
  }

  /**
   * Returns the {@code Class} object associated with the given string name.
   *
   * @param className the fully qualified name of the desired class.
   * @return the {@code Class} object for the class with the
   *         specified name.
   * @throws ClassNotFoundException if the class cannot be located
   */
  public static Class<?> getClass(String className)
      throws ClassNotFoundException {
    if (!classCache.containsKey(className)) {
      classCache.put(className, Class.forName(className));
    }
    return classCache.get(className);
  }

  /**
   * Returns a {@code Method} object that reflects the specified public
   * member method of the given {@code Class} object.
   *
   * @param clazz the class object which has the method
   * @param methodName the name of the method
   * @param arg the list of parameters
   * @return the {@code Method} object that matches the specified
   *         {@code name} and {@code parameterTypes}
   * @throws NoSuchMethodException if a matching method is not found
   *         or if the name is "&lt;init&gt;"or "&lt;clinit&gt;".
   */
  public static Method getMethod(
      final Class<?> clazz, final String methodName, final Class<?>... arg)
      throws NoSuchMethodException {
    return clazz.getMethod(methodName, arg);
  }
}
