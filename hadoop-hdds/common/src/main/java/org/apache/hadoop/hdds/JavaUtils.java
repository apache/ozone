/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds;

import com.sun.tools.javac.util.Pair;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Various reusable utility methods related to Java.
 */
public final class JavaUtils {
  // "1.8"->8, "9"->9, "10"->10
  private static final int JAVA_SPEC_VER = Math.max(8, Integer.parseInt(
      System.getProperty("java.specification.version").split("\\.")[0]));

  /**
   * Query to see if major version of Java specification of the system
   * is equal or greater than the parameter.
   *
   * @param version 8, 9, 10 etc.
   * @return comparison with system property, always true for 8
   */
  public static boolean isJavaVersionAtLeast(int version) {
    return JAVA_SPEC_VER >= version;
  }

  /**
   * Private constructor.
   */
  private JavaUtils() {
  }

  public static <K, V> Map<V, Set<K>> getReverseMapSet(
          Map<K, Set<V>>  map) {
    return map.entrySet().stream().flatMap(e -> e.getValue().stream()
            .map(v -> Pair.of(v, e.getKey())))
            .collect(Collectors.groupingBy(p -> p.fst,
                    Collectors.mapping(r -> r.snd, Collectors.toSet())));
  }
}
