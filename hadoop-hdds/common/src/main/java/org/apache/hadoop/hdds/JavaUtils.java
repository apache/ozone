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

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
   * Returns a collection containing the union of elements from two collections.
   * The result ensures all elements are unique, similar to a set union.
   * And it handles the null of inputs.
   *
   * @param collection1 the first collection
   * @param collection2 the second collection
   * @param <T> the type of elements in the collections
   * @return a collection of unique elements from both collections
   */
  public static <T> Set<T> unionOfCollections(Collection<T> collection1, Collection<T> collection2) {
    Stream<T> stream1 = (collection1 == null) ? Stream.empty() : collection1.stream();
    Stream<T> stream2 = (collection2 == null) ? Stream.empty() : collection2.stream();

    return Stream.concat(stream1, stream2)
        .collect(Collectors.toSet());  // Still using toSet to ensure uniqueness
  }

  /**
   * Private constructor.
   */
  private JavaUtils() {
  }
}
