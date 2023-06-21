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
package org.apache.hadoop.hdds.function;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Common predicates.
 */
public final class Predicates {

  public static <T> Predicate<T> yes() {
    return x -> true;
  }

  public static <T> Predicate<T> no() {
    return x -> false;
  }

  public static <T, U> BiPredicate<T, U> yesBi() {
    return (t, u) -> true;
  }

  public static <T, U> BiPredicate<T, U> noBi() {
    return (t, u) -> false;
  }

  private Predicates() {
    // no instances
  }
}
