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

package org.apache.ozone.lib.util;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * Utility methods to check preconditions.
 * <p>
 * Commonly used for method arguments preconditions.
 */
@InterfaceAudience.Private
public final class Check {

  private Check() {
  }

  /**
   * Verifies a variable is not NULL.
   *
   * @param obj the variable to check.
   * @param name the name to use in the exception message.
   *
   * @return the variable.
   *
   * @throws IllegalArgumentException if the variable is NULL.
   */
  public static <T> T notNull(T obj, String name) {
    if (obj == null) {
      throw new IllegalArgumentException(name + " cannot be null");
    }
    return obj;
  }

  /**
   * Verifies a string is not NULL and not emtpy.
   *
   * @param str the variable to check.
   * @param name the name to use in the exception message.
   *
   * @return the variable.
   *
   * @throws IllegalArgumentException if the variable is NULL or empty.
   */
  public static String notEmpty(String str, String name) {
    if (str == null) {
      throw new IllegalArgumentException(name + " cannot be null");
    }
    if (str.isEmpty()) {
      throw new IllegalArgumentException(name + " cannot be empty");
    }
    return str;
  }

}
