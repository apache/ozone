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

package org.apache.ozone.test;

import java.util.function.IntSupplier;

/** Test utilities for working with lambdas returning int value. */
public interface IntLambda {

  static ToIntExecutable withTextFromSystemIn(String... lines) {
    return runnable -> {
      try (AutoCloseable ignored = GenericTestUtils.supplyOnSystemIn(lines)) {
        return runnable.getAsInt();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  /** Function that takes a block of code returning int, executes it, and returns the value. */
  @FunctionalInterface
  interface ToIntExecutable {
    int execute(IntSupplier code);
  }

}
