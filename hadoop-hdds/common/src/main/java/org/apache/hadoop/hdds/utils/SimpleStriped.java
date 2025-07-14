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

package org.apache.hadoop.hdds.utils;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Striped;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A custom factory to force creation of {@link Striped}
 * locks with fair order policy.
 *
 * The reason of this util is that today guava's {@link Striped} does not
 * support creating locks with fair policy, either supports a mechanism for
 * the client code to do that ({@link Striped#custom(int, Supplier)} is package
 * private). Ref: https://github.com/google/guava/issues/2514
 *
 * So, we have to use reflection to forcibly support fair order policy with
 * {@link Striped}. When the above issue is resolved, we can remove this util.
 */
public final class SimpleStriped {

  private SimpleStriped() {
  }


  /**
   * Creates a {@code Striped<L>} with eagerly initialized, strongly referenced
   * locks. Every lock is obtained from the passed supplier.
   *
   * @param stripes the minimum number of stripes (locks) required.
   * @param supplier a {@code Supplier<L>} object to obtain locks from.
   * @return a new {@code Striped<L>}.
   */
  @SuppressWarnings("unchecked")
  public static <L> Striped<L> custom(int stripes, Supplier<L> supplier) {
    try {
      Method custom =
          Striped.class.getDeclaredMethod("custom", int.class, Supplier.class);
      custom.setAccessible(true);
      return (Striped<L>) custom.invoke(null, stripes, supplier);
    } catch (NoSuchMethodException | IllegalAccessException |
             InvocationTargetException e) {
      throw new IllegalStateException("Error creating custom Striped.", e);
    }
  }

  /**
   * Creates a {@code Striped<ReadWriteLock>} with eagerly initialized,
   * strongly referenced read-write locks. Every lock is reentrant.
   *
   * @param stripes the minimum number of stripes (locks) required
   * @return a new {@code Striped<ReadWriteLock>}
   */
  public static Striped<ReadWriteLock> readWriteLock(int stripes,
      boolean fair) {
    return custom(stripes, () -> new ReentrantReadWriteLock(fair));
  }

}
