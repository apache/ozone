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

import java.util.Arrays;
import java.util.Objects;
import org.apache.ratis.util.Preconditions;

/**
 * This is a utility to combine multiple objects as a key that can be used in
 * hash map access. The advantage of this is that it is cheap in comparison
 * to other methods like string concatenation.
 */
public abstract class CompositeKey {
  /** The same as {@link Arrays#hashCode(Object[])} for one loop step. */
  static int hash(int result, Object next) {
    return 31 * result + next.hashCode();
  }

  private static final class TwoComponents extends CompositeKey {
    private final int hashCode;
    private final Object first;
    private final Object second;

    private TwoComponents(Object first, Object second) {
      this.first = Objects.requireNonNull(first, "first == null");
      this.second = Objects.requireNonNull(second, "second == null");
      this.hashCode = hash(hash(1, first), second);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof TwoComponents)) {
        return false;
      }
      final TwoComponents that = (TwoComponents) obj;
      return this.hashCode == that.hashCode
          && this.first.equals(that.first)
          && this.second.equals(that.second);
    }
  }

  private static final class MultiComponents extends CompositeKey {
    private final int hashCode;
    private final Object[] components;

    MultiComponents(Object[] components) {
      Preconditions.assertTrue(components.length > 2, () -> "components.length " + components.length + " <= 2");
      for (int i = 0; i < components.length; i++) {
        final int j = i;
        Objects.requireNonNull(components[j], () -> "components[" + j + "] == null");
      }

      this.hashCode = Arrays.hashCode(components);
      this.components = components;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof MultiComponents)) {
        return false;
      }
      final MultiComponents that = (MultiComponents) obj;
      return this.hashCode == that.hashCode
          && Arrays.equals(this.components, that.components);
    }
  }

  public static CompositeKey combineTwoKeys(Object first, Object second) {
    return new TwoComponents(first, second);
  }

  public static CompositeKey combineMultiKeys(Object[] components) {
    return new MultiComponents(components);
  }

  public static Object combineKeys(Object[] components) {
    return components.length == 1 ? components[0]
        : components.length == 2 ? CompositeKey.combineTwoKeys(components[0], components[1])
        : CompositeKey.combineMultiKeys(components);
  }
}
