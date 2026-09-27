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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Test {@link CompositeKey}. */
public final class TestCompositeKey {
  private static final Random RANDOM = new Random();

  static String randomString(int length) {
    final StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append(RANDOM.nextInt(10));
    }
    return builder.toString();
  }

  static Object[] randomComponents(int numComponents) {
    final Object[] components = new Object[numComponents];
    for (int i = 0; i < components.length; i++) {
      components[i] = randomString(RANDOM.nextInt(10));
    }
    return components;
  }

  private static final class OldCompositeKey {
    private final int hashCode;
    private final Object[] components;

    OldCompositeKey(Object[] components) {
      this.components = components;
      this.hashCode = Arrays.hashCode(components);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof OldCompositeKey)) {
        return false;
      }
      OldCompositeKey other = (OldCompositeKey) obj;
      return Arrays.equals(components, other.components);
    }

    static Object combineKeys(Object[] components) {
      return components.length == 1 ?
          components[0] : new OldCompositeKey(components);
    }
  }

  static void assertHashCode(Object[] components, int computed) {
    final Object expected = OldCompositeKey.combineKeys(components);
    assertEquals(expected.hashCode(), CompositeKey.combineKeys(components).hashCode());
    assertEquals(expected.hashCode(), computed);
  }

  @Test
  public void testHashCodeOne() {
    for (int i = 0; i < 100; i++) {
      final Object[] components = {randomString(i)};
      assertHashCode(components, components[0].hashCode());
    }
  }

  @Test
  public void testHashCodeTwo() {
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final Object first = randomString(i);
        final Object second = randomString(j);
        final Object[] components = {first, second};
        assertHashCode(components, CompositeKey.combineTwoKeys(first, second).hashCode());
      }
    }
  }

  @Test
  public void testHashCodeMulti() {
    for (int i = 3; i < 100; i++) {
      final Object[] components = randomComponents(i);
      assertHashCode(components, CompositeKey.combineMultiKeys(components).hashCode());
    }
  }
}
