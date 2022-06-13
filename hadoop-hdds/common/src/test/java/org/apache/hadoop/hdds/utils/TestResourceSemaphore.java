/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for ResourceSemaphore.
 */
public class TestResourceSemaphore {
  @Test
  @Timeout(1)
  public void testGroup() {
    final ResourceSemaphore.Group g = new ResourceSemaphore.Group(3, 1);

    assertUsed(g, 0, 0);
    assertAcquire(g, true, 1, 1);
    assertUsed(g, 1, 1);
    assertAcquire(g, false, 1, 1);
    assertUsed(g, 1, 1);
    assertAcquire(g, false, 0, 1);
    assertUsed(g, 1, 1);
    assertAcquire(g, true, 1, 0);
    assertUsed(g, 2, 1);
    assertAcquire(g, true, 1, 0);
    assertUsed(g, 3, 1);
    assertAcquire(g, false, 1, 0);
    assertUsed(g, 3, 1);

    g.release(1, 1);
    assertUsed(g, 2, 0);
    g.release(2, 0);
    assertUsed(g, 0, 0);
    g.release(0, 0);
    assertUsed(g, 0, 0);

    assertThrows(IllegalStateException.class, () -> g.release(1, 0));
    assertThrows(IllegalStateException.class, () -> g.release(0, 1));
  }

  static void assertUsed(ResourceSemaphore.Group g, int... expected) {
    Assertions.assertEquals(expected.length, g.resourceSize());
    for (int i = 0; i < expected.length; i++) {
      Assertions.assertEquals(expected[i], g.get(i).used());
    }
  }

  static void assertAcquire(ResourceSemaphore.Group g, boolean expected,
      int... permits) {
    final boolean computed = g.tryAcquire(permits);
    Assertions.assertEquals(expected, computed);
  }
}
