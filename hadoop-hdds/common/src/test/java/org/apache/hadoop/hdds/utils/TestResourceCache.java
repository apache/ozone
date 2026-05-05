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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Test for ResourceCache.
 */
public class TestResourceCache {

  private static final String ANY_VALUE = "asdf";

  @Test
  public void testResourceCache() throws InterruptedException {
    AtomicLong count = new AtomicLong(0);
    Cache<Integer, String> resourceCache =
        new ResourceCache<>(
            (k, v) -> (int) k, 10,
            (P) -> {
              if (P.wasEvicted()) {
                count.incrementAndGet();
              }
            });
    resourceCache.put(6, "a");
    resourceCache.put(4, "a");

    // put should pass as key 4 will be overwritten
    resourceCache.put(4, "a");

    // put to cache with removing old element "6" as eviction FIFO
    resourceCache.put(1, "a");
    assertNull(resourceCache.get(6));
    assertEquals(1, count.get());

    // add 5 should be success with no removal
    resourceCache.put(5, "a");
    assertNotNull(resourceCache.get(4));

    // remove and check queue
    resourceCache.remove(4);
    assertNull(resourceCache.get(4));
    assertEquals(1, count.get());
  }

  @Test
  public void testRemove() throws Exception {
    testRemove(cache -> cache.remove(2), 2);
  }

  @Test
  public void testRemoveIf() throws Exception {
    testRemove(cache -> cache.removeIf(k -> k <= 2), 1, 2);
  }

  @Test
  public void testClear() throws Exception {
    testRemove(Cache::clear, 1, 2, 3);
  }

  private static void testRemove(Consumer<Cache<Integer, String>> op,
      int... removedKeys) throws InterruptedException {

    // GIVEN
    final int maxSize = 3;
    Cache<Integer, String> resourceCache =
        new ResourceCache<>(
            (k, v) -> 1, maxSize, null);
    for (int i = 1; i <= maxSize; ++i) {
      resourceCache.put(i, ANY_VALUE);
    }

    // WHEN: remove some entries
    op.accept(resourceCache);

    // THEN
    for (Integer k : removedKeys) {
      assertNull(resourceCache.get(k));
    }
    // can put new entries
    for (int i = 1; i <= removedKeys.length; ++i) {
      resourceCache.put(maxSize + i, ANY_VALUE);
    }
  }

}
