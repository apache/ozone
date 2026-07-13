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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.utils.db.CodecTestUtil.gc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.RatisUtilTestUtil;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.WeakValueCache;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test {@link ContainerID}. */
public final class TestContainerID {
  private static final Logger LOG = LoggerFactory.getLogger(TestContainerID.class);

  private static final WeakValueCache<Long, ContainerID> CACHE = ContainerID.getCacheForTesting();

  static String dumpCache() {
    final List<ContainerID> values = RatisUtilTestUtil.getValues(CACHE);
    values.sort(Comparator.comparing(ContainerID::getIdForTesting));
    String header = CACHE + ": " + values.size();
    System.out.println(header);
    System.out.println("  " + values);
    return header;
  }

  static void assertCache(IDs expectedIDs) {
    final List<ContainerID> computed = RatisUtilTestUtil.getValues(CACHE);
    computed.sort(Comparator.comparing(ContainerID::getIdForTesting));

    final List<ContainerID> expected = expectedIDs.getIds();
    expected.sort(Comparator.comparing(ContainerID::getIdForTesting));

    assertEquals(expected, computed, TestContainerID::dumpCache);
  }

  void assertCacheSizeWithGC(IDs expectedIDs) throws Exception {
    JavaUtils.attempt(() -> {
      gc();
      assertCache(expectedIDs);
    }, 5, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS), "assertCacheSizeWithGC", LOG);
  }

  static class IDs {
    private final List<ContainerID> ids = new LinkedList<>();

    List<ContainerID> getIds() {
      return new ArrayList<>(ids);
    }

    int size() {
      return ids.size();
    }

    ContainerID allocate() {
      final ContainerID id = ContainerID.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE));
      LOG.info("allocate {}", id);
      ids.add(id);
      return id;
    }

    void release() {
      final int r = ThreadLocalRandom.current().nextInt(size());
      final ContainerID removed = ids.remove(r);
      LOG.info("release {}", removed);
    }
  }

  @Test
  public void testCaching() throws Exception {
    final int n = 100;
    final IDs ids = new IDs();
    assertEquals(0, ids.size());
    assertCache(ids);

    for (int i = 0; i < n; i++) {
      final ContainerID id = ids.allocate();
      assertSame(id, ContainerID.valueOf(id.getIdForTesting()));
      assertCache(ids);
    }

    for (int i = 0; i < n / 2; i++) {
      ids.release();
      if (ThreadLocalRandom.current().nextInt(10) == 0) {
        assertCacheSizeWithGC(ids);
      }
    }
    assertCacheSizeWithGC(ids);

    for (int i = 0; i < n / 2; i++) {
      final ContainerID id = ids.allocate();
      assertSame(id, ContainerID.valueOf(id.getIdForTesting()));
      assertCache(ids);
    }


    for (int i = 0; i < n; i++) {
      ids.release();
      if (ThreadLocalRandom.current().nextInt(10) == 0) {
        assertCacheSizeWithGC(ids);
      }
    }
    assertCacheSizeWithGC(ids);

    assertEquals(0, ids.size());
  }
}
