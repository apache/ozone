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

package org.apache.hadoop.hdds.utils.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.hadoop.hdds.StringUtils;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MinHeapMergeIterator}.
 */
class TestMinHeapMergeIterator {

  private static final Comparator<byte[]> BYTE_COMPARATOR = UnsignedBytes.lexicographicalComparator();

  /**
   * A closeable iterator which tracks close() calls.
   */
  private static final class TrackingCloseableIterator<T>
      implements Iterator<T>, Closeable {
    private final List<T> data;
    private int idx = 0;
    private int closeCount = 0;

    private TrackingCloseableIterator(List<T> data) {
      this.data = data;
    }

    @Override
    public boolean hasNext() {
      return idx < data.size();
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException("Iterator exhausted");
      }
      return data.get(idx++);
    }

    @Override
    public void close() {
      closeCount++;
    }

    int getCloseCount() {
      return closeCount;
    }
  }

  private static final class MergeResult {
    private final String key;
    private final Set<Integer> sources;

    private MergeResult(byte[] key, Set<Integer> sources) {
      this.key = StringUtils.bytes2String(key);
      this.sources = sources;
    }

    String getKey() {
      return key;
    }

    Set<Integer> getSources() {
      return sources;
    }
  }

  /**
   * Concrete implementation for tests.
   */
  private static final class TestIterator extends MinHeapMergeIterator<byte[],
      TrackingCloseableIterator<byte[]>, MergeResult> {

    private final List<TrackingCloseableIterator<byte[]>> itrs;
    private final List<MergeResult> merged = new ArrayList<>();

    private IOException ioExceptionAtIndex;
    private int exceptionIndex = -1;

    private TestIterator(List<TrackingCloseableIterator<byte[]>> itrs) {
      super(itrs.size(), BYTE_COMPARATOR);
      this.itrs = itrs;
    }

    private TestIterator withGetIteratorIOException(int index, IOException ex) {
      this.exceptionIndex = index;
      this.ioExceptionAtIndex = ex;
      return this;
    }

    @Override
    protected TrackingCloseableIterator<byte[]> getIterator(int idx)
        throws IOException {
      if (idx == exceptionIndex) {
        if (ioExceptionAtIndex != null) {
          throw ioExceptionAtIndex;
        }
      }
      return itrs.get(idx);
    }

    @Override
    protected MergeResult merge(Map<Integer, byte[]> keysToMerge) {
      // All values in keysToMerge are expected to be equal (same key across iterators).
      byte[] key = keysToMerge.values().iterator().next();
      MergeResult r = new MergeResult(key, new HashSet<>(keysToMerge.keySet()));
      merged.add(r);
      return r;
    }

    List<MergeResult> getMerged() {
      return merged;
    }
  }

  private ImmutableList<byte[]> toBytesList(String... keys) {
    return Arrays.stream(keys).map(StringUtils::string2Bytes).collect(ImmutableList.toImmutableList());
  }

  @Test
  void testMergedOrderAndDuplicateGroupingAndAutoCloseOnExhaustion() {
    TrackingCloseableIterator<byte[]> itr0 =
        new TrackingCloseableIterator<>(toBytesList("a", "c", "e", "g"));
    TrackingCloseableIterator<byte[]> itr1 =
        new TrackingCloseableIterator<>(toBytesList("b", "c", "d", "g", "h"));
    TrackingCloseableIterator<byte[]> itr2 =
        new TrackingCloseableIterator<>(toBytesList("c", "e", "f", "h"));

    List<String> keys = new ArrayList<>();
    try (TestIterator mergeItr = new TestIterator(ImmutableList.of(itr0, itr1, itr2))) {
      while (mergeItr.hasNext()) {
        keys.add(mergeItr.next().getKey());
      }

      assertEquals(ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "h"), keys);

      // Validate sources for every merged key.
      java.util.Map<String, Set<Integer>> expectedSources =
          ImmutableMap.<String, Set<Integer>>builder()
              .put("a", ImmutableSet.of(0))
              .put("b", ImmutableSet.of(1))
              .put("c", ImmutableSet.of(0, 1, 2))
              .put("d", ImmutableSet.of(1))
              .put("e", ImmutableSet.of(0, 2))
              .put("f", ImmutableSet.of(2))
              .put("g", ImmutableSet.of(0, 1))
              .put("h", ImmutableSet.of(1, 2))
              .build();

      ImmutableMap.Builder<String, Set<Integer>> actualSourcesBuilder = ImmutableMap.builder();
      for (MergeResult r : mergeItr.getMerged()) {
        actualSourcesBuilder.put(r.getKey(), r.getSources());
      }
      java.util.Map<String, Set<Integer>> actualSources = actualSourcesBuilder.build();
      assertEquals(expectedSources, actualSources);
    }

    // All iterators should have been auto-closed when they became exhausted.
    assertEquals(1, itr0.getCloseCount());
    assertEquals(1, itr1.getCloseCount());
    assertEquals(1, itr2.getCloseCount());
  }

  @Test
  void testInitClosesEmptyIterators() {
    TrackingCloseableIterator<byte[]> empty =
        new TrackingCloseableIterator<>(Collections.emptyList());
    TrackingCloseableIterator<byte[]> nonEmpty =
        new TrackingCloseableIterator<>(toBytesList("a"));

    try (TestIterator mergeItr = new TestIterator(ImmutableList.of(empty, nonEmpty))) {
      assertTrue(mergeItr.hasNext()); // triggers init
      assertEquals(1, empty.getCloseCount(), "Empty iterator should be closed during init");

      assertEquals("a", mergeItr.next().getKey());
      assertFalse(mergeItr.hasNext());
    }
    assertEquals(1, nonEmpty.getCloseCount(), "Iterator should be closed when exhausted");
  }

  @Test
  void testCloseClosesAllIterators() {
    TrackingCloseableIterator<byte[]> itr0 =
        new TrackingCloseableIterator<>(toBytesList("a", "c"));
    TrackingCloseableIterator<byte[]> itr1 =
        new TrackingCloseableIterator<>(toBytesList("b", "d"));

    try (TestIterator mergeItr = new TestIterator(ImmutableList.of(itr0, itr1))) {
      assertTrue(mergeItr.hasNext()); // triggers init
      mergeItr.close();
      assertEquals(1, itr0.getCloseCount());
      assertEquals(1, itr1.getCloseCount());

      // idempotent close
      mergeItr.close();
    }
    assertEquals(1, itr0.getCloseCount());
    assertEquals(1, itr1.getCloseCount());
  }

  @Test
  void testHasNextWrapsIOExceptionFromGetIterator() {
    IOException expected = new IOException("boom");
    TrackingCloseableIterator<byte[]> itr0 =
        new TrackingCloseableIterator<>(toBytesList("a"));
    TrackingCloseableIterator<byte[]> itr1 =
        new TrackingCloseableIterator<>(toBytesList("b"));
    TestIterator mergeItr = new TestIterator(ImmutableList.of(itr0, itr1));
    mergeItr.withGetIteratorIOException(1, expected);
    try (TestIterator ignored = mergeItr) {
      UncheckedIOException ex = assertThrows(UncheckedIOException.class, mergeItr::hasNext);
      assertEquals(expected, ex.getCause());
    }

    // itr0 is registered with MinHeapMergeIterator before idx=1 throws and must be closed by cleanup.
    assertEquals(1, itr0.getCloseCount());
    // itr1 was never registered; close explicitly to avoid leaks.
    itr1.close();
    assertEquals(1, itr1.getCloseCount());
  }

  @Test
  void testHasNextWrapsRocksDBExceptionFromGetIteratorAndClosesOpenedIterators() throws Exception {
    TrackingCloseableIterator<byte[]> itr0 =
        new TrackingCloseableIterator<>(toBytesList("a", "b"));
    TrackingCloseableIterator<byte[]> itr1 =
        new TrackingCloseableIterator<>(toBytesList("c"));
    RocksDatabaseException rdbEx = new RocksDatabaseException("rocks");
    TestIterator mergeItr = new TestIterator(ImmutableList.of(itr0, itr1));
    mergeItr.withGetIteratorIOException(1, rdbEx);
    try (TestIterator ignored = mergeItr) {
      UncheckedIOException ex = assertThrows(UncheckedIOException.class, mergeItr::hasNext);
      assertInstanceOf(RocksDatabaseException.class, ex.getCause());
      assertEquals(rdbEx, ex.getCause());
    }

    // itr0 was created before the exception and should have been closed via initHeap() cleanup.
    assertEquals(1, itr0.getCloseCount());
    // itr1 was never registered; close explicitly to avoid leaks.
    itr1.close();
    assertEquals(1, itr1.getCloseCount());
  }

  @Test
  void testNextWhenEmptyThrowsNoSuchElement() {
    TrackingCloseableIterator<byte[]> empty =
        new TrackingCloseableIterator<>(Collections.emptyList());
    try (TestIterator mergeItr = new TestIterator(ImmutableList.of(empty))) {
      assertFalse(mergeItr.hasNext());
      assertThrows(NoSuchElementException.class, mergeItr::next);
    }
    assertEquals(1, empty.getCloseCount(), "Empty iterator should be closed during init");
  }
}


