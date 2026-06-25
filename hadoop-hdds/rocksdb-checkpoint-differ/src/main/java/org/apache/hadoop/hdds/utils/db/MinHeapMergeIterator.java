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

import jakarta.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.ozone.util.ClosableIterator;

/**
 * An abstract class that provides functionality to merge elements from multiple sorted iterators
 * using a min-heap. The {@code MinHeapMergeIterator} ensures the merged output is in sorted order
 * by repeatedly polling the smallest element from the heap of iterators.
 *
 * @param <K> the type of keys being merged, must be {@link Comparable}
 * @param <I> the type of iterators being used, must extend {@link Iterator} and implement {@link Closeable}
 * @param <V> the type of the final merged output
 */
public abstract class MinHeapMergeIterator<K, I extends Iterator<K> & Closeable, V>
    implements ClosableIterator<V> {
  private final PriorityQueue<HeapEntry<K>> minHeap;
  private final Map<Integer, K> keys;
  private final List<I> iterators;
  private boolean initialized;
  private final Comparator<K> comparator;

  public MinHeapMergeIterator(int numberOfIterators, Comparator<K> comparator) {
    this.minHeap = new PriorityQueue<>(Math.max(numberOfIterators, 1));
    keys = new HashMap<>(numberOfIterators);
    iterators = IntStream.range(0, numberOfIterators).mapToObj(i -> (I) null).collect(Collectors.toList());
    this.initialized = false;
    this.comparator = Objects.requireNonNull(comparator, "comparator cannot be null");
  }

  protected abstract I getIterator(int idx) throws IOException;

  private boolean initHeap() throws IOException {
    if (initialized) {
      return false;
    }
    initialized = true;
    int count = 0;
    try {
      for (int idx = 0; idx < iterators.size(); idx++) {
        I itr = getIterator(idx);
        iterators.set(idx, itr);
        HeapEntry<K> entry = new HeapEntry<>(idx, itr, comparator);
        if (entry.getCurrentKey() != null) {
          minHeap.add(entry);
          count++;
        } else {
          // No valid entries, close the iterator.
          closeItrAtIndex(idx);
        }
      }
    } catch (IOException e) {
      close();
      throw e;
    }
    return count > 0;
  }

  @Override
  public boolean hasNext() {
    try {
      return !minHeap.isEmpty() || initHeap();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected abstract V merge(Map<Integer, K> keysToMerge);

  @Override
  public V next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more elements found.");
    }

    assert minHeap.peek() != null;
    // Get current key from heap
    K currentKey = minHeap.peek().getCurrentKey();
    // Clear the keys list by setting all entries to null.
    keys.clear();
    // Advance all entries with the same key (from different files)
    while (!minHeap.isEmpty() && comparator.compare(minHeap.peek().getCurrentKey(), currentKey) == 0) {
      HeapEntry<K> entry = minHeap.poll();
      int idx = entry.index;
      // Set the key for the current entry in the keys list.
      keys.put(idx, entry.getCurrentKey());
      if (entry.advance()) {
        minHeap.offer(entry);
      } else {
        // Iterator is exhausted, close it to prevent resource leak
        try {
          closeItrAtIndex(idx);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
    return merge(keys);
  }

  private void closeItrAtIndex(int idx) throws IOException {
    if (iterators.get(idx) != null) {
      iterators.get(idx).close();
      iterators.set(idx, null);
    }
  }

  @Override
  public void close() {
    IOException exception = null;
    for (int idx = 0; idx < iterators.size(); idx++) {
      try {
        closeItrAtIndex(idx);
      } catch (IOException e) {
        exception = e;
      }
    }
    if (exception != null) {
      throw new UncheckedIOException(exception);
    }
  }

  /**
   * A wrapper class that holds an iterator and its current value for heap operations.
   */
  private static final class HeapEntry<T> implements Comparable<HeapEntry<T>> {
    private final int index;
    private final Iterator<T> iterator;
    private T currentKey;
    private Comparator<T> comparator;

    private HeapEntry(int index, Iterator<T> iterator, Comparator<T> comparator) {
      this.iterator = iterator;
      this.index = index;
      this.comparator = comparator;
      advance();
    }

    private boolean advance() {
      if (iterator.hasNext()) {
        currentKey = iterator.next();
        return true;
      } else {
        currentKey = null;
        return false;
      }
    }

    private T getCurrentKey() {
      return currentKey;
    }

    @Override
    public int compareTo(@Nonnull HeapEntry<T> other) {
      return Comparator.comparing(HeapEntry<T>::getCurrentKey, this.comparator).compare(this, other);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      HeapEntry<T> other = (HeapEntry<T>) obj;
      return this.compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
      return currentKey.hashCode();
    }
  }
}
