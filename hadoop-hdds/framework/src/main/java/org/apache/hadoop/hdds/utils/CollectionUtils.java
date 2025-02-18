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

import static java.util.Comparator.naturalOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Utility methods for Java Collections. */
public interface CollectionUtils {
  static <KEY, VALUE> Map<KEY, VALUE> newUnmodifiableMap(
      List<VALUE> values, Function<VALUE, KEY> getKey,
      Map<KEY, VALUE> existing) {
    final Map<KEY, VALUE> map = new HashMap<>(existing);
    for (VALUE v : values) {
      final KEY key = getKey.apply(v);
      final VALUE previous = map.put(key, v);
      if (previous != null) {
        throw new IllegalArgumentException("Already exists: " + key
            + ", previous " + previous.getClass());
      }
    }
    return Collections.unmodifiableMap(map);
  }

  static <KEY, VALUE> Map<KEY, List<VALUE>> newUnmodifiableMultiMap(
      List<VALUE> values, Function<VALUE, KEY> getKey) {
    final Map<KEY, List<VALUE>> map = new HashMap<>();
    for (VALUE v : values) {
      final KEY key = getKey.apply(v);
      map.computeIfAbsent(key, k -> new ArrayList<>()).add(v);
    }
    return Collections.unmodifiableMap(map.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            e -> Collections.unmodifiableList(e.getValue()))));
  }

  static <T> Iterator<T> newIterator(Collection<List<T>> values) {
    final Iterator<List<T>> listIterator = values.iterator();
    return new Iterator<T>() {
      private Iterator<T> i = Collections.emptyIterator();

      private Iterator<T> nextIterator() {
        if (i.hasNext()) {
          return i;
        }
        while (listIterator.hasNext()) {
          i = listIterator.next().iterator();
          if (i.hasNext()) {
            return i;
          }
        }
        return Collections.emptyIterator();
      }

      @Override
      public boolean hasNext() {
        return nextIterator().hasNext();
      }

      @Override
      public T next() {
        if (hasNext()) {
          return i.next();
        }
        throw new NoSuchElementException();
      }
    };
  }

  static <T extends Comparable<T>> List<T> findTopN(Iterable<T> input, int n) {
    return findTopN(input, n, any -> true);
  }

  static <T extends Comparable<T>> List<T> findTopN(
      Iterable<T> input,
      int n,
      Predicate<? super T> filter
  ) {
    return findTopN(input, n, naturalOrder(), filter);
  }

  static <T> List<T> findTopN(
      Iterable<T> input,
      int n,
      Comparator<T> comparator
  ) {
    return findTopN(input, n, comparator, any -> true);
  }

  static <T> List<T> findTopN(
      Iterable<T> input,
      int n,
      Comparator<T> comparator,
      Predicate<? super T> filter
  ) {
    PriorityQueue<T> heap = new PriorityQueue<>(comparator);

    for (T item : input) {
      if (filter.test(item)) {
        heap.add(item);

        if (heap.size() > n) {
          heap.poll();
        }
      }
    }

    LinkedList<T> result = new LinkedList<>();
    while (!heap.isEmpty()) {
      result.addFirst(heap.poll());
    }

    return result;
  }
}
