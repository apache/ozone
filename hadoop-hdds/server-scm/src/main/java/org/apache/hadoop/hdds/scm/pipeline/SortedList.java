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

package org.apache.hadoop.hdds.scm.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * A sorted list using bucket-sort.
 * The elements are sorted by their weights
 * while different elements may have the same weight.
 * <p>
 * The number of buckets is assumed to be much smaller than the number of elements.
 * For examples, a cluster may have 5,000 datanodes (elements)
 * but the number of pipelines (buckets) is always less than 100.
 * Therefore, this class (n log b) is more efficient than the usual sorting (n log n),
 * where n is the number of elements and b is the number of buckets.
 * <p>
 * Note that some unused methods in {@link List} are unsupported.
 * <p>
 * This class is not threadsafe.
 */
final class SortedList<E> implements List<E> {
  private final Class<E> clazz;
  private final SortedMap<Integer, List<E>> buckets = new TreeMap<>();
  private int numElements = 0;

  SortedList(Class<E> clazz) {
    Objects.requireNonNull(clazz, "clazz == null");
    this.clazz = clazz;
  }

  @Override
  public int size() {
    return numElements;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  /** Add the given element with the given weight to this list. */
  public boolean add(E element, int weight) {
    Objects.requireNonNull(element, "element == null");
    buckets.computeIfAbsent(weight, k -> new ArrayList<>(64)).add(element);
    numElements++;
    return true;
  }

  private E getOrRmove(String name, int index, BiFunction<List<E>, Integer, E> method) {
    if (index < 0) {
      throw new IndexOutOfBoundsException("index = " + index + " < 0");
    }
    final int s = size();
    if (index >= s) {
      throw new IndexOutOfBoundsException("index = " + index + " >= size = " + s);
    }

    for (Iterator<List<E>> i = buckets.values().iterator(); i.hasNext();) {
      final List<E> bucket = i.next();
      final int n = bucket.size();
      if (index < n) {
        final E e = method.apply(bucket, index);
        if (bucket.isEmpty()) {
          i.remove();
        }
        return e;
      }
      index -= n;
    }
    throw new IllegalStateException("Failed to " + name + " element at index " + index + ", " + this);
  }

  @Override
  public E get(int index) {
    return getOrRmove("get", index, List::get);
  }

  @Override
  public E remove(int index) {
    final E removed = getOrRmove("remove", index, (list, i) -> list.remove((int)i));
    numElements--;
    return removed;
  }

  private boolean containsOrRemove(Object element, Predicate<List<E>> method) {
    if (!clazz.isInstance(element)) {
      return false;
    }
    for (Iterator<List<E>> i = buckets.values().iterator(); i.hasNext();) {
      final List<E> bucket = i.next();
      if (method.test(bucket)) {
        if (bucket.isEmpty()) {
          i.remove();
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean contains(Object element) {
    return containsOrRemove(element, b -> b.contains(element));
  }

  @Override
  public boolean remove(Object element) {
    return containsOrRemove(element, b -> {
      if (b.remove(element)) {
        numElements--;
        return true;
      }
      return false;
    });
  }

  @Override
  public boolean removeAll(Collection<?> elements) {
    boolean changed = false;
    for (Object e : elements) {
      changed |= remove(e);
    }
    return changed;
  }

  @Override
  public void clear() {
    buckets.clear();
    numElements = 0;
  }

  @Override
  public Iterator<E> iterator() {
    return new Iterator<E>() {
      private final Iterator<List<E>> bucketIterator = buckets.values().iterator();
      private Iterator<E> i = bucketIterator.hasNext() ? bucketIterator.next().iterator()
          : Collections.<E>emptyIterator();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public E next() {
        final E element = i.next();
        if (!i.hasNext()) {
          if (bucketIterator.hasNext()) {
            i = bucketIterator.next().iterator();
          }
        }
        return element;
      }
    };
  }

  @Override
  public String toString() {
    if (numElements == 0) {
      return "[]";
    }

    final StringBuilder b = new StringBuilder("[");
    for (Map.Entry<Integer, List<E>> e : buckets.entrySet()) {
      final List<E> list = e.getValue();
      b.append("\n  ").append(e.getKey())
          .append(" #").append(list.size())
          .append(": ").append(list);
    }
    return b.append("\n] (").append("size=").append(size()).append(")\n").toString();
  }

  // ------- The methods below are unsupported. -------
  @Override
  public E set(int index, E element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(E e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(int index, E element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<E> listIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }
}
