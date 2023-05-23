/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import org.apache.hadoop.util.ClosableIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Util classes for mocking Snapshot Persistent DataStructures.
 */
public class SnapshotTestUtils {

  /**
   * Test CloseableIterator containing iterators.
   */
  private static class DummyCloseableIterator<T> implements
      ClosableIterator<T> {
    private Iterator<T> iterator;

    DummyCloseableIterator(Iterator<T> iterator) {
      this.iterator = iterator;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
      return this.iterator.hasNext();
    }

    @Override
    public T next() {
      return this.iterator.next();
    }
  }

  /**
   * HashMap based Persistent Map for testing.
   * @param <K>
   * @param <V>
   */
  public static class HashPersistentMap<K, V> implements PersistentMap<K, V>  {

    private Map<String, Map.Entry<K, V>> map;

    public HashPersistentMap(Map<K, V> map) {
      this();
      map.entrySet().iterator().forEachRemaining(i ->
          this.put(i.getKey(), i.getValue()));
    }

    public HashPersistentMap() {
      this.map = new HashMap<>();
    }

    private String getStringKey(K key) {
      if (key.getClass().isArray()) {
        Class<?> componentType = key.getClass().getComponentType();
        if (componentType == byte.class) {
          return Arrays.toString((byte[])key);
        } else if (componentType == int.class) {
          return Arrays.toString((int[])key);
        } else if (componentType == long.class) {
          return Arrays.toString((long[])key);
        } else if (componentType == float.class) {
          return Arrays.toString((float[])key);
        } else if (componentType == double.class) {
          return Arrays.toString((double[])key);
        } else if (componentType == char.class) {
          return Arrays.toString((char[])key);
        } else {
          return Arrays.toString((Object[])key);
        }
      }
      return key.toString();
    }

    @Override
    public V get(K key) {
      return Optional.ofNullable(this.map.get(getStringKey(key)))
          .map(Map.Entry::getValue).orElse(null);
    }

    @Override
    public void put(K key, V value) {
      this.map.put(getStringKey(key), new Map.Entry<K, V>() {
        @Override
        public K getKey() {
          return key;
        }

        @Override
        public V getValue() {
          return value;
        }

        @Override
        public V setValue(V v) {
          return value;
        }
      });
    }

    @Override
    public void remove(K key) {
      this.map.remove(getStringKey(key));
    }

    @Override
    public ClosableIterator<Map.Entry<K, V>> iterator() {
      return new DummyCloseableIterator<>(
          this.map.values().stream().iterator());
    }
  }

  /**
   * HashMap based Persistent Set for testing.
   * @param <K>
   */
  public static class HashPersistentSet<K> implements PersistentSet<K> {
    private HashPersistentMap<K, Void> map;

    public HashPersistentSet(Set<K> map) {
      this();
      map.iterator().forEachRemaining(this::add);
    }

    public HashPersistentSet() {
      this.map = new HashPersistentMap();
    }

    @Override
    public void add(K entry) {
      map.put(entry, null);
    }

    @Override
    public ClosableIterator<K> iterator() {
      return new DummyCloseableIterator<>(map.map.values().stream()
          .map(Map.Entry::getKey).iterator());
    }
  }

  /**
   * ArrayList based Persistent List for testing.
   * @param <K>
   */
  public static class ArrayPersistentList<K> extends ArrayList<K>
      implements PersistentList<K> {

    @Override
    public boolean addAll(PersistentList<K> from) {
      boolean[] ret = {true};
      from.iterator().forEachRemaining(
          val -> ret[0] = ret[0] && this.add(val));
      return ret[0];
    }

    @Override
    public ClosableIterator<K> iterator() {
      return new DummyCloseableIterator<>(this.stream().iterator());
    }
  }

}
