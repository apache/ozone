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

package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.primitives.UnsignedBytes;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.ozone.util.ClosableIterator;

/**
 * Util classes for Snapshot Persistent DataStructures for tests.
 */
public class SnapshotTestUtils {

  private static final CodecRegistry CODEC_REGISTRY = CodecRegistry.newBuilder().build();

  private static final Comparator<byte[]> UNSIGNED_BYTE_COMPARATOR = UnsignedBytes.lexicographicalComparator();
  private static final Comparator<Object> COMPARATOR = (k1, k2) -> {
    try {
      byte[] k1Bytes = CODEC_REGISTRY.asRawData(k1);
      byte[] k2Bytes = CODEC_REGISTRY.asRawData(k2);
      return UNSIGNED_BYTE_COMPARATOR.compare(k1Bytes, k2Bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  };

  /**
   * Stubbed implementation of CloseableIterator containing iterators.
   */
  private static class StubbedCloseableIterator<T> implements
      ClosableIterator<T> {
    private final Iterator<T> iterator;

    StubbedCloseableIterator(Iterator<T> iterator) {
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
   * Stubbed implementation Persistent Map for testing.
   */
  public static class StubbedPersistentMap<K, V> implements
      PersistentMap<K, V>  {

    private final TreeMap<K, V> map;

    public StubbedPersistentMap(Map<K, V> map) {
      this();
      this.map.putAll(map);
    }

    public StubbedPersistentMap() {

      this.map = new TreeMap<>(COMPARATOR);
    }

    @Override
    public V get(K key) {
      return this.map.get(key);
    }

    @Override
    public void put(K key, V value) {
      this.map.put(key, value);
    }

    @Override
    public void remove(K key) {
      this.map.remove(key);
    }

    @Override
    public ClosableIterator<Map.Entry<K, V>> iterator(
        Optional<K> lowerBoundKey, Optional<K> upperBoundKey) {
      return new StubbedCloseableIterator<>(this.map.entrySet().stream().filter(
          kvEntry ->
              lowerBoundKey.map(k -> this.map.comparator()
                      .compare(kvEntry.getKey(), k) >= 0).orElse(Boolean.TRUE)
                  &&
              upperBoundKey.map(k -> this.map.comparator()
                  .compare(kvEntry.getKey(), k) < 0).orElse(true))
          .iterator());
    }
  }

  /**
   * Stubbed implementation of Persistent Set for testing.
   */
  public static class StubbedPersistentSet<K> implements PersistentSet<K> {
    private final Set<K> set;

    public StubbedPersistentSet(Set<K> map) {
      this();
      map.iterator().forEachRemaining(this::add);
    }

    public StubbedPersistentSet() {
      this.set = new TreeSet<>(COMPARATOR);
    }

    @Override
    public void add(K entry) {
      set.add(entry);
    }

    @Override
    public ClosableIterator<K> iterator() {
      return new StubbedCloseableIterator<>(set.stream().iterator());
    }
  }

  /**
   * Stubbed implementation of Persistent List for testing.
   */
  public static class ArrayPersistentList<K> extends ArrayList<K>
      implements PersistentList<K> {

    @Override
    public boolean addAll(PersistentList<K> from) {
      boolean ret = true;
      Iterator<K> iterator = from.iterator();
      while (iterator.hasNext()) {
        ret = ret && this.add(iterator.next());
      }
      return ret;
    }

    @Override
    public ClosableIterator<K> iterator() {
      return new StubbedCloseableIterator<>(this.stream().iterator());
    }
  }

}
