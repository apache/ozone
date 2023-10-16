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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Cache with FIFO functionality with limit. If resource usages crosses the
 * limit, first entry will be removed. This does not ensure meeting exact limit
 * as first entry removal may not meet the limit.
 */
public class ResourceCache<K, V> implements Cache<K, V> {
  private final Map<K, V> map;
  private final Queue<K> queue;
  private final BiFunction<K, V, long[]> permitsSupplier;
  private final List<Long> resources;
  private List<AtomicLong> usages;

  public ResourceCache(
      BiFunction<K, V, long[]> permitsSupplier, long... limits) {
    Objects.requireNonNull(permitsSupplier);
    Objects.requireNonNull(limits);
    this.map = new ConcurrentHashMap<>();
    this.queue = new ConcurrentLinkedQueue<>();
    this.permitsSupplier = permitsSupplier;
    this.resources = new ArrayList<>();
    this.usages = new ArrayList<>();
    for (long limit : limits) {
      resources.add(limit);
      usages.add(new AtomicLong(0));
    }
  }

  @Override
  public V get(K key) {
    Objects.requireNonNull(key);
    return map.get(key);
  }

  @Override
  public V put(K key, V value) throws InterruptedException {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);

    // remove the old key to release the permits
    V oldVal = remove(key);
    map.put(key, value);
    queue.add(key);

    boolean isPermitCrosses = false;
    long[] permits = permitsSupplier.apply(key, value);
    for (int i = 0; i < permits.length; ++i) {
      if (usages.get(i).addAndGet(permits[i]) > resources.get(i)) {
        isPermitCrosses = true;
      }
    }

    if (isPermitCrosses) {
      // remove first element
      K keyToRemove = queue.poll();
      if (null != keyToRemove) {
        remove(keyToRemove);
      }
    }
    return oldVal;
  }

  @Override
  public V remove(K key) {
    Objects.requireNonNull(key);
    V val = map.remove(key);
    if (val != null) {
      long[] permits = permitsSupplier.apply(key, val);
      for (int i = 0; i < permits.length; ++i) {
        usages.get(i).addAndGet(-permits[i]);
      }
    }
    queue.remove(key);
    return val;
  }

  @Override
  public void removeIf(Predicate<K> predicate) {
    Objects.requireNonNull(predicate);
    for (K key : map.keySet()) {
      if (predicate.test(key)) {
        remove(key);
      }
    }
  }

  @Override
  public void clear() {
    for (K key : map.keySet()) {
      remove(key);
    }
  }
}
