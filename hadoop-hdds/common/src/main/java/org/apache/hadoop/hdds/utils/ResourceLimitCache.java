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


import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Cache with resource limit constraints. At any time all entries in the cache
 * satisfy the resource limit constraints in the constructor. New put
 * operations are blocked until resources are released via remove or clear
 * operation.
 */
public class ResourceLimitCache<K, V> implements Cache<K, V> {
  private final java.util.concurrent.ConcurrentMap<K, V> map;
  private final ResourceSemaphore.Group group;
  private final BiFunction<K, V, int[]> permitsSupplier;

  public ResourceLimitCache(java.util.concurrent.ConcurrentMap<K, V> map,
      BiFunction<K, V, int[]> permitsSupplier, int... limits) {
    Objects.requireNonNull(map);
    Objects.requireNonNull(permitsSupplier);
    Objects.requireNonNull(limits);
    this.map = map;
    this.group = new ResourceSemaphore.Group(limits);
    this.permitsSupplier = permitsSupplier;
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
    int[] permits = permitsSupplier.apply(key, value);
    group.acquire(permits);
    try {
      map.put(key, value);
    } catch (Throwable t) {
      group.release(permits);
    }
    return oldVal;
  }

  @Override
  public V remove(K key) {
    Objects.requireNonNull(key);
    V val = map.remove(key);
    if (val != null) {
      group.release(permitsSupplier.apply(key, val));
    }
    return val;
  }

  @Override
  public void removeIf(Predicate<K> predicate) {
    Objects.requireNonNull(predicate);
    map.keySet().removeIf(predicate);
  }

  @Override
  public void clear() {
    for (K key : map.keySet()) {
      remove(key);
    }
  }
}
