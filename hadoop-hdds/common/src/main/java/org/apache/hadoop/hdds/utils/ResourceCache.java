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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Cache with FIFO functionality with limit. If resource usages crosses the
 * limit, first entry will be removed. This does not ensure meeting exact limit
 * as first entry removal may not meet the limit.
 */
public class ResourceCache<K, V> implements Cache<K, V> {
  private final com.google.common.cache.Cache<K, V> cache;

  public ResourceCache(
      Weigher weigher, int limits,
      BiFunction<Pair<K, V>, Boolean, Void> evictNotifier) {
    Objects.requireNonNull(weigher);
    Objects.requireNonNull(limits);
    RemovalListener<K, V> listener = ele -> {
      if (null != evictNotifier) {
        evictNotifier.apply(Pair.of(ele.getKey(), ele.getValue()),
            ele.wasEvicted());
      }
    };
    cache = CacheBuilder.newBuilder()
        .maximumWeight(limits)
        .weigher(weigher)
        .removalListener(listener)
        .build();
  }

  @Override
  public V get(K key) {
    Objects.requireNonNull(key);
    return cache.getIfPresent(key);
  }

  @Override
  public V put(K key, V value) throws InterruptedException {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    V oldValue = cache.getIfPresent(key);
    cache.put(key, value);
    return oldValue;
  }

  @Override
  public V remove(K key) {
    Objects.requireNonNull(key);
    V value = cache.getIfPresent(key);
    cache.invalidate(key);
    return value;
  }

  @Override
  public void removeIf(Predicate<K> predicate) {
    Objects.requireNonNull(predicate);
    for (K key : cache.asMap().keySet()) {
      if (predicate.test(key)) {
        remove(key);
      }
    }
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }
}
