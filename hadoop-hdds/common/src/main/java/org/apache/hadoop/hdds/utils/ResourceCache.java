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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Cache with FIFO functionality with limit. If resource usages crosses the
 * limit, first entry will be removed. This does not ensure meeting exact limit
 * as first entry removal may not meet the limit.
 */
public class ResourceCache<K, V> implements Cache<K, V> {
  private final com.google.common.cache.Cache<K, V> cache;

  public ResourceCache(
      Weigher<K, V> weigher, long limits,
      RemovalListener<K, V> listener) {
    Objects.requireNonNull(weigher, "weigher == null");
    if (listener == null) {
      cache = CacheBuilder.newBuilder()
          .maximumWeight(limits).weigher(weigher).build();
    } else {
      cache = CacheBuilder.newBuilder()
          .maximumWeight(limits).weigher(weigher)
          .removalListener(listener).build();
    }
  }

  @Override
  public V get(K key) {
    Objects.requireNonNull(key, "key == null");
    return cache.getIfPresent(key);
  }

  @Override
  public void put(K key, V value) throws InterruptedException {
    Objects.requireNonNull(key, "key == null");
    Objects.requireNonNull(value, "value == null");
    cache.put(key, value);
  }

  @Override
  public void remove(K key) {
    Objects.requireNonNull(key, "key == null");
    cache.invalidate(key);
  }

  @Override
  public void removeIf(Predicate<K> predicate) {
    Objects.requireNonNull(predicate, "predicate == null");
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
