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

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Cache with FIFO functionality with limit. If resource usages crosses the
 * limit, the first entries are removed until the usage is within the limit again.
 *
 * <p>This is not a general-purpose cache library on purpose: an admission policy, like Caffeine's TinyLFU, would
 * evict the newest entries, which the cache is for (for example the log entries that followers read next).
 * The removal listener is called without holding the cache lock.
 */
public class ResourceCache<K, V> implements Cache<K, V> {
  private final Weigher<K, V> weigher;
  private final long limits;
  private final RemovalListener<K, V> listener;
  // in insertion order; guarded by this
  private final Map<K, V> entries = new LinkedHashMap<>();
  private long weight;

  public ResourceCache(
      Weigher<K, V> weigher, long limits,
      RemovalListener<K, V> listener) {
    this.weigher = Objects.requireNonNull(weigher, "weigher == null");
    this.limits = limits;
    this.listener = listener;
  }

  @Override
  public synchronized V get(K key) {
    Objects.requireNonNull(key, "key == null");
    return entries.get(key);
  }

  @Override
  public void put(K key, V value) throws InterruptedException {
    Objects.requireNonNull(key, "key == null");
    Objects.requireNonNull(value, "value == null");
    final int valueWeight = weigher.weigh(key, value);
    if (valueWeight < 0) {
      throw new IllegalArgumentException("Negative weight " + valueWeight + " for key " + key);
    }
    final List<Removal<K, V>> removals = new ArrayList<>();
    synchronized (this) {
      final V previous = entries.put(key, value);
      weight += valueWeight;
      if (previous != null) {
        weight -= weigher.weigh(key, previous);
        removals.add(new Removal<>(key, previous, RemovalCause.REPLACED));
      }
      for (Iterator<Map.Entry<K, V>> i = entries.entrySet().iterator(); weight > limits && i.hasNext();) {
        final Map.Entry<K, V> eldest = i.next();
        i.remove();
        weight -= weigher.weigh(eldest.getKey(), eldest.getValue());
        removals.add(new Removal<>(eldest.getKey(), eldest.getValue(), RemovalCause.SIZE));
      }
    }
    notifyListener(removals);
  }

  @Override
  public void remove(K key) {
    Objects.requireNonNull(key, "key == null");
    final V removed;
    synchronized (this) {
      removed = entries.remove(key);
      if (removed != null) {
        weight -= weigher.weigh(key, removed);
      }
    }
    if (removed != null && listener != null) {
      listener.onRemoval(key, removed, RemovalCause.EXPLICIT);
    }
  }

  @Override
  public void removeIf(Predicate<K> predicate) {
    Objects.requireNonNull(predicate, "predicate == null");
    removeAll(predicate);
  }

  @Override
  public void clear() {
    removeAll(key -> true);
  }

  private void removeAll(Predicate<K> predicate) {
    final List<Removal<K, V>> removals = new ArrayList<>();
    synchronized (this) {
      for (Iterator<Map.Entry<K, V>> i = entries.entrySet().iterator(); i.hasNext();) {
        final Map.Entry<K, V> entry = i.next();
        if (predicate.test(entry.getKey())) {
          i.remove();
          weight -= weigher.weigh(entry.getKey(), entry.getValue());
          removals.add(new Removal<>(entry.getKey(), entry.getValue(), RemovalCause.EXPLICIT));
        }
      }
    }
    notifyListener(removals);
  }

  private void notifyListener(List<Removal<K, V>> removals) {
    if (listener != null) {
      for (Removal<K, V> removal : removals) {
        listener.onRemoval(removal.key, removal.value, removal.cause);
      }
    }
  }

  /** An entry removed from the cache, for the listener. */
  private static final class Removal<K, V> {
    private final K key;
    private final V value;
    private final RemovalCause cause;

    Removal(K key, V value, RemovalCause cause) {
      this.key = key;
      this.value = value;
      this.cause = cause;
    }
  }
}
