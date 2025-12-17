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

package org.apache.hadoop.ozone.om.helpers;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Helps incrementally build an immutable map. */
public final class MapBuilder<K, V> {
  /** The original map being built from, used if no changes are made, to reduce copying. */
  private final ImmutableMap<K, V> original;
  /** The updated map being built, created lazily on the first modification. */
  private Map<K, V> updated;
  /** Whether any changes were made. */
  private boolean changed;

  public static <K, V> MapBuilder<K, V> empty() {
    return new MapBuilder<>(ImmutableMap.of());
  }

  public static <K, V> MapBuilder<K, V> of(ImmutableMap<K, V> original) {
    return new MapBuilder<>(original);
  }

  public static <K, V> MapBuilder<K, V> copyOf(Map<K, V> original) {
    return new MapBuilder<>(ImmutableMap.copyOf(original));
  }

  private MapBuilder(ImmutableMap<K, V> original) {
    this.original = original;
  }

  public void put(final K key, final V value) {
    Objects.requireNonNull(key, "key == null");
    Objects.requireNonNull(value, "value == null");

    ensureInitialized();
    V prev = updated.put(key, value);
    changed |= prev != value;
  }

  public void putAll(final Map<? extends K, ? extends V> map) {
    Objects.requireNonNull(map, "map == null");

    if (!map.isEmpty()) {
      ensureInitialized();
      updated.putAll(map);
      changed = true; // assume change instead of checking
    }
  }

  public void remove(final K key) {
    Objects.requireNonNull(key, "key == null");

    if (updated == null && !original.containsKey(key)) {
      return;
    }

    ensureInitialized();
    V prev = updated.remove(key);
    changed |= prev != null;
  }

  /** Set the map being built to {@code map}.
   * For further mutations to work, it must be modifiable,
   * but it can also be immutable if this is the last change. */
  public void set(final Map<K, V> map) {
    Objects.requireNonNull(map, "map == null");

    updated = map;
    changed = map != original; // assume change instead of deep checking equality
  }

  public boolean isChanged() {
    return changed;
  }

  public ImmutableMap<K, V> build() {
    return changed ? ImmutableMap.copyOf(updated) : original;
  }

  ImmutableMap<K, V> initialValue() {
    return original;
  }

  private void ensureInitialized() {
    if (updated == null) {
      updated = new LinkedHashMap<>(original);
    }
  }
}
