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

package com.google.common.util.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Similar to {@link Striped} except that
 * the get methods in this class
 * return {@link StripedLock} which includes the stripe index.
 */
public final class StripedReadWriteLocks {

  public static StripedReadWriteLocks newInstance(int stripes, boolean fair) {
    final Striped<ReentrantReadWriteLock> striped = Striped.custom(
        stripes, () -> new ReentrantReadWriteLock(fair));
    return new StripedReadWriteLocks(striped);
  }

  private final Striped<ReentrantReadWriteLock> striped;

  private StripedReadWriteLocks(Striped<ReentrantReadWriteLock> striped) {
    this.striped = striped;
  }

  public <K> StripedLock<K> get(K key) {
    final int index = striped.indexFor(key);
    final ReentrantReadWriteLock lock = striped.getAt(index);
    return new StripedLock<>(key, lock, index);
  }

  public <K> Iterable<StripedLock<K>> bulkGet(Iterable<K> keys) {
    final Map<Integer, ReentrantReadWriteLock> sorted = new TreeMap<>();
    final List<StripedLock<K>> list = new ArrayList<>();
    for (K k : keys) {
      final int index = striped.indexFor(k);
      final ReentrantReadWriteLock lock = sorted.computeIfAbsent(index, striped::getAt);
      list.add(new StripedLock<>(k, lock, index));
    }
    list.sort(StripedLock.getComparator());
    return Collections.unmodifiableList(list);
  }
}
