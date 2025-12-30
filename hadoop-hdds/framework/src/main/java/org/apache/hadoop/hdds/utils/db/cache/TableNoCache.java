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

package org.apache.hadoop.hdds.utils.db.cache;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Evolving;

/**
 * Dummy cache implementation for the table, means key/value are not cached. 
 * @param <KEY>
 * @param <VALUE>
 */
@Private
@Evolving
public final class TableNoCache<KEY, VALUE> implements TableCache<KEY, VALUE> {
  public static final CacheStats EMPTY_STAT = new CacheStats(0, 0, 0);

  private static final TableCache<?, ?> NO_CACHE_INSTANCE = new TableNoCache<>();

  public static <K, V> TableCache<K, V> instance() {
    return (TableCache<K, V>) NO_CACHE_INSTANCE;
  }

  private TableNoCache() {
  }

  @Override
  public CacheValue<VALUE> get(CacheKey<KEY> cachekey) {
    return null;
  }

  @Override
  public void loadInitial(CacheKey<KEY> key, CacheValue<VALUE> value) {
  }

  @Override
  public void put(CacheKey<KEY> cacheKey, CacheValue<VALUE> value) {
  }

  @Override
  public void cleanup(List<Long> epochs) {
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Iterator<Map.Entry<CacheKey<KEY>, CacheValue<VALUE>>> iterator() {
    return Collections.emptyIterator();
  }

  @VisibleForTesting
  @Override
  public void evictCache(List<Long> epochs) {
  }

  @Override
  public CacheResult<VALUE> lookup(CacheKey<KEY> cachekey) {
    return (CacheResult<VALUE>) MAY_EXIST;
  }

  @VisibleForTesting
  @Override
  public NavigableMap<Long, Set<CacheKey<KEY>>> getEpochEntries() {
    return Collections.emptyNavigableMap();
  }

  @Override
  public CacheStats getStats() {
    return EMPTY_STAT;
  }

  @Override
  public CacheType getCacheType() {
    return CacheType.NO_CACHE;
  }
}
