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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Evolving;

/**
 * Cache used for RocksDB tables.
 * @param <KEY>
 * @param <VALUE>
 */
@Private
@Evolving
public interface TableCache<KEY, VALUE> {
  CacheResult<?> MAY_EXIST = new CacheResult<>(CacheResult.CacheStatus.MAY_EXIST, null);

  /**
   * Return the value for the key if it is present, otherwise return null.
   */
  CacheValue<VALUE> get(CacheKey<KEY> cacheKey);

  /**
   * This method should be called for tables with cache type full cache.
   * {@link TableCache.CacheType#FULL_CACHE} after system
   * restart to fill up the cache.
   */
  void loadInitial(CacheKey<KEY> cacheKey, CacheValue<VALUE> cacheValue);

  /**
   * Add an entry to the cache, if the key already exists it overrides.
   */
  void put(CacheKey<KEY> cacheKey, CacheValue<VALUE> value);

  /**
   * Removes all the entries from the cache which are matching with epoch
   * provided in the epoch list.
   *
   * If clean up policy is NEVER, this is a do nothing operation.
   * If clean up policy is MANUAL, it is caller responsibility to clean up the
   * cache before calling cleanup.
   */
  void cleanup(List<Long> epochs);

  @VisibleForTesting
  void evictCache(List<Long> epochs);

  /**
   * Return the size of the cache.
   * @return size
   */
  int size();

  /**
   * Return an iterator for the cache.
   * @return iterator of the underlying cache for the table.
   */
  Iterator<Map.Entry<CacheKey<KEY>, CacheValue<VALUE>>> iterator();

  /**
   * Check key exist in cache or not.
   *
   * If it exists return CacheResult with value and status as
   * {@link CacheResult.CacheStatus#EXISTS}
   *
   * If it does not exist:
   *  If cache type is
   *  {@link TableCache.CacheType#FULL_CACHE}. It return's {@link CacheResult}
   *  with null and status as {@link CacheResult.CacheStatus#NOT_EXIST}.
   *
   *  If cache type is
   *  {@link TableCache.CacheType#PARTIAL_CACHE}.
   *  It returns {@link CacheResult} with null and status as MAY_EXIST.
   */
  CacheResult<VALUE> lookup(CacheKey<KEY> cachekey);

  @VisibleForTesting
  NavigableMap<Long, Set<CacheKey<KEY>>> getEpochEntries();

  /**
   * Return the stat counters.
   */
  CacheStats getStats();

  /**
   * Return the cache type.
   */
  CacheType getCacheType();

  /**
   * Cache completeness.
   */
  enum CacheType {
    FULL_CACHE, //  This mean's the table maintains full cache. Cache and DB
    // state are same.
    PARTIAL_CACHE, // This is partial table cache, cache state is partial state
    // compared to DB state.
    NO_CACHE
  }
}
