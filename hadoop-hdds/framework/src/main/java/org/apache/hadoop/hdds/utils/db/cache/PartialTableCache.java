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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache implementation for the table. Partial Table cache, where the DB state
 * and cache state will not be same. Partial table cache holds entries until
 * flush to DB happens.
 * @param <KEY>
 * @param <VALUE>
 */
@Private
@Evolving
public class PartialTableCache<KEY, VALUE> implements TableCache<KEY, VALUE> {

  private static final Logger LOG =
      LoggerFactory.getLogger(PartialTableCache.class);

  private final Map<CacheKey<KEY>, CacheValue<VALUE>> cache;
  private final NavigableMap<Long, Set<CacheKey<KEY>>> epochEntries;
  private final ExecutorService executorService;
  private final CacheStatsRecorder statsRecorder;

  public PartialTableCache(String threadNamePrefix) {
    // We use concurrent Hash map for O(1) lookup for get API.
    // During list operation for partial cache we anyway merge between DB and
    // cache state. So entries in cache does not need to be in sorted order.

    // And as concurrentHashMap computeIfPresent which is used by cleanup is
    // atomic operation, and ozone level locks like bucket/volume locks
    // protect updating same key, here it is not required to hold cache
    // level locks during update/cleanup operation.

    // 1. During update, it is caller responsibility to hold volume/bucket
    // locks.
    // 2. During cleanup which removes entry, while request is updating cache
    // that should be guarded by concurrentHashMap guaranty.
    cache = new ConcurrentHashMap<>();

    epochEntries = new ConcurrentSkipListMap<>();
    // Created a singleThreadExecutor, so one cleanup will be running at a
    // time.
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(threadNamePrefix + "PartialTableCache-Cleanup-%d")
        .build();
    executorService = Executors.newSingleThreadExecutor(threadFactory);

    statsRecorder = new CacheStatsRecorder();
  }

  @Override
  public CacheValue<VALUE> get(CacheKey<KEY> cachekey) {
    CacheValue<VALUE> value = cache.get(cachekey);
    statsRecorder.recordValue(value);
    return value;
  }

  @Override
  public void loadInitial(CacheKey<KEY> key, CacheValue<VALUE> value) {
    // Do nothing for partial table cache.
  }

  @Override
  public void put(CacheKey<KEY> cacheKey, CacheValue<VALUE> value) {
    cache.put(cacheKey, value);
    epochEntries.computeIfAbsent(value.getEpoch(), v -> new HashSet<>())
            .add(cacheKey);
  }

  @Override
  public void cleanup(List<Long> epochs) {
    executorService.execute(() -> evictCache(epochs));
  }

  @Override
  public int size() {
    return cache.size();
  }

  @Override
  public Iterator<Map.Entry<CacheKey<KEY>, CacheValue<VALUE>>> iterator() {
    statsRecorder.recordIteration();
    return cache.entrySet().iterator();
  }

  @VisibleForTesting
  @Override
  public void evictCache(List<Long> epochs) {
    Set<CacheKey<KEY>> currentCacheKeys;

    long lastEpoch = epochs.get(epochs.size() - 1);
    for (long currentEpoch : epochEntries.keySet()) {
      currentCacheKeys = epochEntries.get(currentEpoch);

      // If currentEntry epoch is greater than last epoch provided, we have
      // deleted all entries less than specified epoch. So, we can break.
      if (currentEpoch > lastEpoch) {
        break;
      }
      // As ConcurrentHashMap computeIfPresent is atomic, there is no race
      // condition between cache cleanup and requests updating same cache entry.
      if (epochs.contains(currentEpoch)) {
        for (CacheKey<KEY> currentCacheKey : currentCacheKeys) {
          cache.computeIfPresent(currentCacheKey, ((k, v) -> {
            // If cache epoch entry matches with current Epoch, remove entry
            // from cache.
            if (v.getEpoch() == currentEpoch) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("CacheKey {} with epoch {} is removed from cache",
                        k.getCacheKey(), currentEpoch);
              }
              return null;
            }
            return v;
          }));
        }
        // Remove epoch entry, as the entry is there in epoch list.
        epochEntries.remove(currentEpoch);
      }
    }
  }

  @Override
  public CacheResult<VALUE> lookup(CacheKey<KEY> cachekey) {

    CacheValue<VALUE> cachevalue = cache.get(cachekey);
    statsRecorder.recordValue(cachevalue);
    if (cachevalue == null) {
      return (CacheResult<VALUE>) MAY_EXIST;
    } else {
      if (cachevalue.getCacheValue() != null) {
        return new CacheResult<>(CacheResult.CacheStatus.EXISTS, cachevalue);
      } else {
        // When entity is marked for delete, cacheValue will be set to null.
        return new CacheResult<>(CacheResult.CacheStatus.NOT_EXIST, null);
      }
    }
  }

  @VisibleForTesting
  @Override
  public NavigableMap<Long, Set<CacheKey<KEY>>> getEpochEntries() {
    return epochEntries;
  }

  @Override
  public CacheStats getStats() {
    return statsRecorder.snapshot();
  }

  @Override
  public CacheType getCacheType() {
    return CacheType.PARTIAL_CACHE;
  }
}
