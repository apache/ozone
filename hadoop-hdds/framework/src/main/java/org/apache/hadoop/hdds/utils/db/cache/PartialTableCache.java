/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.utils.db.cache;

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache implementation for the table. Partial Table cache, where the DB state
 * and cache state will not be same. Partial table cache holds entries until
 * flush to DB happens.
 */
@Private
@Evolving
public class PartialTableCache<CACHEKEY extends CacheKey,
    CACHEVALUE extends CacheValue> implements TableCache<CACHEKEY, CACHEVALUE> {

  public static final Logger LOG =
      LoggerFactory.getLogger(PartialTableCache.class);

  private final Map<CACHEKEY, CACHEVALUE> cache;
  private final NavigableMap<Long, Set<CACHEKEY>> epochEntries;
  private ExecutorService executorService;
  private final CacheStatsRecorder statsRecorder;


  public PartialTableCache() {
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
    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("PartialTableCache Cleanup Thread - %d").build();
    executorService = Executors.newSingleThreadExecutor(build);

    statsRecorder = new CacheStatsRecorder();
  }

  @Override
  public CACHEVALUE get(CACHEKEY cachekey) {
    CACHEVALUE value = cache.get(cachekey);
    statsRecorder.recordValue(value);
    return value;
  }

  @Override
  public void loadInitial(CACHEKEY cacheKey, CACHEVALUE cacheValue) {
    // Do nothing for partial table cache.
  }

  @Override
  public void put(CACHEKEY cacheKey, CACHEVALUE value) {
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
  public Iterator<Map.Entry<CACHEKEY, CACHEVALUE>> iterator() {
    statsRecorder.recordIteration();
    return cache.entrySet().iterator();
  }

  @VisibleForTesting
  @Override
  public void evictCache(List<Long> epochs) {
    Set<CACHEKEY> currentCacheKeys;
    CACHEKEY cachekey;
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
        for (Iterator<CACHEKEY> iterator = currentCacheKeys.iterator();
             iterator.hasNext();) {
          cachekey = iterator.next();
          cache.computeIfPresent(cachekey, ((k, v) -> {
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
  public CacheResult<CACHEVALUE> lookup(CACHEKEY cachekey) {

    CACHEVALUE cachevalue = cache.get(cachekey);
    statsRecorder.recordValue(cachevalue);
    if (cachevalue == null) {
      return new CacheResult<>(CacheResult.CacheStatus.MAY_EXIST,
            null);
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
  public NavigableMap<Long, Set<CACHEKEY>> getEpochEntries() {
    return epochEntries;
  }

  @Override
  public CacheStats getStats() {
    return statsRecorder.snapshot();
  }
}
