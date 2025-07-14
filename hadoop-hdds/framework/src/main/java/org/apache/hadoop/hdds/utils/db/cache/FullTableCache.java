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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache implementation for the table. Full Table cache, where the DB state
 * and cache state will be same for these tables.
 * @param <KEY>
 * @param <VALUE>
 */
@Private
@Evolving
public class FullTableCache<KEY, VALUE> implements TableCache<KEY, VALUE> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FullTableCache.class);

  private final Map<CacheKey<KEY>, CacheValue<VALUE>> cache;
  private final NavigableMap<Long, Set<CacheKey<KEY>>> epochEntries;
  private final ScheduledExecutorService executorService;
  private final Queue<Long> epochCleanupQueue = new ConcurrentLinkedQueue<>();

  private final ReadWriteLock lock;

  private final CacheStatsRecorder statsRecorder;

  public FullTableCache(String threadNamePrefix) {
    // As for full table cache only we need elements to be inserted in sorted
    // manner, so that list will be easy. But look ups have log(N) time
    // complexity.

    // Here lock is required to protect cache because cleanup is not done
    // under any ozone level locks like bucket/volume, there is a chance of
    // cleanup which are not flushed to disks when request processing thread
    // updates entries.
    cache = new ConcurrentSkipListMap<>();

    lock = new ReentrantReadWriteLock();

    epochEntries = new ConcurrentSkipListMap<>();

    // Created a singleThreadExecutor, so one cleanup will be running at a
    // time.
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(threadNamePrefix + "FullTableCache-Cleanup-%d")
        .build();
    executorService = Executors.newScheduledThreadPool(1, threadFactory);
    executorService.scheduleWithFixedDelay(() -> cleanupTask(), 0, 1000L, TimeUnit.MILLISECONDS);
    statsRecorder = new CacheStatsRecorder();
  }

  @Override
  public CacheValue<VALUE> get(CacheKey<KEY> cachekey) {
    try {
      lock.readLock().lock();
      CacheValue<VALUE> cachevalue = cache.get(cachekey);
      statsRecorder.recordValue(cachevalue);
      return cachevalue;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void loadInitial(CacheKey<KEY> key, CacheValue<VALUE> value) {
    // No need to add entry to epochEntries. Adding to cache is required during
    // normal put operation.
    // No need of acquiring lock, this is performed only during startup. No
    // operations happening at that time.
    cache.put(key, value);
  }

  @Override
  public void put(CacheKey<KEY> cacheKey, CacheValue<VALUE> value) {
    try {
      lock.readLock().lock();
      cache.put(cacheKey, value);
      // add in case of null value for cleanup purpose only when key is deleted
      if (value.getCacheValue() == null) {
        epochEntries.computeIfAbsent(value.getEpoch(),
            v -> new CopyOnWriteArraySet<>()).add(cacheKey);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void cleanup(List<Long> epochs) {
    epochCleanupQueue.clear();
    epochCleanupQueue.addAll(epochs);
  }

  private void cleanupTask() {
    if (epochCleanupQueue.isEmpty()) {
      return;
    }
    ArrayList<Long> epochList = new ArrayList<>(epochCleanupQueue);
    epochCleanupQueue.removeAll(epochList);
    evictCache(epochList);
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
    // when no delete entries, can exit immediately
    if (epochEntries.isEmpty()) {
      return;
    }

    Set<CacheKey<KEY>> currentCacheKeys;

    long lastEpoch = epochs.get(epochs.size() - 1);
    // Acquire lock to avoid race between cleanup and add to cache entry by
    // client requests.
    try {
      lock.writeLock().lock();
      for (long currentEpoch : epochEntries.keySet()) {
        currentCacheKeys = epochEntries.get(currentEpoch);

        // If currentEntry epoch is greater than last epoch provided, we have
        // deleted all entries less than specified epoch. So, we can break.
        if (currentEpoch > lastEpoch) {
          break;
        }

        for (CacheKey<KEY> currentCacheKey : currentCacheKeys) {
          cache.computeIfPresent(currentCacheKey, ((k, v) -> {
            // If cache epoch entry matches with current Epoch, remove entry
            // from cache.
            if (v.getCacheValue() == null && v.getEpoch() == currentEpoch) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("CacheKey {} with epoch {} is removed from cache",
                    k.getCacheKey(), currentEpoch);
              }
              return null;
            }
            return v;
          }));
          // Remove epoch entry, as the entry is there in epoch list.
          epochEntries.remove(currentEpoch);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public CacheResult<VALUE> lookup(CacheKey<KEY> cachekey) {

    CacheValue<VALUE> cachevalue = cache.get(cachekey);
    statsRecorder.recordValue(cachevalue);
    if (cachevalue == null) {
      return new CacheResult<>(CacheResult.CacheStatus.NOT_EXIST, null);
    } else {
      if (cachevalue.getCacheValue() != null) {
        return new CacheResult<>(CacheResult.CacheStatus.EXISTS, cachevalue);
      } else {
        // When entity is marked for delete, cacheValue will be set to null.
        // In that case we can return NOT_EXIST irrespective of cache cleanup
        // policy.
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
    return CacheType.FULL_CACHE;
  }
}
