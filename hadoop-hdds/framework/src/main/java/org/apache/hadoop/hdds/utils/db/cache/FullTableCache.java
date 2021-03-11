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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache implementation for the table. Full Table cache, where the DB state
 * and cache state will be same for these tables.
 */
@Private
@Evolving
public class FullTableCache<CACHEKEY extends CacheKey,
    CACHEVALUE extends CacheValue> implements TableCache<CACHEKEY, CACHEVALUE> {

  public static final Logger LOG =
      LoggerFactory.getLogger(FullTableCache.class);

  private final Map<CACHEKEY, CACHEVALUE> cache;
  private final NavigableSet<EpochEntry<CACHEKEY>> epochEntries;
  private ExecutorService executorService;

  private final ReadWriteLock lock;


  public FullTableCache() {
    // As for full table cache only we need elements to be inserted in sorted
    // manner, so that list will be easy. But look ups have log(N) time
    // complexity.

    // Here lock is required to protect cache because cleanup is not done
    // under any ozone level locks like bucket/volume, there is a chance of
    // cleanup which are not flushed to disks when request processing thread
    // updates entries.
    cache = new ConcurrentSkipListMap<>();

    lock = new ReentrantReadWriteLock();

    epochEntries = new ConcurrentSkipListSet<>();

    // Created a singleThreadExecutor, so one cleanup will be running at a
    // time.
    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("FullTableCache Cleanup Thread - %d").build();
    executorService = Executors.newSingleThreadExecutor(build);
  }

  @Override
  public CACHEVALUE get(CACHEKEY cachekey) {
    try {
      lock.readLock().lock();
      return cache.get(cachekey);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void loadInitial(CACHEKEY cacheKey, CACHEVALUE cacheValue) {
    // No need to add entry to epochEntries. Adding to cache is required during
    // normal put operation.
    // No need of acquiring lock, this is performed only during startup. No
    // operations happening at that time.
    cache.put(cacheKey, cacheValue);
  }

  @Override
  public void put(CACHEKEY cacheKey, CACHEVALUE value) {
    try {
      lock.writeLock().lock();
      cache.put(cacheKey, value);
      epochEntries.add(new EpochEntry<>(value.getEpoch(), cacheKey));
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void cleanup(List<Long> epochs) {
    executorService.execute(() -> evictCache(epochs));
  }

  @Override
  public int size() {
    return cache.size();
  }

  @Override
  public Iterator<Map.Entry<CACHEKEY, CACHEVALUE>> iterator() {
    return cache.entrySet().iterator();
  }

  @VisibleForTesting
  public void evictCache(List<Long> epochs) {
    EpochEntry<CACHEKEY> currentEntry;
    CACHEKEY cachekey;
    long lastEpoch = epochs.get(epochs.size() - 1);
    for (Iterator<EpochEntry<CACHEKEY>> iterator = epochEntries.iterator();
         iterator.hasNext();) {
      currentEntry = iterator.next();
      cachekey = currentEntry.getCachekey();
      long currentEpoch = currentEntry.getEpoch();

      // If currentEntry epoch is greater than last epoch provided, we have
      // deleted all entries less than specified epoch. So, we can break.
      if (currentEpoch > lastEpoch) {
        break;
      }

      // Acquire lock to avoid race between cleanup and add to cache entry by
      // client requests.
      try {
        lock.writeLock().lock();
        if (epochs.contains(currentEpoch)) {
          // Remove epoch entry, as the entry is there in epoch list.
          iterator.remove();
          // Remove only entries which are marked for delete from the cache.
          cache.computeIfPresent(cachekey, ((k, v) -> {
            if (v.getCacheValue() == null && v.getEpoch() == currentEpoch) {
              LOG.debug("CacheKey {} with epoch {} is removed from cache",
                  k.getCacheKey(), currentEpoch);
              return null;
            }
            return v;
          }));
        }
      } finally {
        lock.writeLock().unlock();
      }

    }
  }

  public CacheResult<CACHEVALUE> lookup(CACHEKEY cachekey) {

    CACHEVALUE cachevalue = cache.get(cachekey);
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
  public Set<EpochEntry<CACHEKEY>> getEpochEntrySet() {
    return epochEntries;
  }

}
