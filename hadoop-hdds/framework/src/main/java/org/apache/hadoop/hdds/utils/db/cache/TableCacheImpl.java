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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache implementation for the table. Depending on the cache clean up policy
 * this cache will be full cache or partial cache.
 *
 * If cache cleanup policy is set as {@link CacheCleanupPolicy#MANUAL},
 * this will be a partial cache.
 *
 * If cache cleanup policy is set as {@link CacheCleanupPolicy#NEVER},
 * this will be a full cache.
 */
@Private
@Evolving
public class TableCacheImpl<CACHEKEY extends CacheKey,
    CACHEVALUE extends CacheValue> implements TableCache<CACHEKEY, CACHEVALUE> {

  public static final Logger LOG =
      LoggerFactory.getLogger(TableCacheImpl.class);

  private final Map<CACHEKEY, CACHEVALUE> cache;
  private final NavigableSet<EpochEntry<CACHEKEY>> epochEntries;
  private ExecutorService executorService;
  private CacheCleanupPolicy cleanupPolicy;



  public TableCacheImpl(CacheCleanupPolicy cleanupPolicy) {

    // As for full table cache only we need elements to be inserted in sorted
    // manner, so that list will be easy. For other we can go with Hash map.
    if (cleanupPolicy == CacheCleanupPolicy.NEVER) {
      cache = new ConcurrentSkipListMap<>();
    } else {
      cache = new ConcurrentHashMap<>();
    }
    epochEntries = new ConcurrentSkipListSet<>();
    // Created a singleThreadExecutor, so one cleanup will be running at a
    // time.
    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("PartialTableCache Cleanup Thread - %d").build();
    executorService = Executors.newSingleThreadExecutor(build);
    this.cleanupPolicy = cleanupPolicy;
  }

  @Override
  public CACHEVALUE get(CACHEKEY cachekey) {
    return cache.get(cachekey);
  }

  @Override
  public void loadInitial(CACHEKEY cacheKey, CACHEVALUE cacheValue) {
    // No need to add entry to epochEntries. Adding to cache is required during
    // normal put operation.
    cache.put(cacheKey, cacheValue);
  }

  @Override
  public void put(CACHEKEY cacheKey, CACHEVALUE value) {
    cache.put(cacheKey, value);
    epochEntries.add(new EpochEntry<>(value.getEpoch(), cacheKey));
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
  protected void evictCache(List<Long> epochs) {
    EpochEntry<CACHEKEY> currentEntry;
    final AtomicBoolean removed = new AtomicBoolean();
    CACHEKEY cachekey;
    long lastEpoch = epochs.get(epochs.size() - 1);
    for (Iterator<EpochEntry<CACHEKEY>> iterator = epochEntries.iterator();
         iterator.hasNext();) {
      currentEntry = iterator.next();
      cachekey = currentEntry.getCachekey();
      long currentEpoch = currentEntry.getEpoch();
      CacheValue cacheValue = cache.computeIfPresent(cachekey, ((k, v) -> {
        if (cleanupPolicy == CacheCleanupPolicy.MANUAL) {
          if (v.getEpoch() == currentEpoch && epochs.contains(v.getEpoch())) {
            LOG.debug("CacheKey {} with epoch {} is removed from cache",
                k.getCacheKey(), currentEpoch);
            iterator.remove();
            removed.set(true);
            return null;
          }
        } else if (cleanupPolicy == CacheCleanupPolicy.NEVER) {
          // Remove only entries which are marked for delete.
          if (v.getEpoch() == currentEpoch && epochs.contains(v.getEpoch())
              && v.getCacheValue() == null) {
            LOG.debug("CacheKey {} with epoch {} is removed from cache",
                k.getCacheKey(), currentEpoch);
            removed.set(true);
            iterator.remove();
            return null;
          }
        }
        return v;
      }));

      // If override entries, then for those epoch entries, there will be no
      // entry in cache. This can occur in the case we have cleaned up the
      // override cache entry, but in epoch entry it is still lying around.
      // This is done to cleanup epoch entries.
      if (!removed.get() && cacheValue == null) {
        LOG.debug("CacheKey {} with epoch {} is removed from epochEntry for " +
                "a key not existing in cache", cachekey.getCacheKey(),
            currentEpoch);
        iterator.remove();
      } else if (currentEpoch >= lastEpoch) {
        // If currentEntry epoch is greater than last epoch provided, we have
        // deleted all entries less than specified epoch. So, we can break.
        break;
      }
      removed.set(false);
    }
  }

  public CacheResult<CACHEVALUE> lookup(CACHEKEY cachekey) {

    CACHEVALUE cachevalue = cache.get(cachekey);
    if (cachevalue == null) {
      if (cleanupPolicy == CacheCleanupPolicy.NEVER) {
        return new CacheResult<>(CacheResult.CacheStatus.NOT_EXIST, null);
      } else {
        return new CacheResult<>(CacheResult.CacheStatus.MAY_EXIST,
            null);
      }
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

  /**
   * Cleanup policies for table cache.
   */
  public enum CacheCleanupPolicy {
    NEVER, // Cache will not be cleaned up. This mean's the table maintains
    // full cache.
    MANUAL // Cache will be cleaned up, once after flushing to DB. It is
    // caller's responsibility to flush to DB, before calling cleanup cache.
  }
}
