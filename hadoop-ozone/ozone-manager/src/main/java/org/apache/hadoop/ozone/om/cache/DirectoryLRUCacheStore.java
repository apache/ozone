/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directory LRUCache: cache directories based on LRU (Least Recently Used)
 * cache eviction strategy, wherein if the cache size has reached the maximum
 * allocated capacity, the least recently used objects in the cache will be
 * evicted.
 * <p>
 * TODO: Add cache metrics - occupancy, hit, miss, evictions etc
 */
public class DirectoryLRUCacheStore implements CacheStore {

  private static final Logger LOG =
          LoggerFactory.getLogger(DirectoryLRUCacheStore.class);

  // Initialises Guava based LRU cache.
  private Cache<OMCacheKey, OMCacheValue> mCache;

  /**
   * @param configuration ozone config
   */
  public DirectoryLRUCacheStore(OzoneConfiguration configuration) {
    LOG.info("Initializing DirectoryLRUCacheStore..");
    // defaulting to 1000,00
    int initSize = configuration.getInt(
            OMConfigKeys.OZONE_OM_CACHE_DIR_INIT_CAPACITY,
            OMConfigKeys.OZONE_OM_CACHE_DIR_INIT_CAPACITY_DEFAULT);
    // defaulting to 5000,000
    long maxSize = configuration.getLong(
            OMConfigKeys.OZONE_OM_CACHE_DIR_MAX_CAPACITY,
            OMConfigKeys.OZONE_OM_CACHE_DIR_MAX_CAPACITY_DEFAULT);
    LOG.info("Configured {} = {} and {} = {}",
            OMConfigKeys.OZONE_OM_CACHE_DIR_INIT_CAPACITY, initSize,
            OMConfigKeys.OZONE_OM_CACHE_DIR_MAX_CAPACITY, maxSize);

    mCache = CacheBuilder.newBuilder()
            .initialCapacity(initSize)
            .maximumSize(maxSize)
            .build();
  }

  @Override
  public void put(OMCacheKey key, OMCacheValue value) {
    if (key != null && value != null) {
      mCache.put(key, value);
    }
  }

  @Override
  public OMCacheValue get(OMCacheKey key) {
    if (key != null) {
      return mCache.getIfPresent(key);
    }
    return null;
  }

  @Override
  public void remove(OMCacheKey key) {
    if (key != null) {
      mCache.invalidate(key);
    }
  }

  @Override
  public long size() {
    return mCache.size();
  }

  @Override
  public CachePolicy getCachePolicy() {
    return CachePolicy.DIR_LRU;
  }
}
