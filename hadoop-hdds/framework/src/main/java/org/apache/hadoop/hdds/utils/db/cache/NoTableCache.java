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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No cache type implementation.
 * @param <KEY>
 * @param <VALUE>
 */
@Private
@Evolving
public class NoTableCache<KEY, VALUE> implements TableCache<KEY, VALUE> {
  public static final Logger LOG = LoggerFactory.getLogger(NoTableCache.class);

  public NoTableCache() {
  }

  @Override
  public CacheValue<VALUE> get(CacheKey<KEY> cachekey) {
    return null;
  }

  @Override
  public void loadInitial(CacheKey<KEY> key, CacheValue<VALUE> value) {
    // Do nothing for partial table cache.
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
    Map<CacheKey<KEY>, CacheValue<VALUE>> objectObjectMap = Collections.emptyMap();
    return objectObjectMap.entrySet().iterator();
  }

  @VisibleForTesting
  @Override
  public void evictCache(List<Long> epochs) {
  }

  @Override
  public CacheResult<VALUE> lookup(CacheKey<KEY> cachekey) {
    return new CacheResult<>(CacheResult.CacheStatus.MAY_EXIST, null);
  }

  @VisibleForTesting
  @Override
  public NavigableMap<Long, Set<CacheKey<KEY>>> getEpochEntries() {
    return new ConcurrentSkipListMap<>();
  }

  @Override
  public CacheStats getStats() {
    return new CacheStats(0, 0, 0);
  }

  @Override
  public CacheType getCacheType() {
    return CacheType.PARTIAL_CACHE;
  }
}
