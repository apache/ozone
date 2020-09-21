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

/**
 * Cache used for traversing path components from parent node to the leaf node.
 * <p>
 * Basically, its a write-through cache and ensures that no-stale entries in
 * the cache.
 * <p>
 * TODO: can define specific 'CacheLoader' to handle the OM restart and
 *       define cache loading strategies. It can be NullLoader, LazyLoader,
 *       LevelLoader etc.
 *
 * @param <CACHEKEY>
 * @param <CACHEVALUE>
 */
public interface CacheStore<CACHEKEY extends OMCacheKey,
        CACHEVALUE extends OMCacheValue> {

  /**
   * Add an entry to the cache, if the key already exists it overrides.
   *
   * @param key
   * @param value
   */
  void put(CACHEKEY key, CACHEVALUE value);

  /**
   * Returns value for the given key if it exists, otherwise return
   * null.
   *
   * @param key
   * @return CACHEVALUE
   */
  CACHEVALUE get(CACHEKEY key);

  /**
   * Removes a key from the cache.
   *
   * @param key the key to remove
   */
  void remove(CACHEKEY key);

  /**
   * Returns total size of the cache.
   *
   * @return cache size
   */
  long size();

  /**
   * Returns cache policy.
   *
   * @return CachePolicy
   */
  CachePolicy getCachePolicy();
}
