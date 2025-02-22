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

import java.util.Objects;

/**
 * CacheResult which is returned as response for Key exist in cache or not.
 * @param <VALUE>
 */
public class CacheResult<VALUE> {

  private final CacheStatus cacheStatus;
  private final CacheValue<VALUE> cacheValue;

  public CacheResult(CacheStatus status, CacheValue<VALUE> value) {
    this.cacheStatus = status;
    this.cacheValue = value;
  }

  public CacheStatus getCacheStatus() {
    return cacheStatus;
  }

  public CacheValue<VALUE> getValue() {
    return cacheValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CacheResult< ? > that = (CacheResult< ? >) o;
    return cacheStatus == that.cacheStatus &&
        Objects.equals(cacheValue, that.cacheValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cacheStatus, cacheValue);
  }

  /**
   * Status which tells whether key exists in cache or not.
   */
  public enum CacheStatus {
    EXISTS, // When key exists in cache.

    NOT_EXIST, // We guarantee that it does not exist. This will be returned
    // when the key does not exist in cache, when cache clean up policy is
    // NEVER.
    MAY_EXIST  // This will be returned when the key does not exist in
    // cache, when cache clean up policy is MANUAL. So caller need to check
    // if it might exist in it's rocksdb table.
  }
}
