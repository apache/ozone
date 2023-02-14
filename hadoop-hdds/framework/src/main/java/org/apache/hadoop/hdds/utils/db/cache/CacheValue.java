/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db.cache;

import com.google.common.base.Optional;

import java.util.Objects;

/**
 * CacheValue for the RocksDB Table.
 * @param <VALUE>
 */
public class CacheValue<VALUE> {
  /** Create a {@link CacheValue} with a non-null value. */
  public static <V> CacheValue<V> newInstance(V value, long epoch) {
    Objects.requireNonNull(value, "value == null");
    return new CacheValue<>(value, epoch);
  }

  /** Create a {@link CacheValue} with a null value. */
  public static <V> CacheValue<V> newInstance(long epoch) {
    return new CacheValue<>(null, epoch);
  }

  private final VALUE value;
  // This value is used for evict entries from cache.
  // This value is set with ratis transaction context log entry index.
  private final long epoch;

  private CacheValue(VALUE value, long epoch) {
    this.value = value;
    this.epoch = epoch;
  }

  /**
   * @deprecated
   * use {@link #newInstance(Object, long)} or {@link #newInstance(long)}.
   */
  @Deprecated
  public CacheValue(Optional<VALUE> value, long epoch) {
    this.value = value.orNull();
    this.epoch = epoch;
  }

  public VALUE getCacheValue() {
    return value;
  }

  public long getEpoch() {
    return epoch;
  }

}
