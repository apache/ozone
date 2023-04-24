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
package org.apache.hadoop.ozone.om;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;

import java.time.Duration;
import java.time.temporal.TemporalUnit;

/**
 * S3 secret cache implementation based on in-memory cache.
 */
public class S3InMemoryCache implements S3SecretCache {
  private final Cache<String, S3SecretValue> cache;

  public S3InMemoryCache(Duration expireTime, long maxSize) {
    cache = CacheBuilder.newBuilder()
        .expireAfterWrite(expireTime)
        .maximumSize(maxSize)
        .build();
  }

  public static S3InMemoryCacheBuilder builder() {
    return new S3InMemoryCacheBuilder();
  }

  @Override
  public void put(String id, S3SecretValue secretValue) {
    cache.put(id, secretValue);
  }

  @Override
  public void invalidate(String id) {
    cache.invalidate(id);
  }

  @Override
  public S3SecretValue get(String id) {
    return cache.getIfPresent(id);
  }

  /**
   * Builder for {@link S3InMemoryCache}.
   */
  public static class S3InMemoryCacheBuilder {
    private Duration expireTime;
    private long maxSize;

    @SuppressWarnings("checkstyle:HiddenField")
    public S3InMemoryCacheBuilder setExpireTime(long expireTime,
                                                TemporalUnit unit) {
      this.expireTime = Duration.of(expireTime, unit);
      return this;
    }

    public S3InMemoryCacheBuilder setMaxSize(long maxSize) {
      this.maxSize = maxSize;
      return this;
    }

    public S3InMemoryCache build() {
      return new S3InMemoryCache(expireTime, maxSize);
    }
  }
}
