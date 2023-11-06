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

/**
 * S3 secret cache implementation based on in-memory cache.
 */
public class S3InMemoryCache implements S3SecretCache {
  private final Cache<String, S3SecretValue> cache;

  public S3InMemoryCache() {
    cache = CacheBuilder.newBuilder()
        .build();
  }

  @Override
  public void put(String id, S3SecretValue secretValue) {
    cache.put(id, secretValue);
  }

  @Override
  public void invalidate(String id) {
    S3SecretValue secret = cache.getIfPresent(id);
    if (secret == null) {
      return;
    }
    secret.setDeleted(true);
    secret.setAwsSecret(null);
    cache.put(id, secret);
  }

  /**
   * Clears the cache, removing all entries.
   */
  public void clearCache() {
    cache.invalidateAll();
  }

  @Override
  public S3SecretValue get(String id) {
    return cache.getIfPresent(id);
  }
}
