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

package org.apache.hadoop.ozone.om;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    cache.asMap().computeIfPresent(id, (k, secret) -> secret.deleted());
  }

  /**
   * Clears the cache by removing entries that correspond to transactions
   * flushed by the doubleBuffer.
   *
   * @param flushedTransactionIds A list of transaction IDs that have been
   *                              flushed and should be used to identify and
   *                              remove corresponding cache entries.
   */
  @Override
  public void clearCache(List<Long> flushedTransactionIds) {
    // Create a map to store transactionLogIndex-to-cacheKey mappings
    Map<Long, String> transactionIdToCacheKeys = new HashMap<>();

    // Populate the mapping based on transactionLogIndex to kerberosId.
    // So that we do not have to do nested iteration for every transactionId.
    Set<String> cacheKeys = cache.asMap().keySet();
    for (String cacheKey : cacheKeys) {
      S3SecretValue secretValue = cache.getIfPresent(cacheKey);
      if (secretValue != null) {
        transactionIdToCacheKeys.put(secretValue.getTransactionLogIndex(),
            cacheKey);
      }
    }

    // Iterate over the provided transactionIds
    for (Long transactionId : flushedTransactionIds) {
      // Get the cache key associated with this transactionId
      String cacheKey = transactionIdToCacheKeys.get(transactionId);
      if (cacheKey != null) {
        // Remove the cache entry for this cacheKey.
        cache.invalidate(cacheKey);
      }
    }
  }

  @Override
  public S3SecretValue get(String id) {
    return cache.getIfPresent(id);
  }
}
