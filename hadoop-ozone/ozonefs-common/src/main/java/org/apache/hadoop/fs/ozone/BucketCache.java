/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ozone;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * BucketCache uses a Guava Cache to efficiently store and retrieve metadata
 * about buckets, reducing the need for repeated RPC calls to fetch bucket
 * information. The cache is configured with a maximum size and an expiration
 * duration for entries.
 */
public class BucketCache {
  private final Cache<BucketKey, BucketCacheInfo> cache;

  public BucketCache(long maxSize, long expiryDuration, TimeUnit timeUnit) {
    this.cache = CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterAccess(expiryDuration, timeUnit)
        .build();
  }

  public void put(OzoneBucket bucket) {
    BucketCacheInfo bucketCacheInfo = new BucketCacheInfo(
        bucket.getBucketLayout(),
        bucket.getSourceVolume(),
        bucket.getSourceBucket()
    );
    BucketKey key = new BucketKey(bucket.getVolumeName(), bucket.getName());
    cache.put(key, bucketCacheInfo);
  }

  public BucketCacheInfo get(String volumeName, String bucketName) {
    BucketKey key = new BucketKey(volumeName, bucketName);
    return cache.getIfPresent(key);
  }

  public void invalidate(String volumeName, String bucketName) {
    BucketKey key = new BucketKey(volumeName, bucketName);
    cache.invalidate(key);
  }

  public void invalidateAll() {
    cache.invalidateAll();
  }

  /**
   * BucketCacheInfo maintains a cache of bucket metadata to improve performance
   * in scenarios involving frequent access to bucket details such as the bucket's
   * layout information, the bucket's sourceVolume and sourceBucket information
   * in case of linked buckets.
   */
  public static class BucketCacheInfo {
    private final BucketLayout bucketLayout;
    private final String sourceVolume;
    private final String sourceBucket;

    public BucketCacheInfo(BucketLayout bucketLayout, String sourceVolume,
                           String sourceBucket) {
      this.bucketLayout = bucketLayout;
      this.sourceVolume = sourceVolume;
      this.sourceBucket = sourceBucket;
    }

    public BucketLayout getBucketLayout() {
      return bucketLayout;
    }

    public String getSourceVolume() {
      return sourceVolume;
    }

    public String getSourceBucket() {
      return sourceBucket;
    }
  }

  /**
   * BucketKey represents a unique key used to identify a bucket in the cache.
   * It encapsulates the volume name and bucket name, which together form a unique
   * identifier for a bucket within the cache.
   *
   * The key is used to store and retrieve {@link BucketCacheInfo} objects within the
   * {@link BucketCache}.
   */
  public static class BucketKey {
    private final String volumeName;
    private final String bucketName;

    public BucketKey(String volumeName, String bucketName) {
      this.volumeName = volumeName;
      this.bucketName = bucketName;
    }

    public String getVolumeName() {
      return volumeName;
    }

    public String getBucketName() {
      return bucketName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BucketKey bucketKey = (BucketKey) o;
      return Objects.equals(volumeName, bucketKey.volumeName) &&
          Objects.equals(bucketName, bucketKey.bucketName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(volumeName, bucketName);
    }
  }
}
