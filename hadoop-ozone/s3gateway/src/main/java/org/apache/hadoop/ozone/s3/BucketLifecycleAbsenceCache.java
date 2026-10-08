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

package org.apache.hadoop.ozone.s3;

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_LIFECYCLE_MISSING_CONFIGURATION_CACHE_TTL;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_LIFECYCLE_MISSING_CONFIGURATION_CACHE_TTL_DEFAULT;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Remembers, per bucket, that OM reported no lifecycle configuration.
 * <p>
 * Most buckets carry no lifecycle configuration, and OM signals that by
 * throwing, so a request that reports lifecycle expiration would otherwise pay
 * for a lookup, a bucket read lock and an exception on every single call. Only
 * the absence is cached: the configuration itself is read under a bucket-level
 * ACL check that object requests do not repeat, so serving stored rules from
 * this process would hand them to callers OM never authorized.
 * <p>
 * Entries only expire on their TTL. A configuration added through another
 * gateway cannot be signalled here anyway, and the expiration this feeds is
 * advisory, so a bounded staleness window is the whole contract.
 */
@Singleton
public class BucketLifecycleAbsenceCache {

  /**
   * A key is a bucket name and a value is a marker, so entries are small and
   * the bound only exists to keep a cluster with many buckets from growing the
   * cache without limit. A miss costs one lookup, so it need not be generous.
   */
  private static final long MAX_ENTRIES = 10_000;

  private final Cache<String, Boolean> absent;

  @Inject
  public BucketLifecycleAbsenceCache(OzoneConfiguration conf) {
    long ttlMillis = conf.getTimeDuration(
        OZONE_S3G_LIFECYCLE_MISSING_CONFIGURATION_CACHE_TTL,
        OZONE_S3G_LIFECYCLE_MISSING_CONFIGURATION_CACHE_TTL_DEFAULT,
        TimeUnit.MILLISECONDS);
    absent = ttlMillis <= 0 ? null : CacheBuilder.newBuilder()
        .expireAfterWrite(ttlMillis, TimeUnit.MILLISECONDS)
        .maximumSize(MAX_ENTRIES)
        .build();
  }

  /** @return true if this bucket was recently reported to have no configuration. */
  public boolean isKnownAbsent(String bucketName) {
    return absent != null && bucketName != null
        && absent.getIfPresent(bucketName) != null;
  }

  /** Records that OM reported no lifecycle configuration for this bucket. */
  public void markAbsent(String bucketName) {
    if (absent != null && bucketName != null) {
      absent.put(bucketName, Boolean.TRUE);
    }
  }
}
