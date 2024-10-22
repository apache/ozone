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
package org.apache.hadoop.ozone.om.ratis.execution;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * track changes for bucket quota.
 */
public class BucketQuotaResource {
  private static BucketQuotaResource quotaResource = new BucketQuotaResource();
  public static BucketQuotaResource instance() {
    return quotaResource;
  }

  private AtomicBoolean track = new AtomicBoolean();
  private Map<Long, BucketQuota> bucketQuotaMap = new ConcurrentHashMap<>();

  public void enableTrack() {
    track.set(true);
  }
  public boolean isEnabledTrack() {
    return track.get();
  }

  public BucketQuota get(Long bucketId) {
    return bucketQuotaMap.computeIfAbsent(bucketId, k -> new BucketQuota());
  }

  public void remove(Long bucketId) {
    bucketQuotaMap.remove(bucketId);
  }

  public boolean exist(Long bucketId) {
    return bucketQuotaMap.containsKey(bucketId);
  }

  /**
   * record bucket changes.
   */
  public static class BucketQuota {
    private AtomicLong incUsedBytes = new AtomicLong();
    private AtomicLong incUsedNamespace = new AtomicLong();

    public long addUsedBytes(long bytes) {
      return incUsedBytes.addAndGet(bytes);
    }

    public long getUsedBytes() {
      return incUsedBytes.get();
    }

    public long addUsedNamespace(long bytes) {
      return incUsedNamespace.addAndGet(bytes);
    }

    public long getUsedNamespace() {
      return incUsedNamespace.get();
    }
  }
}
