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
package org.apache.hadoop.ozone.om.helpers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quota resource.
 */
public class QuotaResource {
  private final AtomicLong incUsedBytes = new AtomicLong();
  private final AtomicLong incUsedNamespace = new AtomicLong();
  public QuotaResource(long usedBytes, long usedNamespace) {
    incUsedBytes.set(usedBytes);
    incUsedNamespace.set(usedNamespace);
  }

  public long addUsedBytes(long bytes) {
    return incUsedBytes.addAndGet(bytes);
  }

  public long getUsedBytes() {
    return incUsedBytes.get();
  }

  public long addUsedNamespace(long namespace) {
    return incUsedNamespace.addAndGet(namespace);
  }

  public long getUsedNamespace() {
    return incUsedNamespace.get();
  }

  /**
   * factory class.
   */
  public static class Factory {
    private static final Map<Long, QuotaResource> QUOTA_RESOURCE_MAP = new ConcurrentHashMap<>();
    private static final Map<Long, Map<Long, QuotaResource>> REQ_RESERVED = new ConcurrentHashMap<>();

    public static QuotaResource getQuotaResource(long id) {
      return QUOTA_RESOURCE_MAP.get(id);
    }

    public static void registerQuotaResource(long id) {
      QUOTA_RESOURCE_MAP.putIfAbsent(id, new QuotaResource(0, 0));
    }

    public static void removeQuotaResource(long id) {
      QUOTA_RESOURCE_MAP.remove(id);
    }

    public static void addReserveTracker(long trackId, long id, long bytes, long namespace) {
      Map<Long, QuotaResource> quotaIdMap = REQ_RESERVED.computeIfAbsent(trackId, (k) -> new ConcurrentHashMap<>());
      QuotaResource reserveQuota = quotaIdMap.get(id);
      if (reserveQuota == null) {
        quotaIdMap.put(id, new QuotaResource(bytes, namespace));
      } else {
        reserveQuota.addUsedBytes(bytes);
        reserveQuota.addUsedNamespace(namespace);
      }
    }

    public static void resetReservedSpace(long trackId) {
      Map<Long, QuotaResource> remove = REQ_RESERVED.remove(trackId);
      if (remove != null) {
        for (Map.Entry<Long, QuotaResource> entry : remove.entrySet()) {
          QuotaResource bucketQuota = getQuotaResource(entry.getKey());
          if (null != bucketQuota) {
            bucketQuota.addUsedBytes(-entry.getValue().getUsedBytes());
            bucketQuota.addUsedNamespace(-entry.getValue().getUsedNamespace());
          }
        }
      }
    }
  }
}
