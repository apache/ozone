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

package org.apache.hadoop.ozone.container.replication;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-volume replication handler thread pools for push-based replication.
 */
final class VolumeReplicationThreadPools {

  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeReplicationThreadPools.class);

  private final ConcurrentHashMap<String, ThreadPoolExecutor> pools =
      new ConcurrentHashMap<>();
  private int currentPoolSize;

  void init(Collection<? extends StorageVolume> volumes, int poolSize,
      String threadNamePrefix) {
    currentPoolSize = poolSize;
    List<String> volumeRoots = new ArrayList<>();
    for (StorageVolume volume : volumes) {
      String volumeRoot = volume.getStorageDir().getPath();
      volumeRoots.add(volumeRoot);
      pools.put(volumeRoot, createPool(poolSize, threadNamePrefix, volumeRoot));
    }
    LOG.info("Initialized {} per-volume replication thread pools "
            + "(threads per volume = {}): {}",
        volumeRoots.size(), poolSize, volumeRoots);
  }

  private static ThreadPoolExecutor createPool(int poolSize,
      String threadNamePrefix, String volumeRoot) {
    AtomicInteger threadId = new AtomicInteger();
    ThreadFactory threadFactory = runnable -> {
      Thread thread = new Thread(runnable, threadNamePrefix
          + "ContainerReplicationThread-" + volumeRoot + "-"
          + threadId.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    };
    return new ThreadPoolExecutor(
        poolSize,
        poolSize,
        60, TimeUnit.SECONDS,
        new PriorityBlockingQueue<>(),
        threadFactory);
  }

  ExecutorService getExecutor(String volumeRoot) {
    return pools.get(volumeRoot);
  }

  List<Runnable> shutdownVolume(String volumeRoot) {
    ThreadPoolExecutor pool = pools.remove(volumeRoot);
    if (pool == null) {
      return Collections.emptyList();
    }
    LOG.info("Shutting down per-volume replication thread pool for failed "
        + "volume {}", volumeRoot);
    List<Runnable> drained = Collections.emptyList();
    try {
      drained = pool.shutdownNow();
      if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
        LOG.warn("Per-volume replication thread pool for volume {} did not "
            + "terminate within timeout", volumeRoot);
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while shutting down per-volume replication thread "
          + "pool for volume {}", volumeRoot, e);
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      LOG.warn("Failed to shut down per-volume replication thread pool for "
          + "volume {}: {}", volumeRoot, e.getMessage(), e);
    }
    return drained;
  }

  List<Runnable> shutdownAll() {
    List<Runnable> drained = new ArrayList<>();
    for (String volumeRoot : new ArrayList<>(pools.keySet())) {
      drained.addAll(shutdownVolume(volumeRoot));
    }
    return drained;
  }

  void setPoolSize(int newSize) {
    LOG.info("Resizing per-volume replication thread pools from {} to {}",
        currentPoolSize, newSize);
    int successCount = 0;
    int totalCount = pools.size();
    for (Map.Entry<String, ThreadPoolExecutor> entry : pools.entrySet()) {
      try {
        HddsServerUtil.setPoolSize(entry.getValue(), newSize, LOG);
        successCount++;
      } catch (RuntimeException e) {
        LOG.warn("Failed to resize per-volume replication thread pool for "
            + "volume {}: {}", entry.getKey(), e.getMessage(), e);
      }
    }
    currentPoolSize = newSize;
    if (successCount < totalCount) {
      LOG.warn("Resized {}/{} per-volume replication thread pools to {}",
          successCount, totalCount, newSize);
    } else if (totalCount > 0) {
      LOG.info("Resized all {} per-volume replication thread pools to {}",
          totalCount, newSize);
    }
  }

  int getCurrentPoolSize() {
    return currentPoolSize;
  }

  @VisibleForTesting
  int getPoolSize(String volumeRoot) {
    ThreadPoolExecutor pool = pools.get(volumeRoot);
    return pool == null ? 0 : pool.getMaximumPoolSize();
  }

  @VisibleForTesting
  long getTotalQueueSize() {
    long total = 0;
    for (ThreadPoolExecutor pool : pools.values()) {
      total += pool.getQueue().size();
    }
    return total;
  }

  @VisibleForTesting
  boolean hasPool(String volumeRoot) {
    return pools.containsKey(volumeRoot);
  }
}
