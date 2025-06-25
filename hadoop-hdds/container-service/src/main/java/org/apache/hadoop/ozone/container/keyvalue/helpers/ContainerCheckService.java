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

package org.apache.hadoop.ozone.container.keyvalue.helpers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for performing asynchronous empty container checks per volume.
 */
public class ContainerCheckService {

  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerCheckService.class);

  private final ConcurrentHashMap<HddsVolume, ExecutorService> volumeExecutors;
  private final ConcurrentHashMap<HddsVolume, VolumeStatistics> volumeStats;
  private final AtomicBoolean isShutdown;
  private final String threadNamePrefix;
  private final long checkInterval;

  /**
   * Statistics for a volume's empty container checks.
   */
  private static class VolumeStatistics {
    private final long startTime;
    private final AtomicLong completedChecks;

    VolumeStatistics() {
      this.startTime = Time.monotonicNow();
      this.completedChecks = new AtomicLong();
    }

    public void incrementCompletedChecks() {
      completedChecks.incrementAndGet();
    }

    public long getCompletedChecks() {
      return completedChecks.get();
    }

    public long getDuration() {
      return Time.monotonicNow() - startTime;
    }
  }

  public ContainerCheckService(String threadNamePrefix, long checkInterval) {
    this.volumeExecutors = new ConcurrentHashMap<>();
    this.volumeStats = new ConcurrentHashMap<>();
    this.isShutdown = new AtomicBoolean(false);
    this.threadNamePrefix = threadNamePrefix;
    this.checkInterval = checkInterval;
  }

  /**
   * Initialize executor for a specific volume.
   */
  public void initializeVolumeWorker(HddsVolume volume) {
    if (isShutdown.get()) {
      throw new IllegalStateException("Service is already shutdown");
    }

    volumeExecutors.computeIfAbsent(volume, v -> {
      ThreadFactory threadFactory = r -> {
        Thread t = new Thread(r);
        t.setName(threadNamePrefix + "EmptyCheckWorker-" + v.getVolumeRootDir());
        t.setDaemon(true);
        return t;
      };
      ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
      volumeStats.putIfAbsent(v, new VolumeStatistics());
      LOG.info("Initialized empty container check worker for volume: {}",
          v.getVolumeRootDir());
      return executor;
    });
  }

  /**
   * Submit an async task to check if a container is empty.
   *
   * @param volume The volume containing the container
   * @param task The task to execute (lambda)
   */
  public void checkAsync(HddsVolume volume, Runnable task) {
    if (isShutdown.get()) {
      LOG.warn("Service is shutdown");
      return;
    }

    ExecutorService executor = volumeExecutors.get(volume);
    VolumeStatistics stats = volumeStats.get(volume);

    if (executor == null) {
      LOG.warn("No executor found for volume: {}, executing task synchronously",
          volume.getVolumeRootDir());
      task.run();
      return;
    }

    executor.submit(() -> {
      try {
        if (isShutdown.get()) {
          return;
        }
        if (checkInterval > 0) {
          Thread.sleep(checkInterval);
        }
        task.run();
        if (stats != null) {
          stats.incrementCompletedChecks();
        }
      } catch (InterruptedException e) {
        LOG.warn("Task execution for {} was interrupted.", volume.getVolumeRootDir());
        Thread.currentThread().interrupt();
      }
    });
  }

  /**
   * Submit a completion marker task for a volume.
   * This should be called when all container readings are done for a volume.
   *
   * @param volume The volume for which to mark completion
   */
  public void markVolumeCompleted(HddsVolume volume) {
    if (isShutdown.get()) {
      return;
    }

    ExecutorService executor = volumeExecutors.get(volume);
    VolumeStatistics stats = volumeStats.get(volume);

    if (executor == null || stats == null) {
      LOG.warn("No executor or stats found for volume: {}",
          volume.getVolumeRootDir());
      return;
    }

    // Submit completion marker task
    executor.submit(() -> {
      LOG.info("Container check completed for volume: {} - " +
              "Checked {} containers in {}ms",
          volume.getVolumeRootDir(),
          stats.getCompletedChecks(),
          stats.getDuration());
    });
  }

  /**
   * Shutdown the service and all volume executors.
   */
  public synchronized void shutdown() {
    if (isShutdown.compareAndSet(false, true)) {
      LOG.info("Shutting down empty container check service");
      for (ConcurrentHashMap.Entry<HddsVolume, ExecutorService> entry : volumeExecutors.entrySet()) {
        shutdownExecutor(entry.getValue(), entry.getKey().getVolumeRootDir());
      }

      volumeExecutors.clear();
      volumeStats.clear();
      LOG.info("Empty container check service shutdown completed");
    }
  }

  private void shutdownExecutor(ExecutorService executor, String volumeId) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(10L, TimeUnit.SECONDS)) {
        LOG.warn("Executor for volume {} did not terminate within {} seconds, forcing shutdown",
            volumeId, 10L);
        executor.shutdownNow();
      } else {
        LOG.info("Empty container check worker stopped for volume: {}", volumeId);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while waiting for executor shutdown for volume: {}", volumeId);
      executor.shutdownNow();
    }
  }
}
