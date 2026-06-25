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

package org.apache.hadoop.ozone.om.ha;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic framework for metrics that need to get updated on a specified interval.
 * A single threaded scheduled thread pool executor is created.
 * The implementing class should only define the logic in updateMetrics()
 */
public abstract class OMPeriodicMetrics {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMPeriodicMetrics.class);
  private final AtomicLong lastUpdateTime = new AtomicLong(0);
  private ScheduledExecutorService updateExecutor;
  private ScheduledFuture<?> updateTask;
  private final String metricsTaskName;
  private final long updateInterval;
  private volatile boolean started = false;

  protected OMPeriodicMetrics(String metricsTaskName, long updateInterval) {
    if (metricsTaskName == null || metricsTaskName.isEmpty()) {
      throw new IllegalArgumentException("metricsTaskName cannot be null or empty");
    }
    if (updateInterval <= 0) {
      throw new IllegalArgumentException("updateInterval must be positive");
    }
    this.metricsTaskName = metricsTaskName;
    this.updateInterval = updateInterval;
  }

  public void start() {
    if (started) {
      LOG.warn("Periodic metrics '{}' already started, ignoring duplicate start()",
          metricsTaskName);
      return;
    }
    updateExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, metricsTaskName);
      t.setDaemon(true);
      return t;
    });
    // Schedule periodic updates
    updateTask = updateExecutor.scheduleWithFixedDelay(() -> {
      try {
        boolean success = updateMetrics();
        if (success) {
          lastUpdateTime.set(System.currentTimeMillis());
        }
      } catch (Exception e) {
        LOG.error("Failed to update metrics for periodic metrics", e);
      }
    }, 0, updateInterval, TimeUnit.MILLISECONDS);
    started = true;
  }

  /**
   * Updates the metrics periodically. This method is called by the framework
   * at the configured interval after {@link #start()} is called.
   * <p>
   * Implementations should perform the actual metrics calculation and update
   * logic here. The method should be thread-safe as it may be called from
   * the scheduled executor thread.
   *
   * @return {@code true} if the metrics update was successful,
   *         {@code false} if the update should be considered unsuccessful
   *         (e.g., due to missing prerequisites or non-fatal errors).
   *         When {@code false} is returned, {@link #getLastUpdateTime()}
   *         will not be updated.
   */
  protected abstract boolean updateMetrics();

  /**
   * Stops the periodic metrics update task.
   */
  public void stop() {
    if (!started) {
      return;  // Already stopped or never started
    }
    if (updateTask != null) {
      updateTask.cancel(false); // Don't interrupt if running
      updateTask = null;
    }

    if (updateExecutor != null) {
      updateExecutor.shutdown();
      try {
        // Wait for any running updateMetrics() to complete (with timeout)
        if (!updateExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          LOG.warn("Metrics update executor did not terminate in time, forcing shutdown");
          updateExecutor.shutdownNow();
          // Wait a bit more for cancellation to take effect
          if (!updateExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            LOG.error("Metrics update executor did not terminate after force shutdown");
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        updateExecutor.shutdownNow();
      }
      updateExecutor = null;
    }
    started = false; // Reset
  }

  public long getLastUpdateTime() {
    return lastUpdateTime.get();
  }
}
