/*
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
package org.apache.hadoop.hdds.fs;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.AutoCloseableReadWriteLock;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Stores space usage and refreshes it periodically.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CachingSpaceUsageSource implements SpaceUsageSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(CachingSpaceUsageSource.class);

  private final ScheduledExecutorService executor;
  private final AutoCloseableReadWriteLock lock;
  private long cachedUsedSpace;
  private long cachedAvailable;
  private long cachedCapacity;
  private final Duration refresh;
  private final SpaceUsageSource source;
  private final SpaceUsagePersistence persistence;
  private boolean running;
  private ScheduledFuture<?> updateUsedSpaceFuture;
  private ScheduledFuture<?> updateAvailableFuture;
  private final AtomicBoolean isRefreshRunning;

  public CachingSpaceUsageSource(SpaceUsageCheckParams params) {
    this(params, createExecutor(params));
  }

  CachingSpaceUsageSource(SpaceUsageCheckParams params,
      ScheduledExecutorService executor) {
    Preconditions.assertNotNull(params, "params == null");

    refresh = params.getRefresh();
    source = params.getSource();
    lock = new AutoCloseableReadWriteLock(source.toString());
    persistence = params.getPersistence();
    this.executor = executor;
    isRefreshRunning = new AtomicBoolean();

    Preconditions.assertTrue(refresh.isZero() == (executor == null),
        "executor should be provided if and only if refresh is requested");

    loadInitialValue();
  }

  @Override
  public long getCapacity() {
    try (AutoCloseableLock ignored = lock.readLock(null, null)) {
      return cachedCapacity;
    }
  }

  @Override
  public long getAvailable() {
    try (AutoCloseableLock ignored = lock.readLock(null, null)) {
      return cachedAvailable;
    }
  }

  @Override
  public long getUsedSpace() {
    try (AutoCloseableLock ignored = lock.readLock(null, null)) {
      return cachedUsedSpace;
    }
  }

  @Override
  public SpaceUsageSource snapshot() {
    try (AutoCloseableLock ignored = lock.readLock(null, null)) {
      return new Fixed(cachedCapacity, cachedAvailable, cachedUsedSpace);
    }
  }

  public void incrementUsedSpace(long usedSpace) {
    if (usedSpace == 0) {
      return;
    }
    Preconditions.assertTrue(usedSpace > 0, () -> usedSpace + " < 0");
    final long current, change;
    try (AutoCloseableLock ignored = lock.writeLock(null, null)) {
      current = cachedAvailable;
      change = Math.min(current, usedSpace);
      cachedAvailable -= change;
      cachedUsedSpace += change;
    }

    if (change != usedSpace) {
      LOG.warn("Attempted to decrement available space to a negative value. Current: {}, Decrement: {}, Source: {}",
          current, usedSpace, source);
    }
  }

  public void decrementUsedSpace(long reclaimedSpace) {
    if (reclaimedSpace == 0) {
      return;
    }
    Preconditions.assertTrue(reclaimedSpace > 0, () -> reclaimedSpace + " < 0");
    final long current, change;
    try (AutoCloseableLock ignored = lock.writeLock(null, null)) {
      current = cachedUsedSpace;
      change = Math.min(current, reclaimedSpace);
      cachedUsedSpace -= change;
      cachedAvailable += change;
    }

    if (change != reclaimedSpace) {
      LOG.warn("Attempted to decrement used space to a negative value. Current: {}, Decrement: {}, Source: {}",
          current, reclaimedSpace, source);
    }
  }

  public void start() {
    if (executor != null) {
      long initialDelay = getUsedSpace() > 0 ? refresh.toMillis() : 0;
      if (!running) {
        updateUsedSpaceFuture = executor.scheduleWithFixedDelay(
            this::refresh, initialDelay, refresh.toMillis(), MILLISECONDS);

        long availableUpdateDelay = Math.min(refresh.toMillis(), Duration.ofMinutes(1).toMillis());
        updateAvailableFuture = executor.scheduleWithFixedDelay(
            this::updateAvailable, availableUpdateDelay, availableUpdateDelay, MILLISECONDS);

        running = true;
      }
    } else {
      refresh();
    }
  }

  public void shutdown() {
    persistence.save(this); // save cached value

    if (executor != null) {
      if (running) {
        if (updateUsedSpaceFuture != null) {
          updateUsedSpaceFuture.cancel(true);
        }
        if (updateAvailableFuture != null) {
          updateAvailableFuture.cancel(true);
        }
      }
      running = false;

      executor.shutdown();
    }
  }

  /** Schedule immediate refresh. */
  public void refreshNow() {
    executor.schedule(this::refresh, 0, MILLISECONDS);
  }

  /** Loads {@code usedSpace} value from persistent source, if present.
   * Also updates {@code available} and {@code capacity} from the {@code source}. */
  private void loadInitialValue() {
    final OptionalLong initialValue = persistence.load();
    updateCachedValues(initialValue.orElse(0));
  }

  /** Updates {@code available} and {@code capacity} from the {@code source}. */
  private void updateAvailable() {
    final long capacity = source.getCapacity();
    final long available = source.getAvailable();

    try (AutoCloseableLock ignored = lock.writeLock(null, null)) {
      cachedAvailable = available;
      cachedCapacity = capacity;
    }
  }

  /** Updates {@code available} and {@code capacity} from the {@code source},
   * sets {@code usedSpace} to the specified {@code used} value. */
  private void updateCachedValues(long used) {
    final long capacity = source.getCapacity();
    final long available = source.getAvailable();

    try (AutoCloseableLock ignored = lock.writeLock(null, null)) {
      cachedAvailable = available;
      cachedCapacity = capacity;
      cachedUsedSpace = used;
    }
  }

  /** Refreshes all 3 values. */
  private void refresh() {
    //only one `refresh` can be running at a certain moment
    if (isRefreshRunning.compareAndSet(false, true)) {
      try {
        updateCachedValues(source.getUsedSpace());
      } catch (RuntimeException e) {
        LOG.warn("Error refreshing space usage for {}", source, e);
      } finally {
        isRefreshRunning.set(false);
      }
    }
  }

  private static @Nullable ScheduledExecutorService createExecutor(
      SpaceUsageCheckParams params) {

    if (params.getRefresh().isZero()) {
      return null;
    }

    return Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("DiskUsage-" + params.getPath() + "-%n")
            .build());
  }
}
