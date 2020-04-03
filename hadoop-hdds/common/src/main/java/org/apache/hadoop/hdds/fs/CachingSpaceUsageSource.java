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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

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
  private final AtomicLong cachedValue = new AtomicLong();
  private final Duration refresh;
  private final SpaceUsageSource source;
  private final SpaceUsagePersistence persistence;
  private boolean running;
  private ScheduledFuture<?> scheduledFuture;

  public CachingSpaceUsageSource(SpaceUsageCheckParams params) {
    this(params, createExecutor(params));
  }

  @VisibleForTesting
  CachingSpaceUsageSource(SpaceUsageCheckParams params,
      ScheduledExecutorService executor) {
    Preconditions.checkArgument(params != null, "params == null");

    refresh = params.getRefresh();
    source = params.getSource();
    persistence = params.getPersistence();
    this.executor = executor;

    Preconditions.checkArgument(refresh.isZero() == (executor == null),
        "executor should be provided if and only if refresh is requested");

    loadInitialValue();
  }

  @Override
  public long getCapacity() {
    return source.getCapacity();
  }

  @Override
  public long getAvailable() {
    return source.getAvailable();
  }

  @Override
  public long getUsedSpace() {
    return cachedValue.get();
  }

  public void start() {
    if (executor != null) {
      long initialDelay = cachedValue.get() > 0 ? refresh.toMillis() : 0;
      if (!running) {
        scheduledFuture = executor.scheduleWithFixedDelay(
            this::refresh, initialDelay, refresh.toMillis(), MILLISECONDS);
        running = true;
      }
    } else {
      refresh();
    }
  }

  public void shutdown() {
    persistence.save(this); // save cached value

    if (executor != null) {
      if (running && scheduledFuture != null) {
        scheduledFuture.cancel(true);
      }
      running = false;

      executor.shutdown();
    }
  }

  private void loadInitialValue() {
    final OptionalLong initialValue = persistence.load();
    initialValue.ifPresent(cachedValue::set);
  }

  private void refresh() {
    try {
      cachedValue.set(source.getUsedSpace());
    } catch (RuntimeException e) {
      LOG.warn("Error refreshing space usage for {}", source, e);
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
