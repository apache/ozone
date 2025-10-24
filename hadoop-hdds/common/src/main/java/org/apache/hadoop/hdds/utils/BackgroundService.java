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

package org.apache.hadoop.hdds.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class for a background service in ozone.
 * A background service schedules multiple child tasks in parallel
 * in a certain period. In each interval, it waits until all the tasks
 * finish execution and then schedule next interval.
 */
public abstract class BackgroundService {

  protected static final Logger LOG =
      LoggerFactory.getLogger(BackgroundService.class);

  // Executor to launch child tasks
  private ScheduledThreadPoolExecutor exec;
  private ThreadGroup threadGroup;
  private final String serviceName;
  private long interval;
  private volatile long serviceTimeoutInNanos;
  private TimeUnit unit;
  private final int threadPoolSize;
  private final String threadNamePrefix;
  private final PeriodicalTask service;
  private CompletableFuture<Void> future;
  private final SchedulingMode schedulingMode;

  public BackgroundService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout) {
    this(serviceName, interval, unit, threadPoolSize, serviceTimeout, "", SchedulingMode.FIXED_RATE);
  }

  public BackgroundService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout,
      String threadNamePrefix) {
    this(serviceName, interval, unit, threadPoolSize, serviceTimeout, threadNamePrefix, SchedulingMode.FIXED_RATE);
  }

  /**
   * Constructor with scheduling mode option.
   *
   * @param serviceName name of the service
   * @param interval interval between executions
   * @param unit time unit for interval
   * @param threadPoolSize size of thread pool
   * @param serviceTimeout timeout for service execution
   * @param threadNamePrefix prefix for thread names
   * @param schedulingMode the scheduling mode to use (FIXED_RATE or FIXED_DELAY)
   */
  public BackgroundService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout,
      String threadNamePrefix, SchedulingMode schedulingMode) {
    this.interval = interval;
    this.unit = unit;
    this.serviceName = serviceName;
    this.serviceTimeoutInNanos = TimeDuration.valueOf(serviceTimeout, unit)
            .toLong(TimeUnit.NANOSECONDS);
    this.threadPoolSize = threadPoolSize;
    this.threadNamePrefix = threadNamePrefix;
    this.schedulingMode = schedulingMode != null ? schedulingMode : SchedulingMode.FIXED_RATE;
    initExecutorAndThreadGroup();
    service = new PeriodicalTask();
    this.future = CompletableFuture.completedFuture(null);
  }

  protected CompletableFuture<Void> getFuture() {
    return future;
  }

  @VisibleForTesting
  public synchronized ExecutorService getExecutorService() {
    return this.exec;
  }

  public synchronized void setPoolSize(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("Pool size must be positive.");
    }

    // In ScheduledThreadPoolExecutor, maximumPoolSize is Integer.MAX_VALUE
    // the corePoolSize will always less maximumPoolSize.
    // So we can directly set the corePoolSize
    exec.setCorePoolSize(size);
  }

  public synchronized void setServiceTimeoutInNanos(long newTimeout) {
    LOG.info("{} timeout is set to {} {}", serviceName, newTimeout, TimeUnit.NANOSECONDS.name().toLowerCase());
    this.serviceTimeoutInNanos = newTimeout;
  }

  @VisibleForTesting
  public int getThreadCount() {
    return threadGroup.activeCount();
  }

  @VisibleForTesting
  public void runPeriodicalTaskNow() throws Exception {
    BackgroundTaskQueue tasks = getTasks();
    while (!tasks.isEmpty()) {
      tasks.poll().call();
    }
    execTaskCompletion();
  }

  // start service
  public synchronized void start() {
    if (exec == null || exec.isShutdown() || exec.isTerminated()) {
      initExecutorAndThreadGroup();
    }
    
    if (schedulingMode == SchedulingMode.FIXED_DELAY) {
      LOG.info("Starting service {} with fixed delay {} {} after task completion", 
          serviceName, interval, unit.name().toLowerCase());
      // Use minimal delay for scheduleWithFixedDelay, actual interval controlled in run()
      exec.scheduleWithFixedDelay(service, 0, 1, TimeUnit.MILLISECONDS);
    } else if (schedulingMode == SchedulingMode.FIXED_RATE) {
      LOG.info("Starting service {} with fixed rate interval {} {}", serviceName,
          interval, unit.name().toLowerCase());
      exec.scheduleWithFixedDelay(service, 0, interval, unit);
    } else {
      throw new UnsupportedOperationException("SchedulingMode " + schedulingMode +
          " is not supported");
    }
  }

  protected synchronized void setInterval(long newInterval, TimeUnit newUnit) {
    this.interval = newInterval;
    this.unit = newUnit;
  }

  protected synchronized long getIntervalMillis() {
    return this.unit.toMillis(interval);
  }

  public abstract BackgroundTaskQueue getTasks();

  protected void execTaskCompletion() { }

  /**
   * Run one or more background tasks concurrently.
   * Wait until all tasks to return the result.
   */
  public class PeriodicalTask implements Runnable {
    @Override
    public void run() {
      // wait for previous set of tasks to complete
      try {
        future.join();
      } catch (RuntimeException e) {
        LOG.error("Background service execution failed.", e);
      } finally {
        execTaskCompletion();
      }

      if (schedulingMode.shouldSleepAfterCompletion()) {
        try {
          long delayMillis = getIntervalMillis();
          LOG.debug("Waiting {} ms after task completion before next execution", delayMillis);
          Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for fixed interval");
          execTaskCompletion();
          return;
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Running background service : {}", serviceName);
      }
      BackgroundTaskQueue tasks = getTasks();
      if (tasks.isEmpty()) {
        // No task found, or some problems to init tasks
        // return and retry in next interval.
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Number of background tasks to execute : {}", tasks.size());
      }
      synchronized (BackgroundService.this) {
        while (!tasks.isEmpty()) {
          BackgroundTask task = tasks.poll();
          future = future.thenCombine(CompletableFuture.runAsync(() -> {
            long startTime = System.nanoTime();
            try {
              BackgroundTaskResult result = task.call();
              if (LOG.isDebugEnabled()) {
                LOG.debug("task execution result size {}", result.getSize());
              }
            } catch (Throwable e) {
              LOG.error("Background task execution failed", e);
              if (e instanceof Error) {
                throw (Error) e;
              }
            } finally {
              long endTime = System.nanoTime();
              if (endTime - startTime > serviceTimeoutInNanos) {
                LOG.warn("{} Background task execution took {}ns > {}ns(timeout)",
                    serviceName, endTime - startTime, serviceTimeoutInNanos);
              }
            }
          }, exec).exceptionally(e -> null), (Void1, Void) -> null);
        }
      }
    }
  }

  // shutdown and make sure all threads are properly released.
  public synchronized void shutdown() {
    LOG.info("Shutting down service {}", this.serviceName);
    exec.shutdown();
    try {
      if (!exec.awaitTermination(60, TimeUnit.SECONDS)) {
        exec.shutdownNow();
      }
    } catch (InterruptedException e) {
      // Re-interrupt the thread while catching InterruptedException
      Thread.currentThread().interrupt();
      exec.shutdownNow();
    }
    if (threadGroup.activeCount() == 0 && !threadGroup.isDestroyed()) {
      threadGroup.destroy();
    }
  }

  private void initExecutorAndThreadGroup() {
    threadGroup = new ThreadGroup(serviceName);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setThreadFactory(r -> new Thread(threadGroup, r))
        .setDaemon(true)
        .setNameFormat(threadNamePrefix + serviceName + "#%d")
        .build();
    exec = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
  }
}
