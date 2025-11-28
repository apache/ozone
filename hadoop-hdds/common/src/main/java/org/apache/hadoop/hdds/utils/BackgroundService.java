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
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

  protected static final Logger LOG = LoggerFactory.getLogger(BackgroundService.class);

  // Executor to launch child tasks
  private volatile ForkJoinPool exec;
  private ThreadGroup threadGroup;
  private final String serviceName;
  private volatile long intervalInMillis;
  private volatile long serviceTimeoutInNanos;
  private volatile int threadPoolSize;
  private final String threadNamePrefix;
  private volatile CompletableFuture<Void> future;
  private volatile AtomicBoolean isShutdown;

  public BackgroundService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout) {
    this(serviceName, interval, unit, threadPoolSize, serviceTimeout, "");
  }

  public BackgroundService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout,
      String threadNamePrefix) {
    setInterval(interval, unit);
    this.serviceName = serviceName;
    this.serviceTimeoutInNanos = TimeDuration.valueOf(serviceTimeout, unit)
            .toLong(TimeUnit.NANOSECONDS);
    this.threadPoolSize = threadPoolSize;
    this.threadNamePrefix = threadNamePrefix;
    initExecutorAndThreadGroup();
    this.future = CompletableFuture.completedFuture(null);
  }

  protected CompletableFuture<Void> getFuture() {
    return future;
  }

  @VisibleForTesting
  public synchronized ForkJoinPool getExecutorService() {
    return this.exec;
  }

  /**
   * Set the pool size for background service. This would require a shutdown and restart of the service for the
   * change to take effect.
   * @param size
   */
  public void setPoolSize(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("Pool size must be positive.");
    }
    this.threadPoolSize = size;
  }

  public void setServiceTimeoutInNanos(long newTimeout) {
    LOG.info("{} timeout is set to {} {}", serviceName, newTimeout, TimeUnit.NANOSECONDS.name().toLowerCase());
    this.serviceTimeoutInNanos = newTimeout;
  }

  @VisibleForTesting
  public int getThreadCount() {
    return threadGroup.activeCount();
  }

  @VisibleForTesting
  public void runPeriodicalTaskNow() throws Exception {
    BackgroundTaskQueue tasks = getTasks(false);
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
    LOG.info("Starting service {} with interval {} ms", serviceName, intervalInMillis);
    exec.execute(new PeriodicalTask(-1));
  }

  protected void setInterval(long newInterval, TimeUnit newUnit) {
    this.intervalInMillis = TimeDuration.valueOf(newInterval, newUnit).toLong(TimeUnit.MILLISECONDS);
  }

  protected long getIntervalMillis() {
    return intervalInMillis;
  }

  public BackgroundTaskQueue getTasks(boolean allowTasksToFork) {
    return getTasks();
  }

  public abstract BackgroundTaskQueue getTasks();

  protected void execTaskCompletion() { }

  /**
   * Run one or more background tasks concurrently.
   * Wait until all tasks to return the result.
   */
  public class PeriodicalTask extends RecursiveAction {
    private int numberOfLoops;
    private final Queue<BackgroundTask> tasksInFlight;
    private final AtomicBoolean isShutdown;

    public PeriodicalTask(int numberOfLoops) {
      this.numberOfLoops = numberOfLoops;
      this.tasksInFlight = new LinkedList<>();
      this.isShutdown = BackgroundService.this.isShutdown;
    }

    private boolean waitForNextInterval() {

      if (numberOfLoops > 0) {
        numberOfLoops--;
        if (numberOfLoops == 0) {
          return false;
        }
      }
      // Check if the executor has been shutdown during task execution.
      if (!isShutdown.get()) {
        synchronized (BackgroundService.this) {
          // Get the shutdown flag again after acquiring the lock.
          if (isShutdown.get()) {
            return false;
          }
          try {
            BackgroundService.this.wait(intervalInMillis);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for next interval.", e);
            return false;
          }
        }
      }
      return !isShutdown.get();
    }

    @Override
    public void compute() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running background service : {}", serviceName);
      }
      boolean runAgain = true;
      do {
        future = new CompletableFuture<>();
        BackgroundTaskQueue tasks = getTasks(true);
        if (tasks.isEmpty()) {
          // No task found, or some problems to init tasks
          // return and retry in next interval.
          continue;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Number of background tasks to execute : {}", tasks.size());
        }

        while (!tasks.isEmpty()) {
          BackgroundTask task = tasks.poll();
          // Fork and submit the task back to executor.
          task.fork();
          tasksInFlight.offer(task);
        }

        while (!tasksInFlight.isEmpty()) {
          BackgroundTask taskInFlight = tasksInFlight.poll();
          // Join the tasks forked before and wait for the result one by one.
          BackgroundTask.BackgroundTaskForkResult result = taskInFlight.join();
          // Check for exception first in the task execution.
          if (result.getThrowable() != null) {
            LOG.error("Background task execution failed", result.getThrowable());
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("task execution result size {}", result.getResult().getSize());
            }
          }
          if (result.getTotalExecutionTime() > serviceTimeoutInNanos) {
            LOG.warn("{} Background task execution took {}ns > {}ns(timeout)",
                serviceName, result.getTotalExecutionTime(), serviceTimeoutInNanos);
          }
        }
        future.complete(null);
        runAgain = waitForNextInterval();
      } while (runAgain);
    }
  }

  // shutdown and make sure all threads are properly released.
  public void shutdown() {
    LOG.info("Shutting down service {}", this.serviceName);
    final ThreadGroup threadGroupToBeClosed;
    final ForkJoinPool execToShutdown;
    // Set the shutdown flag to true to prevent new tasks from being submitted.
    synchronized (this) {
      threadGroupToBeClosed = threadGroup;
      execToShutdown = exec;
      exec = null;
      threadGroup = null;
      if (isShutdown != null) {
        this.isShutdown.set(true);
      }
      isShutdown = null;
      this.notify();
    }
    if (execToShutdown != null) {
      execToShutdown.shutdown();
      try {
        if (!execToShutdown.awaitTermination(60, TimeUnit.SECONDS)) {
          execToShutdown.shutdownNow();
        }
      } catch (InterruptedException e) {
        // Re-interrupt the thread while catching InterruptedException
        Thread.currentThread().interrupt();
        execToShutdown.shutdownNow();
      }
      if (threadGroupToBeClosed != null && !threadGroupToBeClosed.isDestroyed()) {
        threadGroupToBeClosed.destroy();
      }
    }
  }

  private void initExecutorAndThreadGroup() {
    try {
      threadGroup = new ThreadGroup(serviceName);
      Thread initThread = new Thread(threadGroup, () -> {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory =
            pool -> {
              ForkJoinWorkerThread thread = new ForkJoinWorkerThread(pool) {
              };
              thread.setDaemon(true);
              thread.setName(threadNamePrefix + serviceName + thread.getPoolIndex());
              return thread;
            };
        exec = new ForkJoinPool(threadPoolSize, factory, null, false);
        isShutdown = new AtomicBoolean(false);
      });
      initThread.start();
      initThread.join();
    } catch (InterruptedException e) {
      shutdown();
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  protected String getServiceName() {
    return serviceName;
  }
}
