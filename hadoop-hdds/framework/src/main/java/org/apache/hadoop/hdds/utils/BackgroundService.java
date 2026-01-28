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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
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
  private UncheckedAutoCloseableSupplier<ScheduledExecutorService> periodicTaskScheduler;
  private volatile ForkJoinPool exec;
  private ThreadGroup threadGroup;
  private final String serviceName;
  private volatile long intervalInMillis;
  private volatile long serviceTimeoutInNanos;
  private volatile int threadPoolSize;
  private final String threadNamePrefix;
  private volatile CompletableFuture<Void> future;
  private volatile AtomicReference<Boolean> isShutdown;

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
    exec.execute(new PeriodicalTask(periodicTaskScheduler.get()));
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
    private final Queue<BackgroundTaskForkJoin> tasksInFlight;
    private final AtomicReference<Boolean> isShutdown;
    private final ScheduledExecutorService scheduledExecuterService;

    public PeriodicalTask(ScheduledExecutorService scheduledExecutorService) {
      this.tasksInFlight = new LinkedList<>();
      this.isShutdown = BackgroundService.this.isShutdown;
      this.scheduledExecuterService = scheduledExecutorService;
    }

    private PeriodicalTask(PeriodicalTask other) {
      this.tasksInFlight = other.tasksInFlight;
      this.isShutdown = other.isShutdown;
      this.scheduledExecuterService = other.scheduledExecuterService;
    }

    private boolean performIfNotShutdown(Runnable runnable) {
      return isShutdown.updateAndGet((shutdown) -> {
        if (!shutdown) {
          runnable.run();
        }
        return shutdown;
      });
    }

    private <T> boolean performIfNotShutdown(Consumer<T> consumer, T t) {
      return isShutdown.updateAndGet((shutdown) -> {
        if (!shutdown) {
          consumer.accept(t);
        }
        return shutdown;
      });
    }

    private boolean runTasks() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running background service : {}", serviceName);
      }
      if (isShutdown.get()) {
        return false;
      }
      if (!tasksInFlight.isEmpty()) {
        LOG.warn("Tasks are still in flight service {}. This should not happen schedule should only begin once all " +
            "tasks from schedules have completed execution.", serviceName);
        tasksInFlight.clear();
      }

      BackgroundTaskQueue tasks = getTasks(true);
      if (tasks.isEmpty()) {
        // No task found, or some problems to init tasks
        // return and retry in next interval.
        return false;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Number of background tasks to execute : {}", tasks.size());
      }
      Consumer<BackgroundTaskForkJoin> taskForkHandler = task -> {
        task.fork();
        tasksInFlight.offer(task);
      };
      while (!tasks.isEmpty()) {
        BackgroundTask task = tasks.poll();
        // Wrap the task in a ForkJoin wrapper and fork it.
        BackgroundTaskForkJoin forkJoinTask = new BackgroundTaskForkJoin(task);
        if (performIfNotShutdown(taskForkHandler, forkJoinTask)) {
          return false;
        }
      }
      Consumer<BackgroundTaskForkJoin> taskCompletionHandler = task -> {
        BackgroundTaskForkJoin.BackgroundTaskForkResult result = task.join();
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
      };
      while (!tasksInFlight.isEmpty()) {
        BackgroundTaskForkJoin taskInFlight = tasksInFlight.poll();
        // Join the tasks forked before and wait for the result one by one.
        if (performIfNotShutdown(taskCompletionHandler, taskInFlight)) {
          return false;
        }
      }
      return true;
    }

    private void scheduleNextTask() {
      performIfNotShutdown(() -> {
        if (scheduledExecuterService != null) {
          scheduledExecuterService.schedule(() -> exec.submit(new PeriodicalTask(this)),
              intervalInMillis, TimeUnit.MILLISECONDS);
        }
      });
    }

    @Override
    public void compute() {
      future = new CompletableFuture<>();
      if (runTasks()) {
        scheduleNextTask();
      } else {
        LOG.debug("Service {} is shutdown. Cancelling all schedules of all tasks.", serviceName);
      }
      future.complete(null);
    }
  }

  // shutdown and make sure all threads are properly released.
  public void shutdown() {
    LOG.info("Shutting down service {}", this.serviceName);
    final ThreadGroup threadGroupToBeClosed;
    final ForkJoinPool execToShutdown;
    final UncheckedAutoCloseableSupplier<ScheduledExecutorService> periodicTaskSchedulerToBeClosed;
    // Set the shutdown flag to true to prevent new tasks from being submitted.
    synchronized (this) {
      periodicTaskSchedulerToBeClosed = periodicTaskScheduler;
      threadGroupToBeClosed = threadGroup;
      execToShutdown = exec;
      exec = null;
      threadGroup = null;
      periodicTaskScheduler = null;
      if (isShutdown != null) {
        this.isShutdown.set(true);
      }
      isShutdown = null;
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
    }
    if (periodicTaskSchedulerToBeClosed != null) {
      periodicTaskSchedulerToBeClosed.close();
    }
    if (threadGroupToBeClosed != null && !threadGroupToBeClosed.isDestroyed()) {
      threadGroupToBeClosed.destroy();
    }
  }

  private synchronized void initExecutorAndThreadGroup() {
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
        isShutdown = new AtomicReference<>(false);
      });
      initThread.start();
      initThread.join();
      periodicTaskScheduler = BackgroundServiceScheduler.get();
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
