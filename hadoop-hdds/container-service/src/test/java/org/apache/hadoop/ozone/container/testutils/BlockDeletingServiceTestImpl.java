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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.testutils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.BlockDeletingService;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test class implementation for {@link BlockDeletingService}.
 */
public class BlockDeletingServiceTestImpl
    extends BlockDeletingService {

  // the service timeout
  private static final int SERVICE_TIMEOUT_IN_MILLISECONDS = 0;

  // tests only
  private CountDownLatch latch;
  private Thread testingThread;
  private AtomicInteger numOfProcessed = new AtomicInteger(0);
  private static final Logger LOG =
          LoggerFactory.getLogger(BlockDeletingServiceTestImpl.class);


  public BlockDeletingServiceTestImpl(OzoneContainer container,
      int serviceInterval, ConfigurationSource conf) {
    super(container, serviceInterval, SERVICE_TIMEOUT_IN_MILLISECONDS,
        TimeUnit.MILLISECONDS, conf);
  }

  @VisibleForTesting
  public void runDeletingTasks() {
    if (latch.getCount() > 0) {
      this.latch.countDown();
    } else {
      throw new IllegalStateException("Count already reaches zero");
    }
  }

  @VisibleForTesting
  public boolean isStarted() {
    return latch != null && testingThread.isAlive();
  }

  public int getTimesOfProcessed() {
    return numOfProcessed.get();
  }

  public class PeriodicalTaskTestImpl extends PeriodicalTask {
    @Override
    public synchronized void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running background service : {}", "BlockDeletingServiceTestImpl");
      }
      long serviceTimeoutInNanos = TimeDuration.valueOf(SERVICE_TIMEOUT_IN_MILLISECONDS, TimeUnit.MILLISECONDS)
              .toLong(TimeUnit.NANOSECONDS);
      BackgroundTaskQueue tasks = getTasks();
      if (tasks.isEmpty()) {
        // No task found, or some problems to init tasks
        // return and retry in next interval.
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Number of background tasks to execute : {}", tasks.size());
      }
      List<CompletableFuture<?> >futureList =
              new ArrayList<>();

      while (tasks.size() > 0) {
        BackgroundTask task = tasks.poll();
        futureList.add(CompletableFuture.runAsync(() -> {
          long startTime = System.nanoTime();
          try {
            BackgroundTaskResult result = task.call();
            if (LOG.isDebugEnabled()) {
              LOG.debug("task execution result size {}", result.getSize());
            }
          } catch (Exception e) {
            LOG.warn("Background task execution failed", e);
          } finally {
            long endTime = System.nanoTime();
            if (endTime - startTime > serviceTimeoutInNanos) {
              LOG.warn("{} Background task execution took {}ns > {}ns(timeout)",
                      "BlockDeletingServiceTestImpl", endTime - startTime, serviceTimeoutInNanos);
            }
          }
        }, getExecutorService()) );
      }
      try {
        CompletableFuture
                .allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
                .get();
      } catch (Exception e) {
        Assert.fail("testAllocateBlockInParallel failed");
      }
    }
  }

  // Override the implementation to start a single on-call control thread.
  @Override
  public void start() {
    PeriodicalTask svc = new PeriodicalTaskTestImpl();
    // In test mode, relies on a latch countdown to runDeletingTasks tasks.
    Runnable r = () -> {
      while (true) {
        latch = new CountDownLatch(1);
        try {
          latch.await();
        } catch (InterruptedException e) {
          break;
        }
        Future<?> future = this.getExecutorService().submit(svc);
        try {
          // for tests, we only wait for 3s for completion
          future.get(3, TimeUnit.SECONDS);
          numOfProcessed.incrementAndGet();
        } catch (Exception e) {
          return;
        }
      }
    };

    testingThread = new ThreadFactoryBuilder()
        .setDaemon(true)
        .build()
        .newThread(r);
    testingThread.start();
  }

  @Override
  public void shutdown() {
    testingThread.interrupt();
    super.shutdown();
  }
}
