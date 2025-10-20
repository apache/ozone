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

package org.apache.hadoop.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.SchedulingMode;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test class for validating the functionality of the base class BackgroundService.
 */
@Timeout(10)
public class TestBackgroundService {

  private BackgroundTaskQueue queue;
  private BackgroundService backgroundService;
  private AtomicInteger runCount;

  private static class TestTask implements BackgroundTask {
    private Map<Integer, Integer> map;
    private Map<Integer, Lock> locks;
    private int index;

    TestTask(int index, Map<Integer, Integer> map, Map<Integer, Lock> locks) {
      this.index = index;
      this.map = map;
      this.locks = locks;
      locks.put(index, new ReentrantLock());
      map.put(index, 0);
    }

    @Override
    public BackgroundTaskResult call() {
      locks.get(index).lock();
      try {
        map.merge(index, 1, Integer::sum);
      } finally {
        locks.get(index).unlock();
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

  @BeforeEach
  public void setUp() {
    queue = new BackgroundTaskQueue();
    runCount = new AtomicInteger(-1);
  }

  @AfterEach
  public void shutDown() {
    if (backgroundService != null) {
      backgroundService.shutdown();
    }
  }

  private BackgroundService createBackgroundService(String name, int interval,
      TimeUnit unit, int workerThreads, int serviceTimeout, SchedulingMode mode) {
    return new BackgroundService(name, interval, unit, workerThreads, serviceTimeout, "", mode) {
      @Override
      public BackgroundTaskQueue getTasks() {
        return queue;
      }

      @Override
      public void execTaskCompletion() {
        runCount.incrementAndGet();
      }
    };
  }

  @ParameterizedTest
  @EnumSource(SchedulingMode.class)
  public void testBackgroundServiceRunWaitsForTasks(SchedulingMode mode) throws InterruptedException, TimeoutException {
    int interval = 100;
    int serviceTimeout = 3000;
    int nTasks = 10;
    List<Integer> expValuesWithEvenLocks = Arrays.asList(0, 1, 0, 1, 0, 1, 0, 1, 0, 1);
    List<Integer> expFinalValues = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    Map<Integer, Integer> map = new HashMap<>();
    Map<Integer, Lock> locks = new HashMap<>();
    // Add tasks to the queue.
    IntStream.range(0, nTasks).forEach(i -> queue.add(new TestTask(i, map, locks)));
    // Acquire locks on all even indices.
    List<Lock> lockList = new ArrayList<>();
    IntStream.range(0, nTasks).filter(i -> (i & 1) == 0).forEach(i -> {
      lockList.add(locks.get(i));
      locks.get(i).lock();
    });
    // Start the background service with the specified mode.
    backgroundService = createBackgroundService("testWaitForTasks", interval, TimeUnit.MILLISECONDS,
        nTasks, serviceTimeout, mode);
    backgroundService.start();
    // Wait for odd index tasks to complete.
    GenericTestUtils.waitFor(() ->
            IntStream.range(1, nTasks).filter(i -> (i & 1) == 1).allMatch(i -> map.get(i) == 1),
        interval / 5, serviceTimeout);
    Thread.sleep(serviceTimeout);
    // Verify that even index tasks have not run even after serviceTimeout.
    assertEquals(expValuesWithEvenLocks, IntStream.range(0, nTasks).boxed().map(map::get).collect(Collectors.toList()));
    // Background service is still waiting for all tasks to complete.
    assertEquals(0, runCount.get());
    // Release the locks on even index tasks.
    lockList.forEach(Lock::unlock);
    // Wait for current run of BackgroundService to complete.
    GenericTestUtils.waitFor(() -> runCount.get() == 1, interval / 5, serviceTimeout);
    // Verify that all tasks have completed.
    assertEquals(expFinalValues, IntStream.range(0, nTasks).boxed().map(map::get).collect(Collectors.toList()));
    assertTrue(queue.isEmpty());
  }

  @ParameterizedTest
  @EnumSource(SchedulingMode.class)
  public void testSingleWorkerThread(SchedulingMode mode) throws InterruptedException, TimeoutException {
    int nTasks = 5;
    int interval = 100;
    int serviceTimeout = 3000;
    Map<Integer, Integer> map = new HashMap<>();
    Map<Integer, Lock> locks = new HashMap<>();
    List<Integer> expFinalValues = Arrays.asList(1, 1, 1, 1, 1);
    // Add tasks to the queue.
    IntStream.range(0, nTasks).forEach(i -> queue.add(new TestTask(i, map, locks)));
    // Start the background service with a single worker thread and the specified mode.
    backgroundService = createBackgroundService("testSingleWorkerThread", interval, TimeUnit.MILLISECONDS,
        1, serviceTimeout, mode);
    backgroundService.start();
    // Wait till current run of BackgroundService completes.
    GenericTestUtils.waitFor(() -> runCount.get() == 1, interval / 5, serviceTimeout);
    // Verify that all tasks have completed.
    assertEquals(expFinalValues, IntStream.range(0, nTasks).boxed().map(map::get).collect(Collectors.toList()));
    assertTrue(queue.isEmpty());
  }

  /**
   * A background task that records execution timing.
   */
  private static class TimedTask implements BackgroundTask {
    private final long executionTimeMs;
    private final AtomicLong startTime;
    private final AtomicLong endTime;

    TimedTask(long executionTimeMs, AtomicLong startTime, AtomicLong endTime) {
      this.executionTimeMs = executionTimeMs;
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      startTime.set(System.currentTimeMillis());
      Thread.sleep(executionTimeMs);
      endTime.set(System.currentTimeMillis());
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

  @Test
  public void testFixedDelayTaskInterval() throws InterruptedException, TimeoutException {
    // For FIXED_DELAY, the next task will always wait for the Interval to execute,
    // no matter how long the previous task took to execute.
    int intervalMs = 500;
    int taskExecutionTime = intervalMs + 100;
    int serviceTimeout = 5_000;

    AtomicInteger taskCount = new AtomicInteger(0);
    AtomicLong task1Start = new AtomicLong(0);
    AtomicLong task1End = new AtomicLong(0);
    AtomicLong task2Start = new AtomicLong(0);
    AtomicLong task2End = new AtomicLong(0);

    backgroundService = new BackgroundService("SchedulingModeTest", intervalMs, 
        TimeUnit.MILLISECONDS, 1, serviceTimeout, "", SchedulingMode.FIXED_DELAY) {
      @Override
      public BackgroundTaskQueue getTasks() {
        BackgroundTaskQueue tasks = new BackgroundTaskQueue();
        int current = taskCount.getAndIncrement();

        if (current == 0) {
          tasks.add(new TimedTask(taskExecutionTime, task1Start, task1End));
        } else if (current == 1) {
          tasks.add(new TimedTask(taskExecutionTime, task2Start, task2End));
        }
        return tasks;
      }
    };

    backgroundService.start();
    GenericTestUtils.waitFor(() -> task2End.get() > 0, 50, serviceTimeout);
    backgroundService.shutdown();

    long gapBetweenTasks = task2Start.get() - task1End.get();
    assertTrue(gapBetweenTasks >= intervalMs);

  }
}
