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
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
      TimeUnit unit, int workerThreads, int serviceTimeout) {
    return new BackgroundService(name, interval, unit, workerThreads, serviceTimeout) {
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

  @Test
  public void testBackgroundServiceRunWaitsForTasks() throws InterruptedException, TimeoutException {
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
    // Start the background service.
    backgroundService = createBackgroundService("testWaitForTasks", interval, TimeUnit.MILLISECONDS,
        nTasks, serviceTimeout);
    backgroundService.start();
    // Wait for odd index tasks to complete.
    GenericTestUtils.waitFor(() ->
            IntStream.range(1, nTasks).filter(i -> (i & 1) == 1).allMatch(i -> map.get(i) == 1),
        interval / 5, serviceTimeout);
    Thread.sleep(serviceTimeout);
    // Verify that even index tasks have not run even after serviceTimeout.
    assertEquals(expValuesWithEvenLocks, IntStream.range(0, nTasks).boxed().map(map::get).collect(Collectors.toList()));
    // Background service is still waiting for all tasks to complete.
    assertEquals(-1, runCount.get());
    // Release the locks on even index tasks.
    lockList.forEach(Lock::unlock);
    // Wait for current run of BackgroundService to complete.
    GenericTestUtils.waitFor(() -> runCount.get() >= 0, interval / 5, serviceTimeout);
    // Verify that all tasks have completed.
    assertEquals(expFinalValues, IntStream.range(0, nTasks).boxed().map(map::get).collect(Collectors.toList()));
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testSingleWorkerThread() throws InterruptedException, TimeoutException {
    int nTasks = 5;
    int interval = 100;
    int serviceTimeout = 3000;
    Map<Integer, Integer> map = new HashMap<>();
    Map<Integer, Lock> locks = new HashMap<>();
    List<Integer> expFinalValues = Arrays.asList(1, 1, 1, 1, 1);
    // Add tasks to the queue.
    IntStream.range(0, nTasks).forEach(i -> queue.add(new TestTask(i, map, locks)));
    // Start the background service with a single worker thread.
    backgroundService = createBackgroundService("testSingleWorkerThread", interval, TimeUnit.MILLISECONDS,
        1, serviceTimeout);
    backgroundService.start();
    // Wait till current run of BackgroundService completes.
    GenericTestUtils.waitFor(() -> runCount.get() >= 0, interval / 5, serviceTimeout);
    // Verify that all tasks have completed.
    assertEquals(expFinalValues, IntStream.range(0, nTasks).boxed().map(map::get).collect(Collectors.toList()));
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testRunWaitsForTaskCompletion() throws TimeoutException, InterruptedException {
    int interval = 100;
    int serviceTimeout = 5000;
    // Lock to control when the task can complete.
    Lock taskLock = new ReentrantLock();
    List<Long> runStartTimes = Collections.synchronizedList(new ArrayList<>());
    List<Long> taskEndTimes = Collections.synchronizedList(new ArrayList<>());

    backgroundService = new BackgroundService("testFixedDelay", interval,
        TimeUnit.MILLISECONDS, 1, serviceTimeout) {
      @Override
      public BackgroundTaskQueue getTasks() {
        BackgroundTaskQueue taskQueue = new BackgroundTaskQueue();
        runStartTimes.add(System.currentTimeMillis());
        taskQueue.add(() -> {
          taskLock.lock();
          try {
            taskEndTimes.add(System.currentTimeMillis());
          } finally {
            taskLock.unlock();
          }
          return BackgroundTaskResult.EmptyTaskResult.newResult();
        });
        return taskQueue;
      }

      @Override
      public void execTaskCompletion() {
        runCount.incrementAndGet();
      }
    };

    // Hold the lock so the first task blocks, the task cannot complete before release lock
    taskLock.lock();
    try {
      backgroundService.start();
      // Wait for the first run to start.
      GenericTestUtils.waitFor(() -> !runStartTimes.isEmpty(), 100, serviceTimeout);
      try {
        // Task cannot complete before release lock
        GenericTestUtils.waitFor(() -> runCount.get() >= 0, 100, 2000);
        fail("BackgroundService should not complete task");
      } catch (TimeoutException e) {
      }
    } finally {
      taskLock.unlock();
    }
    // Wait for first run to complete after unlock
    GenericTestUtils.waitFor(() -> runCount.get() >= 0, 10, serviceTimeout);
  }
}
