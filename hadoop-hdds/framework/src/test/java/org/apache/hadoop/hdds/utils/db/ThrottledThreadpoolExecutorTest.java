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

package org.apache.hadoop.hdds.utils.db;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for ThrottledThreadpoolExecutor.
 */
public class ThrottledThreadpoolExecutorTest {

  private ThrottledThreadpoolExecutor executor;

  @BeforeEach
  public void setup() {
    // coreThreads = 2, maxThreads = 4 => maxTasks = 8
    executor = new ThrottledThreadpoolExecutor(4);
  }

  @AfterEach
  public void tearDown() throws Exception {
    executor.close();
  }

  @Test
  public void testThrottlingDoesNotExceedMaxTasks() throws Exception {
    int maxThreads = 4;
    int maxTasks = 2 * maxThreads; // 8
    AtomicInteger concurrentTasks = new AtomicInteger(0);
    AtomicInteger maxObservedConcurrentTasks = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(maxThreads);
    CountDownLatch releaseLatch = new CountDownLatch(1); // To block all tasks

    for (int i = 0; i < maxTasks; i++) {
      executor.submit(() -> {
        int current = concurrentTasks.incrementAndGet();
        System.out.println(current);
        maxObservedConcurrentTasks.updateAndGet(prev -> Math.max(prev, current));
        latch.countDown();
        releaseLatch.await(); // block the task
        concurrentTasks.decrementAndGet();
      });
    }

    // Wait for all tasks to start (meaning all 8 task slots used)
    assertTrue(latch.await(10, TimeUnit.SECONDS), "Tasks did not start in time");
    assertEquals(maxTasks, executor.getTaskCount());

    // Now try to submit one more task — it should block until a slot frees up
    CompletableFuture<Void> extraTask = CompletableFuture.runAsync(() -> {
      try {
        executor.submit(() -> {
        });
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    // Wait for 1 second and ensure extraTask hasn't completed yet
    Thread.sleep(1000);
    assertFalse(extraTask.isDone(), "Extra task should be throttled and blocked");
    assertEquals(maxTasks, executor.getTaskCount());

    // Finish all initially submitted threads.
    releaseLatch.countDown();

    // Now the extra task should complete within a few seconds
    assertDoesNotThrow(() -> extraTask.get(5, TimeUnit.SECONDS));

    // Make sure we never had more than maxTasks in flight
    assertEquals(4, maxObservedConcurrentTasks.get(), "Throttling failed");
  }
}
