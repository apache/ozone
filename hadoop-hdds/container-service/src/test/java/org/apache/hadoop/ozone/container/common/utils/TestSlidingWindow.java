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

package org.apache.hadoop.ozone.container.common.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for {@link SlidingWindow} class.
 */
public class TestSlidingWindow {

  private SlidingWindow slidingWindow;

  @BeforeEach
  public void setup() {
    slidingWindow = new SlidingWindow(3, 5, TimeUnit.SECONDS);
  }

  @Test
  public void testAddSuccessfulResult() {
    for (int i = 0; i < 10; i++) {
      slidingWindow.add(true);
      assertFalse(slidingWindow.isFailed());
    }
  }

  @Test
  public void testAddFailedResult() {
    for (int i = 0; i < 3; i++) {
      slidingWindow.add(false);
      assertFalse(slidingWindow.isFailed());
    }

    // Adding one more failed result should mark as failed
    slidingWindow.add(false);
    assertTrue(slidingWindow.isFailed());
  }

  @Test
  public void testMixedResults() {
    slidingWindow.add(false);
    slidingWindow.add(false);
    slidingWindow.add(false);
    assertFalse(slidingWindow.isFailed());

    // Add successful result - should not affect failure count
    slidingWindow.add(true);
    assertFalse(slidingWindow.isFailed());

    // Add one more failed result - should mark as failed
    slidingWindow.add(false);
    assertTrue(slidingWindow.isFailed());

    // Add more successful results - should not affect failure status
    slidingWindow.add(true);
    slidingWindow.add(true);
    assertTrue(slidingWindow.isFailed());
  }

  @Test
  public void testFailureExpiration() throws InterruptedException {
    slidingWindow = new SlidingWindow(2, 500, TimeUnit.MILLISECONDS);

    // Add failed results to reach failure threshold
    slidingWindow.add(false);
    slidingWindow.add(false);
    slidingWindow.add(false);
    assertTrue(slidingWindow.isFailed());

    // Wait for failures to expire
    Thread.sleep(600);

    assertFalse(slidingWindow.isFailed());

    // Add one more failure - should not be enough to mark as failed
    slidingWindow.add(false);
    assertFalse(slidingWindow.isFailed());
  }

  @Test
  public void testPartialExpiration() throws InterruptedException {
    slidingWindow = new SlidingWindow(3, 1, TimeUnit.SECONDS);

    slidingWindow.add(false);
    slidingWindow.add(false);
    slidingWindow.add(false);
    slidingWindow.add(false);
    assertTrue(slidingWindow.isFailed());

    Thread.sleep(600);
    slidingWindow.add(false); // this will remove the oldest failure as the window is full

    // Wait for the oldest failures to expire
    Thread.sleep(500);
    assertFalse(slidingWindow.isFailed());
  }

  @Test
  public void testZeroFailureTolerance() {
    // Window with zero failure tolerance
    SlidingWindow zeroToleranceWindow = new SlidingWindow(0, 5, TimeUnit.SECONDS);

    // Any failure should mark as failed
    zeroToleranceWindow.add(false);
    assertTrue(zeroToleranceWindow.isFailed());
  }

  @Test
  public void testHighFailureTolerance() {
    SlidingWindow highToleranceWindow = new SlidingWindow(10, 5, TimeUnit.SECONDS);

    // Add failures less than tolerance
    for (int i = 0; i < 10; i++) {
      highToleranceWindow.add(false);
      assertFalse(highToleranceWindow.isFailed());
    }

    // Add one more to reach tolerance
    highToleranceWindow.add(false);
    assertTrue(highToleranceWindow.isFailed());
  }

  @Test
  public void testFailureQueueManagement() {
    SlidingWindow window = new SlidingWindow(3, 5, TimeUnit.SECONDS);

    // Add more failures than the tolerance
    for (int i = 0; i < 10; i++) {
      window.add(false);
    }

    // Should be failed
    assertTrue(window.isFailed());

    // Add successful results - should not affect failure status
    for (int i = 0; i < 5; i++) {
      window.add(true);
    }

    // Should still be failed
    assertTrue(window.isFailed());
  }

  @Test
  @Timeout(value = 10)
  public void testConcurrentAccess() throws InterruptedException {
    // Create a sliding window with tolerance of 5
    final SlidingWindow concurrentWindow = new SlidingWindow(5, 5, TimeUnit.SECONDS);
    final int threadCount = 10;
    final int operationsPerThread = 100;
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch finishLatch = new CountDownLatch(threadCount);
    final AtomicBoolean hasError = new AtomicBoolean(false);

    // Create and submit tasks
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await(); // Wait for all threads to be ready
          for (int j = 0; j < operationsPerThread; j++) {
            // Alternate between adding success and failure based on thread ID and iteration
            boolean result = (threadId + j) % 2 == 0;
            concurrentWindow.add(result);
            // Check failure status occasionally
            if (j % 10 == 0) {
              concurrentWindow.isFailed();
            }
          }
        } catch (Exception e) {
          hasError.set(true);
          e.printStackTrace();
        } finally {
          finishLatch.countDown();
        }
      });
    }

    // Start all threads
    startLatch.countDown();

    // Wait for all threads to finish
    finishLatch.await();
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);

    // Verify no exceptions occurred
    assertFalse(hasError.get(), "Concurrent operations caused errors");
  }

  @Test
  public void testEdgeCases() {
    // Test with minimum values
    SlidingWindow minWindow = new SlidingWindow(1, 1, TimeUnit.MILLISECONDS);
    minWindow.add(false);
    assertFalse(minWindow.isFailed());
    minWindow.add(false);
    assertTrue(minWindow.isFailed());

    // Test with large values
    SlidingWindow maxWindow = new SlidingWindow(Integer.MAX_VALUE - 1,
        Long.MAX_VALUE / 1000, TimeUnit.SECONDS);
    for (int i = 0; i < 100; i++) {
      maxWindow.add(false);
      assertFalse(maxWindow.isFailed());
    }
  }
}
