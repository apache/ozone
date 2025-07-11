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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
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
    slidingWindow = new SlidingWindow(3, Duration.ofSeconds(5));
  }

  @Test
  public void testConstructorValidation() {
    // Test invalid window size
    assertThrows(IllegalArgumentException.class, () -> 
        new SlidingWindow(0, Duration.ofMillis(100)));
    assertThrows(IllegalArgumentException.class, () -> 
        new SlidingWindow(-1, Duration.ofMillis(100)));

    // Test invalid expiry duration
    assertThrows(IllegalArgumentException.class, () -> 
        new SlidingWindow(1, Duration.ofMillis(0)));
    assertThrows(IllegalArgumentException.class, () -> 
        new SlidingWindow(1, Duration.ofMillis(-1)));
  }

  @Test
  public void testAdd() {
    for (int i = 0; i < slidingWindow.getWindowSize(); i++) {
      slidingWindow.add();
      assertFalse(slidingWindow.isFull());
    }

    slidingWindow.add();
    assertTrue(slidingWindow.isFull());
  }

  @Test
  public void testEventExpiration() throws InterruptedException {
    slidingWindow = new SlidingWindow(2, Duration.ofMillis(500));

    // Add events to reach threshold
    slidingWindow.add();
    slidingWindow.add();
    slidingWindow.add();
    assertTrue(slidingWindow.isFull());

    // Wait for events to expire
    Thread.sleep(600);

    assertFalse(slidingWindow.isFull());

    // Add one more event - should not be enough to mark as full
    slidingWindow.add();
    assertFalse(slidingWindow.isFull());
  }

  @Test
  public void testPartialExpiration() throws InterruptedException {
    slidingWindow = new SlidingWindow(3, Duration.ofSeconds(1));

    slidingWindow.add();
    slidingWindow.add();
    slidingWindow.add();
    slidingWindow.add();
    assertTrue(slidingWindow.isFull());

    Thread.sleep(600);
    slidingWindow.add(); // this will remove the oldest event as the window is full

    // Wait for the oldest events to expire
    Thread.sleep(500);
    assertFalse(slidingWindow.isFull());
  }

  @Test
  @Timeout(value = 10)
  public void testConcurrentAccess() throws InterruptedException {
    // Create a sliding window with size of 5
    final SlidingWindow concurrentWindow = new SlidingWindow(5, Duration.ofSeconds(5));
    final int threadCount = 10;
    final int operationsPerThread = 100;
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch finishLatch = new CountDownLatch(threadCount);
    final AtomicBoolean hasError = new AtomicBoolean(false);

    // Create and submit tasks
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          startLatch.await(); // Wait for all threads to be ready
          for (int j = 0; j < operationsPerThread; j++) {
            concurrentWindow.add();
            // Check window status occasionally
            if (j % 10 == 0) {
              concurrentWindow.isFull();
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

    // Window should be full after all those operations
    assertTrue(concurrentWindow.isFull());
  }
}
