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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * A time-based sliding window implementation that tracks only failed test results within a specified time duration.
 * It determines failure based on a configured tolerance threshold.
 *
 * The queue saves one failure more than the configured tolerance threshold,
 * so that the window can be considered failed.
 */
public class SlidingWindow {
  private final long windowDuration;
  private final TimeUnit timeUnit;
  private final int failureTolerance;
  private final Deque<Long> failureTimestamps;

  /**
   * @param failureTolerance the number of failures that can be tolerated before the window is considered failed
   * @param windowDuration   the duration of the sliding window
   * @param timeUnit         the time unit of the window duration
   */
  public SlidingWindow(int failureTolerance, long windowDuration, TimeUnit timeUnit) {
    this.windowDuration = windowDuration;
    this.timeUnit = timeUnit;
    this.failureTolerance = failureTolerance;
    // If the failure tolerance is high, we limit the queue size to 100 as we want to control the memory usage
    this.failureTimestamps = new ArrayDeque<>(Math.min(failureTolerance + 1, 100));
  }

  public synchronized void add(boolean result) {
    if (!result) {
      if (failureTolerance > 0 && failureTimestamps.size() > failureTolerance) {
        failureTimestamps.remove();
      }
      long currentTime = System.currentTimeMillis();
      failureTimestamps.addLast(currentTime);
    }

    removeExpiredFailures();
  }

  public synchronized boolean isFailed() {
    removeExpiredFailures();
    return failureTimestamps.size() > failureTolerance;
  }

  private void removeExpiredFailures() {
    long currentTime = System.currentTimeMillis();
    long expirationThreshold = currentTime - timeUnit.toMillis(windowDuration);

    while (!failureTimestamps.isEmpty() && failureTimestamps.peek() < expirationThreshold) {
      failureTimestamps.remove();
    }
  }
}
