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

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.hadoop.util.Time;

/**
 * A time-based sliding window implementation that tracks event timestamps.
 */
public class SlidingWindow {
  private final Object lock = new Object();
  private final int windowSize;
  private final Deque<Long> timestamps;
  private final long expiryDurationMillis;

  /**
   * @param windowSize     the maximum number of events that are tracked
   * @param expiryDuration the duration after which an entry in the window expires
   */
  public SlidingWindow(int windowSize, Duration expiryDuration) {
    if (windowSize <= 0) {
      throw new IllegalArgumentException("Window size must be greater than 0");
    }
    if (expiryDuration.isNegative() || expiryDuration.isZero()) {
      throw new IllegalArgumentException("Expiry duration must be greater than 0");
    }
    this.expiryDurationMillis = expiryDuration.toMillis();
    this.windowSize = windowSize;
    // We limit the initial queue size to 100 to control the memory usage
    this.timestamps = new ArrayDeque<>(Math.min(windowSize + 1, 100));
  }

  public void add() {
    synchronized (lock) {
      removeExpired();

      if (isFull()) {
        timestamps.remove();
      }

      timestamps.add(getCurrentTime());
    }
  }

  /**
   * Checks if the sliding window has exceeded its maximum size.
   * This is useful to track if we have encountered more events than the window's defined limit.
   * @return true if the number of tracked timestamps in the sliding window
   *         exceeds the specified window size, false otherwise.
   */
  public boolean isFull() {
    synchronized (lock) {
      removeExpired();
      return timestamps.size() > windowSize;
    }
  }

  private void removeExpired() {
    synchronized (lock) {
      long currentTime = getCurrentTime();
      long expirationThreshold = currentTime - expiryDurationMillis;

      while (!timestamps.isEmpty() && timestamps.peek() < expirationThreshold) {
        timestamps.remove();
      }
    }
  }

  public int getWindowSize() {
    return windowSize;
  }

  private long getCurrentTime() {
    return Time.monotonicNow();
  }
}
