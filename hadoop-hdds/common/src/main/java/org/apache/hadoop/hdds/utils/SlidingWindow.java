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
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 *
 * A sliding window implementation that combines time-based expiry with a
 * maximum size constraint. The window tracks event timestamps and maintains two
 * limits:
 * <ul>
 * <li>Time-based: Events older than the specified expiry duration are
 *     automatically removed
 * <li>Size-based: The window maintains at most windowSize latest events, removing
 *     older events when this limit is exceeded
 * </ul>
 *
 * The window is considered full when the number of non-expired events exceeds
 * the specified window size. Events are automatically pruned based on both
 * their age and the maximum size constraint.
 */
public class SlidingWindow {
  private final Object lock = new Object();
  private final int windowSize;
  private final Deque<Long> timestamps;
  private final long expiryDurationMillis;
  private final Clock clock;

  /**
   * Default constructor that uses a monotonic clock.
   *
   * @param windowSize     the maximum number of events that are tracked
   * @param expiryDuration the duration after which an entry in the window expires
   */
  public SlidingWindow(int windowSize, Duration expiryDuration) {
    this(windowSize, expiryDuration, new MonotonicClock());
  }

  /**
   * Constructor with a custom clock for testing.
   *
   * @param windowSize     the maximum number of events that are tracked
   * @param expiryDuration the duration after which an entry in the window expires
   * @param clock          the clock to use for time measurements
   */
  public SlidingWindow(int windowSize, Duration expiryDuration, Clock clock) {
    if (windowSize < 0) {
      throw new IllegalArgumentException("Window size must be greater than 0");
    }
    if (expiryDuration.isNegative() || expiryDuration.isZero()) {
      throw new IllegalArgumentException("Expiry duration must be greater than 0");
    }
    this.windowSize = windowSize;
    this.expiryDurationMillis = expiryDuration.toMillis();
    this.clock = clock;
    // We limit the initial queue size to 100 to control the memory usage
    this.timestamps = new ArrayDeque<>(Math.min(windowSize + 1, 100));
  }

  public void add() {
    synchronized (lock) {
      if (isExceeded()) {
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
  public boolean isExceeded() {
    synchronized (lock) {
      removeExpired();
      return timestamps.size() > windowSize;
    }
  }

  /**
   * Returns the current number of events that are tracked within the sliding window queue.
   * The number of events can exceed the window size.
   * This method ensures that expired events are removed before computing the count.
   *
   * @return the number of valid timestamps currently in the sliding window
   */
  @VisibleForTesting
  public int getNumEvents() {
    synchronized (lock) {
      removeExpired();
      return timestamps.size();
    }
  }

  /**
   * Returns the current number of events that are tracked within the sliding window queue.
   * The number of events cannot exceed the window size.
   * This method ensures that expired events are removed before computing the count.
   *
   * @return the number of valid timestamps currently in the sliding window
   */
  public int getNumEventsInWindow() {
    synchronized (lock) {
      removeExpired();
      return Math.min(timestamps.size(), windowSize);
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
    return clock.millis();
  }

  /**
   * A custom monotonic clock implementation.
   * Implementation of Clock that uses System.nanoTime() for real usage.
   * See {@see org.apache.ozone.test.TestClock}
   */
  private static final class MonotonicClock extends Clock {
    @Override
    public long millis() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    @Override
    public java.time.Instant instant() {
      return java.time.Instant.ofEpochMilli(millis());
    }

    @Override
    public java.time.ZoneId getZone() {
      return java.time.ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      // Ignore zone for monotonic clock
      throw new UnsupportedOperationException("Sliding Window class does not allow changing the timezone");
    }
  }
}
