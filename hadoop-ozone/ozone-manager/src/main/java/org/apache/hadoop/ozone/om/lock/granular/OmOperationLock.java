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
package org.apache.hadoop.ozone.om.lock.granular;

import org.apache.hadoop.util.Time;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.UncheckedAutoCloseable;
import java.util.List;
import java.util.Objects;

/**
 * For operations such as creating a key, reading a key, etc.
 * A {@link OmOperationLock} encapsulates one or more {@link OmComponentLock}s.
 * <p>
 * final OmOperationLock lock = ...;
 * try (UncheckedAutoCloseable ignored = acquire()) {
 *   // execute the operation
 * }
 */
public abstract class OmOperationLock {
  static class TimeInfo {
    private Long acquireTimestamp = null;
    private volatile long waitTimeNanos = 0;
    private volatile long lockTimeNanos = 0;

    private synchronized void acquire(long startTime) {
      Preconditions.assertNull(acquireTimestamp, "acquireTimestamp");
      acquireTimestamp = Time.monotonicNowNanos();
      waitTimeNanos += acquireTimestamp - startTime;
    }

    private synchronized void release() {
      Objects.requireNonNull(acquireTimestamp, "acquireTimestamp == null");
      lockTimeNanos += Time.monotonicNowNanos() - acquireTimestamp;
    }

    public long getLockTimeNanos() {
      return lockTimeNanos;
    }

    public long getWaitTimeNanos() {
      return waitTimeNanos;
    }
  }

  private TimeInfo timeInfo = new TimeInfo();

  TimeInfo getTimeInfo() {
    return timeInfo;
  }

  /**
   * Acquire this lock.
   *
   * @return an object to auto-release this lock when using try-with-resource.
   */
  public synchronized UncheckedAutoCloseable acquire() {
    final long startTime = Time.monotonicNowNanos();
    acquireImpl();
    getTimeInfo().acquire(startTime);
    return this::release;
  }

  abstract void acquireImpl();

  /**
   * Release this lock.
   * Instead of calling this method directly,
   * it is better to call {@link #acquire()} with try-with-resource.
   */
  public synchronized void release() {
    getTimeInfo().release();
    releaseImpl();
  }

  abstract void releaseImpl();

  static OmOperationLock newInstance(OmComponentLock first, OmComponentLock second) {
    return new CompCompLocks(first, second);
  }

  static OmOperationLock newInstance(OmComponentLock first, List<OmComponentLock> second) {
    return new CompListLock(first, second);
  }

  private static class CompCompLocks extends OmOperationLock {
    private final OmComponentLock first;
    private final OmComponentLock second;

    private CompCompLocks(OmComponentLock first, OmComponentLock second) {
      this.first = Objects.requireNonNull(first, "first == null");
      this.second = Objects.requireNonNull(second, "second == null");
      Preconditions.assertTrue(OmComponentLock.getComparator().compare(first, second) < 0,
          () -> "Unexpect order: first = " + first + " >= second = " + second);
    }

    @Override
    void acquireImpl() {
      first.acquire();
      second.acquire();
    }

    @Override
    void releaseImpl() {
      second.release();
      first.release();
    }
  }

  private static class CompListLock extends OmOperationLock {
    private final OmComponentLock first;
    private final List<OmComponentLock> second;

    private CompListLock(OmComponentLock first, List<OmComponentLock> second) {
      this.first = Objects.requireNonNull(first, "first == null");
      this.second = Objects.requireNonNull(second, "second == null");

      // assert lock ordering
      OmComponentLock prev = first;
      for (OmComponentLock current : second) {
        final OmComponentLock previous = prev; // need final for Supplier
        Preconditions.assertTrue(OmComponentLock.getComparator().compare(previous, current) < 0,
            () -> "Unexpect order: previous = " + previous + " >= current = " + current
                + ", first = " + first + ", second = " + second);
        prev = current;
      }
    }

    @Override
    void acquireImpl() {
      first.acquire();
      for (OmComponentLock lock : second) {
        lock.acquire();
      }
    }

    @Override
    void releaseImpl() {
      for (int i = second.size() - 1; i >= 0; i--) {
        second.get(i).release();
      }
      first.release();
    }
  }
}
