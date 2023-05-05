/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Add reference counter to an object instance.
 */
public class ReferenceCounted<T> implements AutoCloseable {

  /**
   * Object that is being reference counted. e.g. OmSnapshot
   */
  private final T obj;

  /**
   * A map of thread IDs holding the reference of the object and its count.
   */
  private final ConcurrentHashMap<Long, Long> threadMap;

  /**
   * Sum of reference counts from all threads.
   */
  private final AtomicLong refCount;

  public ReferenceCounted(T obj) {
    this.threadMap = new ConcurrentHashMap<>();
    this.refCount = new AtomicLong(0L);
    this.obj = obj;
  }

  /**
   * @return Object being referenced counted.
   */
  public T get() {
    return obj;
  }

  public long incrementRefCount() { // TODO: [SNAPSHOT] Rename to increment()
    long tid = Thread.currentThread().getId();

    // Put the new mapping if absent, atomically
    threadMap.putIfAbsent(tid, 0L);

    // Update the value and do some checks, atomically
    threadMap.computeIfPresent(tid, (k, v) -> {
      long newVal = v + 1;
      Preconditions.checkState(newVal > 0L, "Thread reference count overflown");

      long newValTotal = refCount.incrementAndGet();
      Preconditions.checkState(newValTotal > 0L,
          "Total reference count overflown");

      return newVal;
    });

    return refCount.get();
  }

  public long decrementRefCount() {
    long tid = Thread.currentThread().getId();

    Preconditions.checkState(threadMap.containsKey(tid),
        "Current thread have not holden reference before");

    Preconditions.checkNotNull(threadMap.get(tid), "This thread " + tid +
        " has not incremented the reference count before.");

    // Atomically update value and purge entry if count reaches zero
    threadMap.computeIfPresent(tid, (k, v) -> {
      long newValue = v - 1L;
      Preconditions.checkState(newValue >= 0L, "Reference count underflow");

      long newValTotal = refCount.decrementAndGet();
      Preconditions.checkState(newValTotal >= 0L,
          "Total reference count underflow");

      // Remove entry by returning null here when thread ref count reaches zero
      return newValue != 0L ? newValue : null;
    });

    return refCount.get();
  }

  /**
   * @return The total number of times the object has been held reference to.
   */
  public long getTotalRefCount() {
    return refCount.get();
  }

  /**
   * @return Number of times current thread has held reference to the object.
   */
  public long getCurrentThreadRefCount() {
    long tid = Thread.currentThread().getId();
    return threadMap.getOrDefault(tid, 0L);
  }

  @Override
  public void close() throws Exception {
    // Decrease ref count by 1 when close() is called on this object
    // so it is eligible to be used with try-with-resources.
    // TODO: Double check.
    decrementRefCount();
  }
}
