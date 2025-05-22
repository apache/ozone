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

package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.base.Preconditions;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Add reference counter to an object instance.
 */
class ReferenceCounted<T> {

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

  /**
   * Object lock to synchronize refCount and threadMap operations.
   */
  private final Object refCountLock = new Object();

  /**
   * Parent instance whose callback will be triggered upon this RC closure.
   */
  private final ReferenceCountedCallback parentWithCallback;

  ReferenceCounted(T obj, boolean disableCounter,
      ReferenceCountedCallback parentWithCallback) {
    // A param to allow disabling ref counting to reduce active DB
    //  access penalties due to AtomicLong operations.
    this.obj = obj;
    if (disableCounter) {
      this.threadMap = null;
      this.refCount = null;
    } else {
      this.threadMap = new ConcurrentHashMap<>();
      this.refCount = new AtomicLong(0L);
    }
    this.parentWithCallback = parentWithCallback;
  }

  /**
   * @return Object being referenced counted.
   */
  public T get() {
    return obj;
  }

  public long incrementRefCount() {
    if (refCount == null || threadMap == null) {
      return -1L;
    }

    long tid = Thread.currentThread().getId();

    threadMap.putIfAbsent(tid, 0L);

    synchronized (refCountLock) {
      threadMap.computeIfPresent(tid, (k, v) -> {
        long newVal = v + 1;
        Preconditions.checkState(newVal > 0L,
            "Thread reference count overflown");
        return newVal;
      });

      long newValTotal = refCount.incrementAndGet();
      Preconditions.checkState(newValTotal > 0L,
          "Total reference count overflown");
    }

    return refCount.get();
  }

  public long decrementRefCount() {
    if (refCount == null || threadMap == null) {
      return -1L;
    }

    long tid = Thread.currentThread().getId();

    Preconditions.checkState(threadMap.containsKey(tid),
        "Current thread have not holden reference before");

    Preconditions.checkState(threadMap.get(tid) > 0L, "This thread " + tid +
        " already have a reference count of zero.");

    synchronized (refCountLock) {
      threadMap.computeIfPresent(tid, (k, v) -> {
        long newValue = v - 1L;
        Preconditions.checkState(newValue >= 0L,
            "Thread reference count underflow");
        // Remove entry by returning null here if thread ref count reaches zero
        return newValue != 0L ? newValue : null;
      });

      long newValTotal = refCount.decrementAndGet();
      Preconditions.checkState(newValTotal >= 0L,
          "Total reference count underflow");
    }
    if (refCount.get() == 0) {
      this.parentWithCallback.callback(this);
    }
    return refCount.get();
  }

  /**
   * @return The total number of times the object has been held reference to.
   */
  public long getTotalRefCount() {
    if (refCount == null) {
      return -1L;
    }

    return refCount.get();
  }

  /**
   * @return Number of times current thread has held reference to the object.
   */
  public long getCurrentThreadRefCount() {
    if (refCount == null || threadMap == null) {
      return -1L;
    }

    long tid = Thread.currentThread().getId();
    return threadMap.getOrDefault(tid, 0L);
  }
}
