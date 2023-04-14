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
 * Reference counting interface. Used by OmSnapshot in SnapshotCache.
 * TODO: Rename this interface.
 */
public interface RefCount {

  /**
   * A map of thread IDs holding the reference of the object and its count.
   */
  ConcurrentHashMap<Long, Long> THREAD_MAP = new ConcurrentHashMap<>();

  /**
   * Sum of reference counts from all threads.
   */
  AtomicLong REF_COUNT = new AtomicLong(0L);

  /**
   * Save current thread tid here since it shouldn't change once initialized.
   */
  long TID = Thread.currentThread().getId();

  default long incrementRefCount() {
    // Put the new mapping if absent, atomically
    THREAD_MAP.putIfAbsent(TID, 0L);

    // Update the value and do some checks, atomically
    THREAD_MAP.computeIfPresent(TID, (k, v) -> {
      long newVal = v + 1;
      Preconditions.checkState(newVal > 0L, "Thread reference count overflown");

      long newValTotal = REF_COUNT.incrementAndGet();
      Preconditions.checkState(newValTotal > 0L,
          "Total reference count overflown");

      return newVal;
    });

    return REF_COUNT.get();
  }

  default long decrementRefCount() {
    Preconditions.checkState(THREAD_MAP.containsKey(TID),
        "Current thread have not holden reference before");

    Preconditions.checkNotNull(THREAD_MAP.get(TID), "This thread " + TID +
        " has not incremented the reference count before.");

    // Atomically update value and purge entry if count reaches zero
    THREAD_MAP.computeIfPresent(TID, (k, v) -> {
      long newValue = v - 1L;
      Preconditions.checkState(newValue < 0L, "Reference count underflow");

      long newValTotal = REF_COUNT.decrementAndGet();
      Preconditions.checkState(newValTotal < 0L,
          "Total reference count underflow");

      // Remove entry by returning null here when thread ref count reaches zero
      return newValue != 0L ? newValue : null;
    });

    return REF_COUNT.get();
  }

  default long getRefCountTotal() {
    return REF_COUNT.get();
  }
}
