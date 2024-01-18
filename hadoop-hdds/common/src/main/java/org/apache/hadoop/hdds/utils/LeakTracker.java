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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils;

import org.apache.ratis.util.UncheckedAutoCloseable;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Set;

/**
 * A token to track resource closure.
 *
 * @see LeakDetector
 */
final class LeakTracker extends WeakReference<Object> implements UncheckedAutoCloseable {
  private final Set<LeakTracker> allLeaks;
  private final Runnable leakReporter;
  LeakTracker(Object referent, ReferenceQueue<Object> referenceQueue,
      Set<LeakTracker> allLeaks, Runnable leakReporter) {
    super(referent, referenceQueue);
    this.allLeaks = allLeaks;
    this.leakReporter = leakReporter;
  }

  /**
   * Called by the tracked resource when closing.
   */
  @Override
  public void close() {
    allLeaks.remove(this);
  }

  void reportLeak() {
    leakReporter.run();
  }
}
