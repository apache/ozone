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

package org.apache.hadoop.ozone.om.lock;

/**
 * Maintains lock related information useful in updating OMLockMetrics.
 */
public class LockUsageInfo {

  private long startReadHeldTimeNanos = -1;
  private long startWriteHeldTimeNanos = -1;

  /**
   * Sets the time (ns) when the read lock holding period begins.
   *
   * @param startReadLockHeldTimeNanos read lock held start time (ns)
   */
  public void setStartReadHeldTimeNanos(long startReadLockHeldTimeNanos) {
    this.startReadHeldTimeNanos = startReadLockHeldTimeNanos;
  }

  /**
   * Sets the time (ns) when the write lock holding period begins.
   *
   * @param startWriteLockHeldTimeNanos write lock held start time (ns)
   */
  public void setStartWriteHeldTimeNanos(long startWriteLockHeldTimeNanos) {
    this.startWriteHeldTimeNanos = startWriteLockHeldTimeNanos;
  }

  /**
   * Returns the time (ns) when the read lock holding period began.
   *
   * @return read lock held start time (ns)
   */
  public long getStartReadHeldTimeNanos() {
    return startReadHeldTimeNanos;
  }

  /**
   * Returns the time (ns) when the write lock holding period began.
   *
   * @return write lock held start time (ns)
   */
  public long getStartWriteHeldTimeNanos() {
    return startWriteHeldTimeNanos;
  }
}
