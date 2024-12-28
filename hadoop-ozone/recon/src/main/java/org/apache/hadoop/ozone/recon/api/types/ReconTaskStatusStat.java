/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class provides the model for storing the statistics for the
 * various tasks that are run by Recon.
 */
public class ReconTaskStatusStat {
  // Store the initialization time for the task stats for a specific task
  private final AtomicInteger successCount;
  private final AtomicInteger failureCount;
  private long initializationTime;

  public ReconTaskStatusStat() {
    this.initializationTime = System.currentTimeMillis();
    this.successCount = new AtomicInteger(0);
    this.failureCount = new AtomicInteger(0);
  }

  @VisibleForTesting
  public ReconTaskStatusStat(AtomicInteger successCount, AtomicInteger failureCount) {
    this.successCount = successCount;
    this.failureCount = failureCount;
    this.initializationTime = System.currentTimeMillis();
  }

  public void incrementSuccess() {
    successCount.incrementAndGet();
  }

  public void incrementFailure() {
    failureCount.incrementAndGet();
  }

  public long getInitializationTime() {
    return initializationTime;
  }

  public int getSuccessCount() {
    return successCount.get();
  }

  public int getFailureCount() {
    return failureCount.get();
  }

  public void reset() {
    this.successCount.set(0);
    this.failureCount.set(0);
    this.initializationTime = System.currentTimeMillis();
  }
}
