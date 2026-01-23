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
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * This interface is for maintaining DB checkpoint statistics.
 */
@InterfaceAudience.Private
@Metrics(about = "DB checkpoint Metrics", context = "dfs")
public class DBCheckpointMetrics {
  private static final String SOURCE_NAME =
      DBCheckpointMetrics.class.getSimpleName();

  // Metrics to track checkpoint statistics from last run.
  private @Metric MutableGaugeLong lastCheckpointCreationTimeTaken;
  private @Metric MutableGaugeLong lastCheckpointStreamingTimeTaken;
  private @Metric MutableGaugeLong lastCheckpointStreamingNumSSTExcluded;
  // NOTE: numCheckpoints includes numIncrementalCheckpoints
  private @Metric MutableCounterLong numCheckpoints;
  private @Metric MutableCounterLong numCheckpointFails;
  private @Metric MutableCounterLong numIncrementalCheckpoints;

  public DBCheckpointMetrics() {
  }

  public static DBCheckpointMetrics create(String parent) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        parent,
        new DBCheckpointMetrics());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @VisibleForTesting
  public void setLastCheckpointCreationTimeTaken(long val) {
    this.lastCheckpointCreationTimeTaken.set(val);
  }

  @VisibleForTesting
  public void setLastCheckpointStreamingTimeTaken(long val) {
    this.lastCheckpointStreamingTimeTaken.set(val);
  }

  public void setLastCheckpointStreamingNumSSTExcluded(long val) {
    this.lastCheckpointStreamingNumSSTExcluded.set(val);
  }

  @VisibleForTesting
  public void incNumCheckpoints() {
    numCheckpoints.incr();
  }

  @VisibleForTesting
  public void incNumCheckpointFails() {
    numCheckpointFails.incr();
  }

  @VisibleForTesting
  public void incNumIncrementalCheckpoint() {
    numIncrementalCheckpoints.incr();
  }

  @VisibleForTesting
  public long getLastCheckpointCreationTimeTaken() {
    return lastCheckpointCreationTimeTaken.value();
  }

  @VisibleForTesting
  public long getNumCheckpoints() {
    return numCheckpoints.value();
  }

  @VisibleForTesting
  public long getNumIncrementalCheckpoints() {
    return numIncrementalCheckpoints.value();
  }

  @VisibleForTesting
  public long getNumCheckpointFails() {
    return numCheckpointFails.value();
  }

  @VisibleForTesting
  public long getLastCheckpointStreamingTimeTaken() {
    return lastCheckpointStreamingTimeTaken.value();
  }

  public long getLastCheckpointStreamingNumSSTExcluded() {
    return lastCheckpointStreamingNumSSTExcluded.value();
  }
}
