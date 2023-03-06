/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.BlockDeletingService;

/**
 * Metrics related to Block Deleting Service running on Datanode.
 */
@Metrics(name = "BlockDeletingService Metrics", about = "Metrics related to "
    + "background block deleting service on Datanode", context = "dfs")
public final class BlockDeletingServiceMetrics {

  private static BlockDeletingServiceMetrics instance;
  public static final String SOURCE_NAME =
      BlockDeletingService.class.getSimpleName();

  @Metric(about = "The number of successful delete blocks")
  private MutableCounterLong successCount;

  @Metric(about = "The total bytes for blocks successfully deleted.")
  private MutableCounterLong successBytes;

  @Metric(about = "The number of failed delete blocks.")
  private MutableCounterLong failureCount;

  private BlockDeletingServiceMetrics() {
  }

  public static BlockDeletingServiceMetrics create() {
    if (instance == null) {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      instance = ms.register(SOURCE_NAME, "BlockDeletingService",
          new BlockDeletingServiceMetrics());
    }

    return instance;
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    instance = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void incrSuccessCount(long count) {
    this.successCount.incr(count);
  }

  public void incrSuccessBytes(long bytes) {
    this.successBytes.incr(bytes);
  }

  public void incrFailureCount() {
    this.failureCount.incr();
  }

  public long getSuccessCount() {
    return successCount.value();
  }

  public long getSuccessBytes() {
    return successBytes.value();
  }

  public long getFailureCount() {
    return failureCount.value();
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("successCount = " + successCount.value()).append("\t")
        .append("successBytes = " + successBytes.value()).append("\t")
        .append("failureCount = " + failureCount.value()).append("\t");
    return buffer.toString();
  }
}
