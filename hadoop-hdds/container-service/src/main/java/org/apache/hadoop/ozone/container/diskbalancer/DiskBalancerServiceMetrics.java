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

package org.apache.hadoop.ozone.container.diskbalancer;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Metrics related to DiskBalancer Service running on Datanode.
 */
@Metrics(name = "DiskBalancerService Metrics", about = "Metrics related to "
    + "background disk balancer service on Datanode", context = "dfs")
public final class DiskBalancerServiceMetrics {

  private static DiskBalancerServiceMetrics instance;
  public static final String SOURCE_NAME =
      DiskBalancerServiceMetrics.class.getSimpleName();

  @Metric(about = "The number of successful balance job")
  private MutableCounterLong successCount;

  @Metric(about = "The total bytes for successfully balanced job.")
  private MutableCounterLong successBytes;

  @Metric(about = "The number of failed balance job.")
  private MutableCounterLong failureCount;

  @Metric(about = "The time spent on successful container moves.")
  private MutableRate moveSuccessTime;

  @Metric(about = "The time spent on failed container moves.")
  private MutableRate moveFailureTime;

  @Metric(about = "The number of total running loop")
  private MutableCounterLong runningLoopCount;

  @Metric(about = "The number of idle loop that can not generate volume pair.")
  private MutableCounterLong idleLoopNoAvailableVolumePairCount;

  @Metric(about = "The number of idle loop due to bandwidth limits.")
  private MutableCounterLong idleLoopExceedsBandwidthCount;

  private DiskBalancerServiceMetrics() {
  }

  public static DiskBalancerServiceMetrics create() {
    if (instance == null) {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      instance = ms.register(SOURCE_NAME, "DiskBalancerService",
          new DiskBalancerServiceMetrics());
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

  public void incrFailureCount(long count) {
    this.failureCount.incr(count);
  }

  public void incrRunningLoopCount() {
    this.runningLoopCount.incr();
  }

  public void incrIdleLoopNoAvailableVolumePairCount() {
    this.idleLoopNoAvailableVolumePairCount.incr();
  }

  public void incrIdleLoopExceedsBandwidthCount() {
    this.idleLoopExceedsBandwidthCount.incr();
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

  public long getRunningLoopCount() {
    return runningLoopCount.value();
  }

  public long getIdleLoopNoAvailableVolumePairCount() {
    return idleLoopNoAvailableVolumePairCount.value();
  }

  public long getIdleLoopExceedsBandwidthCount() {
    return idleLoopExceedsBandwidthCount.value();
  }

  public MutableRate getMoveSuccessTime() {
    return moveSuccessTime;
  }

  public MutableRate getMoveFailureTime() {
    return moveFailureTime;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("successCount = ").append(successCount.value()).append('\t')
        .append("successBytes = ").append(successBytes.value()).append('\t')
        .append("failureCount = ").append(failureCount.value()).append('\t')
        .append("moveSuccessTime = ").append(moveSuccessTime.lastStat().mean()).append('\t')
        .append("moveFailureTime = ").append(moveFailureTime.lastStat().mean()).append('\t')
        .append("idleLoopNoAvailableVolumePairCount = ")
        .append(idleLoopNoAvailableVolumePairCount.value()).append('\t')
        .append("idleLoopExceedsBandwidthCount = ").append(idleLoopExceedsBandwidthCount.value());
    return buffer.toString();
  }
}
