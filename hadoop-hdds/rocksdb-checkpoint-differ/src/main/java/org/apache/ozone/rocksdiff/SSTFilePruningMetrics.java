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

package org.apache.ozone.rocksdiff;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class contains metrics for monitoring SST file pruning operations in RocksDBCheckpointDiffer.
 */
@Metrics(about = "SST File Pruning Metrics", context = OzoneConsts.OZONE)
public final class SSTFilePruningMetrics implements MetricsSource {

  private static final String METRICS_SOURCE_NAME_PREFIX = SSTFilePruningMetrics.class.getSimpleName();
  private final String metricSourceName;
  private final MetricsRegistry registry;

  /*
   * Pruning Throughput Metrics.
   */
  @Metric("Total no. of SST files pruned")
  private MutableCounterLong filesPrunedTotal;
  @Metric("No. of SST files pruned in the last batch")
  private MutableGaugeLong filesPrunedLast;
  @Metric("Total no. of SST files removed")
  private MutableCounterLong filesSkippedTotal;
  @Metric("Total no. of compactions processed")
  private MutableCounterLong compactionsProcessed;
  @Metric("No. of pending pruning jobs in queue")
  private MutableGaugeLong pruneQueueSize;

  /*
   * Pruning failure Metrics.
   */
  @Metric("No. of pruning job failures")
  private MutableCounterLong pruningFailures;

  private SSTFilePruningMetrics(String sourceName) {
    this.metricSourceName = sourceName;
    this.registry = new MetricsRegistry(metricSourceName);
  }

  /**
   * Creates and returns SSTFilePruningMetrics instance.
   *
   * @return SSTFilePruningMetrics
   */
  public static SSTFilePruningMetrics create(String dbLocation) {
    String sourceName = METRICS_SOURCE_NAME_PREFIX +
        (dbLocation == null || dbLocation.isEmpty() ? "" : "-" + dbLocation.replaceAll("[/\\:\\s]", "_"));
    return DefaultMetricsSystem.instance().register(sourceName, "SST File Pruning Metrics",
        new SSTFilePruningMetrics(sourceName));
  }

  /**
   * Unregister the metrics instance.
   */
  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(metricSourceName);
  }

  public void updateQueueSize(long queueSize) {
    pruneQueueSize.set(queueSize);
  }

  public void updateBatchLevelMetrics(long filesPruned, long filesSkipped, int compactions, long queueSize) {
    filesPrunedTotal.incr(filesPruned);
    filesPrunedLast.set(filesPruned);
    filesSkippedTotal.incr(filesSkipped);
    compactionsProcessed.incr(compactions);
    updateQueueSize(queueSize);
  }

  public void incrPruningFailures() {
    pruningFailures.incr();
  }

  public long getFilesPrunedTotal() {
    return filesPrunedTotal.value();
  }

  public long getFilesPrunedLast() {
    return filesPrunedLast.value();
  }

  public long getFilesRemovedTotal() {
    return filesSkippedTotal.value();
  }

  public long getCompactionsProcessed() {
    return compactionsProcessed.value();
  }

  public long getPruneQueueSize() {
    return pruneQueueSize.value();
  }

  public long getPruningFailures() {
    return pruningFailures.value();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(metricSourceName);
    filesPrunedTotal.snapshot(recordBuilder, all);
    filesPrunedLast.snapshot(recordBuilder, all);
    filesSkippedTotal.snapshot(recordBuilder, all);
    compactionsProcessed.snapshot(recordBuilder, all);
    pruneQueueSize.snapshot(recordBuilder, all);
    pruningFailures.snapshot(recordBuilder, all);
  }
}
