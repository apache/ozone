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

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;

/**
 * Node-level storage totals for a DataNode, aggregated over its HDDS data volumes only
 * ({@code VolumeType.DATA_VOLUME}) via {@link MutableVolumeSet#getStorageReportSnapshot()}.
 * This is the same scope as the {@code storageReport} entries produced by
 * {@code OzoneContainer.getNodeReport()}; meta and DB volumes are excluded.
 * Registered as {@code Hadoop:service=HddsDatanode,name=DatanodeStorageMetrics}.
 * In mini-cluster mode, where many datanodes share one metrics system, the name
 * is suffixed with the datanode UUID ({@code DatanodeStorageMetrics-<uuid>}) so
 * registration and unregistration stay unique per datanode.
 */
@Metrics(about = "Ozone DataNode node-level storage totals",
    context = OzoneConsts.OZONE)
public final class DatanodeStorageMetrics implements MetricsSource {

  public static final String SOURCE_NAME = DatanodeStorageMetrics.class.getSimpleName();

  private static final MetricsInfo CAPACITY = Interns.info("OzoneCapacity",
      "Total Ozone usable capacity across the DataNode's data volumes (bytes,"
          + " post reserved-space adjustment)");
  private static final MetricsInfo USED = Interns.info("OzoneUsed",
      "Total Ozone used space across the DataNode's data volumes (bytes)");
  private static final MetricsInfo USED_PERCENTAGE =
      Interns.info("OzoneUsedPercentage",
          "100 * OzoneUsed / OzoneCapacity across the DataNode's data volumes;"
              + " 0 when OzoneCapacity is 0");

  private final MetricsRegistry registry;
  private final MutableVolumeSet volumeSet;
  private final String sourceName;

  private DatanodeStorageMetrics(MutableVolumeSet volumeSet, String sourceName) {
    this.volumeSet = volumeSet;
    this.sourceName = sourceName;
    this.registry = new MetricsRegistry(sourceName);
  }

  /**
   * Creates a new {@code DatanodeStorageMetrics} instance and registers it
   * with the default Metrics2 system.
   */
  public static DatanodeStorageMetrics create(MutableVolumeSet volumeSet) {
    // In mini-cluster mode many datanodes share one metrics system, so the
    // metrics system uniquifies the constant source name on registration
    // (DatanodeStorageMetrics-1, -2, ...). Unregistering by the constant base
    // name would then leak every source past the first, and each leaked source
    // pins a shut-down datanode's MutableVolumeSet. Make the name unique per
    // datanode up front so register and unregister stay symmetric. In
    // production there is one instance per JVM, so keep the plain name for
    // stable JMX and Prometheus metric names.
    String sourceName = DefaultMetricsSystem.inMiniClusterMode()
        ? SOURCE_NAME + '-' + volumeSet.getDatanodeUuid()
        : SOURCE_NAME;
    DatanodeStorageMetrics datanodeStorageMetrics = new DatanodeStorageMetrics(volumeSet, sourceName);
    DefaultMetricsSystem.instance().register(sourceName,
        "DataNode node-level storage totals", datanodeStorageMetrics);
    return datanodeStorageMetrics;
  }

  /**
   * Unregisters this source from the Metrics2 system.
   */
  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(sourceName);
  }

  /**
   * Metrics are computed on demand from the latest volume reports
   * instead of maintaining cached counters.
   */
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(sourceName);
    registry.snapshot(builder, all);

    long capacity = 0L;
    long used = 0L;
    // getMetrics() runs while the DefaultMetricsSystem monitor is held. Read a
    // lock-free snapshot instead of getStorageReport(), which takes the
    // volume-set lock: a volume-failure handler holds that lock while
    // unregistering volume metrics (which needs the same monitor), so locking
    // here can deadlock the metrics system.
    for (StorageLocationReport report : volumeSet.getStorageReportSnapshot()) {
      capacity = Math.addExact(capacity, report.getCapacity());
      used = Math.addExact(used, report.getScmUsed());
    }
    double usedPercentage = capacity > 0 ? (100.0 * used / capacity) : 0.0;

    builder
        .addGauge(CAPACITY, capacity)
        .addGauge(USED, used)
        .addGauge(USED_PERCENTAGE, usedPercentage);
  }
}
