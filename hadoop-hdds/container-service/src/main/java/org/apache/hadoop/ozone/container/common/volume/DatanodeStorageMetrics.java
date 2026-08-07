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
 * ({@code VolumeType.DATA_VOLUME}) via {@link MutableVolumeSet#getStorageReport()}.
 * This is the same scope as the {@code storageReport} entries produced by
 * {@code OzoneContainer.getNodeReport()}; meta and DB volumes are excluded.
 * Registered as {@code Hadoop:service=HddsDatanode,name=DatanodeStorageMetrics}.
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

  private DatanodeStorageMetrics(MutableVolumeSet volumeSet) {
    this.volumeSet = volumeSet;
    this.registry = new MetricsRegistry(SOURCE_NAME);
  }

  /**
   * Creates a new {@code DatanodeStorageMetrics} instance and registers it
   * with the default Metrics2 system.
   */
  public static DatanodeStorageMetrics create(MutableVolumeSet volumeSet) {
    DatanodeStorageMetrics datanodeStorageMetrics = new DatanodeStorageMetrics(volumeSet);
    DefaultMetricsSystem.instance().register(
        SOURCE_NAME, "DataNode node-level storage totals", datanodeStorageMetrics);
    return datanodeStorageMetrics;
  }

  /**
   * Unregisters this source from the Metrics2 system.
   */
  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  /**
   * Metrics are computed on demand from the latest volume reports
   * instead of maintaining cached counters.
   */
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);
    registry.snapshot(builder, all);

    long capacity = 0L;
    long used = 0L;
    for (StorageLocationReport report : volumeSet.getStorageReport()) {
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
