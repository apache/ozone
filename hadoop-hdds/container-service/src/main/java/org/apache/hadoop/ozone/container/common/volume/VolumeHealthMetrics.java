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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class is used to track Volume Health metrics for all volumes on a datanode.
 */
@Metrics(about = "Ozone Volume Health Metrics",
    context = OzoneConsts.OZONE)
public final class VolumeHealthMetrics implements MetricsSource {

  private static final String SOURCE_BASENAME =
      VolumeHealthMetrics.class.getSimpleName();

  private static final MetricsInfo TOTAL_VOLUMES =
      Interns.info("TotalVolumes", "Total number of volumes");
  private static final MetricsInfo HEALTHY_VOLUMES =
      Interns.info("NumHealthyVolumes", "Number of healthy volumes");
  private static final MetricsInfo FAILED_VOLUMES =
      Interns.info("NumFailedVolumes", "Number of failed volumes");

  private final MetricsRegistry registry;
  private final String metricsSourceName;
  private final AtomicInteger healthyVolumes;
  private final AtomicInteger failedVolumes;

  /**
   * Constructor for VolumeHealthMetrics.
   *
   * @param volumeType Type of volumes (DATA_VOLUME, META_VOLUME, DB_VOLUME)
   */
  private VolumeHealthMetrics(StorageVolume.VolumeType volumeType) {
    this.healthyVolumes = new AtomicInteger(0);
    this.failedVolumes = new AtomicInteger(0);
    metricsSourceName = SOURCE_BASENAME + '-' + volumeType.name();
    registry = new MetricsRegistry(metricsSourceName);
  }

  /**
   * Creates and registers a new VolumeHealthMetrics instance.
   *
   * @param volumeType Type of volumes (DATA_VOLUME, META_VOLUME, DB_VOLUME)
   * @return The registered VolumeHealthMetrics instance
   */
  public static VolumeHealthMetrics create(StorageVolume.VolumeType volumeType) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    VolumeHealthMetrics metrics = new VolumeHealthMetrics(volumeType);
    return ms.register(metrics.metricsSourceName, "Volume Health Statistics", metrics);
  }

  public void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(metricsSourceName);
  }

  public void incrementHealthyVolumes() {
    healthyVolumes.incrementAndGet();
  }

  public void incrementFailedVolumes() {
    failedVolumes.incrementAndGet();
  }

  public void decrementHealthyVolumes() {
    healthyVolumes.decrementAndGet();
  }

  public void decrementFailedVolumes() {
    failedVolumes.decrementAndGet();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(metricsSourceName);
    registry.snapshot(builder, all);

    builder
        .addGauge(TOTAL_VOLUMES, healthyVolumes.get() + failedVolumes.get())
        .addGauge(HEALTHY_VOLUMES, healthyVolumes.get())
        .addGauge(FAILED_VOLUMES, failedVolumes.get());
  }
}
