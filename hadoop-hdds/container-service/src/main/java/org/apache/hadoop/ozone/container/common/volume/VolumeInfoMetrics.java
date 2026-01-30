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

import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class is used to track Volume Info stats for each HDDS Volume.
 */
@Metrics(about = "Ozone Volume Information Metrics",
    context = OzoneConsts.OZONE)
public class VolumeInfoMetrics implements MetricsSource {

  private static final String SOURCE_BASENAME =
      VolumeInfoMetrics.class.getSimpleName();

  private static final MetricsInfo CAPACITY =
      Interns.info("OzoneCapacity", "Ozone usable capacity (after reserved space adjustment)");
  private static final MetricsInfo AVAILABLE =
      Interns.info("OzoneAvailable", "Ozone available space (after reserved space adjustment)");
  private static final MetricsInfo USED =
      Interns.info("OzoneUsed", "Ozone used space");
  private static final MetricsInfo RESERVED =
      Interns.info("Reserved", "Reserved Space");
  private static final MetricsInfo TOTAL_CAPACITY =
      Interns.info("TotalCapacity", "Ozone capacity + reserved space");
  private static final MetricsInfo FS_CAPACITY =
      Interns.info("FilesystemCapacity", "Filesystem capacity as reported by the local filesystem");
  private static final MetricsInfo FS_AVAILABLE =
      Interns.info("FilesystemAvailable", "Filesystem available space as reported by the local filesystem");
  private static final MetricsInfo FS_USED =
      Interns.info("FilesystemUsed", "Filesystem used space (FilesystemCapacity - FilesystemAvailable)");

  private final MetricsRegistry registry;
  private final String metricsSourceName;
  private final HddsVolume volume;
  @Metric("Returns the RocksDB compact times of the Volume")
  private MutableRate dbCompactLatency;
  @Metric("Volume reserved space crosses reserved usages limit")
  private MutableGaugeInt reservedCrossesLimit;
  @Metric("Volume available space is insufficient")
  private MutableGaugeInt availableSpaceInsufficient;

  @Metric("Number of times the volume is scanned")
  private MutableCounterLong numScans;

  @Metric("Number of scans skipped for the volume")
  private MutableCounterLong numScansSkipped;

  /**
   * @param identifier Typically, path to volume root. E.g. /data/hdds
   */
  public VolumeInfoMetrics(String identifier, HddsVolume volume) {
    this.volume = volume;

    metricsSourceName = SOURCE_BASENAME + '-' + identifier;
    registry = new MetricsRegistry(metricsSourceName);

    init();
  }

  public void init() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register(metricsSourceName, "Volume Info Statistics", this);
  }

  public void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(metricsSourceName);
  }

  @Metric("Metric to return the Storage Type")
  public String getStorageType() {
    return volume.getStorageType().toString();
  }

  @Metric("Returns the Directory name for the volume")
  public String getStorageDirectory() {
    return volume.getStorageDir().toString();
  }

  @Metric("Return the DataNode UID for the respective volume")
  public String getDatanodeUuid() {
    return volume.getDatanodeUuid();
  }

  @Metric("Return the Layout Version for the volume")
  public int getLayoutVersion() {
    return volume.getLayoutVersion();
  }

  @Metric("Returns the Volume State")
  public String getVolumeState() {
    return volume.getStorageState().name();
  }

  @Metric("Returns the Volume Type")
  public String getVolumeType() {
    return volume.getType().name();
  }

  @Metric("Returns the Committed bytes of the Volume")
  public long getCommitted() {
    return volume.getCommittedBytes();
  }

  public void dbCompactTimesNanoSecondsIncr(long time) {
    dbCompactLatency.add(time);
  }

  public int getAvailableSpaceInsufficient() {
    return availableSpaceInsufficient.value();
  }

  public void setAvailableSpaceInsufficient(boolean isInSufficient) {
    if (isInSufficient) {
      availableSpaceInsufficient.set(1);
    } else {
      availableSpaceInsufficient.set(0);
    }
  }

  public int getReservedCrossesLimit() {
    return reservedCrossesLimit.value();
  }

  public void setReservedCrossesLimit(boolean isLimitCrossed) {
    if (isLimitCrossed) {
      reservedCrossesLimit.set(1);
    } else {
      reservedCrossesLimit.set(0);
    }
  }

  /**
   * Return the Container Count of the Volume.
   */
  @Metric("Returns the Container Count of the Volume")
  public long getContainers() {
    return volume.getContainers();
  }

  public long getNumScans() {
    return numScans.value();
  }

  public void incNumScans() {
    numScans.incr();
  }

  public long getNumScansSkipped() {
    return numScansSkipped.value();
  }

  public void incNumScansSkipped() {
    numScansSkipped.incr();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(metricsSourceName);
    registry.snapshot(builder, all);
    VolumeUsage volumeUsage = volume.getVolumeUsage();
    if (volumeUsage != null) {
      SpaceUsageSource.Fixed fsUsage = volumeUsage.realUsage();
      SpaceUsageSource usage = volumeUsage.getCurrentUsage(fsUsage);
      long reserved = volumeUsage.getReservedInBytes();
      builder
          .addGauge(CAPACITY, usage.getCapacity())
          .addGauge(AVAILABLE, usage.getAvailable())
          .addGauge(USED, usage.getUsedSpace())
          .addGauge(RESERVED, reserved)
          .addGauge(TOTAL_CAPACITY, usage.getCapacity() + reserved)
          .addGauge(FS_CAPACITY, fsUsage.getCapacity())
          .addGauge(FS_AVAILABLE, fsUsage.getAvailable())
          .addGauge(FS_USED, fsUsage.getUsedSpace());
    }
  }
}
