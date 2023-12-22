/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class is used to track Volume Info stats for each HDDS Volume.
 */
@Metrics(about = "Ozone Volume Information Metrics",
    context = OzoneConsts.OZONE)
public class VolumeInfoMetrics {

  private String metricsSourceName = VolumeInfoMetrics.class.getSimpleName();
  private String volumeRootStr;
  private HddsVolume volume;

  /**
   * @param identifier Typically, path to volume root. e.g. /data/hdds
   */
  public VolumeInfoMetrics(String identifier, HddsVolume ref) {
    this.metricsSourceName += '-' + identifier;
    this.volumeRootStr = identifier;
    this.volume = ref;
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

  public String getMetricsSourceName() {
    return metricsSourceName;
  }

  /**
   * Test conservative avail space.
   * |----used----|   (avail)   |++++++++reserved++++++++|
   * |<------- capacity ------->|
   * |<------------------- Total capacity -------------->|
   * A) avail = capacity - used
   * B) capacity = used + avail
   * C) Total capacity = used + avail + reserved
   */

  /**
   * Return the Storage type for the Volume.
   */
  @Metric("Returns the Used space")
  public long getUsed() {
    return volume.getVolumeInfo().map(VolumeInfo::getScmUsed)
            .orElse(0L);
  }

  /**
   * Return the Total Available capacity of the Volume.
   */
  @Metric("Returns the Available space")
  public long getAvailable() {
    return volume.getVolumeInfo().map(VolumeInfo::getAvailable)
            .orElse(0L);
  }

  /**
   * Return the Total Reserved of the Volume.
   */
  @Metric("Fetches the Reserved Space")
  public long getReserved() {
    return volume.getVolumeInfo().map(VolumeInfo::getReservedInBytes)
            .orElse(0L);
  }

  /**
   * Return the Total capacity of the Volume.
   */
  @Metric("Returns the Capacity of the Volume")
  public long getCapacity() {
    return getUsed() + getAvailable();
  }

  /**
   * Return the Total capacity of the Volume.
   */
  @Metric("Returns the Total Capacity of the Volume")
  public long getTotalCapacity() {
    return (getUsed() + getAvailable() + getReserved());
  }

  @Metric("Returns the Committed bytes of the Volume")
  public long getCommitted() {
    return volume.getCommittedBytes();
  }

}
