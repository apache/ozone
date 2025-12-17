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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class defines configuration parameters for the container scanners.
 **/
@ConfigGroup(prefix = "hdds.container.scrub")
public class ContainerScannerConfiguration {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerScannerConfiguration.class);

  // only for log
  public static final String HDDS_CONTAINER_SCRUB_ENABLED =
      "hdds.container.scrub.enabled";
  public static final String HDDS_CONTAINER_SCRUB_DEV_DATA_ENABLED =
      "hdds.container.scrub.dev.data.enabled";
  public static final String HDDS_CONTAINER_SCRUB_DEV_METADATA_ENABLED =
      "hdds.container.scrub.dev.metadata.enabled";
  public static final String METADATA_SCAN_INTERVAL_KEY =
      "hdds.container.scrub.metadata.scan.interval";
  public static final String DATA_SCAN_INTERVAL_KEY =
      "hdds.container.scrub.data.scan.interval";
  public static final String VOLUME_BYTES_PER_SECOND_KEY =
      "hdds.container.scrub.volume.bytes.per.second";
  public static final String ON_DEMAND_VOLUME_BYTES_PER_SECOND_KEY =
      "hdds.container.scrub.on.demand.volume.bytes.per.second";
  public static final String CONTAINER_SCAN_MIN_GAP =
      "hdds.container.scrub.min.gap";

  static final long CONTAINER_SCAN_MIN_GAP_DEFAULT =
      Duration.ofMinutes(15).toMillis();

  public static final long METADATA_SCAN_INTERVAL_DEFAULT =
      Duration.ofHours(3).toMillis();
  public static final long DATA_SCAN_INTERVAL_DEFAULT =
      Duration.ofDays(7).toMillis();

  public static final long BANDWIDTH_PER_VOLUME_DEFAULT = OzoneConsts.MB * 5L;
  public static final long ON_DEMAND_BANDWIDTH_PER_VOLUME_DEFAULT =
      OzoneConsts.MB * 5L;

  @Config(key = "hdds.container.scrub.enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "true",
      tags = {ConfigTag.STORAGE},
      description = "Config parameter to enable all container scanners.")
  private boolean enabled = true;

  @Config(key = "hdds.container.scrub.dev.data.scan.enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "true",
      tags = {ConfigTag.STORAGE},
      description = "Can be used to disable the background container data " +
          "scanner for developer testing purposes.")
  private boolean dataScanEnabled = true;

  @Config(key = "hdds.container.scrub.dev.metadata.scan.enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "true",
      tags = {ConfigTag.STORAGE},
      description = "Can be used to disable the background container metadata" +
          " scanner for developer testing purposes.")
  private boolean metadataScanEnabled = true;

  @Config(key = "hdds.container.scrub.metadata.scan.interval",
      type = ConfigType.TIME,
      defaultValue = "3h",
      tags = {ConfigTag.STORAGE},
      description = "Config parameter define time interval" +
          " between two metadata scans by container scanner." +
          " Unit could be defined with postfix (ns,ms,s,m,h,d).")
  private long metadataScanInterval = METADATA_SCAN_INTERVAL_DEFAULT;

  @Config(key = "hdds.container.scrub.data.scan.interval",
      type = ConfigType.TIME,
      defaultValue = "7d",
      tags = {ConfigTag.STORAGE},
      description = "Minimum time interval between two iterations of container"
          + " data scanning.  If an iteration takes less time than this, the"
          + " scanner will wait before starting the next iteration." +
          " Unit could be defined with postfix (ns,ms,s,m,h,d).")
  private long dataScanInterval = DATA_SCAN_INTERVAL_DEFAULT;

  @Config(key = "hdds.container.scrub.volume.bytes.per.second",
      type = ConfigType.LONG,
      defaultValue = "5242880",
      tags = {ConfigTag.STORAGE},
      description = "Config parameter to throttle I/O bandwidth used"
          + " by scanner per volume.")
  private long bandwidthPerVolume = BANDWIDTH_PER_VOLUME_DEFAULT;

  @Config(key = "hdds.container.scrub.on.demand.volume.bytes.per.second",
      type = ConfigType.LONG,
      defaultValue = "5242880",
      tags = {ConfigTag.STORAGE},
      description = "Config parameter to throttle I/O bandwidth used"
          + " by the demand container scanner per volume.")
  private long onDemandBandwidthPerVolume
      = ON_DEMAND_BANDWIDTH_PER_VOLUME_DEFAULT;

  @Config(key = "hdds.container.scrub.min.gap",
      defaultValue = "15m",
      type = ConfigType.TIME,
      tags = { DATANODE },
      description = "The minimum gap between two successive scans of the same"
          + " container. Unit could be defined with"
          + " postfix (ns,ms,s,m,h,d)."
  )
  private long containerScanMinGap = CONTAINER_SCAN_MIN_GAP_DEFAULT;

  @PostConstruct
  public void validate() {
    if (metadataScanInterval < 0) {
      LOG.warn(METADATA_SCAN_INTERVAL_KEY +
              " must be >= 0 and was set to {}. Defaulting to {}",
          metadataScanInterval, METADATA_SCAN_INTERVAL_DEFAULT);
      metadataScanInterval = METADATA_SCAN_INTERVAL_DEFAULT;
    }

    if (dataScanInterval < 0) {
      LOG.warn(DATA_SCAN_INTERVAL_KEY +
              " must be >= 0 and was set to {}. Defaulting to {}",
          dataScanInterval, DATA_SCAN_INTERVAL_DEFAULT);
      dataScanInterval = DATA_SCAN_INTERVAL_DEFAULT;
    }

    if (containerScanMinGap < 0) {
      LOG.warn(CONTAINER_SCAN_MIN_GAP +
              " must be >= 0 and was set to {}. Defaulting to {}",
          containerScanMinGap, CONTAINER_SCAN_MIN_GAP);
      containerScanMinGap = CONTAINER_SCAN_MIN_GAP_DEFAULT;
    }

    if (bandwidthPerVolume < 0) {
      LOG.warn(VOLUME_BYTES_PER_SECOND_KEY +
              " must be >= 0 and was set to {}. Defaulting to {}",
          bandwidthPerVolume, BANDWIDTH_PER_VOLUME_DEFAULT);
      bandwidthPerVolume = BANDWIDTH_PER_VOLUME_DEFAULT;
    }
    if (onDemandBandwidthPerVolume < 0) {
      LOG.warn(ON_DEMAND_VOLUME_BYTES_PER_SECOND_KEY +
              " must be >= 0 and was set to {}. Defaulting to {}",
          onDemandBandwidthPerVolume, ON_DEMAND_BANDWIDTH_PER_VOLUME_DEFAULT);
      onDemandBandwidthPerVolume = ON_DEMAND_BANDWIDTH_PER_VOLUME_DEFAULT;
    }
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isDataScanEnabled() {
    return dataScanEnabled;
  }

  public boolean isMetadataScanEnabled() {
    return metadataScanEnabled;
  }

  public void setMetadataScanInterval(long metadataScanInterval) {
    this.metadataScanInterval = metadataScanInterval;
  }

  public long getMetadataScanInterval() {
    return metadataScanInterval;
  }

  public void setDataScanInterval(long dataScanInterval) {
    this.dataScanInterval = dataScanInterval;
  }

  public long getDataScanInterval() {
    return dataScanInterval;
  }

  public long getBandwidthPerVolume() {
    return bandwidthPerVolume;
  }

  public long getOnDemandBandwidthPerVolume() {
    return onDemandBandwidthPerVolume;
  }

  public long getContainerScanMinGap() {
    return containerScanMinGap;
  }

  public void setContainerScanMinGap(long scanGap) {
    containerScanMinGap = scanGap;
  }
}
