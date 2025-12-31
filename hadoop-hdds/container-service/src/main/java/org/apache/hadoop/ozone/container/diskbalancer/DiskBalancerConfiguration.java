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

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;

import jakarta.annotation.Nonnull;
import java.time.Duration;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains configuration values for the DiskBalancer.
 */
@ConfigGroup(prefix = "hdds.datanode.disk.balancer")
public final class DiskBalancerConfiguration {
  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerConfiguration.class);

  @Config(key = "info.dir", type = ConfigType.STRING,
      defaultValue = "", tags = {ConfigTag.DISKBALANCER},
      description = "The path where datanode diskBalancer's conf is to be " +
          "written to. if this property is not defined, ozone will fall " +
          "back to use metadata directory instead.")
  private String infoDir;

  @Config(key = "volume.density.threshold.percent", type = ConfigType.DOUBLE,
      defaultValue = "10", tags = {ConfigTag.DISKBALANCER},
      description = "Threshold is a percentage in the range of 0 to 100. A " +
          "datanode is considered balanced if for each volume, the " +
          "utilization of the volume(used space to capacity ratio) differs" +
          " from the utilization of the datanode(used space to capacity ratio" +
          " of the entire datanode) no more than the threshold.")
  private double threshold = 10d;

  @Config(key = "max.disk.throughputInMBPerSec", type = ConfigType.LONG,
      defaultValue = "10", tags = {ConfigTag.DISKBALANCER},
      description = "The max balance speed.")
  private long diskBandwidthInMB = 10;

  @Config(key = "parallel.thread", type = ConfigType.INT,
      defaultValue = "5", tags = {ConfigTag.DISKBALANCER},
      description = "The max parallel balance thread count.")
  private int parallelThread = 5;

  @Config(key = "should.run.default",
      defaultValue = "false",
      type = ConfigType.BOOLEAN,
      tags = { DATANODE, ConfigTag.DISKBALANCER},
      description =
          "If DiskBalancer fails to get information from diskbalancer.info, " +
              "it will choose this value to decide if this service should be " +
              "running."
  )
  private boolean diskBalancerShouldRun = false;

  @Config(key = "service.interval",
      defaultValue = "60s",
      type = ConfigType.TIME,
      tags = { DATANODE, ConfigTag.DISKBALANCER},
      description = "Time interval of the Datanode DiskBalancer service. " +
          "The Datanode will check the service periodically and update " +
          "the config and running status for DiskBalancer service. " +
          "Unit could be defined with postfix (ns,ms,s,m,h,d). "
  )
  private long diskBalancerInterval = Duration.ofSeconds(60).toMillis();

  @Config(key = "service.timeout",
      defaultValue = "300s",
      type = ConfigType.TIME,
      tags = { DATANODE, ConfigTag.DISKBALANCER},
      description = "Timeout for the Datanode DiskBalancer service. "
          + "Unit could be defined with postfix (ns,ms,s,m,h,d). "
  )
  private long diskBalancerTimeout = Duration.ofSeconds(300).toMillis();

  @Config(key = "volume.choosing.policy", type = ConfigType.CLASS,
      defaultValue = "org.apache.hadoop.ozone.container.diskbalancer.policy" +
          ".DefaultVolumeChoosingPolicy",
      tags = {ConfigTag.DISKBALANCER},
      description = "The volume choosing policy of the disk balancer service.")
  private Class<?> volumeChoosingPolicyClass;

  @Config(key = "container.choosing.policy", type = ConfigType.CLASS,
      defaultValue = "org.apache.hadoop.ozone.container.diskbalancer.policy" +
          ".DefaultContainerChoosingPolicy",
      tags = {ConfigTag.DISKBALANCER},
      description = "The container choosing policy of the disk balancer " +
          "service.")
  private Class<?> containerChoosingPolicyClass;

  @Config(key = "stop.after.disk.even",
      type = ConfigType.BOOLEAN,
      defaultValue = "true",
      tags = {ConfigTag.DISKBALANCER},
      description = "If true, the DiskBalancer will automatically stop once disks are balanced.")
  private boolean stopAfterDiskEven = true;

  public DiskBalancerConfiguration(Double threshold,
      Long bandwidthInMB,
      Integer parallelThread,
      Boolean stopAfterDiskEven) {
    if (threshold != null) {
      this.threshold = threshold;
    }
    if (bandwidthInMB != null) {
      this.diskBandwidthInMB = bandwidthInMB;
    }
    if (parallelThread != null) {
      this.parallelThread = parallelThread;
    }
    if (stopAfterDiskEven != null) {
      this.stopAfterDiskEven = stopAfterDiskEven;
    }
  }

  public DiskBalancerConfiguration() {
  }

  public String getDiskBalancerInfoDir() {
    return infoDir;
  }

  public boolean getDiskBalancerShouldRun() {
    return diskBalancerShouldRun;
  }

  public void setDiskBalancerShouldRun(boolean shouldRun) {
    this.diskBalancerShouldRun = shouldRun;
  }

  public Duration getDiskBalancerInterval() {
    return Duration.ofMillis(diskBalancerInterval);
  }

  public void setDiskBalancerInterval(Duration duration) {
    this.diskBalancerInterval = duration.toMillis();
  }

  public Duration getDiskBalancerTimeout() {
    return Duration.ofMillis(diskBalancerTimeout);
  }

  public void setDiskBalancerTimeout(Duration duration) {
    this.diskBalancerTimeout = duration.toMillis();
  }

  public Class<?> getVolumeChoosingPolicyClass() {
    return volumeChoosingPolicyClass;
  }

  public Class<?> getContainerChoosingPolicyClass() {
    return containerChoosingPolicyClass;
  }

  public boolean isStopAfterDiskEven() {
    return stopAfterDiskEven;
  }
  
  public void setStopAfterDiskEven(boolean stopAfterDiskEven) {
    this.stopAfterDiskEven = stopAfterDiskEven;
  }

  /**
   * Gets the threshold value for DiskBalancer.
   *
   * @return percentage value in the range 0 to 100
   */
  public double getThreshold() {
    return threshold;
  }

  public double getThresholdAsRatio() {
    return threshold / 100;
  }

  /**
   * Sets the threshold value for Disk Balancer.
   *
   * @param threshold a percentage value in the range (0 to 100) both exclusive
   */
  public void setThreshold(double threshold) {
    if (threshold <= 0d || threshold >= 100d) {
      throw new IllegalArgumentException(
          "Threshold must be a percentage(double) in the range 0 to 100 both exclusive.");
    }
    this.threshold = threshold;
  }

  /**
   * Gets the disk bandwidth value for Disk Balancer.
   *
   * @return max disk bandwidth per second
   */

  public long getDiskBandwidthInMB() {
    return diskBandwidthInMB;
  }

  /**
   * Sets the disk bandwidth value for Disk Balancer.
   *
   * @param diskBandwidthInMB the bandwidth to control balance speed
   */
  public void setDiskBandwidthInMB(long diskBandwidthInMB) {
    if (diskBandwidthInMB <= 0L) {
      throw new IllegalArgumentException(
          "diskBandwidthInMB must be a value larger than 0.");
    }
    this.diskBandwidthInMB = diskBandwidthInMB;
  }

  /**
   * Gets the parallel thread for Disk Balancer.
   *
   * @return parallel thread
   */
  public int getParallelThread() {
    return parallelThread;
  }

  /**
   * Sets the parallel thread for Disk Balancer.
   *
   * @param parallelThread the parallel thread count
   */
  public void setParallelThread(int parallelThread) {
    if (parallelThread <= 0) {
      throw new IllegalArgumentException(
          "parallelThread must be a value larger than 0.");
    }
    this.parallelThread = parallelThread;
  }

  @Override
  public String toString() {
    return String.format("Disk Balancer Configuration values:%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n",
            "Key", "Value",
        "Threshold", threshold, "Max disk bandwidth", diskBandwidthInMB,
        "Parallel Thread", parallelThread, "Stop After Disk Even", stopAfterDiskEven);
  }

  public HddsProtos.DiskBalancerConfigurationProto.Builder toProtobufBuilder() {
    HddsProtos.DiskBalancerConfigurationProto.Builder builder =
        HddsProtos.DiskBalancerConfigurationProto.newBuilder();

    builder.setThreshold(threshold)
        .setDiskBandwidthInMB(diskBandwidthInMB)
        .setParallelThread(parallelThread)
        .setStopAfterDiskEven(stopAfterDiskEven);
    return builder;
  }

  /**
   * Merges proto configuration into an existing configuration object.
   * Only fields present in the proto will be updated.
   *
   * @param newConfigProto the proto containing fields to update
   * @param existingConfig the existing configuration to merge into
   * @return the updated configuration object
   */
  public static DiskBalancerConfiguration updateFromProtobuf(
      @Nonnull HddsProtos.DiskBalancerConfigurationProto newConfigProto,
      @Nonnull DiskBalancerConfiguration existingConfig) {
    if (newConfigProto.hasThreshold()) {
      existingConfig.setThreshold(newConfigProto.getThreshold());
    }
    if (newConfigProto.hasDiskBandwidthInMB()) {
      existingConfig.setDiskBandwidthInMB(newConfigProto.getDiskBandwidthInMB());
    }
    if (newConfigProto.hasParallelThread()) {
      existingConfig.setParallelThread(newConfigProto.getParallelThread());
    }
    if (newConfigProto.hasStopAfterDiskEven()) {
      existingConfig.setStopAfterDiskEven(newConfigProto.getStopAfterDiskEven());
    }
    return existingConfig;
  }
}

