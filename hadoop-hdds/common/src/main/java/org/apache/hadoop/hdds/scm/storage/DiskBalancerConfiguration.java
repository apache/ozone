/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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

  @Config(key = "volume.density.threshold", type = ConfigType.AUTO,
      defaultValue = "10", tags = {ConfigTag.DISKBALANCER},
      description = "Threshold is a percentage in the range of 0 to 100. A " +
          "datanode is considered balanced if for each volume, the " +
          "utilization of the volume(used space to capacity ratio) differs" +
          " from the utilization of the datanode(used space to capacity ratio" +
          " of the entire datanode) no more than the threshold.")
  private double threshold = 10d;

  @Config(key = "max.disk.throughputInMBPerSec", type = ConfigType.AUTO,
      defaultValue = "10", tags = {ConfigTag.DISKBALANCER},
      description = "The max balance speed.")
  private double diskBandwidth = 10;

  @Config(key = "parallel.thread", type = ConfigType.AUTO,
      defaultValue = "5", tags = {ConfigTag.DISKBALANCER},
      description = "The max parallel balance thread count.")
  private int parallelThread = 5;

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
   * @param threshold a percentage value in the range 0 to 100
   */
  public void setThreshold(double threshold) {
    if (threshold < 0d || threshold >= 100d) {
      throw new IllegalArgumentException(
          "Threshold must be a percentage(double) in the range 0 to 100.");
    }
    this.threshold = threshold;
  }

  /**
   * Gets the disk bandwidth value for Disk Balancer.
   *
   * @return max disk bandwidth per second
   */
  public double getDiskBandwidth() {
    return diskBandwidth;
  }

  /**
   * Sets the disk bandwidth value for Disk Balancer.
   *
   * @param diskBandwidth the bandwidth to control balance speed
   */
  public void setDiskBandwidth(double diskBandwidth) {
    if (diskBandwidth <= 0d) {
      throw new IllegalArgumentException(
          "diskBandwidth must be a value larger than 0.");
    }
    this.diskBandwidth = diskBandwidth;
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
            "%-50s %s%n",
            "Key", "Value",
        "Threshold", threshold, "Max disk bandwidth", diskBandwidth,
        "Parallel Thread", parallelThread);
  }

  public HddsProtos.DiskBalancerConfigurationProto.Builder toProtobufBuilder() {
    HddsProtos.DiskBalancerConfigurationProto.Builder builder =
        HddsProtos.DiskBalancerConfigurationProto.newBuilder();

    builder.setThreshold(threshold)
        .setDiskBandwidth(diskBandwidth)
        .setParallelThread(parallelThread);
    return builder;
  }

  static DiskBalancerConfiguration fromProtobuf(
      @Nonnull HddsProtos.DiskBalancerConfigurationProto proto,
      @Nonnull OzoneConfiguration ozoneConfiguration) {
    DiskBalancerConfiguration config =
        ozoneConfiguration.getObject(DiskBalancerConfiguration.class);
    if (proto.hasThreshold()) {
      config.setThreshold(proto.getThreshold());
    }
    if (proto.hasDiskBandwidth()) {
      config.setDiskBandwidth(proto.getDiskBandwidth());
    }
    if (proto.hasParallelThread()) {
      config.setParallelThread(proto.getParallelThread());
    }
    return config;
  }
}
