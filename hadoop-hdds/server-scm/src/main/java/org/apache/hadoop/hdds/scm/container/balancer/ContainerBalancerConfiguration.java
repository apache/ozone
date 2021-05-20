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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains configuration values for the ContainerBalancer.
 */
@ConfigGroup(prefix = "hdds.container.balancer.")
public final class ContainerBalancerConfiguration {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancerConfiguration.class);

  @Config(key = "utilization.threshold", type = ConfigType.AUTO, defaultValue =
      "0.1", tags = {ConfigTag.BALANCER},
      description = "Threshold is a fraction in the range of 0 to 1. A " +
          "cluster is considered balanced if for each datanode, the " +
          "utilization of the datanode (used space to capacity ratio) differs" +
          " from the utilization of the cluster (used space to capacity ratio" +
          " of the entire cluster) no more than the threshold value.")
  private String threshold = "0.1";

  @Config(key = "datanodes.balanced.max", type = ConfigType.INT,
      defaultValue = "5", tags = {ConfigTag.BALANCER}, description = "The " +
      "maximum number of datanodes that should be balanced. Container " +
      "Balancer will not balance more number of datanodes than this limit.")
  private int maxDatanodesToBalance = 5;

  @Config(key = "size.moved.max", type = ConfigType.SIZE,
      defaultValue = "10GB", tags = {ConfigTag.BALANCER},
      description = "The maximum size of data in bytes that will be moved " +
          "by Container Balancer.")
  private long maxSizeToMove = 10 * OzoneConsts.GB;

  /**
   * Gets the threshold value for Container Balancer.
   *
   * @return a fraction in the range 0 to 1
   */
  public double getThreshold() {
    return Double.parseDouble(threshold);
  }

  /**
   * Sets the threshold value for Container Balancer.
   *
   * @param threshold a fraction in the range 0 to 1
   */
  public void setThreshold(double threshold) {
    if (threshold < 0 || threshold > 1) {
      throw new IllegalArgumentException(
          "Threshold must be a fraction in the range 0 to 1.");
    }
    this.threshold = String.valueOf(threshold);
  }

  /**
   * Gets the value of maximum number of datanodes that will be balanced by
   * Container Balancer.
   *
   * @return maximum number of datanodes
   */
  public int getMaxDatanodesToBalance() {
    return maxDatanodesToBalance;
  }

  /**
   * Sets the value of maximum number of datanodes that will be balanced by
   * Container Balancer.
   *
   * @param maxDatanodesToBalance maximum number of datanodes
   */
  public void setMaxDatanodesToBalance(int maxDatanodesToBalance) {
    this.maxDatanodesToBalance = maxDatanodesToBalance;
  }

  /**
   * Gets the maximum size that will be moved by Container Balancer.
   *
   * @return maximum size in Bytes
   */
  public long getMaxSizeToMove() {
    return maxSizeToMove;
  }

  /**
   * Sets the value of maximum size that will be moved by Container Balancer.
   *
   * @param maxSizeToMove maximum number of Bytes
   */
  public void setMaxSizeToMove(long maxSizeToMove) {
    this.maxSizeToMove = maxSizeToMove;
  }

  @Override
  public String toString() {
    return String.format("Container Balancer Configuration values:%n" +
            "%-30s %s%n" +
            "%-30s %s%n" +
            "%-30s %d%n" +
            "%-30s %dB%n", "Key", "Value", "Threshold",
        threshold, "Max Datanodes to Balance", maxDatanodesToBalance,
        "Max Size to Move", maxSizeToMove);
  }
}
