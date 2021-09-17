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

package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.DUFactory;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class contains configuration values for the ContainerBalancer.
 */
@ConfigGroup(prefix = "hdds.container.balancer.")
public final class ContainerBalancerConfiguration {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancerConfiguration.class);
  private OzoneConfiguration ozoneConfiguration;

  @Config(key = "utilization.threshold", type = ConfigType.AUTO, defaultValue =
      "0.1", tags = {ConfigTag.BALANCER},
      description = "Threshold is a fraction in the range of 0 to 1. A " +
          "cluster is considered balanced if for each datanode, the " +
          "utilization of the datanode (used space to capacity ratio) differs" +
          " from the utilization of the cluster (used space to capacity ratio" +
          " of the entire cluster) no more than the threshold value.")
  private String threshold = "0.1";

  @Config(key = "datanodes.involved.max.ratio.per.iteration", type =
      ConfigType.AUTO,
      defaultValue = "0.2", tags = {ConfigTag.BALANCER}, description = "The " +
      "ratio of maximum number of datanodes that should be involved in " +
      "balancing in one iteration to the total number of healthy, in service " +
      "nodes known to container balancer.")
  private String maxDatanodesRatioToInvolvePerIteration = "0.2";

  @Config(key = "size.moved.max.per.iteration", type = ConfigType.SIZE,
      defaultValue = "30GB", tags = {ConfigTag.BALANCER},
      description = "The maximum size of data in bytes that will be moved " +
          "by Container Balancer in one iteration.")
  private long maxSizeToMovePerIteration = 30 * OzoneConsts.GB;

  @Config(key = "size.entering.target.max", type = ConfigType.SIZE,
      defaultValue = "", tags = {ConfigTag.BALANCER}, description = "The " +
      "maximum size in Gigabytes that can enter a target datanode in each " +
      "iteration while balancing. This is the sum of data from multiple " +
      "sources. The default value is greater than the configured" +
      " (or default) ozone.scm.container.size by 1GB.")
  private long maxSizeEnteringTarget;

  @Config(key = "size.leaving.source.max", type = ConfigType.SIZE,
      defaultValue = "", tags = {ConfigTag.BALANCER}, description = "The " +
      "maximum size in Gigabytes that can leave a source datanode in each " +
      "iteration while balancing. This is the sum of data moving to multiple " +
      "targets. The default value is greater than the configured" +
      " (or default) ozone.scm.container.size by 1GB.")
  private long maxSizeLeavingSource;

  @Config(key = "idle.iterations", type = ConfigType.INT,
      defaultValue = "10", tags = {ConfigTag.BALANCER},
      description = "The idle iteration count of Container Balancer.")
  private int idleIterations = 10;

  @Config(key = "exclude.containers", type = ConfigType.STRING, defaultValue =
      "", tags = {ConfigTag.BALANCER}, description = "List of container IDs " +
      "to exclude from balancing. For example \"1, 4, 5\" or \"1,4,5\".")
  private String excludeContainers = "";

  @Config(key = "move.timeout", type = ConfigType.TIME, defaultValue = "30m",
      timeUnit = TimeUnit.MINUTES, tags = {ConfigTag.BALANCER}, description =
      "The amount of time in minutes to allow a single container to move " +
          "from source to target.")
  private long moveTimeout = Duration.ofMinutes(30).toMillis();

  @Config(key = "balancing.iteration.interval", type = ConfigType.TIME,
      defaultValue = "1h", timeUnit = TimeUnit.MINUTES, tags = {
      ConfigTag.BALANCER}, description = "The interval period between each " +
      "iteration of Container Balancer.")
  private long balancingInterval;

  private DUFactory.Conf duConf;

  /**
   * Create configuration with default values.
   *
   * @param config Ozone configuration
   */
  public ContainerBalancerConfiguration(OzoneConfiguration config) {
    Preconditions.checkNotNull(config,
        "OzoneConfiguration should not be null.");
    this.ozoneConfiguration = config;

    // sizeEnteringTargetMax and sizeLeavingSourceMax should by default be
    // greater than container size
    long size = (long) ozoneConfiguration.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.GB) +
        OzoneConsts.GB;
    maxSizeEnteringTarget = size;
    maxSizeLeavingSource = size;

    // balancing interval should be greater than DUFactory refresh period
    duConf = ozoneConfiguration.getObject(DUFactory.Conf.class);
    balancingInterval = duConf.getRefreshPeriod().toMillis() +
        Duration.ofMinutes(10).toMillis();
  }

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
   * Gets the idle iteration value for Container Balancer.
   *
   * @return a idle iteration count larger than 0
   */
  public int getIdleIteration() {
    return idleIterations;
  }

  /**
   * Sets the idle iteration value for Container Balancer.
   *
   * @param count a idle iteration count larger than 0
   */
  public void setIdleIteration(int count) {
    if (count < -1 || 0 == count) {
      throw new IllegalArgumentException(
          "Idle iteration count must be larger than 0 or " +
              "-1(for infinitely running).");
    }
    this.idleIterations = count;
  }

  /**
   * Gets the ratio of maximum number of datanodes that will be involved in
   * balancing by Container Balancer in one iteration to the total number of
   * healthy, in-service nodes known to balancer.
   *
   * @return maximum datanodes to involve divided by total healthy,
   * in-service nodes
   */
  public double getMaxDatanodesRatioToInvolvePerIteration() {
    return Double.parseDouble(maxDatanodesRatioToInvolvePerIteration);
  }

  /**
   * Sets the ratio of maximum number of datanodes that will be involved in
   * balancing by Container Balancer in one iteration to the total number of
   * healthy, in-service nodes known to balancer.
   *
   * @param maxDatanodesRatioToInvolvePerIteration number of datanodes to
   *                                               involve divided by total
   *                                               number of healthy, in
   *                                               service nodes
   */
  public void setMaxDatanodesRatioToInvolvePerIteration(
      double maxDatanodesRatioToInvolvePerIteration) {
    if (maxDatanodesRatioToInvolvePerIteration < 0 ||
        maxDatanodesRatioToInvolvePerIteration > 1) {
      throw new IllegalArgumentException("Max datanodes to involve ratio must" +
          " be a double greater than equal to zero and lesser than equal to " +
          "one.");
    }
    this.maxDatanodesRatioToInvolvePerIteration =
        String.valueOf(maxDatanodesRatioToInvolvePerIteration);
  }

  /**
   * Gets the maximum size that will be moved by Container Balancer in one
   * iteration.
   *
   * @return maximum size in Bytes
   */
  public long getMaxSizeToMovePerIteration() {
    return maxSizeToMovePerIteration;
  }

  /**
   * Sets the value of maximum size that will be moved by Container Balancer
   * in one iteration.
   *
   * @param maxSizeToMovePerIteration maximum number of Bytes
   */
  public void setMaxSizeToMovePerIteration(long maxSizeToMovePerIteration) {
    this.maxSizeToMovePerIteration = maxSizeToMovePerIteration;
  }

  public long getMaxSizeEnteringTarget() {
    return maxSizeEnteringTarget;
  }

  public void setMaxSizeEnteringTarget(long maxSizeEnteringTarget) {
    this.maxSizeEnteringTarget = maxSizeEnteringTarget;
  }

  public long getMaxSizeLeavingSource() {
    return maxSizeLeavingSource;
  }

  public void setMaxSizeLeavingSource(long maxSizeLeavingSource) {
    this.maxSizeLeavingSource = maxSizeLeavingSource;
  }

  public Set<ContainerID> getExcludeContainers() {
    if (excludeContainers.isEmpty()) {
      return new HashSet<>();
    }
    return Arrays.stream(excludeContainers.split(","))
        .map(s -> {
          s = s.trim();
          return ContainerID.valueOf(Long.parseLong(s));
        }).collect(Collectors.toSet());
  }

  /**
   * Sets containers to exclude from balancing.
   * @param excludeContainers String of {@link ContainerID} to exclude. For
   *                          example, "1, 4, 5" or "1,4,5".
   */
  public void setExcludeContainers(String excludeContainers) {
    this.excludeContainers = excludeContainers;
  }

  public Duration getMoveTimeout() {
    return Duration.ofMillis(moveTimeout);
  }

  public void setMoveTimeout(Duration duration) {
    this.moveTimeout = duration.toMillis();
  }

  public Duration getBalancingInterval() {
    return Duration.ofMillis(balancingInterval);
  }

  public void setBalancingInterval(Duration balancingInterval) {
    if (balancingInterval.toMillis() > duConf.getRefreshPeriod().toMillis()) {
      this.balancingInterval = balancingInterval.toMillis();
    } else {
      LOG.warn("Balancing interval duration must be greater than du refresh " +
          "period, {} milliseconds", duConf.getRefreshPeriod().toMillis());
    }
  }

  /**
   * Gets the {@link OzoneConfiguration} using which this configuration was
   * constructed.
   * @return the {@link OzoneConfiguration} being used by this configuration
   */
  public OzoneConfiguration getOzoneConfiguration() {
    return this.ozoneConfiguration;
  }

  @Override
  public String toString() {
    return String.format("Container Balancer Configuration values:%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %dB%n", "Key", "Value", "Threshold",
        threshold, "Max Datanodes to Involve per Iteration(ratio)",
        maxDatanodesRatioToInvolvePerIteration,
        "Max Size to Move per Iteration", maxSizeToMovePerIteration);
  }
}
