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

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class contains configuration values for the ContainerBalancer.
 */
@ConfigGroup(prefix = "hdds.container.balancer")
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

  @Config(key = "datanodes.involved.max.percentage.per.iteration", type =
      ConfigType.INT, defaultValue = "20", tags = {ConfigTag.BALANCER},
      description = "Maximum percentage of healthy, in service datanodes " +
          "that can be involved in balancing in one iteration.")
  private int maxDatanodesPercentageToInvolvePerIteration = 20;

  @Config(key = "size.moved.max.per.iteration", type = ConfigType.SIZE,
      defaultValue = "500GB", tags = {ConfigTag.BALANCER},
      description = "The maximum size of data in bytes that will be moved " +
          "by Container Balancer in one iteration.")
  private long maxSizeToMovePerIteration = 500 * OzoneConsts.GB;

  @Config(key = "size.entering.target.max", type = ConfigType.SIZE,
      defaultValue = "26GB", tags = {ConfigTag.BALANCER}, description = "The " +
      "maximum size that can enter a target datanode in each " +
      "iteration while balancing. This is the sum of data from multiple " +
      "sources. The value must be greater than the configured" +
      " (or default) ozone.scm.container.size.")
  private long maxSizeEnteringTarget;

  @Config(key = "size.leaving.source.max", type = ConfigType.SIZE,
      defaultValue = "26GB", tags = {ConfigTag.BALANCER}, description = "The " +
      "maximum size that can leave a source datanode in each " +
      "iteration while balancing. This is the sum of data moving to multiple " +
      "targets. The value must be greater than the configured" +
      " (or default) ozone.scm.container.size.")
  private long maxSizeLeavingSource;

  @Config(key = "iterations", type = ConfigType.INT,
      defaultValue = "10", tags = {ConfigTag.BALANCER},
      description = "The number of iterations that Container Balancer will " +
          "run for.")
  private int iterations = 10;

  @Config(key = "exclude.containers", type = ConfigType.STRING, defaultValue =
      "", tags = {ConfigTag.BALANCER}, description = "List of container IDs " +
      "to exclude from balancing. For example \"1, 4, 5\" or \"1,4,5\".")
  private String excludeContainers = "";

  @Config(key = "move.timeout", type = ConfigType.TIME, defaultValue = "30m",
      tags = {ConfigTag.BALANCER}, description =
      "The amount of time in minutes to allow a single container to move " +
          "from source to target.")
  private long moveTimeout = Duration.ofMinutes(30).toMillis();

  @Config(key = "balancing.iteration.interval", type = ConfigType.TIME,
      defaultValue = "70m", tags = {
      ConfigTag.BALANCER}, description = "The interval period between each " +
      "iteration of Container Balancer.")
  private long balancingInterval = Duration.ofMinutes(70).toMillis();

  @Config(key = "include.datanodes", type = ConfigType.STRING, defaultValue =
      "", tags = {ConfigTag.BALANCER}, description = "A list of Datanode " +
      "hostnames or ip addresses separated by commas. Only the Datanodes " +
      "specified in this list are balanced. This configuration is empty by " +
      "default and is applicable only if it is non-empty.")
  private String includeNodes = "";

  @Config(key = "exclude.datanodes", type = ConfigType.STRING, defaultValue =
      "", tags = {ConfigTag.BALANCER}, description = "A list of Datanode " +
      "hostnames or ip addresses separated by commas. The Datanodes specified" +
      " in this list are excluded from balancing. This configuration is empty" +
      " by default.")
  private String excludeNodes = "";

  @Config(key = "move.networkTopology.enable", type = ConfigType.BOOLEAN,
      defaultValue = "false", tags = {ConfigTag.BALANCER},
      description = "whether to take network topology into account when " +
          "selecting a target for a source. " +
          "This configuration is false by default.")
  private boolean networkTopologyEnable = false;

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
   * Gets the iteration count for Container Balancer. A value of -1 means
   * infinite number of iterations.
   *
   * @return a value greater than 0, or -1
   */
  public int getIterations() {
    return iterations;
  }

  /**
   * Sets the number of iterations for Container Balancer.
   *
   * @param count a value greater than 0, or -1 for running balancer infinitely
   */
  public void setIterations(int count) {
    if (count < -1 || 0 == count) {
      throw new IllegalArgumentException(
          "Iteration count must be greater than 0, or " +
              "-1(for running balancer infinitely).");
    }
    this.iterations = count;
  }

  /**
   * Gets the maximum percentage of healthy, in-service datanodes that will be
   * involved in balancing in one iteration.
   *
   * @return percentage as an integer from 0 up to and including 100
   */
  public int getMaxDatanodesPercentageToInvolvePerIteration() {
    return maxDatanodesPercentageToInvolvePerIteration;
  }

  /**
   * Get the NetworkTopologyEnable value for Container Balancer.
   *
   * @return the boolean value of networkTopologyEnable
   */
  public Boolean getNetworkTopologyEnable() {
    return networkTopologyEnable;
  }

  /**
   * Set the NetworkTopologyEnable value for Container Balancer.
   *
   * @param enable the boolean value to be set to networkTopologyEnable
   */
  public void setNetworkTopologyEnable(Boolean enable) {
    networkTopologyEnable = enable;
  }

  /**
   * Gets the ratio of maximum datanodes involved in balancing to the total
   * number of healthy, in-service datanodes known to SCM.
   *
   * @return ratio as a double from 0 up to and including 1
   */
  public double getMaxDatanodesRatioToInvolvePerIteration() {
    return maxDatanodesPercentageToInvolvePerIteration / 100d;
  }

  /**
   * Sets the maximum percentage of healthy, in-service datanodes that will be
   * involved in balancing in one iteration.
   *
   * @param maxDatanodesPercentageToInvolvePerIteration number of datanodes
   *                                                    to involve divided by
   *                                                    total number of
   *                                                    healthy, in-service
   *                                                    datanodes multiplied
   *                                                    by 100
   */
  public void setMaxDatanodesPercentageToInvolvePerIteration(
      int maxDatanodesPercentageToInvolvePerIteration) {
    if (maxDatanodesPercentageToInvolvePerIteration < 0 ||
        maxDatanodesPercentageToInvolvePerIteration > 100) {
      throw new IllegalArgumentException(String.format("Argument %d is " +
              "illegal. Percentage must be from 0 up to and including 100.",
          maxDatanodesPercentageToInvolvePerIteration));
    }
    this.maxDatanodesPercentageToInvolvePerIteration =
        maxDatanodesPercentageToInvolvePerIteration;
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
    this.balancingInterval = balancingInterval.toMillis();
  }

  /**
   * Gets a set of datanode hostnames or ip addresses that will be the exclusive
   * participants in balancing.
   * @return Set of hostname or ip address strings, or an empty set if the
   * configuration is empty
   */
  public Set<String> getIncludeNodes() {
    if (includeNodes.isEmpty()) {
      return Collections.emptySet();
    }
    return Arrays.stream(includeNodes.split(","))
        .map(String::trim)
        .collect(Collectors.toSet());
  }

  /**
   * Sets the datanodes that will be the exclusive participants in balancing.
   * Applicable only if the specified string is non-empty.
   * @param includeNodes a String of datanode hostnames or ip addresses
   *                     separated by commas
   */
  public void setIncludeNodes(String includeNodes) {
    this.includeNodes = includeNodes;
  }

  /**
   * Gets a set of datanode hostnames or ip addresses that will be excluded
   * from balancing.
   * @return Set of hostname or ip address strings, or an empty set if the
   * configuration is empty
   */
  public Set<String> getExcludeNodes() {
    if (excludeNodes.isEmpty()) {
      return Collections.emptySet();
    }
    return Arrays.stream(excludeNodes.split(","))
        .map(String::trim)
        .collect(Collectors.toSet());
  }

  /**
   * Sets the datanodes that will be excluded from balancing.
   * @param excludeNodes a String of datanode hostnames or ip addresses
   *                     separated by commas
   */
  public void setExcludeNodes(String excludeNodes) {
    this.excludeNodes = excludeNodes;
  }

  @Override
  public String toString() {
    return String.format("Container Balancer Configuration values:%n" +
            "%-50s %s%n" +
            "%-50s %s%n" +
            "%-50s %d%n" +
            "%-50s %dGB%n"+
            "%-50s %dGB%n"+
            "%-50s %dGB%n", "Key", "Value", "Threshold",
        threshold, "Max Datanodes to Involve per Iteration(percent)",
        maxDatanodesPercentageToInvolvePerIteration,
        "Max Size to Move per Iteration",
        maxSizeToMovePerIteration / OzoneConsts.GB,
        "Max Size Entering Target per Iteration",
        maxSizeEnteringTarget / OzoneConsts.GB,
        "Max Size Leaving Source per Iteration",
        maxSizeLeavingSource / OzoneConsts.GB);
  }
}
